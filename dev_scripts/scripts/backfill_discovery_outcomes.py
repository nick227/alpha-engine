"""
Retrospective discovery run + outcome computation.

Runs score_candidates() for every historical date that has full feature_snapshot coverage
AND has at least `min_forward_days` of subsequent data. Saves candidates to
discovery_candidates and outcomes to discovery_candidate_outcomes.

This lets the discovery stats pipeline aggregate performance without waiting for live
forward returns to accumulate.

Usage:
  python scripts/backfill_discovery_outcomes.py
  python scripts/backfill_discovery_outcomes.py --db data/alpha.db --lookback 90 --horizons 1,5,20

Only runs dates not already in discovery_candidates (idempotent).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path


def _asof(s: str | date) -> str:
    if isinstance(s, date):
        return s.isoformat()
    return date.fromisoformat(str(s).strip()).isoformat()


def _get_eligible_dates(
    conn: sqlite3.Connection,
    *,
    min_symbols: int,
    min_forward_days: int,
    lookback: int,
) -> list[str]:
    """Get dates with full snapshot coverage and enough forward data."""
    rows = conn.execute(
        """
        SELECT as_of_date, COUNT(DISTINCT symbol) as n
        FROM feature_snapshot
        GROUP BY as_of_date
        HAVING COUNT(DISTINCT symbol) >= ?
        ORDER BY as_of_date ASC
        """,
        (min_symbols,),
    ).fetchall()
    all_dates = [str(r[0]) for r in rows]
    if not all_dates:
        return []

    # Need min_forward_days bars after each date — require that many calendar days + some buffer
    max_entry_date = all_dates[-1]
    # Walk back min_forward_days trading days from end
    end_idx = len(all_dates) - 1
    if end_idx < min_forward_days:
        return []
    eligible_end_idx = end_idx - min_forward_days
    all_dates = all_dates[: eligible_end_idx + 1]

    # Apply lookback
    if lookback > 0 and len(all_dates) > lookback:
        all_dates = all_dates[-lookback:]

    return all_dates


def _already_run_dates(conn: sqlite3.Connection, *, tenant_id: str) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT as_of_date FROM discovery_candidates WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchall()
    return {str(r[0]) for r in rows}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Retrospective discovery + outcomes backfill")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--lookback", type=int, default=90, help="Trading days to backfill (default: 90)")
    p.add_argument("--horizons", default="1,5,20", help="Outcome horizons in days (default: 1,5,20)")
    p.add_argument("--min-symbols", type=int, default=2000, help="Min symbols per date (default: 2000)")
    p.add_argument("--top-n", type=int, default=50, help="Top N candidates per strategy per date (default: 50)")
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-run dates already in discovery_candidates",
    )
    args = p.parse_args(argv)

    horizons = [int(x) for x in str(args.horizons).split(",") if x.strip()]
    max_h = max(horizons)

    # Import discovery internals
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from app.discovery.strategies import STRATEGIES, score_candidates, to_repo_rows
    from app.discovery.types import FeatureRow
    from app.discovery.outcomes import compute_candidate_outcomes
    from app.db.repository import AlphaRepository

    db_path = str(args.db)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print(f"Finding eligible dates (min_symbols={args.min_symbols}, min_forward_days={max_h}, lookback={args.lookback})...")
    eligible = _get_eligible_dates(
        conn,
        min_symbols=int(args.min_symbols),
        min_forward_days=int(max_h),
        lookback=int(args.lookback),
    )
    print(f"  {len(eligible)} eligible dates.")

    if not args.force:
        already = _already_run_dates(conn, tenant_id=str(args.tenant_id))
        eligible = [d for d in eligible if d not in already]
        print(f"  {len(eligible)} dates not yet processed (use --force to rerun all).")

    conn.close()

    if not eligible:
        print("Nothing to do.")
        return 0

    # Clean up stale candidates/outcomes for all eligible dates before re-inserting.
    # upsert_discovery_candidates only does INSERT OR REPLACE on PK conflict — it does NOT
    # delete old candidates that fail the new filter. We must delete-then-insert.
    cleanup_conn = sqlite3.connect(db_path)
    for d in eligible:
        cleanup_conn.execute(
            "DELETE FROM discovery_candidates WHERE tenant_id = ? AND as_of_date = ?",
            (str(args.tenant_id), d),
        )
        cleanup_conn.execute(
            "DELETE FROM discovery_candidate_outcomes WHERE tenant_id = ? AND as_of_date = ?",
            (str(args.tenant_id), d),
        )
    cleanup_conn.commit()
    cleanup_conn.close()

    repo = AlphaRepository(db_path=db_path)
    total_candidates = 0
    total_outcomes = 0

    for i, d in enumerate(eligible):
        # Load feature_snapshot for this date
        snap_conn = sqlite3.connect(db_path)
        snap_conn.row_factory = sqlite3.Row
        rows = snap_conn.execute(
            """
            SELECT symbol, as_of_date, close,
                   return_63d, volatility_20d, price_percentile_252d,
                   dollar_volume, volume_zscore_20d
            FROM feature_snapshot
            WHERE as_of_date = ?
            """,
            (d,),
        ).fetchall()
        snap_conn.close()

        if not rows:
            continue

        features: dict[str, FeatureRow] = {}
        for r in rows:
            sym = str(r["symbol"])
            features[sym] = FeatureRow(
                symbol=sym,
                as_of_date=str(r["as_of_date"]),
                close=float(r["close"]) if r["close"] is not None else None,
                volume=None,
                dollar_volume=float(r["dollar_volume"]) if r["dollar_volume"] is not None else None,
                avg_dollar_volume_20d=float(r["dollar_volume"]) if r["dollar_volume"] is not None else None,
                return_1d=None,
                return_5d=None,
                peer_relative_return_63d=None,
                price_bucket=None,
                return_20d=None,
                return_63d=float(r["return_63d"]) if r["return_63d"] is not None else None,
                return_252d=None,
                volatility_20d=float(r["volatility_20d"]) if r["volatility_20d"] is not None else None,
                max_drawdown_252d=None,
                price_percentile_252d=float(r["price_percentile_252d"]) if r["price_percentile_252d"] is not None else None,
                volume_zscore_20d=float(r["volume_zscore_20d"]) if r["volume_zscore_20d"] is not None else None,
                dollar_volume_zscore_20d=None,
                revenue_ttm=None,
                revenue_growth=None,
                shares_outstanding=None,
                shares_growth=None,
                sector=None,
                industry=None,
                sector_return_63d=None,
            )

        date_candidates = 0
        for strat in STRATEGIES.keys():
            cands = score_candidates(features, strategy_type=strat)
            top = cands[: int(args.top_n)]
            if top:
                repo_rows = to_repo_rows(top)
                repo.upsert_discovery_candidates(as_of_date=d, candidates=repo_rows, tenant_id=str(args.tenant_id))
                date_candidates += len(top)

        # Compute outcomes using feature_snapshot (via updated outcomes.compute_candidate_outcomes)
        outcome_rows = compute_candidate_outcomes(
            db_path=db_path,
            tenant_id=str(args.tenant_id),
            as_of_date=d,
            horizons=horizons,
        )
        if outcome_rows:
            repo.upsert_discovery_candidate_outcomes(
                as_of_date=d,
                rows_in=outcome_rows,
                tenant_id=str(args.tenant_id),
            )
            total_outcomes += len(outcome_rows)

        total_candidates += date_candidates
        if (i + 1) % 10 == 0 or (i + 1) == len(eligible):
            print(f"  [{i+1}/{len(eligible)}] {d}: {date_candidates} candidates, {len(outcome_rows)} outcomes")

    repo.close()

    print(f"\nDone. {total_candidates:,} candidate rows, {total_outcomes:,} outcome rows written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
