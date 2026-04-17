from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from app.db.repository import AlphaRepository
from app.discovery.fundamentals_fmp import fetch_fmp_fundamentals_batch
from app.discovery.outcomes import compute_candidate_outcomes, compute_watchlist_outcomes, outcomes_to_repo_rows
from app.discovery.promotion import select_high_conviction, watchlist_to_queue_rows, watchlist_to_repo_rows
from app.discovery.runner import format_summary_json, run_discovery
from app.discovery.stats import compute_discovery_stats, stats_to_repo_rows


def _parse_date(s: str) -> date:
    return date.fromisoformat(str(s).strip())

def _latest_price_bar_date(*, db_path: str, tenant_id: str, timeframe: str) -> str | None:
    """
    Return MAX(DATE(timestamp)) available in price_bars for the tenant/timeframe.

    Note: MAX(date) can be misleading if the most recent day is only partially loaded
    (e.g., one ticker updated but the rest are stale). Use _latest_coherent_trading_day()
    for "best-effort latest full day" semantics.
    """
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            vix = conn.execute(
                "SELECT MAX(DATE(timestamp)) FROM price_bars WHERE tenant_id=? AND ticker='^VIX' AND timeframe=?",
                (str(tenant_id), str(timeframe)),
            ).fetchone()
            vix_max = str(vix[0]) if vix and vix[0] else None

            all_ = conn.execute(
                "SELECT MAX(DATE(timestamp)) FROM price_bars WHERE tenant_id=? AND timeframe=?",
                (str(tenant_id), str(timeframe)),
            ).fetchone()
            all_max = str(all_[0]) if all_ and all_[0] else None

            if vix_max and all_max:
                return max(vix_max, all_max)
            return vix_max or all_max
        finally:
            conn.close()
    except Exception:
        return None
    return None


def _latest_coherent_trading_day(
    *,
    db_path: str,
    tenant_id: str,
    timeframe: str,
    lookback_days: int = 21,
    min_coverage_ratio: float = 0.80,
) -> str | None:
    """
    Return the most recent DATE(timestamp) with "high enough" ticker coverage.

    This avoids picking a partially loaded max(date) where only a tiny subset of tickers updated.
    Coverage is measured as distinct tickers on that day relative to the maximum distinct-ticker
    count observed in the lookback window.
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT DATE(timestamp) as d, COUNT(DISTINCT ticker) as n
                FROM price_bars
                WHERE tenant_id = ?
                  AND timeframe = ?
                  AND DATE(timestamp) >= date('now', '-' || ? || ' day')
                GROUP BY DATE(timestamp)
                ORDER BY d DESC
                """,
                (str(tenant_id), str(timeframe), int(lookback_days)),
            ).fetchall()
            if not rows:
                return None
            counts = [(str(r["d"]), int(r["n"] or 0)) for r in rows if r and r["d"]]
            if not counts:
                return None
            max_n = max(n for _d, n in counts)
            if max_n <= 0:
                return None
            threshold = int(max_n * float(min_coverage_ratio))
            for d, n in counts:
                if n >= threshold:
                    return d
            return counts[0][0]
        finally:
            conn.close()
    except Exception:
        return None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Discovery layer CLI (symbol selection)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.discovery.discovery_cli sync-fundamentals --symbols AAPL,MSFT\n"
            "  python -m app.discovery.discovery_cli run --date 2026-04-12 --top 50 --min-adv 2000000\n"
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    sf = sub.add_parser("sync-fundamentals", help="Fetch minimal fundamentals from FMP and upsert fundamentals_snapshot")
    sf.add_argument("--symbols", required=True, help="Comma-separated tickers")
    sf.add_argument("--db", default="data/alpha.db")
    sf.add_argument("--tenant-id", default="default")
    sf.add_argument("--api-key", default=None, help="FMP API key (default: env FMP_API_KEY)")

    r = sub.add_parser("run", help="Run discovery strategies and store top candidates per strategy")
    r.add_argument("--date", required=True, help="As-of date (YYYY-MM-DD)")
    r.add_argument("--db", default="data/alpha.db")
    r.add_argument("--tenant-id", default="default")
    r.add_argument("--timeframe", default="1d")
    r.add_argument("--top", type=int, default=50, help="Top N to store per strategy (default: 50)")
    r.add_argument("--min-adv", type=float, default=2_000_000.0, help="Min avg dollar volume 20d (default: 2,000,000)")
    r.add_argument(
        "--use-target-universe",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use canonical Target Stocks universe (default: true)",
    )
    r.add_argument("--symbols", default=None, help="Optional comma-separated symbols override (disables target universe)")

    promo = sub.add_parser("promote", help="Select high-conviction (overlap + persistence) and write watchlist + prediction_queue")
    promo.add_argument("--date", required=True, help="As-of date (YYYY-MM-DD)")
    promo.add_argument("--db", default="data/alpha.db")
    promo.add_argument("--tenant-id", default="default")
    promo.add_argument("--top", type=int, default=20)
    promo.add_argument("--window-days", type=int, default=5)
    promo.add_argument("--min-overlap", type=int, default=2)
    promo.add_argument("--min-days-seen", type=int, default=3)
    promo.add_argument("--min-score", type=float, default=0.85)

    outc = sub.add_parser("outcomes", help="Compute forward returns for a watchlist date and store discovery_outcomes")
    outc.add_argument("--date", required=True, help="Watchlist date (YYYY-MM-DD)")
    outc.add_argument("--db", default="data/alpha.db")
    outc.add_argument("--tenant-id", default="default")
    outc.add_argument("--horizons", default="1,5,20", help="Comma-separated horizons in days (default: 1,5,20)")
    outc.add_argument(
        "--scope",
        default="both",
        choices=["watchlist", "candidates", "both"],
        help="Which cohort to compute (default: both)",
    )

    stats = sub.add_parser("stats", help="Aggregate discovery_outcomes into discovery_stats over a trailing window")
    stats.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    stats.add_argument("--db", default="data/alpha.db")
    stats.add_argument("--tenant-id", default="default")
    stats.add_argument("--window", type=int, default=30)
    stats.add_argument("--horizon", type=int, default=None, help="Single horizon days (deprecated; prefer --horizons)")
    stats.add_argument("--horizons", default="5,20", help="Comma-separated horizons (default: 5,20)")

    nightly = sub.add_parser("nightly", help="Run discovery -> promote -> outcomes -> stats in one command")
    nightly.add_argument("--date", default=None, help="As-of date (YYYY-MM-DD). Default: today")
    nightly.add_argument("--db", default="data/alpha.db")
    nightly.add_argument("--tenant-id", default="default")
    nightly.add_argument("--timeframe", default="1d")
    nightly.add_argument("--top", type=int, default=50, help="Discovery top N per strategy (default: 50)")
    nightly.add_argument("--min-adv", type=float, default=2_000_000.0)
    nightly.add_argument("--use-target-universe", action=argparse.BooleanOptionalAction, default=True)
    nightly.add_argument("--promote-top", type=int, default=20)
    nightly.add_argument("--promote-window-days", type=int, default=5)
    nightly.add_argument("--promote-min-overlap", type=int, default=2)
    nightly.add_argument("--promote-min-days-seen", type=int, default=3)
    nightly.add_argument("--promote-min-score", type=float, default=0.85)
    nightly.add_argument(
        "--bootstrap",
        action="store_true",
        help="Bootstrap mode: relax promote gates for cold start (overlap=1, days_seen=1, score>=0.6, window=1)",
    )
    nightly.add_argument("--outcomes-scope", default="both", choices=["watchlist", "candidates", "both"])
    nightly.add_argument("--outcomes-horizons", default="1,5,20")
    nightly.add_argument("--stats-window", type=int, default=30)
    nightly.add_argument("--stats-horizons", default="5,20")
    nightly.add_argument(
        "--no-threshold-supplement",
        action="store_true",
        help="Skip multi-strategy threshold enqueue after watchlist rows are queued",
    )
    nightly.add_argument(
        "--supplement-target",
        type=int,
        default=120,
        help="Max additional symbols to enqueue via score/confidence gates (default: 120)",
    )
    nightly.add_argument("--supplement-min-confidence", type=float, default=0.42)
    nightly.add_argument("--supplement-per-strategy-cap", type=int, default=22)

    return p


def _get_latest_snapshot(conn: sqlite3.Connection, *, tenant_id: str, ticker: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM fundamentals_snapshot
        WHERE tenant_id = ? AND ticker = ?
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (tenant_id, ticker),
    ).fetchone()
    return dict(row) if row else None


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    def _run_with_job(job_type: str, fn) -> int:
        repo = AlphaRepository(db_path=str(getattr(args, "db", "data/alpha.db")))
        job_id = None
        tenant = str(getattr(args, "tenant_id", "default"))
        try:
            job_id = repo.start_discovery_job(job_type=str(job_type), tenant_id=tenant)
            repo.close()
            rc = int(fn() or 0)
            repo = AlphaRepository(db_path=str(getattr(args, "db", "data/alpha.db")))
            repo.finish_discovery_job(job_id=job_id, status="success", tenant_id=tenant)
            return rc
        except Exception as e:
            try:
                if job_id:
                    repo = AlphaRepository(db_path=str(getattr(args, "db", "data/alpha.db")))
                    repo.finish_discovery_job(
                        job_id=job_id,
                        status="failed",
                        message=f"{type(e).__name__}: {e}",
                        tenant_id=tenant,
                    )
            except Exception:
                pass
            raise
        finally:
            try:
                repo.close()
            except Exception:
                pass

    if args.command == "sync-fundamentals":
        symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
        snapshots = fetch_fmp_fundamentals_batch(symbols, api_key=args.api_key)

        repo = AlphaRepository(db_path=str(args.db))
        try:
            conn = repo.conn
            for snap in snapshots:
                prev = _get_latest_snapshot(conn, tenant_id=str(args.tenant_id), ticker=snap.ticker)
                revenue_growth = None
                shares_growth = None
                if prev:
                    if snap.revenue_ttm is not None and prev.get("revenue_ttm") not in (None, 0):
                        revenue_growth = (float(snap.revenue_ttm) / float(prev["revenue_ttm"])) - 1.0
                    if snap.shares_outstanding is not None and prev.get("shares_outstanding") not in (None, 0):
                        shares_growth = (float(snap.shares_outstanding) / float(prev["shares_outstanding"])) - 1.0

                repo.upsert_fundamentals_snapshot(
                    {
                        "ticker": snap.ticker,
                        "as_of_date": snap.as_of_date,
                        "revenue_ttm": snap.revenue_ttm,
                        "revenue_growth": revenue_growth,
                        "shares_outstanding": snap.shares_outstanding,
                        "shares_growth": shares_growth,
                        "sector": snap.sector,
                        "industry": snap.industry,
                    },
                    tenant_id=str(args.tenant_id),
                )
            print(json.dumps({"upserted": len(snapshots), "symbols": symbols}, indent=2))
            return 0
        finally:
            repo.close()

    if args.command == "run":
        symbols = None
        if args.symbols:
            symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
        summary = run_discovery(
            db_path=str(args.db),
            tenant_id=str(args.tenant_id),
            as_of=_parse_date(args.date),
            top_n=int(args.top),
            min_avg_dollar_volume_20d=(float(args.min_adv) if args.min_adv is not None else None),
            timeframe=str(args.timeframe),
            use_target_universe=bool(args.use_target_universe) and symbols is None,
            symbols=symbols,
        )
        print(format_summary_json(summary))
        return 0

    if args.command == "promote":
        wl = select_high_conviction(
            db_path=str(args.db),
            tenant_id=str(args.tenant_id),
            as_of_date=_parse_date(args.date),
            window_days=int(args.window_days),
            min_overlap=int(args.min_overlap),
            min_days_seen=int(args.min_days_seen),
            min_avg_score=float(args.min_score),
            top_k=int(args.top),
        )
        repo = AlphaRepository(db_path=str(args.db))
        try:
            repo.upsert_discovery_watchlist(
                as_of_date=_parse_date(args.date),
                rows_in=watchlist_to_repo_rows(wl),
                tenant_id=str(args.tenant_id),
            )
            repo.upsert_prediction_queue(
                as_of_date=_parse_date(args.date),
                rows_in=watchlist_to_queue_rows(wl),
                tenant_id=str(args.tenant_id),
            )
        finally:
            repo.close()

        print(
            json.dumps(
                {
                    "as_of_date": _parse_date(args.date).isoformat(),
                    "selected": len(wl),
                    "symbols": [r.symbol for r in wl],
                },
                indent=2,
            )
        )
        return 0

    if args.command == "outcomes":
        def _do() -> int:
            hs = [int(x) for x in str(args.horizons).split(",") if str(x).strip()]
            repo = AlphaRepository(db_path=str(args.db))
            try:
                watch_rows = []
                cand_rows = []
                if str(args.scope) in ("watchlist", "both"):
                    watch_rows = compute_watchlist_outcomes(
                        db_path=str(args.db),
                        tenant_id=str(args.tenant_id),
                        watchlist_date=_parse_date(args.date),
                        horizons=hs,
                    )
                    repo.upsert_discovery_outcomes(
                        watchlist_date=_parse_date(args.date).isoformat(),
                        rows_in=outcomes_to_repo_rows(watch_rows),
                        tenant_id=str(args.tenant_id),
                    )

                if str(args.scope) in ("candidates", "both"):
                    cand_rows = compute_candidate_outcomes(
                        db_path=str(args.db),
                        tenant_id=str(args.tenant_id),
                        as_of_date=_parse_date(args.date),
                        horizons=hs,
                    )
                    repo.upsert_discovery_candidate_outcomes(
                        as_of_date=_parse_date(args.date).isoformat(),
                        rows_in=cand_rows,
                        tenant_id=str(args.tenant_id),
                    )
            finally:
                repo.close()
            print(
                json.dumps(
                    {
                        "date": _parse_date(args.date).isoformat(),
                        "scope": str(args.scope),
                        "watchlist_rows": len(watch_rows),
                        "candidate_rows": len(cand_rows),
                    },
                    indent=2,
                )
            )
            return 0

        return _run_with_job("outcomes", _do)

    if args.command == "stats":
        def _do() -> int:
            if args.horizon is not None:
                horizons = [int(args.horizon)]
            else:
                horizons = [int(x) for x in str(args.horizons).split(",") if str(x).strip()]
                if not horizons:
                    horizons = [5, 20]

            all_rows = []
            for h in horizons:
                all_rows.extend(
                    compute_discovery_stats(
                        db_path=str(args.db),
                        tenant_id=str(args.tenant_id),
                        end_date=_parse_date(args.end_date),
                        window_days=int(args.window),
                        horizon_days=int(h),
                    )
                )

            _apply_playbook_status(all_rows)

            repo = AlphaRepository(db_path=str(args.db))
            try:
                repo.insert_discovery_stats(stats_to_repo_rows(all_rows), tenant_id=str(args.tenant_id))
            finally:
                repo.close()
            print(
                json.dumps(
                    {"end_date": _parse_date(args.end_date).isoformat(), "rows": len(all_rows), "horizons": horizons},
                    indent=2,
                )
            )
            return 0

        return _run_with_job("stats", _do)

    if args.command == "nightly":
        def _do() -> int:
            if args.date:
                asof = _parse_date(args.date).isoformat()
            else:
                asof = _latest_coherent_trading_day(
                    db_path=str(args.db),
                    tenant_id=str(args.tenant_id),
                    timeframe=str(args.timeframe),
                ) or date.today().isoformat()

            # 1) Discovery run
            disc_summary = run_discovery(
                db_path=str(args.db),
                tenant_id=str(args.tenant_id),
                as_of=asof,
                top_n=int(args.top),
                min_avg_dollar_volume_20d=float(args.min_adv) if args.min_adv is not None else None,
                timeframe=str(args.timeframe),
                use_target_universe=bool(args.use_target_universe),
                symbols=None,
            )

            # 2) Promote
            promote_window_days = int(args.promote_window_days)
            promote_min_overlap = int(args.promote_min_overlap)
            promote_min_days_seen = int(args.promote_min_days_seen)
            promote_min_score = float(args.promote_min_score)
            if bool(args.bootstrap):
                promote_window_days = 1
                promote_min_overlap = 1
                promote_min_days_seen = 1
                promote_min_score = 0.60

            wl = select_high_conviction(
                db_path=str(args.db),
                tenant_id=str(args.tenant_id),
                as_of_date=asof,
                window_days=promote_window_days,
                min_overlap=promote_min_overlap,
                min_days_seen=promote_min_days_seen,
                min_avg_score=promote_min_score,
                top_k=int(args.promote_top),
            )
            repo = AlphaRepository(db_path=str(args.db))
            try:
                repo.upsert_discovery_watchlist(
                    as_of_date=asof,
                    rows_in=watchlist_to_repo_rows(wl),
                    tenant_id=str(args.tenant_id),
                )
                repo.upsert_prediction_queue(
                    as_of_date=asof,
                    rows_in=watchlist_to_queue_rows(wl),
                    tenant_id=str(args.tenant_id),
                )

                supplement_summary: dict[str, Any] = {}
                if not bool(args.no_threshold_supplement):
                    from app.engine.discovery_integration import supplement_prediction_queue_from_discovery

                    supplement_summary = supplement_prediction_queue_from_discovery(
                        repo=repo,
                        disc_summary=disc_summary,
                        as_of_date=asof,
                        tenant_id=str(args.tenant_id),
                        target_signals=int(args.supplement_target),
                        min_confidence=float(args.supplement_min_confidence),
                        per_strategy_cap=int(args.supplement_per_strategy_cap),
                    )

                # quick counts for observability
                cand_n = int(
                    (repo.conn.execute(
                        "SELECT COUNT(*) as n FROM discovery_candidates WHERE tenant_id=? AND as_of_date=?",
                        (str(args.tenant_id), str(asof)),
                    ).fetchone() or {"n": 0})["n"]
                )
                pq_pending = int(
                    (repo.conn.execute(
                        "SELECT COUNT(*) as n FROM prediction_queue WHERE tenant_id=? AND as_of_date=? AND status='pending'",
                        (str(args.tenant_id), str(asof)),
                    ).fetchone() or {"n": 0})["n"]
                )

                # 3) Outcomes
                hs = [int(x) for x in str(args.outcomes_horizons).split(",") if str(x).strip()]
                if str(args.outcomes_scope) in ("watchlist", "both"):
                    watch_rows = compute_watchlist_outcomes(
                        db_path=str(args.db),
                        tenant_id=str(args.tenant_id),
                        watchlist_date=asof,
                        horizons=hs,
                    )
                    repo.upsert_discovery_outcomes(
                        watchlist_date=asof,
                        rows_in=outcomes_to_repo_rows(watch_rows),
                        tenant_id=str(args.tenant_id),
                    )
                if str(args.outcomes_scope) in ("candidates", "both"):
                    cand_rows = compute_candidate_outcomes(
                        db_path=str(args.db),
                        tenant_id=str(args.tenant_id),
                        as_of_date=asof,
                        horizons=hs,
                    )
                    repo.upsert_discovery_candidate_outcomes(
                        as_of_date=asof,
                        rows_in=cand_rows,
                        tenant_id=str(args.tenant_id),
                    )
            finally:
                repo.close()

            # 4) Stats (default 5d+20d)
            horizons = [int(x) for x in str(args.stats_horizons).split(",") if str(x).strip()] or [5, 20]
            all_rows = []
            for h in horizons:
                all_rows.extend(
                    compute_discovery_stats(
                        db_path=str(args.db),
                        tenant_id=str(args.tenant_id),
                        end_date=asof,
                        window_days=int(args.stats_window),
                        horizon_days=int(h),
                    )
                )
            _apply_playbook_status(all_rows)
            repo = AlphaRepository(db_path=str(args.db))
            try:
                repo.insert_discovery_stats(stats_to_repo_rows(all_rows), tenant_id=str(args.tenant_id))
            finally:
                repo.close()

            print(
                json.dumps(
                    {
                        "date": asof,
                        "discovery": {
                            "feature_rows": int(disc_summary.get("feature_rows") or 0),
                            "universe_size": disc_summary.get("universe_size"),
                            "candidates_rows": int(cand_n),
                        },
                        "prediction_queue": {
                            "pending": int(pq_pending),
                        },
                        "threshold_supplement": supplement_summary,
                        "watchlist_size": len(wl),
                        "stats_rows": len(all_rows),
                        "outcomes_scope": str(args.outcomes_scope),
                    },
                    indent=2,
                )
            )
            return 0

        return _run_with_job("nightly", _do)

    return 2


def _apply_playbook_status(rows: list[Any]) -> None:
    """
    Mutates StatRow objects in-place (via object.__setattr__) to set status on playbook_eval rows.
    """
    eval_map: dict[str, dict[int, dict[str, float]]] = {}
    for r in rows:
        if getattr(r, "group_type", None) != "playbook_eval":
            continue
        pb = str(getattr(r, "group_value"))
        hd = int(getattr(r, "horizon_days"))
        eval_map.setdefault(pb, {})[hd] = {"avg_return": float(getattr(r, "avg_return")), "lift": float(getattr(r, "lift"))}

    def _status(pb: str) -> str:
        d = eval_map.get(pb, {})
        r5 = d.get(5)
        r20 = d.get(20)
        if r5 and (r5["avg_return"] > 0) and (r5["lift"] > 0):
            return "working"
        if r20 and (r20["avg_return"] > 0):
            return "long_horizon"
        return "weak"

    for r in rows:
        if getattr(r, "group_type", None) == "playbook_eval":
            pb = str(getattr(r, "group_value"))
            try:
                object.__setattr__(r, "status", _status(pb))
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
