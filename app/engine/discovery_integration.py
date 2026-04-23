"""
Discovery Integration Module

Queues discovery candidates into prediction_queue using threshold rules across
registered strategies (see app.engine.threshold_queue). PROMOTED_STRATEGIES
supplies per-strategy price bands and horizons where we have validated IC.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from app.db.repository import AlphaRepository
from app.discovery.runner import run_discovery
from app.engine.threshold_queue import build_threshold_queue_rows


# Strategies promoted directly to prediction queue (bypassing multi-strategy gate).
# Only strategies with confirmed IC on filtered universe belong here.
PROMOTED_STRATEGIES: dict[str, dict[str, Any]] = {
    "silent_compounder": {
        # Equity vol band (~2% daily) + positive 63d drift → bullish continuation
        # Confirmed (2026-04-14): 58.2% at 5d, 64.3% at 20d, avg +1.08% on $20+ equity universe
        # Stop-loss at -15%: CORT lost -58% without it; capped losses improve Sharpe materially
        "direction": "UP",
        "horizon_days": 20,      # 20d is the confirmed best horizon (64.3% win, n=6035)
        "max_candidates": 15,
        "min_close": 20.0,       # $20+ is the quality sweet spot (58% vs 49% for $10-$20)
        "max_loss_pct": 0.15,    # exit if position down 15% — wired into compute_candidate_outcomes
        "priority_base": 20,
    },
    "balance_sheet_survivor": {
        # Distress (negative 63d) + volatility stabilization → mean-reversion bounce
        # Sweet spot is $10-$20 (56.9% at 5d, avg +2.16%). $20+ inverts to 46% — do not use.
        "direction": "UP",
        "horizon_days": 5,
        "max_candidates": 10,
        "min_close": 10.0,
        "max_close": 20.0,       # cap at $20 — above that the thesis breaks down
        "max_loss_pct": 0.15,
        "priority_base": 15,
    },
}


def _seed_consensus_for_queue_rows(
    repo: AlphaRepository,
    queue_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
) -> int:
    seeds_created = 0
    for row in queue_rows:
        symbol = str(row["symbol"])
        meta = json.loads(str(row["metadata_json"]))
        avg_score = float(meta.get("avg_score") or 0.7)
        p_final = max(-1.0, min(1.0, (avg_score * 2.0) - 1.0))
        conf = abs(p_final)
        strategy_name = str(meta.get("strategy") or "discovery")

        try:
            existing = repo.conn.execute(
                "SELECT COUNT(*) as n FROM consensus_signals WHERE tenant_id=? AND ticker=?",
                (tenant_id, symbol),
            ).fetchone()
            if existing is None or int(existing["n"] or 0) == 0:
                repo.save_consensus_signal(
                    {
                        "ticker": symbol,
                        "regime": "DISCOVERY",
                        "sentiment_strategy_id": f"{strategy_name}_v1",
                        "quant_strategy_id": f"{strategy_name}_v1",
                        "sentiment_score": conf,
                        "quant_score": conf,
                        "ws": 0.5,
                        "wq": 0.5,
                        "agreement_bonus": 0.0,
                        "p_final": p_final,
                        "stability_score": conf,
                    },
                    tenant_id=tenant_id,
                )
                seeds_created += 1
        except Exception as e:
            print(f"[WARN] consensus seed failed for {symbol}: {e}")
    return seeds_created


def supplement_prediction_queue_from_discovery(
    *,
    repo: AlphaRepository,
    disc_summary: dict[str, Any],
    as_of_date: str,
    tenant_id: str = "default",
    target_signals: int = 120,
    min_confidence: float = 0.42,
    per_strategy_cap: int = 22,
) -> dict[str, Any]:
    """
    After watchlist rows are queued, add threshold-based discovery rows without
    duplicating symbols already present for this as-of date.
    """
    ex = repo.conn.execute(
        """
        SELECT DISTINCT UPPER(TRIM(symbol)) AS s
        FROM prediction_queue
        WHERE tenant_id = ? AND as_of_date = ?
        """,
        (tenant_id, as_of_date),
    ).fetchall()
    exclude_symbols = {str(r["s"]) for r in ex if r and r["s"]}

    queue_rows, by_strategy = build_threshold_queue_rows(
        disc_summary=disc_summary,
        as_of_str=as_of_date,
        target_signals=target_signals,
        per_strategy_cap=per_strategy_cap,
        min_confidence=min_confidence,
        promoted_overrides=PROMOTED_STRATEGIES,
        exclude_symbols=exclude_symbols,
        source_pipeline="threshold_supplement",
    )
    if not queue_rows:
        return {"added": 0, "by_strategy": {}, "consensus_seeded": 0}

    repo.conn.execute("PRAGMA journal_mode=WAL;")
    repo.upsert_prediction_queue(
        as_of_date=as_of_date,
        rows_in=queue_rows,
        tenant_id=tenant_id,
    )
    seeded = _seed_consensus_for_queue_rows(repo, queue_rows, tenant_id=tenant_id)
    return {"added": len(queue_rows), "by_strategy": by_strategy, "consensus_seeded": seeded}


def queue_discovery_predictions(
    *,
    repo: AlphaRepository,
    as_of: date,
    tenant_id: str = "default",
    min_adv: float = 2_000_000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run discovery and enqueue candidates using global score/confidence gates
    (threshold_queue) plus per-strategy overrides in PROMOTED_STRATEGIES.
    """
    as_of_str = as_of.isoformat()

    result = run_discovery(
        db_path=repo.db_path,
        as_of=as_of,
        min_avg_dollar_volume_20d=min_adv,
        use_feature_snapshot=True,
        top_n=50,
    )

    queue_rows, by_strategy_counts = build_threshold_queue_rows(
        disc_summary=result,
        as_of_str=as_of_str,
        promoted_overrides=PROMOTED_STRATEGIES,
        exclude_symbols=set(),
        source_pipeline="nightly_discovery",
    )

    by_strategy: dict[str, int] = {}
    for row in queue_rows:
        src = str(row["source"])
        by_strategy[src] = by_strategy.get(src, 0) + 1

    if dry_run:
        return {
            "as_of_date": as_of_str,
            "dry_run": True,
            "total_queued": len(queue_rows),
            "by_strategy": by_strategy,
            "by_strategy_scoring": by_strategy_counts,
            "feature_rows": result.get("feature_rows", 0),
            "candidates": queue_rows,
        }

    seeds_created = 0
    if queue_rows:
        repo.conn.execute("PRAGMA journal_mode=WAL;")
        repo.upsert_prediction_queue(
            as_of_date=as_of_str,
            rows_in=queue_rows,
            tenant_id=tenant_id,
        )
        seeds_created = _seed_consensus_for_queue_rows(repo, queue_rows, tenant_id=tenant_id)

    return {
        "as_of_date": as_of_str,
        "total_queued": len(queue_rows),
        "by_strategy": by_strategy,
        "by_strategy_scoring": by_strategy_counts,
        "consensus_seeded": seeds_created,
        "feature_rows": result.get("feature_rows", 0),
    }


def batch_queue_discovery(
    *,
    repo: AlphaRepository,
    start_date: date,
    end_date: date,
    tenant_id: str = "default",
    min_adv: float = 2_000_000,
) -> dict[str, Any]:
    """
    Queue discovery predictions for a range of dates.
    Useful for backfilling paper trade history.
    """
    total = 0
    current = start_date
    daily_summaries: list[dict[str, Any]] = []

    while current <= end_date:
        try:
            summary = queue_discovery_predictions(
                repo=repo,
                as_of=current,
                tenant_id=tenant_id,
                min_adv=min_adv,
            )
            total += int(summary.get("total_queued") or 0)
            daily_summaries.append(summary)
        except Exception as e:
            print(f"[WARN] {current}: {e}")

        current += timedelta(days=1)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_queued": total,
        "days_processed": len(daily_summaries),
    }
