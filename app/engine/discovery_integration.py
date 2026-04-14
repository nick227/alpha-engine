"""
Discovery Integration Module

Wires discovery strategies (silent_compounder, balance_sheet_survivor) directly
into the prediction pipeline without requiring multi-strategy promotion gate.

Bypassing select_high_conviction() intentionally: both strategies have confirmed
positive IC on filtered universe (silent_compounder 66% win at 5d, balance_sheet_survivor
61% win at 5d on $20+ stocks). We don't need multi-strategy overlap to queue them.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from app.db.repository import AlphaRepository
from app.discovery.runner import run_discovery


# Strategies promoted directly to prediction queue (bypassing multi-strategy gate).
# Only strategies with confirmed IC on filtered universe belong here.
PROMOTED_STRATEGIES: dict[str, dict[str, Any]] = {
    "silent_compounder": {
        # Equity vol band (~2% daily) + positive 63d drift → bullish continuation
        # CORRECTED (2026-04-14): 58.2% at 5d, 64.3% at 20d on $20+ equity universe
        # (Prior 73-83% numbers were bond ETF artifact — resolved by ideal_vol=0.02 filter)
        "direction": "UP",
        "horizon_days": 20,      # 20d is the confirmed best horizon (64.3% win, n=6035)
        "max_candidates": 15,
        "min_close": 20.0,       # $20+ is the quality sweet spot (58% vs 49% for $10-$20)
        "priority_base": 20,
    },
    "balance_sheet_survivor": {
        # Distress (negative 63d) + volatility stabilization → mean-reversion bounce
        # Sweet spot is $10-$20 (64.6% win at 5d). $20+ floor inverts to 46% — do not use.
        # min_close=$10, max_close=$20 targets the distressed-but-established range.
        "direction": "UP",
        "horizon_days": 5,
        "max_candidates": 10,
        "min_close": 10.0,
        "max_close": 20.0,   # cap at $20 — above that the thesis breaks down
        "priority_base": 15,
    },
}


def queue_discovery_predictions(
    *,
    repo: AlphaRepository,
    as_of: date,
    tenant_id: str = "default",
    min_adv: float = 2_000_000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run discovery and queue top candidates for promoted strategies.

    Inserts into prediction_queue and seeds consensus signals so
    PredictedSeriesBuilder can process them via prediction_cli run-queue.

    Returns a summary dict.
    """
    as_of_str = as_of.isoformat()

    result = run_discovery(
        db_path=repo.db_path,
        as_of=as_of,
        min_avg_dollar_volume_20d=min_adv,
        use_feature_snapshot=True,
        top_n=50,
    )

    queue_rows: list[dict[str, Any]] = []
    seeds_created = 0

    for strategy_name, cfg in PROMOTED_STRATEGIES.items():
        candidates = result.get("strategies", {}).get(strategy_name, {}).get("top", [])
        min_close = float(cfg["min_close"])
        max_close = float(cfg.get("max_close") or 1e9)
        max_n = int(cfg["max_candidates"])
        direction = str(cfg["direction"])
        horizon_days = int(cfg["horizon_days"])
        priority_base = int(cfg["priority_base"])

        # Apply price range gate (on top of strategies.py MIN_CLOSE=10 gate)
        filtered = [
            c for c in candidates
            if min_close <= float(c.get("metadata", {}).get("close") or 0) < max_close
        ][:max_n]

        for cand in filtered:
            symbol = str(cand["symbol"]).upper()
            raw_score = float(cand["score"])   # percentile rank [0, 1]
            close = float(cand.get("metadata", {}).get("close") or 0)
            
            # Skip low-confidence predictions
            if raw_score < 0.20:
                continue

            # avg_score is read by _seed_consensus_from_queue_metadata in prediction_cli:
            # p_final = clamp((avg_score * 2.0) - 1.0, -1.0, 1.0)
            # For raw_score=0.8 → p_final=+0.6 (bullish), for raw_score=0.6 → p_final=+0.2
            avg_score = raw_score

            metadata = {
                "strategy": strategy_name,
                "direction": direction,
                "avg_score": avg_score,
                "horizon_days": horizon_days,
                "close": close,
                "raw_score": raw_score,
                "source_pipeline": "nightly_discovery",
                "as_of_date": as_of_str,
            }

            queue_rows.append({
                "symbol": symbol,
                "source": f"discovery_{strategy_name}",
                "priority": priority_base + int((1.0 - raw_score) * 10),
                "status": "pending",
                "metadata_json": json.dumps(metadata, sort_keys=True),
            })

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
            "feature_rows": result.get("feature_rows", 0),
            "candidates": queue_rows,
        }

    if queue_rows:
        repo.conn.execute("PRAGMA journal_mode=WAL;")
        repo.upsert_prediction_queue(
            as_of_date=as_of_str,
            rows_in=queue_rows,
            tenant_id=tenant_id,
        )

        # Seed consensus signals for tickers with no existing signal.
        # PredictedSeriesBuilder requires at least one consensus_signals row.
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

    return {
        "as_of_date": as_of_str,
        "total_queued": len(queue_rows),
        "by_strategy": by_strategy,
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
