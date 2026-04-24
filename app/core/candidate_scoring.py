"""
Promotion-side scoring primitives for candidate_queue.
These are pure functions (no DB, no discovery types) so they live in core
and can be imported by db.repository without creating a db->discovery cycle.
"""

from __future__ import annotations

import json


def compute_multiplier_score(
    *,
    price_percentile_252d: float | None,
    volatility_20d: float | None,
    signal_count: int,
) -> float:
    """
    Long-term / promotion heuristic only (depressed vs range, stability, recurrence).
    Not fed into RankingEngine.
    """
    if price_percentile_252d is not None:
        dep = max(0.0, min(1.0, 1.0 - float(price_percentile_252d)))
    else:
        dep = 0.5
    vol = float(volatility_20d) if volatility_20d is not None else 0.02
    stab = max(0.0, min(1.0, 1.0 / (1.0 + vol * 50.0)))
    rec = min(1.0, max(0, int(signal_count)) / 5.0)
    return float(dep * 0.4 + stab * 0.35 + rec * 0.25)


def merge_strategy_tags_json(
    existing_json: str | None,
    *,
    strategy_type: str,
    score: float,
    discovery_lens: str,
    as_of_date: str,
    cap: int = 24,
) -> str:
    try:
        cur = json.loads(existing_json or "[]")
    except json.JSONDecodeError:
        cur = []
    if not isinstance(cur, list):
        cur = []
    cur.append(
        {
            "strategy_type": str(strategy_type),
            "score": float(score),
            "discovery_lens": str(discovery_lens),
            "as_of_date": str(as_of_date),
        }
    )
    cur = cur[-cap:]
    return json.dumps(cur, separators=(",", ":"))
