"""
Tagging and promotion-side scoring for candidate_queue (not used in daily ranking).
"""

from __future__ import annotations

import json
from typing import Any

from app.discovery.types import FeatureRow


def market_cap_bucket_from_features(fr: FeatureRow | None) -> str | None:
    """Liquidity proxy until true market cap is wired everywhere."""
    if fr is None:
        return None
    adv = fr.avg_dollar_volume_20d or fr.dollar_volume
    if adv is None:
        return None
    x = float(adv)
    if x < 1_000_000:
        return "micro"
    if x < 20_000_000:
        return "small"
    if x < 200_000_000:
        return "mid"
    return "large"


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


def tag_fields_from_feature_row(fr: FeatureRow | None) -> dict[str, Any]:
    if fr is None:
        return {
            "price_bucket": None,
            "market_cap_bucket": None,
            "sector": None,
            "industry": None,
            "price_percentile_252d": None,
            "volatility_20d": None,
        }
    return {
        "price_bucket": fr.price_bucket,
        "market_cap_bucket": market_cap_bucket_from_features(fr),
        "sector": fr.sector,
        "industry": fr.industry,
        "price_percentile_252d": fr.price_percentile_252d,
        "volatility_20d": fr.volatility_20d,
    }
