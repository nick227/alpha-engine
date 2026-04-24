"""
Tagging helpers for candidate_queue that require discovery types (FeatureRow).
Scoring primitives (compute_multiplier_score, merge_strategy_tags_json) live in
app.core.candidate_scoring so db.repository can import them without a db->discovery cycle.
"""

from __future__ import annotations

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
