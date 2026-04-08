from __future__ import annotations

from uuid import uuid4

from app.core.types import MRAOutcome, ScoredEvent


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(value, high))


def compute_mra(scored_event: ScoredEvent, price_context: dict) -> MRAOutcome:
    r1 = float(price_context.get("return_1m", 0.0))
    r5 = float(price_context.get("return_5m", 0.0))
    r15 = float(price_context.get("return_15m", 0.0))
    r1h = float(price_context.get("return_1h", 0.0))
    volume_ratio = float(price_context.get("volume_ratio", 1.0))
    vwap_distance = float(price_context.get("vwap_distance", 0.0))
    range_expansion = float(price_context.get("range_expansion", 1.0))
    continuation_slope = float(price_context.get("continuation_slope", 0.0))
    pullback_depth = float(price_context.get("pullback_depth", 0.0))

    directional_bias = -1.0 if scored_event.direction == "negative" else 1.0
    raw_score = (
        (r5 * 24.0 * directional_bias)
        + (r15 * 14.0 * directional_bias)
        + (max(volume_ratio - 1.0, 0.0) * 0.22)
        + (abs(vwap_distance) * 10.0)
        + (continuation_slope * 0.16)
        + (max(range_expansion - 1.0, 0.0) * 0.12)
        - (pullback_depth * 14.0)
    )
    mra_score = _clamp(raw_score)

    return MRAOutcome(
        id=str(uuid4()),
        scored_event_id=scored_event.id,
        return_1m=r1,
        return_5m=r5,
        return_15m=r15,
        return_1h=r1h,
        volume_ratio=volume_ratio,
        vwap_distance=vwap_distance,
        range_expansion=range_expansion,
        continuation_slope=continuation_slope,
        pullback_depth=pullback_depth,
        mra_score=mra_score,
        market_context=price_context,
    )
