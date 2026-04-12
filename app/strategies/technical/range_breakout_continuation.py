from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, PredictionDirection, ScoredEvent
from app.strategies.base import StrategyBase


def _safe_float(x: object, default: float = 0.0) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float(default)


class RangeBreakoutContinuationStrategy(StrategyBase):
    """
    Structural breakout -> continuation.

    Uses prior rolling range (rolling_high_20/rolling_low_20) plus expansion filters:
    - breakout buffer above/below prior range
    - range_expansion and volume_ratio confirmation
    """

    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: dict,
        event_timestamp: datetime,
    ) -> Prediction | None:
        cfg = dict(self.config.config or {})

        entry_price = _safe_float(price_context.get("entry_price"), 0.0)
        if entry_price <= 0:
            return None

        hi = price_context.get("rolling_high_20")
        lo = price_context.get("rolling_low_20")
        if hi is None or lo is None:
            return None
        rolling_high = float(_safe_float(hi, 0.0))
        rolling_low = float(_safe_float(lo, 0.0))
        if rolling_high <= 0 or rolling_low <= 0 or rolling_high <= rolling_low:
            return None

        buffer = float(_safe_float(cfg.get("breakout_buffer", 0.0025), 0.0025))
        up_level = rolling_high * (1.0 + buffer)
        down_level = rolling_low * (1.0 - buffer)

        direction: PredictionDirection | None = None
        if entry_price >= up_level:
            direction = "up"
        elif entry_price <= down_level:
            direction = "down"
        if direction is None:
            return None

        range_expansion = float(_safe_float(price_context.get("range_expansion", getattr(mra, "range_expansion", 1.0)), 1.0))
        min_range_expansion = float(_safe_float(cfg.get("min_range_expansion", 1.15), 1.15))
        if range_expansion < min_range_expansion:
            return None

        volume_ratio = float(_safe_float(price_context.get("volume_ratio", getattr(mra, "volume_ratio", 1.0)), 1.0))
        min_volume_ratio = float(_safe_float(cfg.get("min_volume_ratio", 1.2), 1.2))
        if volume_ratio < min_volume_ratio:
            return None

        # Optional: avoid breakouts in high volatility chop if configured.
        vol_regime = str(price_context.get("regime") or "NORMAL")
        deny_vol = cfg.get("deny_volatility_regimes")
        if isinstance(deny_vol, list) and vol_regime in {str(x) for x in deny_vol}:
            return None

        dist = abs(entry_price - (up_level if direction == "up" else down_level)) / max(entry_price, 1e-6)
        dist_boost = max(0.0, min(dist / 0.01, 1.0))
        range_boost = max(0.0, min((range_expansion - min_range_expansion) / 1.5, 1.0))
        vol_boost = max(0.0, min((volume_ratio - min_volume_ratio) / 2.0, 1.0))

        confidence = 0.58 + (0.14 * dist_boost) + (0.10 * range_boost) + (0.06 * vol_boost)
        confidence = max(0.1, min(confidence, 0.92))

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=float(confidence),
            horizon=str(cfg.get("horizon", "30d")),
            entry_price=float(entry_price),
            mode=self.config.mode,
            feature_snapshot={
                "family": "breakout",
                "setup": "range_breakout_continuation",
                "rolling_high_20": float(rolling_high),
                "rolling_low_20": float(rolling_low),
                "buffer": float(buffer),
                "up_level": float(up_level),
                "down_level": float(down_level),
                "range_expansion": float(range_expansion),
                "volume_ratio": float(volume_ratio),
            },
        )

