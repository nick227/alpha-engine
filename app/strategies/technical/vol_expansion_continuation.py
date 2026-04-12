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


def _zscore(current: float, window: list[float]) -> float:
    vals = [float(v) for v in (window or []) if v is not None]
    if len(vals) < 2:
        return 0.0
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / max(1, len(vals))
    std = var ** 0.5
    if std == 0.0:
        return 0.0
    return (float(current) - float(mean)) / float(std)


class VolExpansionContinuationStrategy(StrategyBase):
    """
    Volatility expansion -> continuation.

    Idea: when realized volatility spikes (vs recent) AND range expands with directional slope,
    continuation is more likely than mean reversion.
    """

    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: dict,
        event_timestamp: datetime,
    ) -> Prediction | None:
        cfg = dict(self.config.config or {})

        realized_vol = _safe_float(price_context.get("realized_volatility"), 0.0)
        hist = price_context.get("historical_volatility_window")
        if not isinstance(hist, list) or not hist:
            hist = price_context.get("historical_volatility")
        hist = hist if isinstance(hist, list) else []
        hist_f = [_safe_float(v, realized_vol) for v in hist if v is not None]
        if not hist_f:
            hist_f = [realized_vol for _ in range(20)]

        vol_z = _zscore(realized_vol, hist_f)
        min_vol_z = _safe_float(cfg.get("min_vol_z", 1.0), 1.0)
        if vol_z < min_vol_z:
            return None

        range_expansion = _safe_float(price_context.get("range_expansion", getattr(mra, "range_expansion", 1.0)), 1.0)
        min_range_expansion = _safe_float(cfg.get("min_range_expansion", 1.2), 1.2)
        if range_expansion < min_range_expansion:
            return None

        # Direction source: prefer mra.continuation_slope if available; fall back to short_trend/returns.
        slope = _safe_float(getattr(mra, "continuation_slope", None), 0.0)
        if slope == 0.0:
            slope = _safe_float(price_context.get("short_trend", 0.0), 0.0)
        if slope == 0.0:
            slope = _safe_float(price_context.get("return_15m", 0.0), 0.0)

        min_abs_slope = _safe_float(cfg.get("min_abs_slope", 0.0015), 0.0015)
        if abs(slope) < min_abs_slope:
            return None

        direction: PredictionDirection = "up" if slope > 0 else "down"

        volume_ratio = _safe_float(price_context.get("volume_ratio", getattr(mra, "volume_ratio", 1.0)), 1.0)
        vol_boost = max(0.0, min((vol_z - min_vol_z) / 2.0, 1.0))
        range_boost = max(0.0, min((range_expansion - min_range_expansion) / 1.5, 1.0))
        volume_boost = max(0.0, min((volume_ratio - 1.0) / 2.0, 1.0))

        confidence = 0.55 + (0.20 * vol_boost) + (0.10 * range_boost) + (0.05 * volume_boost)
        confidence = max(0.1, min(confidence, 0.90))

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=float(confidence),
            horizon=cfg.get("horizon", "1d"),
            entry_price=float(price_context.get("entry_price", 100.0)),
            mode=self.config.mode,
            feature_snapshot={
                "family": "volatility",
                "setup": "vol_expansion_continuation",
                "realized_volatility": float(realized_vol),
                "vol_z": float(round(vol_z, 4)),
                "range_expansion": float(range_expansion),
                "continuation_slope": float(slope),
                "volume_ratio": float(volume_ratio),
            },
        )

