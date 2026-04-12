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


class VolCrushMeanReversionStrategy(StrategyBase):
    """
    Vol crush -> mean reversion.

    Idea: after a volatility spike starts collapsing, overshot moves are more likely to revert.

    This first version uses:
    - a simple "crush" heuristic over a volatility window (previous max vs current)
    - a price stretch proxy (`zscore_20`) to pick direction.
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
            return None

        # Heuristic: prior high vol then a sharp drop to current.
        if len(hist_f) < 5:
            return None
        prev_max = max(hist_f[:-1])
        min_prev_max = _safe_float(cfg.get("min_prev_max_vol", 0.02), 0.02)
        crush_ratio = _safe_float(cfg.get("crush_ratio", 0.75), 0.75)
        if prev_max < min_prev_max:
            return None
        if realized_vol > (prev_max * crush_ratio):
            return None

        vol_z = _zscore(realized_vol, hist_f)
        max_vol_z = _safe_float(cfg.get("max_vol_z", -0.3), -0.3)
        if vol_z > max_vol_z:
            return None

        stretch = _safe_float(price_context.get("zscore_20"), 0.0)
        min_abs_stretch = _safe_float(cfg.get("min_abs_price_z", 1.5), 1.5)
        if abs(stretch) < min_abs_stretch:
            return None

        direction: PredictionDirection = "down" if stretch > 0 else "up"

        pullback = _safe_float(getattr(mra, "pullback_depth", None), 0.0)
        range_expansion = _safe_float(price_context.get("range_expansion", getattr(mra, "range_expansion", 1.0)), 1.0)
        confidence = 0.56
        confidence += max(0.0, min((abs(stretch) - min_abs_stretch) / 2.0, 1.0)) * 0.18
        confidence += max(0.0, min((range_expansion - 1.0) / 2.0, 1.0)) * 0.06
        confidence += max(0.0, min(pullback / 0.03, 1.0)) * 0.05
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
                "setup": "vol_crush_mean_reversion",
                "realized_volatility": float(realized_vol),
                "prev_max_vol": float(prev_max),
                "vol_z": float(round(vol_z, 4)),
                "price_z": float(round(stretch, 4)),
                "range_expansion": float(range_expansion),
                "pullback_depth": float(pullback),
            },
        )

