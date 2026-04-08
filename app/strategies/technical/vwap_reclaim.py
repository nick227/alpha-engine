from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, PredictionDirection, ScoredEvent
from app.strategies.base import StrategyBase


class VWAPReclaimStrategy(StrategyBase):
    def maybe_predict(self, scored_event: ScoredEvent, mra: MRAOutcome, price_context: dict, event_timestamp: datetime) -> Prediction | None:
        reclaim = bool(price_context.get("vwap_reclaim", False))
        reject = bool(price_context.get("vwap_reject", False))
        volume_ratio = float(price_context.get("volume_ratio", 1.0))
        min_volume_ratio = float(self.config.config.get("min_volume_ratio", 1.5))

        direction: PredictionDirection | None = None
        if reclaim and volume_ratio >= min_volume_ratio:
            direction = "up"
        elif reject and volume_ratio >= min_volume_ratio:
            direction = "down"

        if direction is None:
            return None

        confidence = min(0.5 + max(volume_ratio - 1.0, 0.0) * 0.12 + abs(float(price_context.get("vwap_distance", 0.0))) * 8.0, 0.9)
        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=confidence,
            horizon=self.config.config.get("horizon", "15m"),
            entry_price=float(price_context.get("entry_price", 100.0)),
            mode=self.config.mode,
            feature_snapshot={
                "vwap_reclaim": reclaim,
                "vwap_reject": reject,
                "volume_ratio": volume_ratio,
                "family": "microstructure",
            },
        )
