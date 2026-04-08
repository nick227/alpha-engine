from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, PredictionDirection, ScoredEvent
from app.strategies.base import StrategyBase


class BollingerReversionStrategy(StrategyBase):
    def maybe_predict(self, scored_event: ScoredEvent, mra: MRAOutcome, price_context: dict, event_timestamp: datetime) -> Prediction | None:
        zscore = float(price_context.get("zscore_20", 0.0))
        threshold = float(self.config.config.get("zscore_threshold", 2.0))

        direction: PredictionDirection | None = None
        edge = 0.0
        if zscore <= -threshold:
            direction = "up"
            edge = min(abs(zscore) / max(threshold, 1.0), 1.0)
        elif zscore >= threshold:
            direction = "down"
            edge = min(abs(zscore) / max(threshold, 1.0), 1.0)

        if direction is None:
            return None

        confidence = min(0.5 + edge * 0.3, 0.88)
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
            feature_snapshot={"zscore_20": zscore, "family": "statistical", "edge": edge},
        )
