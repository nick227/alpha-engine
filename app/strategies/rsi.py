from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, PredictionDirection, ScoredEvent
from app.strategies.base import StrategyBase


class RSIBaselineStrategy(StrategyBase):
    def maybe_predict(self, scored_event: ScoredEvent, mra: MRAOutcome, price_context: dict, event_timestamp: datetime) -> Prediction | None:
        horizon = str(self.config.config.get("horizon", "15m"))

        # Lightweight placeholder baseline using short-term reversal logic.
        if mra.return_5m <= -0.01 and mra.volume_ratio > 1.0:
            direction: PredictionDirection = "up"
            confidence = 0.58
        elif mra.return_5m >= 0.01 and mra.volume_ratio > 1.0:
            direction = "down"
            confidence = 0.58
        else:
            return None

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=float(confidence),
            horizon=horizon,
            entry_price=float(price_context.get("entry_price", 100.0)),
            mode=self.config.mode,
            feature_snapshot={
                "mra_score": float(mra.mra_score),
                "return_5m": float(mra.return_5m),
                "volume_ratio": float(mra.volume_ratio),
            },
        )
