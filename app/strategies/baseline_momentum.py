from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import Prediction, PredictionDirection, ScoredEvent, MRAOutcome
from app.strategies.base import StrategyBase


class BaselineMomentumStrategy(StrategyBase):
    def maybe_predict(self, scored_event: ScoredEvent, mra: MRAOutcome, price_context: dict, event_timestamp: datetime) -> Prediction | None:
        short_trend = float(price_context.get("short_trend", 0.0))
        if abs(short_trend) < self.config.config.get("min_short_trend", 0.003):
            return None

        direction: PredictionDirection = "up" if short_trend > 0 else "down"
        confidence = min(abs(short_trend) * 60.0, 0.85)

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
            feature_snapshot={"short_trend": short_trend},
        )
