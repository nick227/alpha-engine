from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, PredictionDirection, ScoredEvent
from app.strategies.base import StrategyBase


class RSIMeanReversionStrategy(StrategyBase):
    def maybe_predict(self, scored_event: ScoredEvent, mra: MRAOutcome, price_context: dict, event_timestamp: datetime) -> Prediction | None:
        rsi = float(price_context.get("rsi_14", 50.0))
        oversold = float(self.config.config.get("oversold", 30.0))
        overbought = float(self.config.config.get("overbought", 70.0))

        direction: PredictionDirection | None = None
        edge = 0.0
        if rsi <= oversold:
            direction = "up"
            edge = min((oversold - rsi) / max(oversold, 1.0), 1.0)
        elif rsi >= overbought:
            direction = "down"
            edge = min((rsi - overbought) / max(100.0 - overbought, 1.0), 1.0)

        if direction is None:
            return None

        confidence = min(0.52 + edge * 0.35, 0.9)
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
            feature_snapshot={"rsi_14": rsi, "family": "statistical", "edge": edge},
        )
