from __future__ import annotations

from typing import Optional

from app.core.types import MRAOutcome, Prediction, ScoredEvent
from app.strategies.base import BaseStrategy, StrategyContext


class RSIBaselineStrategy(BaseStrategy):
    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        ctx: StrategyContext,
    ) -> Optional[Prediction]:
        horizon = str(self.config.get("horizon", "15m"))

        # Lightweight placeholder baseline using short-term reversal logic.
        if mra.return_5m <= -0.01 and mra.volume_ratio > 1.0:
            direction = "up"
            confidence = 0.58
        elif mra.return_5m >= 0.01 and mra.volume_ratio > 1.0:
            direction = "down"
            confidence = 0.58
        else:
            return None

        return Prediction(
            id=f"pred_{self.strategy_id}_{scored_event.id}",
            strategy_id=self.strategy_id,
            strategy_name=f"{self.name}:{self.version}",
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=ctx.event_timestamp,
            prediction=direction,  # type: ignore[arg-type]
            confidence=confidence,
            horizon=horizon,
            entry_price=ctx.entry_price,
        )
