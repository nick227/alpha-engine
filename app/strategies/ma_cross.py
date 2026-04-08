from __future__ import annotations

from typing import Optional

from app.core.types import MRAOutcome, Prediction, ScoredEvent
from app.strategies.base import BaseStrategy, StrategyContext


class MACrossBaselineStrategy(BaseStrategy):
    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        ctx: StrategyContext,
    ) -> Optional[Prediction]:
        horizon = str(self.config.get("horizon", "15m"))

        if mra.return_5m > 0.004 and mra.return_15m > 0.006:
            direction = "up"
            confidence = min(0.85, 0.45 + (mra.mra_score * 0.5))
        elif mra.return_5m < -0.004 and mra.return_15m < -0.006:
            direction = "down"
            confidence = min(0.85, 0.45 + (mra.mra_score * 0.5))
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
