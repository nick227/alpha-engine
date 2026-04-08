from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from app.core.types import MRAOutcome, Prediction, ScoredEvent, StrategyConfig


class StrategyBase(ABC):
    def __init__(self, config: StrategyConfig) -> None:
        self.config = config

    @abstractmethod
    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: dict,
        event_timestamp: datetime,
    ) -> Prediction | None:
        raise NotImplementedError
