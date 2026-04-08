from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterable, Optional


@dataclass
class PredictionRecord:
    id: str
    ticker: str
    created_at: datetime
    horizon_minutes: int
    entry_price: float
    direction: str
    confidence: float
    mode: str = "LIVE"
    evaluated_at: Optional[datetime] = None


@dataclass
class PredictionOutcome:
    prediction_id: str
    exit_price: float
    realized_return: float
    direction_correct: bool
    evaluated_at: datetime


class ReplayService:
    def __init__(self, get_exit_price: Callable[[str, datetime], float]):
        self.get_exit_price = get_exit_price

    def due_for_evaluation(
        self,
        predictions: Iterable[PredictionRecord],
        now: datetime,
    ) -> list[PredictionRecord]:
        result: list[PredictionRecord] = []
        for prediction in predictions:
            if prediction.evaluated_at is not None:
                continue
            expires_at = prediction.created_at + timedelta(minutes=prediction.horizon_minutes)
            if now >= expires_at:
                result.append(prediction)
        return result

    def evaluate(self, prediction: PredictionRecord, now: datetime) -> PredictionOutcome:
        exit_ts = prediction.created_at + timedelta(minutes=prediction.horizon_minutes)
        exit_price = self.get_exit_price(prediction.ticker, exit_ts)
        realized_return = (exit_price - prediction.entry_price) / prediction.entry_price

        if prediction.direction.lower() == "down":
            direction_correct = realized_return < 0
        elif prediction.direction.lower() == "flat":
            direction_correct = abs(realized_return) < 0.001
        else:
            direction_correct = realized_return > 0

        return PredictionOutcome(
            prediction_id=prediction.id,
            exit_price=exit_price,
            realized_return=realized_return,
            direction_correct=direction_correct,
            evaluated_at=now,
        )
