from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PredictionOutcome:
    prediction_id: str
    strategy_id: str
    ticker: str
    track: str
    mode: str
    regime: str | None
    entry_price: float
    exit_price: float
    return_pct: float
    residual_alpha: float
    direction_correct: bool
    evaluated_at: str
