from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Protocol


@dataclass
class PredictionRecord:
    id: str
    strategy_id: str
    ticker: str
    track: str
    mode: str  # backtest | paper | live
    horizon_minutes: int
    created_at: datetime
    entry_price: float
    direction: str
    regime: str | None = None
    market_return: float | None = None


class PredictionRepository(Protocol):
    def list_unscored_predictions(self, now: datetime) -> Iterable[PredictionRecord]: ...
    def mark_scored(self, prediction_id: str, outcome_id: str) -> None: ...


class PriceRepository(Protocol):
    def get_exit_price(self, ticker: str, at: datetime) -> float | None: ...


class OutcomeWriter(Protocol):
    def write_outcome(self, payload: dict) -> str: ...


class MetricsUpdater(Protocol):
    def update_strategy_performance(self, strategy_id: str) -> None: ...
    def update_regime_performance(self, regime: str | None) -> None: ...
    def update_stability(self, strategy_id: str) -> None: ...
    def refresh_weight_engine_inputs(self) -> None: ...


def compute_residual_alpha(asset_return: float, market_return: float | None) -> float:
    baseline = market_return or 0.0
    return asset_return - baseline


def compute_direction_correct(asset_return: float) -> bool:
    return asset_return > 0


class ReplayWorker:
    """Scores expired predictions and closes the prediction→outcome feedback loop."""

    def __init__(
        self,
        predictions: PredictionRepository,
        prices: PriceRepository,
        outcomes: OutcomeWriter,
        metrics: MetricsUpdater,
    ) -> None:
        self.predictions = predictions
        self.prices = prices
        self.outcomes = outcomes
        self.metrics = metrics

    def run_once(self, now: datetime | None = None) -> int:
        now = now or datetime.now(timezone.utc)
        scored_count = 0

        for prediction in self.predictions.list_unscored_predictions(now):
            expiry = prediction.created_at + timedelta(minutes=prediction.horizon_minutes)
            if expiry > now:
                continue

            exit_price = self.prices.get_exit_price(prediction.ticker, expiry)
            if exit_price is None or prediction.entry_price <= 0:
                continue

            asset_return = (exit_price - prediction.entry_price) / prediction.entry_price
            residual_alpha = compute_residual_alpha(asset_return, prediction.market_return)

            direction = str(prediction.direction).lower()
            if direction == "down":
                correct = asset_return < 0
            elif direction == "flat":
                correct = abs(asset_return) < 0.001
            else:
                correct = asset_return > 0

            outcome_id = self.outcomes.write_outcome(
                {
                    "prediction_id": prediction.id,
                    "strategy_id": prediction.strategy_id,
                    "ticker": prediction.ticker,
                    "track": prediction.track,
                    "mode": prediction.mode,
                    "regime": prediction.regime,
                    "entry_price": prediction.entry_price,
                    "exit_price": exit_price,
                    "return_pct": round(asset_return, 6),
                    "residual_alpha": round(residual_alpha, 6),
                    "direction_correct": correct,
                    "evaluated_at": now.isoformat(),
                }
            )

            self.predictions.mark_scored(prediction.id, outcome_id)
            self.metrics.update_strategy_performance(prediction.strategy_id)
            self.metrics.update_regime_performance(prediction.regime)
            self.metrics.update_stability(prediction.strategy_id)
            scored_count += 1

        self.metrics.refresh_weight_engine_inputs()
        return scored_count
