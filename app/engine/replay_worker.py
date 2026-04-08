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
    def get_exit_price_at_or_after(self, ticker: str, at: datetime) -> float | None: ...


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
            expiry_raw = prediction.created_at + timedelta(minutes=prediction.horizon_minutes)
            # Align expiry to 1-minute bar timestamps (floor) to avoid off-by-one errors with minute bars.
            expiry = expiry_raw.astimezone(timezone.utc).replace(second=0, microsecond=0)
            if expiry > now:
                continue

            grace_minutes = max(30, int(prediction.horizon_minutes * 4))
            grace_deadline = expiry + timedelta(minutes=grace_minutes)

            if prediction.entry_price <= 0:
                outcome_id = self.outcomes.write_outcome(
                    {
                        "prediction_id": prediction.id,
                        "strategy_id": prediction.strategy_id,
                        "ticker": prediction.ticker,
                        "track": prediction.track,
                        "mode": prediction.mode,
                        "regime": prediction.regime,
                        "entry_price": prediction.entry_price,
                        "exit_price": prediction.entry_price,
                        "return_pct": 0.0,
                        "residual_alpha": 0.0,
                        "direction_correct": False,
                        "exit_reason": "invalid_entry_price",
                        "evaluated_at": now.isoformat(),
                    }
                )
                self.predictions.mark_scored(prediction.id, outcome_id)
                continue

            exit_price = self.prices.get_exit_price_at_or_after(prediction.ticker, expiry)
            if exit_price is None:
                # If we don't have a bar at/after expiry yet, defer until a grace deadline.
                if now < grace_deadline:
                    continue

                outcome_id = self.outcomes.write_outcome(
                    {
                        "prediction_id": prediction.id,
                        "strategy_id": prediction.strategy_id,
                        "ticker": prediction.ticker,
                        "track": prediction.track,
                        "mode": prediction.mode,
                        "regime": prediction.regime,
                        "entry_price": prediction.entry_price,
                        "exit_price": prediction.entry_price,
                        "return_pct": 0.0,
                        "residual_alpha": 0.0,
                        "direction_correct": False,
                        "exit_reason": "no_data",
                        "evaluated_at": now.isoformat(),
                    }
                )
                self.predictions.mark_scored(prediction.id, outcome_id)
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
                    "exit_reason": "horizon",
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
