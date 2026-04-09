from __future__ import annotations

from typing import List
from uuid import uuid4

import pandas as pd

from app.core.types import Prediction, PredictionOutcome


HORIZON_STEPS = {"5m": 5, "15m": 15, "1h": 60, "1d": 390, "7d": 390 * 7, "30d": 390 * 30}


def evaluate_predictions(predictions: List[Prediction], bars: pd.DataFrame) -> List[PredictionOutcome]:
    outcomes: List[PredictionOutcome] = []
    bars = bars.copy()
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)

    for pred in predictions:
        ticker_bars = bars[bars["ticker"] == pred.ticker].sort_values("timestamp").reset_index(drop=True)
        pred_ts = pd.to_datetime(pred.timestamp, utc=True)
        valid = ticker_bars[ticker_bars["timestamp"] >= pred_ts]
        if valid.empty:
            continue

        start_idx = int(valid.index[0])
        steps = HORIZON_STEPS.get(pred.horizon, 15)
        end_idx = min(start_idx + steps, len(ticker_bars) - 1)
        window = ticker_bars.iloc[start_idx : end_idx + 1]
        entry_price = pred.entry_price
        exit_price = float(window.iloc[-1]["close"])
        raw_return = (exit_price - entry_price) / entry_price

        if pred.prediction == "down":
            return_pct = -raw_return
        elif pred.prediction == "flat":
            return_pct = -abs(raw_return)
        else:
            return_pct = raw_return

        best_high = float(window["high"].max())
        best_low = float(window["low"].min())
        max_runup = (best_high - entry_price) / entry_price
        max_drawdown = (best_low - entry_price) / entry_price
        if pred.prediction == "down":
            max_runup = -max_drawdown
            max_drawdown = -max_runup

        direction_correct = return_pct > 0
        outcomes.append(
            PredictionOutcome(
                id=str(uuid4()),
                prediction_id=pred.id,
                exit_price=exit_price,
                return_pct=float(return_pct),
                direction_correct=direction_correct,
                max_runup=float(max_runup),
                max_drawdown=float(max_drawdown),
                evaluated_at=pd.to_datetime(window.iloc[-1]["timestamp"], utc=True).to_pydatetime(),
            )
        )

    return outcomes
