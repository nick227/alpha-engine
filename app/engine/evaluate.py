from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean, pstdev
from uuid import uuid4

from app.core.types import Prediction, PredictionOutcome

HORIZON_TO_RETURN_KEY = {
    "5m": "future_return_5m",
    "15m": "future_return_15m",
    "1h": "future_return_1h",
    "4h": "future_return_4h",
    "1d": "future_return_1d",
    "7d": "future_return_7d",
    "30d": "future_return_30d",
}


def evaluate_prediction(prediction: Prediction, price_context: dict) -> PredictionOutcome:
    realized_return = float(price_context.get(HORIZON_TO_RETURN_KEY.get(prediction.horizon, "future_return_15m"), 0.0))

    if prediction.prediction == "up":
        correct = realized_return > 0
    elif prediction.prediction == "down":
        correct = realized_return < 0
    else:
        correct = abs(realized_return) < 0.001

    exit_price = prediction.entry_price * (1.0 + realized_return)

    return PredictionOutcome(
        id=str(uuid4()),
        prediction_id=prediction.id,
        exit_price=exit_price,
        return_pct=realized_return,
        direction_correct=correct,
        max_runup=float(price_context.get("max_runup", max(realized_return, 0.0))),
        max_drawdown=float(price_context.get("max_drawdown", min(realized_return, 0.0))),
        evaluated_at=datetime.now(timezone.utc),
    )


def summarize_outcomes(rows: list[dict]) -> list[dict]:
    by_key: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (row["strategy_name"], row["horizon"])
        by_key.setdefault(key, []).append(row)

    out: list[dict] = []
    for (strategy_name, horizon), group in sorted(by_key.items()):
        returns = [float(g["return_pct"]) for g in group]
        accuracy_flags = [1.0 if g["direction_correct"] else 0.0 for g in group]
        confidences = [float(g["confidence"]) for g in group]
        weighted_hits = [c if correct else 0.0 for c, correct in zip(confidences, accuracy_flags)]
        conf_error = [abs(c - a) for c, a in zip(confidences, accuracy_flags)]
        std = pstdev(returns) if len(returns) > 1 else 0.0
        sharpe_proxy = (mean(returns) / std) if std else 0.0
        sample = group[0]

        out.append({
            "strategy": strategy_name,
            "strategy_type": sample.get("strategy_type", "unknown"),
            "mode": sample.get("mode", "backtest"),
            "horizon": horizon,
            "total_predictions": len(group),
            "accuracy": mean(accuracy_flags) if accuracy_flags else 0.0,
            "weighted_accuracy": mean(weighted_hits) if weighted_hits else 0.0,
            "avg_return": mean(returns) if returns else 0.0,
            "sharpe_proxy": sharpe_proxy,
            "calibration_score": 1.0 - (mean(conf_error) if conf_error else 1.0),
            "avg_confidence": mean(confidences) if confidences else 0.0,
        })
    return out
