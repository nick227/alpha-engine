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
    """
    Evaluate a prediction against realized future price context.

    Preserves semantic distinction:
    - actual_return: underlying asset return (exit_price - entry_price) / entry_price
    - return_pct: directional strategy return (+ if direction correct, - if incorrect, 0 if flat/missing)
    """
    horizon_key = HORIZON_TO_RETURN_KEY.get(prediction.horizon, "future_return_15m")
    raw_return = price_context.get(horizon_key)

    # Check for missing price or invalid entry price
    if raw_return is None or prediction.entry_price is None or prediction.entry_price <= 0:
        entry_price = float(prediction.entry_price or 0.0)
        return PredictionOutcome(
            id=str(uuid4()),
            prediction_id=prediction.id,
            exit_price=entry_price,
            return_pct=0.0,
            direction_correct=False,
            max_runup=0.0,
            max_drawdown=0.0,
            evaluated_at=datetime.now(timezone.utc),
            exit_reason="missing_price",
            actual_return=0.0,
        )

    realized_return = float(raw_return)
    raw_dir = getattr(prediction, "direction", None) or getattr(prediction, "prediction", "flat")
    dir_norm = str(raw_dir or "flat").strip().lower()

    if dir_norm in {"up", "buy", "long", "1"}:
        correct = realized_return > 0
        directional_return = realized_return
    elif dir_norm in {"down", "sell", "short", "-1"}:
        correct = realized_return < 0
        directional_return = -realized_return
    elif dir_norm in {"flat", "neutral", "hold", "0"}:
        correct = abs(realized_return) < 0.001
        directional_return = 0.0
    else:
        correct = False
        directional_return = 0.0

    exit_price = float(prediction.entry_price) * (1.0 + realized_return)

    default_runup = max(directional_return, 0.0)
    default_drawdown = min(directional_return, 0.0)

    return PredictionOutcome(
        id=str(uuid4()),
        prediction_id=prediction.id,
        exit_price=exit_price,
        return_pct=directional_return,
        direction_correct=correct,
        max_runup=float(price_context.get("max_runup", default_runup)),
        max_drawdown=float(price_context.get("max_drawdown", default_drawdown)),
        evaluated_at=datetime.now(timezone.utc),
        exit_reason="horizon",
        actual_return=realized_return,
    )


def summarize_outcomes(rows: list[dict]) -> list[dict]:
    by_key: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (row["strategy_name"], row["horizon"])
        by_key.setdefault(key, []).append(row)

    out: list[dict] = []
    for (strategy_name, horizon), group in sorted(by_key.items()):
        returns = [float(g["return_pct"]) for g in group]
        asset_returns = [float(g.get("actual_return", g["return_pct"])) for g in group]
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
            "avg_asset_return": mean(asset_returns) if asset_returns else 0.0,
            "sharpe_proxy": sharpe_proxy,
            "calibration_score": 1.0 - (mean(conf_error) if conf_error else 1.0),
            "avg_confidence": mean(confidences) if confidences else 0.0,
        })
    return out
