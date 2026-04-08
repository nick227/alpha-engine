from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Iterable


def summarize_strategy_performance(outcomes: Iterable[dict]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in outcomes:
        grouped[row["strategy_id"]].append(row)

    summary: dict[str, dict] = {}
    for strategy_id, rows in grouped.items():
        returns = [float(r.get("return_pct", 0.0)) for r in rows]
        correctness = [1.0 if r.get("direction_correct") else 0.0 for r in rows]
        residuals = [float(r.get("residual_alpha", 0.0)) for r in rows]

        summary[strategy_id] = {
            "strategy_id": strategy_id,
            "prediction_count": len(rows),
            "accuracy": round(mean(correctness), 4) if correctness else 0.0,
            "avg_return": round(mean(returns), 6) if returns else 0.0,
            "avg_residual_alpha": round(mean(residuals), 6) if residuals else 0.0,
        }
    return summary


def summarize_regime_performance(outcomes: Iterable[dict]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in outcomes:
        regime = row.get("regime") or "UNKNOWN"
        grouped[regime].append(row)

    summary: dict[str, dict] = {}
    for regime, rows in grouped.items():
        returns = [float(r.get("return_pct", 0.0)) for r in rows]
        correctness = [1.0 if r.get("direction_correct") else 0.0 for r in rows]
        summary[regime] = {
            "regime": regime,
            "prediction_count": len(rows),
            "accuracy": round(mean(correctness), 4) if correctness else 0.0,
            "avg_return": round(mean(returns), 6) if returns else 0.0,
        }
    return summary


def compute_stability(backtest_accuracy: float, live_accuracy: float) -> float:
    if backtest_accuracy <= 0:
        return 0.0
    ratio = live_accuracy / backtest_accuracy
    return round(max(0.0, min(1.5, ratio)), 4)
