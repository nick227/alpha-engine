from __future__ import annotations

from typing import Any, Dict


def suggest_weight_shift(backtest_metrics: Dict[str, Any], live_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Very small placeholder for future optimization logic.
    """
    avg_live_conf = float(live_metrics.get("avg_confidence", 0.0))
    slice_report = backtest_metrics.get("slice_report", {})
    comparisons = slice_report.get("comparisons", [])

    if not comparisons:
        return {
            "recommendation": "hold",
            "reason": "insufficient comparison data",
        }

    avg_stability = sum(float(c.get("stability_score", 0.0)) for c in comparisons) / len(comparisons)

    if avg_stability < 0.6:
        return {
            "recommendation": "reduce_complexity",
            "reason": "drift between backtest slices is high",
            "avg_stability": avg_stability,
            "avg_live_confidence": avg_live_conf,
        }

    return {
        "recommendation": "keep_and_iterate",
        "reason": "slice stability acceptable; continue parallel testing",
        "avg_stability": avg_stability,
        "avg_live_confidence": avg_live_conf,
    }
