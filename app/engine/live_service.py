from __future__ import annotations

from statistics import mean
from typing import Any, Dict, Iterable, List


def run_live_signal_summary(predictions: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(predictions)
    active = [r for r in rows if not r.get("resolved", False)]
    return {
        "mode": "live",
        "active_prediction_count": len(active),
        "avg_confidence": float(mean(float(r.get("confidence", 0.0)) for r in active)) if active else 0.0,
        "tickers": sorted({str(r.get("ticker", "")) for r in active}),
    }
