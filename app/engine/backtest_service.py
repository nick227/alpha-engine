from __future__ import annotations

from typing import Any, Dict, Iterable, Sequence

from app.core.time_analysis import SliceWindow, build_rolling_slice_report
from app.core.track_aggregation import build_track_overlay


def run_backtest_time_analysis(
    predictions: Iterable[Dict[str, Any]],
    windows: Sequence[SliceWindow],
) -> Dict[str, Any]:
    rows = list(predictions)
    return {
        "mode": "backtest",
        "slice_report": build_rolling_slice_report(rows, windows),
        "track_overlay": build_track_overlay(rows),
    }
