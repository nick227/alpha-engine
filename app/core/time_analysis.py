from __future__ import annotations

from dataclasses import dataclass, asdict
from statistics import mean
from typing import Any, Dict, Iterable, List, Sequence


@dataclass
class SliceWindow:
    label: str
    start: str
    end: str


@dataclass
class SliceMetrics:
    slice_label: str
    prediction_count: int
    accuracy: float
    avg_return: float
    avg_confidence: float
    calibration_gap: float
    hit_rate: float
    sample_size_ok: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SliceComparison:
    train_label: str
    forward_label: str
    train_accuracy: float
    forward_accuracy: float
    accuracy_drift: float
    train_avg_return: float
    forward_avg_return: float
    return_drift: float
    stability_score: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _safe_mean(values: Sequence[float]) -> float:
    return float(mean(values)) if values else 0.0


def _prediction_correct(prediction: Dict[str, Any]) -> bool:
    if "direction_correct" in prediction:
        return bool(prediction["direction_correct"])
    pred = str(prediction.get("prediction", "")).lower()
    realized = float(prediction.get("realized_return", 0.0))
    if pred == "up":
        return realized > 0
    if pred == "down":
        return realized < 0
    return abs(realized) < 1e-9


def compute_slice_metrics(
    predictions: Iterable[Dict[str, Any]],
    slice_label: str,
    min_sample_size: int = 5,
) -> SliceMetrics:
    rows = list(predictions)
    count = len(rows)
    if count == 0:
        return SliceMetrics(
            slice_label=slice_label,
            prediction_count=0,
            accuracy=0.0,
            avg_return=0.0,
            avg_confidence=0.0,
            calibration_gap=1.0,
            hit_rate=0.0,
            sample_size_ok=False,
        )

    confidences = [float(r.get("confidence", 0.0)) for r in rows]
    returns = [float(r.get("realized_return", 0.0)) for r in rows]
    correctness = [1.0 if _prediction_correct(r) else 0.0 for r in rows]

    accuracy = _safe_mean(correctness)
    avg_confidence = _safe_mean(confidences)
    avg_return = _safe_mean(returns)
    calibration_gap = abs(avg_confidence - accuracy)

    return SliceMetrics(
        slice_label=slice_label,
        prediction_count=count,
        accuracy=accuracy,
        avg_return=avg_return,
        avg_confidence=avg_confidence,
        calibration_gap=calibration_gap,
        hit_rate=accuracy,
        sample_size_ok=count >= min_sample_size,
    )


def compare_slices(train_metrics: SliceMetrics, forward_metrics: SliceMetrics) -> SliceComparison:
    accuracy_drift = forward_metrics.accuracy - train_metrics.accuracy
    return_drift = forward_metrics.avg_return - train_metrics.avg_return
    stability_score = max(
        0.0,
        1.0 - ((abs(accuracy_drift) + abs(return_drift)) / 2.0),
    )

    return SliceComparison(
        train_label=train_metrics.slice_label,
        forward_label=forward_metrics.slice_label,
        train_accuracy=train_metrics.accuracy,
        forward_accuracy=forward_metrics.accuracy,
        accuracy_drift=accuracy_drift,
        train_avg_return=train_metrics.avg_return,
        forward_avg_return=forward_metrics.avg_return,
        return_drift=return_drift,
        stability_score=stability_score,
    )


def rolling_window_pairs(
    windows: Sequence[SliceWindow],
) -> List[tuple[SliceWindow, SliceWindow]]:
    pairs: List[tuple[SliceWindow, SliceWindow]] = []
    for idx in range(len(windows) - 1):
        pairs.append((windows[idx], windows[idx + 1]))
    return pairs


def filter_predictions_by_window(
    predictions: Iterable[Dict[str, Any]],
    start_ts: str,
    end_ts: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in predictions:
        ts = str(row.get("timestamp", ""))
        if start_ts <= ts < end_ts:
            rows.append(row)
    return rows


def build_rolling_slice_report(
    predictions: Iterable[Dict[str, Any]],
    windows: Sequence[SliceWindow],
    min_sample_size: int = 5,
) -> Dict[str, Any]:
    rows = list(predictions)
    slice_metrics: List[SliceMetrics] = []
    comparisons: List[SliceComparison] = []

    for window in windows:
        sliced = filter_predictions_by_window(rows, window.start, window.end)
        slice_metrics.append(
            compute_slice_metrics(
                predictions=sliced,
                slice_label=window.label,
                min_sample_size=min_sample_size,
            )
        )

    for current_metrics, next_metrics in zip(slice_metrics, slice_metrics[1:]):
        comparisons.append(compare_slices(current_metrics, next_metrics))

    return {
        "slices": [m.to_dict() for m in slice_metrics],
        "comparisons": [c.to_dict() for c in comparisons],
    }
