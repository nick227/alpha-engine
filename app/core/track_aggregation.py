from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any, Dict, Iterable, List


def group_predictions_by_track(predictions: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in predictions:
        track = str(row.get("track", "unknown")).lower()
        grouped[track].append(row)
    return dict(grouped)


def summarize_track(predictions: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(predictions)
    if not rows:
        return {
            "prediction_count": 0,
            "avg_confidence": 0.0,
            "avg_return": 0.0,
            "accuracy": 0.0,
        }

    avg_conf = mean(float(r.get("confidence", 0.0)) for r in rows)
    avg_ret = mean(float(r.get("realized_return", 0.0)) for r in rows)
    accuracy = mean(
        1.0 if bool(r.get("direction_correct", False)) else 0.0 for r in rows
    )
    return {
        "prediction_count": len(rows),
        "avg_confidence": float(avg_conf),
        "avg_return": float(avg_ret),
        "accuracy": float(accuracy),
    }


def build_track_overlay(predictions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = group_predictions_by_track(predictions)
    results: List[Dict[str, Any]] = []
    for track, rows in grouped.items():
        summary = summarize_track(rows)
        summary["track"] = track
        results.append(summary)
    return sorted(results, key=lambda row: row["accuracy"], reverse=True)


def aggregate_signal_overlay(predictions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Creates a per-timestamp/ticker overlay of multiple tracks.
    Useful for showing consensus between sentiment and quantitative systems.
    """
    buckets: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in predictions:
        key = (str(row.get("timestamp", "")), str(row.get("ticker", "")))
        buckets[key].append(row)

    overlays: List[Dict[str, Any]] = []
    for (timestamp, ticker), rows in buckets.items():
        directions = [str(r.get("prediction", "flat")).lower() for r in rows]
        confidence = [float(r.get("confidence", 0.0)) for r in rows]
        unique_tracks = sorted({str(r.get("track", "unknown")).lower() for r in rows})
        consensus = len(set(directions)) == 1
        overlays.append(
            {
                "timestamp": timestamp,
                "ticker": ticker,
                "track_count": len(unique_tracks),
                "tracks": unique_tracks,
                "consensus": consensus,
                "consensus_direction": directions[0] if consensus and directions else "mixed",
                "avg_confidence": float(mean(confidence)) if confidence else 0.0,
            }
        )
    return sorted(overlays, key=lambda row: (row["consensus"], row["avg_confidence"]), reverse=True)
