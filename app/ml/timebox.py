"""
Walk-forward timeboxing for ML training.

Generates non-overlapping (train, predict) window pairs that slide forward
through the available date range. Prevents any future data from leaking
into training by keeping the windows strictly sequential.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterator


@dataclass(frozen=True)
class TimeWindow:
    train_start: date
    train_end: date       # inclusive
    predict_start: date
    predict_end: date     # inclusive

    def __repr__(self) -> str:
        return (
            f"TimeWindow(train={self.train_start}–{self.train_end}, "
            f"predict={self.predict_start}–{self.predict_end})"
        )


def generate_windows(
    data_start: date,
    data_end: date,
    train_days: int = 180,
    predict_days: int = 30,
    step_days: int = 30,
) -> Iterator[TimeWindow]:
    """
    Yield walk-forward (train, predict) window pairs.

    Each predict window immediately follows its training window with no gap
    and no overlap. The first training window starts at data_start.

    Args:
        data_start:   earliest date in the dataset
        data_end:     latest date available (inclusive)
        train_days:   calendar days per training window
        predict_days: calendar days per predict/validation window
        step_days:    how far to slide forward each iteration (controls overlap
                      between successive training windows; 0 < step <= train_days)

    Example (train=90d, predict=30d, step=30d):
        Window 1: train Jan1–Mar31, predict Apr1–Apr30
        Window 2: train Jan31–Apr30, predict May1–May30
        ...
    """
    if step_days <= 0:
        raise ValueError("step_days must be positive")

    start = data_start
    while True:
        train_end = start + timedelta(days=train_days - 1)
        predict_start = train_end + timedelta(days=1)
        predict_end = predict_start + timedelta(days=predict_days - 1)

        if predict_end > data_end:
            break

        yield TimeWindow(
            train_start=start,
            train_end=train_end,
            predict_start=predict_start,
            predict_end=predict_end,
        )

        start += timedelta(days=step_days)
