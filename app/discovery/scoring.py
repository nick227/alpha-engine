from __future__ import annotations

import math


def clamp01(x: float | None) -> float:
    if x is None or math.isnan(x):
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return float(lo)
    if x > hi:
        return float(hi)
    return float(x)


def pct_rank(values: list[float]) -> list[float]:
    """
    Percentile rank (0..1) with stable behavior for duplicates.

    Returns ranks aligned to original order.
    """
    n = len(values)
    if n <= 1:
        return [1.0 for _ in values]
    order = sorted(range(n), key=lambda i: values[i])
    ranks = [0.0] * n
    for r, idx in enumerate(order):
        ranks[idx] = r / (n - 1)
    return ranks


def bucket_price(close: float | None) -> str | None:
    if close is None:
        return None
    p = float(close)
    if p < 1:
        return "<1"
    if p < 2:
        return "1-2"
    if p < 5:
        return "2-5"
    if p < 10:
        return "5-10"
    if p < 20:
        return "10-20"
    return "20+"

