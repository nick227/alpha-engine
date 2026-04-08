from __future__ import annotations

from dataclasses import dataclass
from math import exp, sqrt
from typing import Iterable

import numpy as np


@dataclass(frozen=True)
class EfficiencyWeights:
    sync: float = 0.35
    direction: float = 0.25
    horizon: float = 0.20
    magnitude: float = 0.10
    total_return: float = 0.10


@dataclass(frozen=True)
class EfficiencyScales:
    daily_return_scale: float = 0.02


@dataclass(frozen=True)
class EfficiencyConfig:
    weights: EfficiencyWeights = EfficiencyWeights()
    scales: EfficiencyScales = EfficiencyScales()


@dataclass(frozen=True)
class SyncScore:
    forecast_days: int
    direction_hit_rate: float
    sync_rate: float
    total_return_actual: float
    total_return_pred: float
    total_return_error: float
    magnitude_error: float
    horizon_weight: float
    efficiency_rating: float


def _to_np(values: Iterable[float]) -> np.ndarray:
    arr = np.asarray(list(values), dtype=float)
    if arr.ndim != 1:
        raise ValueError("series must be 1D")
    return arr


def normalize_to_start(values: Iterable[float]) -> np.ndarray:
    """
    Normalize a price/value series to a start value of 1.0.

    If the first value is 0, this returns the raw series (to avoid division by zero).
    """
    arr = _to_np(values)
    if arr.size == 0:
        return arr
    start = float(arr[0])
    if start == 0.0:
        return arr
    return arr / start


def returns_from_levels(levels: Iterable[float]) -> np.ndarray:
    arr = _to_np(levels)
    if arr.size <= 1:
        return np.asarray([], dtype=float)
    prev = arr[:-1]
    nxt = arr[1:]
    with np.errstate(divide="ignore", invalid="ignore"):
        ret = np.where(prev != 0.0, (nxt / prev) - 1.0, 0.0)
    return ret.astype(float)


def direction_match_rate(pred_returns: Iterable[float], actual_returns: Iterable[float], *, eps: float = 0.0) -> float:
    pr = _to_np(pred_returns)
    ar = _to_np(actual_returns)
    n = min(pr.size, ar.size)
    if n == 0:
        return 0.0
    pr = pr[:n]
    ar = ar[:n]
    ps = np.sign(np.where(np.abs(pr) <= eps, 0.0, pr))
    a_s = np.sign(np.where(np.abs(ar) <= eps, 0.0, ar))
    return float(np.mean(ps == a_s))


def shape_sync_rate(pred_returns: Iterable[float], actual_returns: Iterable[float]) -> float:
    """
    Shape alignment in [0,1] based on Pearson correlation of returns.

    Falls back to 0.5 when correlation is undefined.
    """
    pr = _to_np(pred_returns)
    ar = _to_np(actual_returns)
    n = min(pr.size, ar.size)
    if n < 2:
        return 0.5
    pr = pr[:n]
    ar = ar[:n]
    if float(np.std(pr)) == 0.0 or float(np.std(ar)) == 0.0:
        return 0.5
    corr = float(np.corrcoef(pr, ar)[0, 1])
    if np.isnan(corr):
        return 0.5
    corr = max(-1.0, min(1.0, corr))
    return float((corr + 1.0) / 2.0)


def horizon_weight(forecast_days: int) -> float:
    """
    Saturating reward for longer horizons.

    1 day ~ 0.095, 10 days ~ 0.63, 30 days ~ 0.95
    """
    d = max(0, int(forecast_days))
    return float(1.0 - exp(-d / 10.0))


def score_sync(
    predicted_values: Iterable[float],
    actual_values: Iterable[float],
    *,
    config: EfficiencyConfig | None = None,
) -> SyncScore:
    cfg = config or EfficiencyConfig()

    pred_norm = normalize_to_start(predicted_values)
    act_norm = normalize_to_start(actual_values)

    pred_ret = returns_from_levels(pred_norm)
    act_ret = returns_from_levels(act_norm)
    n = min(pred_ret.size, act_ret.size)
    forecast_days = int(n)

    if n == 0:
        return SyncScore(
            forecast_days=0,
            direction_hit_rate=0.0,
            sync_rate=0.5,
            total_return_actual=0.0,
            total_return_pred=0.0,
            total_return_error=0.0,
            magnitude_error=0.0,
            horizon_weight=0.0,
            efficiency_rating=0.0,
        )

    pred_ret = pred_ret[:n]
    act_ret = act_ret[:n]

    direction_hit = direction_match_rate(pred_ret, act_ret)
    sync = shape_sync_rate(pred_ret, act_ret)

    total_actual = float(act_norm[min(act_norm.size - 1, n)] - 1.0)
    total_pred = float(pred_norm[min(pred_norm.size - 1, n)] - 1.0)
    total_err = float(abs(total_pred - total_actual))
    mag_err = float(np.mean(np.abs(pred_ret - act_ret)))

    h_weight = horizon_weight(forecast_days)

    daily_scale = float(cfg.scales.daily_return_scale)
    total_scale = daily_scale * sqrt(max(1, forecast_days))
    magnitude_score = 1.0 - (mag_err / daily_scale) if daily_scale > 0 else 0.0
    total_return_score = 1.0 - (total_err / total_scale) if total_scale > 0 else 0.0

    w = cfg.weights
    eff = (
        w.sync * sync
        + w.direction * direction_hit
        + w.horizon * h_weight
        + w.magnitude * magnitude_score
        + w.total_return * total_return_score
    )

    return SyncScore(
        forecast_days=forecast_days,
        direction_hit_rate=float(direction_hit),
        sync_rate=float(sync),
        total_return_actual=float(total_actual),
        total_return_pred=float(total_pred),
        total_return_error=float(total_err),
        magnitude_error=float(mag_err),
        horizon_weight=float(h_weight),
        efficiency_rating=float(eff),
    )

