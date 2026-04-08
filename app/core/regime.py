from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Optional
import math


VolatilityRegime = Literal["LOW", "NORMAL", "HIGH"]
TrendRegime = Literal["CHOP", "TRENDING", "UNKNOWN"]


@dataclass(frozen=True)
class RegimeSnapshot:
    volatility_regime: VolatilityRegime
    trend_regime: TrendRegime
    volatility_value: float
    trend_value: Optional[float]
    sentiment_weight: float
    quant_weight: float


def safe_mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def safe_std(values: Iterable[float]) -> float:
    values = list(values)
    if len(values) < 2:
        return 0.0
    mean = safe_mean(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def classify_volatility_regime(current_vol: float, recent_vols: list[float]) -> VolatilityRegime:
    if not recent_vols:
        return "NORMAL"

    mean = safe_mean(recent_vols)
    std = safe_std(recent_vols)
    if std == 0:
        return "NORMAL"

    z = (current_vol - mean) / std

    if z >= 1.0:
        return "HIGH"
    if z <= -1.0:
        return "LOW"
    return "NORMAL"


def classify_trend_regime(adx_value: Optional[float]) -> TrendRegime:
    if adx_value is None:
        return "UNKNOWN"
    if adx_value >= 25:
        return "TRENDING"
    return "CHOP"


def base_track_weights(volatility_regime: VolatilityRegime) -> tuple[float, float]:
    # Volatility first: sentiment dominates in high-vol regimes,
    # quant dominates in quiet regimes.
    if volatility_regime == "HIGH":
        return (0.8, 0.2)
    if volatility_regime == "LOW":
        return (0.2, 0.8)
    return (0.5, 0.5)


def apply_trend_modifier(
    sentiment_weight: float,
    quant_weight: float,
    trend_regime: TrendRegime,
) -> tuple[float, float]:
    # Trend strength comes second. It gently re-centers toward quant in trend conditions.
    if trend_regime == "TRENDING":
        sentiment_weight -= 0.1
        quant_weight += 0.1
    elif trend_regime == "CHOP":
        sentiment_weight += 0.05
        quant_weight -= 0.05

    sentiment_weight = max(0.0, min(1.0, sentiment_weight))
    quant_weight = max(0.0, min(1.0, quant_weight))

    total = sentiment_weight + quant_weight
    if total == 0:
        return (0.5, 0.5)

    return (sentiment_weight / total, quant_weight / total)


def build_regime_snapshot(
    *,
    current_volatility: float,
    recent_volatilities: list[float],
    adx_value: Optional[float] = None,
) -> RegimeSnapshot:
    vol_regime = classify_volatility_regime(current_volatility, recent_volatilities)
    trend_regime = classify_trend_regime(adx_value)

    sentiment_weight, quant_weight = base_track_weights(vol_regime)
    sentiment_weight, quant_weight = apply_trend_modifier(
        sentiment_weight,
        quant_weight,
        trend_regime,
    )

    return RegimeSnapshot(
        volatility_regime=vol_regime,
        trend_regime=trend_regime,
        volatility_value=current_volatility,
        trend_value=adx_value,
        sentiment_weight=sentiment_weight,
        quant_weight=quant_weight,
    )
