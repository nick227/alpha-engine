from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence


class VolatilityRegime(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"


@dataclass(frozen=True)
class RegimeSnapshot:
    volatility_regime: VolatilityRegime
    volatility_value: float
    volatility_zscore: float
    adx_value: float | None
    trend_strength: str
    sentiment_weight: float
    quant_weight: float
    agreement_bonus: float


class RegimeManager:
    """
    Volatility-first, ADX-second regime classifier.

    v2.7 goals:
    - classify LOW / NORMAL / HIGH volatility regimes
    - shift sentiment vs quant weights dynamically
    - expose a consistent snapshot that can be stored on predictions
    """

    def __init__(
        self,
        high_vol_z: float = 1.0,
        low_vol_z: float = -1.0,
        strong_adx: float = 25.0,
        weak_adx: float = 18.0,
        agreement_bonus: float = 0.05,
    ) -> None:
        self.high_vol_z = high_vol_z
        self.low_vol_z = low_vol_z
        self.strong_adx = strong_adx
        self.weak_adx = weak_adx
        self.default_agreement_bonus = agreement_bonus

    def classify(
        self,
        realized_volatility: float,
        historical_volatility_window: Sequence[float],
        adx_value: float | None = None,
        agreement_bonus: float | None = None,
    ) -> RegimeSnapshot:
        z = self._zscore(realized_volatility, historical_volatility_window)
        vol_regime = self._volatility_regime(z)
        trend_strength = self._trend_strength(adx_value)
        sentiment_weight, quant_weight = self._weights(vol_regime, trend_strength)

        # Trend-aware agreement bonus: in strong trends, agreement matters more.
        if agreement_bonus is None:
            base_bonus = self.default_agreement_bonus
            if trend_strength == "STRONG":
                agreement_bonus = base_bonus + 0.03
            elif trend_strength == "WEAK":
                agreement_bonus = max(0.0, base_bonus - 0.02)
            else:
                agreement_bonus = base_bonus

        return RegimeSnapshot(
            volatility_regime=vol_regime,
            volatility_value=realized_volatility,
            volatility_zscore=z,
            adx_value=adx_value,
            trend_strength=trend_strength,
            sentiment_weight=sentiment_weight,
            quant_weight=quant_weight,
            agreement_bonus=agreement_bonus,
        )

    def weighted_consensus(
        self,
        sentiment_score: float,
        quant_score: float,
        snapshot: RegimeSnapshot,
        same_direction: bool,
    ) -> float:
        """
        P = Ws*Ss + Wq*Sq + bonus
        """
        score = (
            snapshot.sentiment_weight * sentiment_score
            + snapshot.quant_weight * quant_score
        )
        if same_direction:
            score += snapshot.agreement_bonus
        return max(0.0, min(1.0, score))

    def _volatility_regime(self, zscore: float) -> VolatilityRegime:
        if zscore >= self.high_vol_z:
            return VolatilityRegime.HIGH
        if zscore <= self.low_vol_z:
            return VolatilityRegime.LOW
        return VolatilityRegime.NORMAL

    def _trend_strength(self, adx_value: float | None) -> str:
        if adx_value is None:
            return "UNKNOWN"
        if adx_value >= self.strong_adx:
            return "STRONG"
        if adx_value <= self.weak_adx:
            return "WEAK"
        return "NORMAL"

    def _weights(self, vol_regime: VolatilityRegime, trend_strength: str) -> tuple[float, float]:
        """
        Volatility first, ADX second.
        """
        if vol_regime == VolatilityRegime.HIGH:
            sentiment_weight, quant_weight = 0.80, 0.20
        elif vol_regime == VolatilityRegime.LOW:
            sentiment_weight, quant_weight = 0.20, 0.80
        else:
            sentiment_weight, quant_weight = 0.50, 0.50

        # Secondary trend-strength modifier.
        if trend_strength == "STRONG":
            quant_weight += 0.10
            sentiment_weight -= 0.10
        elif trend_strength == "WEAK":
            sentiment_weight += 0.05
            quant_weight -= 0.05

        # Clamp / normalize
        sentiment_weight = max(0.0, sentiment_weight)
        quant_weight = max(0.0, quant_weight)
        total = sentiment_weight + quant_weight
        return sentiment_weight / total, quant_weight / total

    @staticmethod
    def _zscore(current_value: float, values: Sequence[float]) -> float:
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / max(1, len(values))
        std = variance ** 0.5
        if std == 0:
            return 0.0
        return (current_value - mean) / std
