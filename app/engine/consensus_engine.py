from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from app.core.regime_manager import RegimeManager


@dataclass(frozen=True)
class TrackSignal:
    ticker: str
    direction: str
    confidence: float
    track: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ConsensusPrediction:
    ticker: str
    direction: str
    confidence: float
    sentiment_confidence: float
    quant_confidence: float
    regime: dict[str, Any]
    weighted_consensus: float
    metadata: dict[str, Any]


def _weights_from_stability(sentiment_stability: float | None, quant_stability: float | None) -> tuple[float, float]:
    s = max(0.0, float(sentiment_stability or 0.0))
    q = max(0.0, float(quant_stability or 0.0))
    total = s + q
    if total <= 0.0:
        return 0.5, 0.5
    return s / total, q / total


class ConsensusEngine:
    """
    Dual-track consensus that blends:
      - regime-based base weights (volatility + trend),
      - stability-based preference (winning track),
      - agreement bonus when both tracks align.
    """

    def __init__(self, regime_manager: RegimeManager | None = None) -> None:
        self.regime_manager = regime_manager or RegimeManager()

    def combine(
        self,
        *,
        sentiment_signal: TrackSignal,
        quant_signal: TrackSignal,
        realized_volatility: float,
        historical_volatility_window: list[float],
        adx_value: float | None = None,
        sentiment_stability: float | None = None,
        quant_stability: float | None = None,
    ) -> ConsensusPrediction:
        regime_snapshot = self.regime_manager.classify(
            realized_volatility=float(realized_volatility),
            historical_volatility_window=[float(x) for x in historical_volatility_window],
            adx_value=adx_value,
        )

        base_ws = float(regime_snapshot.sentiment_weight)
        base_wq = float(regime_snapshot.quant_weight)

        stab_ws, stab_wq = _weights_from_stability(sentiment_stability, quant_stability)

        # Blend base (regime) with stability, then renormalize.
        ws_raw = max(0.0, base_ws * float(stab_ws))
        wq_raw = max(0.0, base_wq * float(stab_wq))
        total = ws_raw + wq_raw
        if total <= 0.0:
            ws, wq = 0.5, 0.5
        else:
            ws, wq = ws_raw / total, wq_raw / total

        same_direction = str(sentiment_signal.direction) == str(quant_signal.direction)
        agreement_bonus = float(regime_snapshot.agreement_bonus) if same_direction else 0.0

        weighted = (ws * float(sentiment_signal.confidence)) + (wq * float(quant_signal.confidence))
        confidence = max(0.0, min(1.0, weighted + agreement_bonus))

        # Pick direction: aligned direction, else the heavier track signal.
        if same_direction:
            direction = str(sentiment_signal.direction)
        else:
            left = ws * float(sentiment_signal.confidence)
            right = wq * float(quant_signal.confidence)
            direction = str(sentiment_signal.direction) if left >= right else str(quant_signal.direction)

        return ConsensusPrediction(
            ticker=str(sentiment_signal.ticker),
            direction=direction,
            confidence=float(confidence),
            sentiment_confidence=float(sentiment_signal.confidence),
            quant_confidence=float(quant_signal.confidence),
            regime=asdict(regime_snapshot),
            weighted_consensus=float(weighted),
            metadata={
                "ws": round(ws, 4),
                "wq": round(wq, 4),
                "base_ws": round(base_ws, 4),
                "base_wq": round(base_wq, 4),
                "stability_ws": round(stab_ws, 4),
                "stability_wq": round(stab_wq, 4),
                "agreement_bonus": agreement_bonus,
            },
        )

