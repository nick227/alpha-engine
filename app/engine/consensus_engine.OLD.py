from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from app.core.regime_manager import RegimeManager, RegimeSnapshot
from app.engine.weight_engine import derive_track_weights_from_stability


@dataclass
class TrackSignal:
    ticker: str
    direction: str
    confidence: float
    track: str
    metadata: dict[str, Any]


@dataclass
class ConsensusPrediction:
    ticker: str
    direction: str
    confidence: float
    sentiment_confidence: float
    quant_confidence: float
    regime: dict[str, Any]
    weighted_consensus: float
    metadata: dict[str, Any]


class ConsensusEngine:
    """
    Combines sentiment and quant track signals into one adaptive dual-track prediction.
    """

    def __init__(self, regime_manager: RegimeManager | None = None) -> None:
        self.regime_manager = regime_manager or RegimeManager()

    def combine(
        self,
        sentiment_signal: TrackSignal,
        quant_signal: TrackSignal,
        realized_volatility: float,
        historical_volatility_window: list[float],
        adx_value: float | None = None,
        sentiment_stability: float | None = None,
        quant_stability: float | None = None,
    ) -> ConsensusPrediction:
        snapshot = self.regime_manager.classify(
            realized_volatility=realized_volatility,
            historical_volatility_window=historical_volatility_window,
            adx_value=adx_value,
        )

        same_direction = sentiment_signal.direction == quant_signal.direction

        # Regime-aware base weights (HIGH VOL -> sentiment heavy, LOW VOL -> quant heavy).
        base_ws, base_wq = snapshot.sentiment_weight, snapshot.quant_weight

        # Stability-aware weights (winning track preference). If provided, blend with base weights.
        stab_ws, stab_wq = 0.5, 0.5
        if sentiment_stability is not None or quant_stability is not None:
            stab = derive_track_weights_from_stability(sentiment_stability, quant_stability)
            stab_ws, stab_wq = stab["ws"], stab["wq"]

        # Blend: base (regime) * stability, then renormalize.
        ws_raw = max(0.0, float(base_ws) * float(stab_ws))
        wq_raw = max(0.0, float(base_wq) * float(stab_wq))
        total = ws_raw + wq_raw
        if total <= 0:
            ws, wq = 0.5, 0.5
        else:
            ws, wq = ws_raw / total, wq_raw / total

        bonus = snapshot.agreement_bonus if same_direction else 0.0
        weighted_confidence = max(
            0.0,
            min(1.0, (ws * sentiment_signal.confidence) + (wq * quant_signal.confidence) + bonus),
        )

        direction = (
            sentiment_signal.direction
            if same_direction or (ws * sentiment_signal.confidence) >= (wq * quant_signal.confidence)
            else quant_signal.direction
        )

        return ConsensusPrediction(
            ticker=sentiment_signal.ticker,
            direction=direction,
            confidence=weighted_confidence,
            sentiment_confidence=sentiment_signal.confidence,
            quant_confidence=quant_signal.confidence,
            regime=asdict(snapshot),
            weighted_consensus=weighted_confidence,
            metadata={
                "same_direction": same_direction,
                "ws": round(ws, 4),
                "wq": round(wq, 4),
                "base_ws": round(base_ws, 4),
                "base_wq": round(base_wq, 4),
                "stability_ws": round(stab_ws, 4),
                "stability_wq": round(stab_wq, 4),
                "sentiment_stability": sentiment_stability,
                "quant_stability": quant_stability,
                "sentiment_track": sentiment_signal.metadata,
                "quant_track": quant_signal.metadata,
            },
        )
