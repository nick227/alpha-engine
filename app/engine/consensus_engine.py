from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from app.core.regime_manager import RegimeManager, RegimeSnapshot


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


class ConsensusEngine:
    """
    Canonical v2.7+ dual-track consensus combiner used by `app/engine/runner.py`.

    Contract:
    - takes one sentiment TrackSignal and one quant TrackSignal for the same ticker
    - uses RegimeManager to compute base weights + agreement bonus
    - if `sentiment_stability`/`quant_stability` are supplied, they override base weights
      (this is what the tests expect, and keeps the behavior simple + explainable)
    """

    def __init__(self, regime_manager: RegimeManager | None = None) -> None:
        self.regime_manager = regime_manager or RegimeManager()

    @staticmethod
    def _stability_weights(
        sentiment_stability: float | None,
        quant_stability: float | None,
        snapshot: RegimeSnapshot,
    ) -> tuple[float, float]:
        if sentiment_stability is None and quant_stability is None:
            return float(snapshot.sentiment_weight), float(snapshot.quant_weight)

        ss = float(sentiment_stability or 0.0)
        qs = float(quant_stability or 0.0)
        total = ss + qs
        if total <= 0:
            return float(snapshot.sentiment_weight), float(snapshot.quant_weight)
        return ss / total, qs / total

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
        if sentiment_signal.ticker != quant_signal.ticker:
            raise ValueError("TrackSignal tickers must match for consensus.")

        snapshot = self.regime_manager.classify(
            realized_volatility=float(realized_volatility),
            historical_volatility_window=[float(v) for v in historical_volatility_window],
            adx_value=adx_value,
        )

        ws, wq = self._stability_weights(sentiment_stability, quant_stability, snapshot)
        same_direction = str(sentiment_signal.direction) == str(quant_signal.direction)

        # Choose a final direction even on disagreement.
        direction = (
            str(sentiment_signal.direction)
            if same_direction or (ws * float(sentiment_signal.confidence)) >= (wq * float(quant_signal.confidence))
            else str(quant_signal.direction)
        )

        # P = Ws*Ss + Wq*Sq (+ bonus if same direction)
        weighted = self.regime_manager.weighted_consensus(
            sentiment_score=float(sentiment_signal.confidence),
            quant_score=float(quant_signal.confidence),
            snapshot=RegimeSnapshot(
                volatility_regime=snapshot.volatility_regime,
                volatility_value=snapshot.volatility_value,
                volatility_zscore=snapshot.volatility_zscore,
                adx_value=snapshot.adx_value,
                trend_strength=snapshot.trend_strength,
                sentiment_weight=ws,
                quant_weight=wq,
                agreement_bonus=snapshot.agreement_bonus,
            ),
            same_direction=same_direction,
        )

        regime_payload = asdict(snapshot)
        # Make the persisted value stable (Enum -> value).
        if "volatility_regime" in regime_payload and getattr(snapshot.volatility_regime, "value", None) is not None:
            regime_payload["volatility_regime"] = snapshot.volatility_regime.value

        return ConsensusPrediction(
            ticker=str(sentiment_signal.ticker),
            direction=direction,
            confidence=float(weighted),
            sentiment_confidence=float(sentiment_signal.confidence),
            quant_confidence=float(quant_signal.confidence),
            regime=regime_payload,
            weighted_consensus=float(weighted),
            metadata={
                "same_direction": same_direction,
                "ws": ws,
                "wq": wq,
                "agreement_bonus": snapshot.agreement_bonus,
                "sentiment_track": dict(sentiment_signal.metadata or {}),
                "quant_track": dict(quant_signal.metadata or {}),
                "sentiment_stability": sentiment_stability,
                "quant_stability": quant_stability,
            },
        )
