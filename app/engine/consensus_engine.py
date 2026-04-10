from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional

from app.core.regime_manager import RegimeManager, RegimeSnapshot
from app.engine.confidence_calibration import CalibrationIntegrator, ConfidenceCalibrator


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

    def __init__(self, regime_manager: RegimeManager | None = None, 
                 calibration_integrator: Optional[CalibrationIntegrator] = None,
                 weight_engine: Optional[Any] = None) -> None:
        self.regime_manager = regime_manager or RegimeManager()
        self.calibration_integrator = calibration_integrator
        self.weight_engine = weight_engine  # For adaptive strategy weighting

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
        sentiment_strategy_weight: float = 1.0,  # Adaptive weight from WeightEngine
        quant_strategy_weight: float = 1.0,      # Adaptive weight from WeightEngine
    ) -> ConsensusPrediction:
        if sentiment_signal.ticker != quant_signal.ticker:
            raise ValueError("TrackSignal tickers must match for consensus.")

        snapshot = self.regime_manager.classify(
            realized_volatility=float(realized_volatility),
            historical_volatility_window=[float(v) for v in historical_volatility_window],
            adx_value=adx_value,
        )

        ws, wq = self._stability_weights(sentiment_stability, quant_stability, snapshot)
        
        # Apply adaptive strategy weights from learning loop
        # Winning strategies get higher effective weights
        ws = ws * sentiment_strategy_weight
        wq = wq * quant_strategy_weight
        
        # Renormalize to ensure weights sum to 1.0
        total_weight = ws + wq
        if total_weight > 0:
            ws = ws / total_weight
            wq = wq / total_weight
        
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

        # Apply confidence calibration if available
        calibrated_confidence = float(weighted)
        calibration_applied = False
        
        if self.calibration_integrator:
            # Create prediction dict for calibration
            prediction_dict = {
                'ticker': str(sentiment_signal.ticker),
                'direction': direction,
                'confidence': calibrated_confidence,
                'strategy_id': 'consensus',  # Consensus doesn't have specific strategy
                'regime': snapshot.volatility_regime.value
            }
            
            calibrated_prediction = self.calibration_integrator.calibrate_prediction(prediction_dict)
            calibrated_confidence = calibrated_prediction['confidence']
            calibration_applied = calibrated_prediction.get('calibration_applied', False)
        
        return ConsensusPrediction(
            ticker=str(sentiment_signal.ticker),
            direction=direction,
            confidence=calibrated_confidence,
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
                "sentiment_strategy_weight": sentiment_strategy_weight,
                "quant_strategy_weight": quant_strategy_weight,
                "adaptive_weighting_applied": (sentiment_strategy_weight != 1.0 or quant_strategy_weight != 1.0),
                "calibration_applied": calibration_applied,
                "raw_confidence": float(weighted),
            },
        )
