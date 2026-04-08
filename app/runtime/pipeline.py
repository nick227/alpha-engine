"""
Main Pipeline for Alpha Engine
Orchestrates the complete flow from raw events to predictions
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List

from app.core.types import RawEvent, ScoredEvent, MRAOutcome, Prediction
from app.core.scoring import score_event
from app.core.mra import compute_mra
from app.runtime.consensus import ConsensusEngine, TrackSignal
from app.runtime.weighting import derive_track_weights
from app.core.regime import build_regime_snapshot
from app.db.repository import AlphaRepository


class AlphaPipeline:
    """
    Main pipeline orchestrator for the Alpha Engine.
    Handles the complete flow: events → scoring → MRA → strategies → predictions → consensus.
    """

    def __init__(self, repository: AlphaRepository | None = None) -> None:
        self.repository = repository or AlphaRepository()
        self.consensus_engine = ConsensusEngine()

    def run_pipeline(
        self,
        raw_events: List[RawEvent],
        price_contexts: Dict[str, Dict[str, Any]],
        persist: bool = True
    ) -> Dict[str, Any]:
        """
        Run the complete Alpha Engine pipeline.
        
        Args:
            raw_events: List of raw news/market events
            price_contexts: Price context data for each event
            persist: Whether to save results to database
            
        Returns:
            Dictionary with summary and prediction results
        """
        # Step 1: Score events
        scored_events = []
        for event in raw_events:
            scored = score_event(event)
            scored_events.append(scored)
            
            if persist:
                # Save scored event
                event_data = asdict(scored)
                event_data["raw_event_id"] = event.id
                # TODO: Use repository save method when available
                # self.repository.save_scored_event(event_data)

        # Step 2: MRA analysis
        mra_outcomes = []
        for scored_event in scored_events:
            event_id = scored_event.id
            price_context = price_contexts.get(event_id, {})
            
            mra = compute_mra(scored_event, price_context)
            mra_outcomes.append(mra)
            
            if persist:
                # Save MRA outcome
                mra_data = asdict(mra)
                mra_data["scored_event_id"] = event_id
                # TODO: Use repository save method when available
                # self.repository.save_mra_outcome(mra_data)

        # Step 3: Generate predictions using strategies
        predictions = []
        for scored_event, mra in zip(scored_events, mra_outcomes):
            # For now, create simple predictions
            # TODO: Integrate actual strategy runners
            prediction = self._create_simple_prediction(scored_event, mra, price_contexts.get(scored_event.id, {}))
            predictions.append(prediction)
            
            if persist:
                # Save prediction
                pred_data = asdict(prediction)
                # TODO: Use repository save method when available
                # self.repository.save_prediction(pred_data)

        # Step 4: Generate consensus signals
        consensus_signals = []
        for prediction in predictions:
            # Create mock track signals for consensus
            sentiment_signal = TrackSignal(
                ticker=prediction.ticker,
                direction=prediction.prediction,
                confidence=prediction.confidence,
                track="sentiment",
                metadata={"source": "text_mra"}
            )
            
            quant_signal = TrackSignal(
                ticker=prediction.ticker,
                direction=prediction.prediction,
                confidence=prediction.confidence * 0.8,  # Slightly lower confidence
                track="quant",
                metadata={"source": "technical"}
            )
            
            # Get price context for regime detection
            price_context = price_contexts.get(prediction.scored_event_id, {})
            realized_vol = price_context.get("realized_volatility", 0.02)
            historical_vols = price_context.get("historical_volatility", [0.015, 0.018, 0.022, 0.016, 0.019])
            adx_value = price_context.get("adx_value")
            
            consensus = self.consensus_engine.combine(
                sentiment_signal=sentiment_signal,
                quant_signal=quant_signal,
                realized_volatility=realized_vol,
                historical_volatility_window=historical_vols,
                adx_value=adx_value
            )
            
            consensus_signals.append(consensus)
            
            if persist:
                # Save consensus signal
                consensus_data = {
                    "ticker": prediction.ticker,
                    "regime": consensus.regime.get("volatility_regime", "NORMAL"),
                    "sentiment_strategy_id": "sentiment_mock",
                    "quant_strategy_id": "quant_mock",
                    "sentiment_score": sentiment_signal.confidence,
                    "quant_score": quant_signal.confidence,
                    "ws": consensus.metadata.get("ws", 0.5),
                    "wq": consensus.metadata.get("wq", 0.5),
                    "agreement_bonus": consensus.metadata.get("agreement_bonus", 0.0),
                    "p_final": consensus.weighted_consensus,
                    "stability_score": 0.75  # Mock stability
                }
                # TODO: Use repository save method when available
                # self.repository.save_consensus_signal(consensus_data)

        # Step 5: Generate summary
        summary = self._generate_summary(predictions, consensus_signals)

        return {
            "summary": summary,
            "scored_events": [asdict(event) for event in scored_events],
            "mra_outcomes": [asdict(mra) for mra in mra_outcomes],
            "predictions": [asdict(pred) for pred in predictions],
            "consensus_signals": [asdict(consensus) for consensus in consensus_signals]
        }

    def _create_simple_prediction(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: Dict[str, Any]
    ) -> Prediction:
        """Create a simple prediction from scored event and MRA"""
        from uuid import uuid4
        
        # Simple prediction logic based on MRA score and event confidence
        combined_score = (scored_event.confidence + mra.mra_score) / 2
        
        if combined_score > 0.6:
            direction = "up"
        elif combined_score < 0.4:
            direction = "down"
        else:
            direction = "neutral"
            
        return Prediction(
            id=str(uuid4()),
            strategy_id="simple_mra_strategy",
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=datetime.utcnow(),
            prediction=direction,
            confidence=combined_score,
            horizon="15m",
            entry_price=price_context.get("entry_price", 100.0),
            mode="demo"
        )

    def _generate_summary(
        self,
        predictions: List[Prediction],
        consensus_signals: List[Any]
    ) -> Dict[str, Any]:
        """Generate pipeline execution summary"""
        
        total_predictions = len(predictions)
        total_consensus = len(consensus_signals)
        
        # Count predictions by direction
        up_count = sum(1 for p in predictions if p.prediction == "up")
        down_count = sum(1 for p in predictions if p.prediction == "down")
        neutral_count = sum(1 for p in predictions if p.prediction == "neutral")
        
        # Average confidence
        avg_confidence = sum(p.confidence for p in predictions) / total_predictions if predictions else 0
        
        # Consensus averages
        if consensus_signals:
            avg_consensus_confidence = sum(c.confidence for c in consensus_signals) / total_consensus
            avg_sentiment_weight = sum(c.metadata.get("ws", 0.5) for c in consensus_signals) / total_consensus
            avg_quant_weight = sum(c.metadata.get("wq", 0.5) for c in consensus_signals) / total_consensus
        else:
            avg_consensus_confidence = 0
            avg_sentiment_weight = 0
            avg_quant_weight = 0
        
        return {
            "total_predictions": total_predictions,
            "total_consensus_signals": total_consensus,
            "prediction_directions": {
                "up": up_count,
                "down": down_count,
                "neutral": neutral_count
            },
            "average_confidence": round(avg_confidence, 4),
            "consensus_metrics": {
                "average_confidence": round(avg_consensus_confidence, 4),
                "average_sentiment_weight": round(avg_sentiment_weight, 4),
                "average_quant_weight": round(avg_quant_weight, 4)
            },
            "pipeline_timestamp": datetime.utcnow().isoformat()
        }


def run_pipeline(
    raw_events: List[RawEvent],
    price_contexts: Dict[str, Dict[str, Any]],
    persist: bool = True
) -> Dict[str, Any]:
    """
    Main pipeline entry point - replaces missing run_pipeline function
    
    This function provides the missing functionality that demo_run.py expects.
    """
    pipeline = AlphaPipeline()
    return pipeline.run_pipeline(raw_events, price_contexts, persist)
