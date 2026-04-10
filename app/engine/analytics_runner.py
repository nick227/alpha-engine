"""
Analytics Runner

Orchestrates the complete analytics pipeline:
prediction_outcomes → strategy_performance → strategy_stability → strategy_weights → consensus_signals → promotion_events

This bridges the gap between data replay and decision-capable Alpha Engine.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any

from app.core.repository import Repository
from app.engine.continuous_learning import ContinuousLearner, Signal, SignalOutcome, StrategyPerformance
from app.engine.weight_engine import WeightEngine
from app.engine.consensus_engine import ConsensusEngine, TrackSignal, ConsensusPrediction
from app.core.regime_manager import RegimeManager
from app.engine.confidence_calibration import ConfidenceCalibrator, CalibrationIntegrator
from app.engine.trust_engine import TrustEngine

logger = logging.getLogger(__name__)


class AnalyticsRunner:
    """
    Runs the full analytics pipeline on prediction outcomes.
    
    Flow:
    1. Load prediction outcomes and pair with signals
    2. Compute strategy performance via ContinuousLearner
    3. Compute adaptive weights via WeightEngine  
    4. Generate real consensus signals via ConsensusEngine
    5. Select champions and create promotion events
    """
    
    def __init__(self, repo: Repository):
        self.repo = repo
        self.learner = ContinuousLearner()
        self.weight_engine = WeightEngine()
        self.trust_engine = TrustEngine()
        
        # Initialize calibration system
        self.calibrator = ConfidenceCalibrator()
        self.calibration_integrator = CalibrationIntegrator(self.calibrator)
        self.consensus_engine = ConsensusEngine(calibration_integrator=self.calibration_integrator)
        self.regime_manager = RegimeManager()
    
    def run(self, tenant_id: str = "backfill") -> Dict[str, int]:
        """
        Execute complete analytics pipeline.
        
        Returns:
            Dict with counts of processed records
        """
        logger.info(f"Starting analytics pipeline for tenant: {tenant_id}")
        
        # 1. Load prediction outcomes and signals
        outcomes = self.repo.get_prediction_outcomes(tenant_id=tenant_id)
        signals = self.repo.get_signals(tenant_id=tenant_id)
        
        if not outcomes:
            logger.warning("No prediction outcomes found for analytics")
            return {"outcomes": 0, "signals": 0, "performance": 0, "weights": 0, "consensus": 0, "promotions": 0}
        
        # 2. Pair signals with outcomes for learning
        self._pair_signals_with_outcomes(signals, outcomes)
        
        # 3. Compute strategy performance (horizon-scoped)
        performances = self.learner.evaluate_all_by_horizon()
        logger.info(f"Computed horizon-scoped performance for {len(performances)} (strategy_id, ticker, horizon) combinations")
        
        # 4. Persist strategy performance (grouped by horizon)
        performance_count = 0
        for (strategy_id, ticker, horizon), perf in performances.items():
            self.repo.persist_strategy_performance(
                strategy_id=strategy_id,
                horizon=horizon,
                score=perf.win_rate,
                accuracy=perf.win_rate,
                avg_return=perf.alpha / 100.0,  # Convert basis points to decimal
                sample_size=len(self.learner.history.get(strategy_id, [])),
                timestamp=datetime.now(timezone.utc),
                tenant_id=tenant_id
            )
            performance_count += 1
        
        logger.info(f"Persisted {performance_count} strategy performance records grouped by (strategy_id, ticker, horizon)")

        # 4b. Trust metrics (informational only)
        as_of = None
        try:
            evs = []
            for o in outcomes:
                v = o.get("evaluated_at")
                if not v:
                    continue
                s = str(v).replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                evs.append(dt.astimezone(timezone.utc))
            if evs:
                as_of = max(evs).replace(microsecond=0)
        except Exception:
            as_of = None

        strategy_horizons = [(sid, h) for (sid, _ticker, h) in performances.keys()]
        trust = self.trust_engine.compute_and_persist_strategy_trust(
            self.repo.conn,
            tenant_id=tenant_id,
            strategy_horizons=strategy_horizons,
            as_of=as_of,
        )
        try:
            self.trust_engine.apply_trust_to_signals(self.repo.conn, tenant_id=tenant_id, trust_by_strategy_horizon=trust)
        except Exception:
            pass
        
        # 5. Compute adaptive weights with real regime detection
        current_regime = self._detect_current_regime(outcomes)
        weights = self.weight_engine.update_all(performances, current_regime)
        logger.info(f"Computed weights for {len(weights)} strategies")
        
        # 6. Normalize weights per (ticker, horizon) and persist
        normalized_weights = self._normalize_weights_per_ticker(weights, signals)
        weight_count = 0
        for (strategy_id, ticker, horizon), weight in normalized_weights.items():
            self.repo.persist_strategy_weight(
                strategy_id=strategy_id,
                horizon=horizon,
                weight=weight,
                timestamp=datetime.now(timezone.utc),
                tenant_id=tenant_id
            )
            weight_count += 1
        
        logger.info(f"Persisted {weight_count} normalized weights grouped by (ticker, horizon)")
        
        # 7. Generate real consensus signals
        consensus_count = self._generate_consensus_signals(signals, weights, tenant_id)
        try:
            self.trust_engine.apply_trust_to_consensus(self.repo.conn, tenant_id=tenant_id, trust_by_strategy_horizon=trust)
        except Exception:
            pass
        
        # 8. Update calibration curves
        calibration_updated = self._update_calibration(outcomes, signals)
        
        # 9. Select champions and create promotions
        # Note: performances is now Dict[tuple[str, str, str], StrategyPerformance] 
        # from the horizon-scoped evaluation, but _select_champions expects this format
        promotion_count = self._select_champions(performances, weights, tenant_id)
        
        result = {
            "outcomes": len(outcomes),
            "signals": len(signals),
            "performance": len(performances),
            "weights": len(weights),
            "consensus": consensus_count,
            "promotions": promotion_count,
            "calibration_updated": calibration_updated
        }
        
        logger.info(f"Analytics pipeline complete: {result}")
        return result
    
    def _pair_signals_with_outcomes(self, signals: List[Dict], outcomes: List[Dict]) -> None:
        """Pair signals with their resolved outcomes for learning."""
        # Create lookup: prediction_id -> signal
        signal_lookup = {s["prediction_id"]: s for s in signals}
        
        # Create lookup: prediction_id -> outcome
        outcome_lookup = {o["prediction_id"]: o for o in outcomes}
        
        # Group by (strategy_id, ticker, horizon) for performance calculation
        performance_groups: Dict[tuple[str, str, str], List[tuple[Dict, Dict]]] = {}
        
        # Pair and group by horizon
        paired_count = 0
        for prediction_id, signal in signal_lookup.items():
            outcome = outcome_lookup.get(prediction_id)
            if not outcome:
                continue
            
            # Group by (strategy_id, ticker, horizon)
            group_key = (
                str(signal["strategy_id"]), 
                str(signal["ticker"]), 
                str(signal.get("horizon", "1d"))
            )
            if group_key not in performance_groups:
                performance_groups[group_key] = []
            performance_groups[group_key].append((signal, outcome))
            
            # Convert to domain models
            signal_model = Signal(
                id=str(signal["id"]),
                strategy_id=str(signal["strategy_id"]),
                ticker=str(signal["ticker"]),
                direction=self._direction_to_numeric(signal["direction"]),
                confidence=float(signal["confidence"]),
                timestamp=str(signal["timestamp"]),
                regime=str(signal.get("regime", "UNKNOWN"))
            )
            
            outcome_model = SignalOutcome(
                signal_id=str(signal["id"]),
                actual_return_pct=float(outcome["actual_return"]) * 100.0  # Convert to percentage
            )
            
            self.learner.ingest_pairing(signal_model, outcome_model)
            paired_count += 1
        
        logger.info(f"Paired {paired_count} signals with outcomes")
        logger.info(f"Performance groups by (strategy_id, ticker, horizon): {len(performance_groups)}")
    
    def _direction_to_numeric(self, direction: str) -> int:
        """Convert direction string to numeric."""
        direction_map = {"up": 1, "down": -1, "flat": 0}
        return direction_map.get(direction.lower(), 0)
    
    def _normalize_weights_per_ticker(self, weights: Dict[str, float], signals: List[Dict]) -> Dict[tuple[str, str, str], float]:
        """Normalize weights per (ticker, horizon) so they sum to 1.0."""
        # Group weights by (ticker, horizon)
        ticker_horizon_weights: Dict[tuple[str, str], Dict[str, float]] = {}
        
        # Extract ticker and horizon from signals for each strategy
        signal_lookup = {s["strategy_id"]: s for s in signals}
        
        for strategy_id, weight in weights.items():
            signal = signal_lookup.get(strategy_id)
            if not signal:
                continue
                
            ticker = str(signal["ticker"])
            horizon = str(signal.get("horizon", "1d"))
            group_key = (ticker, horizon)
            
            if group_key not in ticker_horizon_weights:
                ticker_horizon_weights[group_key] = {}
            ticker_horizon_weights[group_key][strategy_id] = weight
        
        # Normalize each (ticker, horizon) group
        normalized_weights: Dict[tuple[str, str, str], float] = {}
        for (ticker, horizon), group_weights in ticker_horizon_weights.items():
            total_weight = sum(group_weights.values())
            if total_weight == 0:
                # Equal weights if all are zero
                equal_weight = 1.0 / len(group_weights)
                for strategy_id in group_weights:
                    normalized_weights[(strategy_id, ticker, horizon)] = equal_weight
            else:
                # Normalize to sum to 1.0
                for strategy_id, weight in group_weights.items():
                    normalized_weights[(strategy_id, ticker, horizon)] = weight / total_weight
        
        logger.info(f"Normalized weights for {len(ticker_horizon_weights)} (ticker, horizon) groups")
        return normalized_weights
    
    def _generate_consensus_signals(self, signals: List[Dict], weights: Dict[str, float], tenant_id: str) -> int:
        """Generate real consensus signals using strategy weights."""
        # Group signals by ticker and horizon
        ticker_signals: Dict[str, List[Dict]] = {}
        for signal in signals:
            ticker = str(signal["ticker"])
            if ticker not in ticker_signals:
                ticker_signals[ticker] = []
            ticker_signals[ticker].append(signal)
        
        consensus_count = 0
        for ticker, ticker_signal_list in ticker_signals.items():
            # Separate sentiment and quant signals
            sentiment_signals = [s for s in ticker_signal_list if self._is_sentiment(s["strategy_id"])]
            quant_signals = [s for s in ticker_signal_list if self._is_quant(s["strategy_id"])]
            
            if not sentiment_signals or not quant_signals:
                continue
            
            # Get best signals of each type
            best_sentiment = max(sentiment_signals, key=lambda s: s["confidence"])
            best_quant = max(quant_signals, key=lambda s: s["confidence"])
            
            # Create TrackSignal objects
            sentiment_track = TrackSignal(
                ticker=ticker,
                direction=best_sentiment["direction"],
                confidence=float(best_sentiment["confidence"]),
                track="sentiment",
                metadata={"strategy_id": best_sentiment["strategy_id"]}
            )
            
            quant_track = TrackSignal(
                ticker=ticker,
                direction=best_quant["direction"],
                confidence=float(best_quant["confidence"]),
                track="quant",
                metadata={"strategy_id": best_quant["strategy_id"]}
            )
            
            # Get stability scores
            sentiment_stability = self.repo.get_strategy_stability_score(best_sentiment["strategy_id"])
            quant_stability = self.repo.get_strategy_stability_score(best_quant["strategy_id"])
            
            # Compute consensus
            consensus = self.consensus_engine.combine(
                sentiment_signal=sentiment_track,
                quant_signal=quant_track,
                realized_volatility=0.2,  # TODO: Get from market data
                historical_volatility_window=[0.15, 0.18, 0.22, 0.2, 0.19],  # TODO: Get from market data
                sentiment_stability=sentiment_stability,
                quant_stability=quant_stability
            )
            
            # Persist consensus signal (overwrite placeholder)
            self.repo.persist_consensus_signal(
                prediction_id=f"analytics_{ticker}_{datetime.now(timezone.utc).isoformat()}",
                ticker=ticker,
                timestamp=datetime.now(timezone.utc),
                horizon="1d",  # TODO: Multi-horizon support
                direction=consensus.direction,
                confidence=consensus.confidence,
                regime=consensus.regime.get("volatility_regime"),
                sentiment_strategy_id=sentiment_track.metadata["strategy_id"],
                quant_strategy_id=quant_track.metadata["strategy_id"],
                sentiment_score=consensus.sentiment_confidence,
                quant_score=consensus.quant_confidence,
                ws=consensus.metadata.get("ws"),
                wq=consensus.metadata.get("wq"),
                agreement_bonus=consensus.metadata.get("agreement_bonus"),
                p_final=consensus.weighted_consensus,
                stability_score=(sentiment_stability + quant_stability) / 2.0,
                tenant_id=tenant_id
            )
            
            consensus_count += 1
        
        logger.info(f"Generated {consensus_count} real consensus signals")
        return consensus_count
    
    def _is_sentiment(self, strategy_id: str) -> bool:
        """Check if strategy is sentiment-based."""
        sentiment_keywords = ["sentiment", "reddit", "news", "social"]
        return any(keyword in strategy_id.lower() for keyword in sentiment_keywords)
    
    def _is_quant(self, strategy_id: str) -> bool:
        """Check if strategy is quant-based."""
        quant_keywords = ["technical", "baseline", "momentum", "quant", "trend"]
        return any(keyword in strategy_id.lower() for keyword in quant_keywords)
    
    def _select_champions(self, performances: Dict[tuple[str, str, str], StrategyPerformance], weights: Dict[str, float], tenant_id: str) -> int:
        """Select champions per (ticker, horizon) and create promotion events."""
        if not performances:
            return 0
        
        # Group performances by (ticker, horizon) for champion selection
        ticker_horizon_perfs: Dict[tuple[str, str], List[tuple[str, StrategyPerformance, float]]] = {}
        
        for (strategy_id, ticker, horizon), perf in performances.items():
            weight = weights.get(strategy_id, 0.0)
            combined_score = perf.win_rate * perf.stability * weight
            group_key = (ticker, horizon)
            
            if group_key not in ticker_horizon_perfs:
                ticker_horizon_perfs[group_key] = []
            ticker_horizon_perfs[group_key].append((strategy_id, perf, combined_score))
        
        promotion_count = 0
        
        # Select top champion per (ticker, horizon)
        for (ticker, horizon), strategy_list in ticker_horizon_perfs.items():
            # Sort by combined score descending
            strategy_list.sort(key=lambda x: x[2], reverse=True)
            
            # Select top strategy as champion for this ticker/horizon
            champion = strategy_list[0]  # Best strategy for this ticker/horizon
            strategy_id, perf, score = champion
            
            # Create promotion event
            self.repo.persist_promotion_event(
                strategy_id=strategy_id,
                horizon=horizon,
                rank=1,  # Champion rank = 1 for this ticker/horizon
                score=score,
                win_rate=perf.win_rate,
                stability=perf.stability,
                weight=weights.get(strategy_id, 0.0),
                timestamp=datetime.now(timezone.utc),
                tenant_id=tenant_id
            )
            promotion_count += 1
        
        logger.info(f"Promoted {promotion_count} champions across {len(ticker_horizon_perfs)} (ticker, horizon) groups")
        return promotion_count
    
    def _detect_current_regime(self, outcomes: List[Any]) -> str:
        """
        Detect current market regime using recent volatility and trend data
        
        Args:
            outcomes: Recent prediction outcomes with market data
            
        Returns:
            Regime string for weight engine
        """
        if not outcomes:
            logger.warning("No outcomes available for regime detection, defaulting to NORMAL")
            return "NORMAL"
        
        # Extract recent volatility data from outcomes
        recent_vols = []
        adx_values = []
        
        # Get volatility from recent performance data
        for outcome in outcomes[-100:]:  # Use last 100 outcomes
            # Try to get volatility from feature snapshots if available
            if hasattr(outcome, 'feature_snapshot') and outcome.feature_snapshot:
                vol = outcome.feature_snapshot.get('realized_volatility')
                adx = outcome.feature_snapshot.get('adx_value')
                if vol is not None:
                    recent_vols.append(vol)
                if adx is not None:
                    adx_values.append(adx)
        
        # Fallback: use historical volatility from strategy performance
        if not recent_vols:
            # Calculate average volatility from performance data
            volatilities = []
            for outcome in outcomes[-50:]:
                if hasattr(outcome, 'realized_return') and outcome.realized_return is not None:
                    # Use absolute returns as volatility proxy
                    volatilities.append(abs(outcome.realized_return))
            
            if volatilities:
                recent_vols = volatilities
        
        if not recent_vols:
            logger.warning("No volatility data available for regime detection, defaulting to NORMAL")
            return "NORMAL"
        
        # Calculate current volatility (most recent)
        current_vol = recent_vols[-1] if recent_vols else 0.02
        
        # Use regime manager to classify
        try:
            regime_snapshot = self.regime_manager.classify(
                realized_volatility=current_vol,
                historical_volatility_window=recent_vols[-20:] if len(recent_vols) >= 20 else recent_vols,
                adx_value=adx_values[-1] if adx_values else None
            )
            
            # Convert to weight engine format
            vol_regime = regime_snapshot.volatility_regime.value
            trend_strength = regime_snapshot.trend_strength
            
            # Create comprehensive regime string
            if vol_regime == "HIGH":
                if trend_strength == "STRONG":
                    return "HIGH_VOL_STRONG_TREND"
                else:
                    return "HIGH_VOL_WEAK_TREND"
            elif vol_regime == "LOW":
                if trend_strength == "STRONG":
                    return "LOW_VOL_STRONG_TREND"
                else:
                    return "LOW_VOL_WEAK_TREND"
            else:  # NORMAL
                if trend_strength == "STRONG":
                    return "NORMAL_VOL_STRONG_TREND"
                else:
                    return "NORMAL_VOL_WEAK_TREND"
                    
        except Exception as e:
            logger.error(f"Error detecting regime: {e}, defaulting to NORMAL")
            return "NORMAL"
    
    def _update_calibration(self, outcomes: List[Any], signals: List[Dict]) -> bool:
        """Update calibration curves with new outcome data"""
        try:
            # Create calibration points from outcomes
            calibration_points = []
            
            # Create signal lookup for confidence data
            signal_lookup = {}
            for signal in signals:
                if 'prediction_id' in signal:
                    signal_lookup[signal['prediction_id']] = signal
            
            for outcome in outcomes:
                # Find corresponding signal
                signal = signal_lookup.get(outcome.prediction_id)
                if not signal:
                    continue
                
                # Extract confidence and regime information
                confidence = signal.get('confidence', 0.5)
                actual_outcome = outcome.direction_correct  # True for win, False for loss
                strategy_id = signal.get('strategy_id', 'unknown')
                regime = signal.get('regime', 'UNKNOWN')
                ticker = signal.get('ticker', 'unknown')
                
                # Create calibration point
                from app.engine.confidence_calibration import CalibrationPoint
                calibration_point = CalibrationPoint(
                    confidence=confidence,
                    actual_outcome=actual_outcome,
                    timestamp=outcome.evaluated_at,
                    strategy_id=strategy_id,
                    regime=regime,
                    ticker=ticker
                )
                calibration_points.append(calibration_point)
            
            if calibration_points:
                # Update calibration curves
                new_curves = self.calibrator.calibrate_from_outcomes(calibration_points)
                logger.info(f"Updated calibration with {len(calibration_points)} outcomes, created {len(new_curves)} curves")
                return True
            else:
                logger.warning("No calibration points created from outcomes")
                return False
                
        except Exception as e:
            logger.error(f"Error updating calibration: {e}")
            return False
