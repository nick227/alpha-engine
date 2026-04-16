"""
ML Integration Layer

Integrates simple ML model with existing Alpha Engine pipeline.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import numpy as np
import logging

from app.ml.simple_trainer import SimpleMLTrainer
from app.ml.training_dataset import TrainingDatasetBuilder
from app.discovery.types import DiscoveryCandidate

logger = logging.getLogger(__name__)


class MLIntegration:
    """Integrate simple ML model with existing pipeline."""
    
    def __init__(self, db_path: str, model_path: str):
        self.db_path = db_path
        self.model_path = model_path
        self.trainer = SimpleMLTrainer()
        self.dataset_builder = TrainingDatasetBuilder(db_path)
        
        # Load existing model if available
        try:
            self.trainer.load_model(model_path)
            logger.info("ML model loaded successfully")
        except:
            logger.info("No existing model found - will train new one")
            self.trainer = None
    
    def train_new_model(self, days_back: int = 252) -> Dict[str, Any]:
        """Train new model on recent trade data."""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Build training dataset
        logger.info("Building training dataset...")
        examples = self.dataset_builder.build_training_dataset(start_date, end_date)
        
        if len(examples) < 100:
            error_msg = f"Insufficient training data: {len(examples)} examples (minimum 100)"
            logger.error(error_msg)
            return {'error': error_msg}
        
        # Save training data
        training_path = "data/training_data.csv"
        self.dataset_builder.save_training_dataset(examples, training_path)
        
        # Train model
        logger.info("Training ML model...")
        results = self.trainer.train_model(training_path, model_type='gradient_boosting')
        
        if 'error' in results:
            logger.error(f"Model training failed: {results['error']}")
            return results
        
        # Save model
        try:
            self.trainer.save_model(self.model_path)
            logger.info(f"Model saved to {self.model_path}")
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            return {'error': str(e)}
        
        return results
    
    def filter_signals_with_ml(
        self,
        candidates: List[DiscoveryCandidate],
        features: Dict[str, Any]
    ) -> List[DiscoveryCandidate]:
        """Filter discovery candidates using ML model."""
        
        if self.trainer is None:
            logger.warning("ML model not available - returning all candidates")
            return candidates
        
        filtered_candidates = []
        ml_scores = {}
        
        for candidate in candidates:
            # Extract features for this candidate
            symbol_features = self._extract_features_for_candidate(candidate, features)
            
            if not symbol_features:
                logger.warning(f"No features available for {candidate.symbol}")
                continue
            
            # Get ML prediction
            try:
                win_probability = self.trainer.predict_win_probability(symbol_features)
                ml_scores[candidate.symbol] = win_probability
                
                # Apply ML filter (only keep high-probability trades)
                threshold = 0.6  # Can be tuned
                if win_probability > threshold:
                    candidate.metadata['ml_win_probability'] = win_probability
                    candidate.metadata['ml_filtered'] = True
                    candidate.metadata['ml_threshold'] = threshold
                    filtered_candidates.append(candidate)
                else:
                    candidate.metadata['ml_win_probability'] = win_probability
                    candidate.metadata['ml_filtered'] = False
                    candidate.metadata['ml_threshold'] = threshold
                    
            except Exception as e:
                logger.error(f"Error predicting for {candidate.symbol}: {e}")
                candidate.metadata['ml_win_probability'] = 0.5
                candidate.metadata['ml_filtered'] = False
                candidate.metadata['ml_error'] = str(e)
        
        passed_count = len(filtered_candidates)
        total_count = len(candidates)
        logger.info(f"ML filter: {passed_count}/{total_count} candidates passed ({passed_count/total_count:.1%})")
        
        # Sort by ML probability
        filtered_candidates.sort(key=lambda c: c.metadata.get('ml_win_probability', 0), reverse=True)
        
        return filtered_candidates
    
    def get_ml_scores_for_candidates(
        self,
        candidates: List[DiscoveryCandidate],
        features: Dict[str, Any]
    ) -> Dict[str, float]:
        """Get ML scores for all candidates (without filtering)."""
        
        if self.trainer is None:
            logger.warning("ML model not available - returning neutral scores")
            return {c.symbol: 0.5 for c in candidates}
        
        ml_scores = {}
        
        for candidate in candidates:
            symbol_features = self._extract_features_for_candidate(candidate, features)
            
            if symbol_features:
                try:
                    win_probability = self.trainer.predict_win_probability(symbol_features)
                    ml_scores[candidate.symbol] = win_probability
                except Exception as e:
                    logger.error(f"Error predicting for {candidate.symbol}: {e}")
                    ml_scores[candidate.symbol] = 0.5
            else:
                ml_scores[candidate.symbol] = 0.5
        
        return ml_scores
    
    def _extract_features_for_candidate(
        self,
        candidate: DiscoveryCandidate,
        all_features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract ML features for discovery candidate."""
        
        symbol = candidate.symbol
        if symbol not in all_features:
            return {}
        
        features = all_features[symbol]
        
        # Extract compressed features
        return {
            'trend_strength': self._calculate_trend_strength(features),
            'volatility_regime': self._classify_volatility_regime(features),
            'position_in_range': features.get('price_percentile_252d', 0.5),
            'volume_anomaly': features.get('volume_zscore_20d', 0.0),
            'spy_trend': self._get_spy_trend(),  # Current market trend
            'vix_level': self._get_vix_level(),   # Current VIX level
            'macro_context': self._classify_macro_context()
        }
    
    def _calculate_trend_strength(self, features: Dict[str, Any]) -> float:
        """Calculate trend strength from features."""
        
        returns = [
            features.get('return_5d', 0),
            features.get('return_20d', 0),
            features.get('return_63d', 0)
        ]
        weights = [0.5, 0.3, 0.2]
        
        weighted_return = sum(abs(r) * w for r, w in zip(returns, weights))
        return min(1.0, weighted_return / 0.1)  # Normalize to 0-1
    
    def _classify_volatility_regime(self, features: Dict[str, Any]) -> str:
        """Classify volatility regime."""
        
        vol = features.get('volatility_20d', 0.02)
        if vol > 0.03:
            return 'expansion'
        elif vol < 0.015:
            return 'contraction'
        else:
            return 'normal'
    
    def _get_spy_trend(self) -> float:
        """Get current SPY trend."""
        
        # This would query current SPY data
        # For now, return mock data - REPLACE WITH ACTUAL QUERY
        try:
            # Mock implementation - replace with actual data query
            return np.random.normal(0, 0.02)
        except:
            return 0.0
    
    def _get_vix_level(self) -> float:
        """Get current VIX level."""
        
        # This would query current VIX data
        # For now, return mock data - REPLACE WITH ACTUAL QUERY
        try:
            # Mock implementation - replace with actual data query
            return np.random.uniform(0.2, 0.8)
        except:
            return 0.5
    
    def _classify_macro_context(self) -> str:
        """Classify current macro context."""
        
        spy_trend = self._get_spy_trend()
        vix_level = self._get_vix_level()
        
        if spy_trend > 0.02 and vix_level < 0.5:
            return 'risk_on'
        elif spy_trend < -0.02 and vix_level > 0.7:
            return 'risk_off'
        else:
            return 'neutral'
    
    def get_model_status(self) -> Dict[str, Any]:
        """Get status of ML model."""
        
        if self.trainer is None:
            return {
                'model_loaded': False,
                'model_path': self.model_path,
                'status': 'not_trained'
            }
        
        model_info = self.trainer.get_model_info()
        model_info['model_loaded'] = True
        model_info['model_path'] = self.model_path
        
        return model_info
    
    def update_model_periodically(self, retrain_interval_days: int = 30) -> bool:
        """Update model periodically based on new trade data."""
        
        try:
            # Check when model was last trained
            if self.trainer and hasattr(self.trainer, 'training_metadata'):
                training_date_str = self.trainer.training_metadata.get('training_date')
                if training_date_str:
                    training_date = datetime.fromisoformat(training_date_str)
                    days_since_training = (datetime.now() - training_date).days
                    
                    if days_since_training < retrain_interval_days:
                        logger.info(f"Model trained {days_since_training} days ago, skipping retrain")
                        return False
            
            # Train new model
            logger.info("Training new model due to periodic update")
            results = self.train_new_model()
            
            return 'error' not in results
            
        except Exception as e:
            logger.error(f"Error in periodic model update: {e}")
            return False
    
    def create_ml_enhanced_candidates(
        self,
        candidates: List[DiscoveryCandidate],
        features: Dict[str, Any],
        enhancement_mode: str = 'filter'
    ) -> List[DiscoveryCandidate]:
        """
        Create ML-enhanced candidates.
        
        Args:
            candidates: Original discovery candidates
            features: Feature data for all symbols
            enhancement_mode: 'filter' or 'score'
            
        Returns:
            Enhanced candidates with ML information
        """
        
        if self.trainer is None:
            logger.warning("ML model not available - returning original candidates")
            return candidates
        
        # Get ML scores for all candidates
        ml_scores = self.get_ml_scores_for_candidates(candidates, features)
        
        enhanced_candidates = []
        
        for candidate in candidates:
            ml_score = ml_scores.get(candidate.symbol, 0.5)
            
            # Create enhanced candidate
            enhanced_candidate = DiscoveryCandidate(
                symbol=candidate.symbol,
                strategy_type=f"ml_enhanced_{candidate.strategy_type}",
                score=ml_score if enhancement_mode == 'score' else candidate.score,
                reason=f"{candidate.reason} (ML: {ml_score:.2f})",
                metadata={
                    **candidate.metadata,
                    'ml_score': ml_score,
                    'ml_enhanced': True,
                    'original_score': candidate.score,
                    'original_strategy': candidate.strategy_type
                }
            )
            
            enhanced_candidates.append(enhanced_candidate)
        
        # Sort by ML score if in score mode
        if enhancement_mode == 'score':
            enhanced_candidates.sort(key=lambda c: c.metadata['ml_score'], reverse=True)
        
        logger.info(f"Created {len(enhanced_candidates)} ML-enhanced candidates (mode: {enhancement_mode})")
        
        return enhanced_candidates
