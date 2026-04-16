"""
Tight ML Integration Layer

Production-ready ML integration with strict constraints to prevent overfitting and leakage.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import numpy as np
import logging

from app.ml.simple_trainer import SimpleMLTrainer
from app.ml.training_dataset import TrainingDatasetBuilder
from app.discovery.types import DiscoveryCandidate

logger = logging.getLogger(__name__)


class TightMLIntegration:
    """
    Production ML integration with strict constraints:
    - Max 8 features total
    - No future data leakage
    - ML only filters, never overrides
    - Fixed training cadence
    """
    
    # STRICT: Only 8 core features - no expansion
    CORE_FEATURES = [
        'trend_strength',      # Compressed from multiple returns
        'volatility_regime',   # expansion/contraction/normal
        'position_in_range',   # 0-1 normalized position
        'volume_anomaly',      # z-score of volume
        'spy_trend',          # Market direction (5d return)
        'vix_level',          # Fear gauge (percentile)
        'sector_trend',       # Sector relative performance
        'price_momentum'      # 5d price momentum
    ]
    
    # STRICT: ML can ONLY filter, never override
    def __init__(self, db_path: str, model_path: str):
        self.db_path = db_path
        self.model_path = model_path
        self.trainer = SimpleMLTrainer()
        self.dataset_builder = TrainingDatasetBuilder(db_path)
        
        # STRICT: Fixed threshold - test these values
        self.win_probability_threshold = 0.60  # Test: 0.55, 0.60, 0.65, 0.70
        
        # STRICT: Training cadence - not daily
        self.last_training_date = None
        self.min_retrain_interval_days = 7  # Weekly minimum
        
        # Load existing model if available
        try:
            self.trainer.load_model(model_path)
            logger.info("ML model loaded successfully")
        except:
            logger.info("No existing model found - will train new one")
            self.trainer = None
    
    def filter_signals_with_ml(
        self,
        candidates: List[DiscoveryCandidate],
        features: Dict[str, Any],
        regime_context: Dict[str, Any] = None
    ) -> List[DiscoveryCandidate]:
        """
        STRICT: ML can ONLY filter candidates, never override direction or logic.
        """
        
        if self.trainer is None:
            logger.warning("ML model not available - returning all candidates")
            return candidates
        
        filtered_candidates = []
        
        for candidate in candidates:
            # STRICT: Extract only pre-entry features (no future data)
            symbol_features = self._extract_entry_features_only(
                candidate, features, regime_context
            )
            
            if not symbol_features:
                logger.warning(f"No valid entry features for {candidate.symbol}")
                continue
            
            # STRICT: Validate no leakage
            if self._has_future_leakage(symbol_features):
                logger.error(f"Future data leakage detected for {candidate.symbol}")
                continue
            
            # Get ML prediction
            try:
                win_probability = self.trainer.predict_win_probability(symbol_features)
                
                # STRICT: Apply threshold - this is the real system edge
                if win_probability > self.win_probability_threshold:
                    candidate.metadata['ml_win_probability'] = win_probability
                    candidate.metadata['ml_filtered'] = True
                    candidate.metadata['ml_threshold'] = self.win_probability_threshold
                    filtered_candidates.append(candidate)
                else:
                    candidate.metadata['ml_win_probability'] = win_probability
                    candidate.metadata['ml_filtered'] = False
                    candidate.metadata['ml_threshold'] = self.win_probability_threshold
                    
            except Exception as e:
                logger.error(f"Error predicting for {candidate.symbol}: {e}")
                candidate.metadata['ml_win_probability'] = 0.5
                candidate.metadata['ml_filtered'] = False
        
        passed_count = len(filtered_candidates)
        total_count = len(candidates)
        logger.info(f"ML filter (threshold={self.win_probability_threshold}): {passed_count}/{total_count} passed")
        
        return filtered_candidates
    
    def _extract_entry_features_only(
        self,
        candidate: DiscoveryCandidate,
        all_features: Dict[str, Any],
        regime_context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        STRICT: Extract only pre-entry features - no future data allowed.
        """
        
        symbol = candidate.symbol
        if symbol not in all_features:
            return {}
        
        features = all_features[symbol]
        
        # STRICT: Only use data available at entry time
        entry_features = {
            # Core compressed features
            'trend_strength': self._calculate_trend_strength_safe(features),
            'volatility_regime': self._classify_volatility_regime_safe(features),
            'position_in_range': features.get('price_percentile_252d', 0.5),
            'volume_anomaly': features.get('volume_zscore_20d', 0.0),
            
            # Cross-asset context (must be current market data, not future)
            'spy_trend': self._get_current_spy_trend(),  # Current, not future
            'vix_level': self._get_current_vix_level(),   # Current, not future
            
            # Sector context (relative to market)
            'sector_trend': self._get_sector_trend_safe(features),
            'price_momentum': features.get('return_5d', 0.0)  # Recent momentum only
        }
        
        return entry_features
    
    def _calculate_trend_strength_safe(self, features: Dict[str, Any]) -> float:
        """Calculate trend strength from historical returns only."""
        
        # STRICT: Only use historical returns, no future data
        returns = [
            features.get('return_1d', 0),
            features.get('return_5d', 0),
            features.get('return_20d', 0)
        ]
        
        # Weight recent more heavily
        weights = [0.2, 0.5, 0.3]
        weighted_return = sum(abs(r) * w for r, w in zip(returns, weights))
        
        return min(1.0, weighted_return / 0.1)  # Normalize to 0-1
    
    def _classify_volatility_regime_safe(self, features: Dict[str, Any]) -> str:
        """Classify volatility regime from historical data only."""
        
        vol = features.get('volatility_20d', 0.02)
        if vol > 0.03:
            return 'expansion'
        elif vol < 0.015:
            return 'contraction'
        else:
            return 'normal'
    
    def _get_current_spy_trend(self) -> float:
        """Get current SPY trend - must be real-time data."""
        
        # STRICT: This must be real-time market data, not future data
        try:
            # Replace with actual real-time SPY query
            # For now, return mock data - MUST REPLACE
            return np.random.normal(0, 0.02)
        except:
            return 0.0
    
    def _get_current_vix_level(self) -> float:
        """Get current VIX level - must be real-time data."""
        
        # STRICT: This must be real-time VIX data, not future data
        try:
            # Replace with actual real-time VIX query
            # For now, return mock data - MUST REPLACE
            return np.random.uniform(0.2, 0.8)
        except:
            return 0.5
    
    def _get_sector_trend_safe(self, features: Dict[str, Any]) -> float:
        """Get sector relative trend - must be historical data only."""
        
        # Use sector relative return if available
        sector_return = features.get('sector_return_63d', 0.0)
        market_return = features.get('return_63d', 0.0)
        
        # Relative performance
        relative_trend = sector_return - market_return
        
        # Normalize to reasonable range
        return np.clip(relative_trend, -0.2, 0.2)
    
    def _has_future_leakage(self, features: Dict[str, Any]) -> bool:
        """
        STRICT: Check for future data leakage.
        """
        
        # Check for any features that could contain future information
        suspicious_features = [
            'return_1d',  # This could be future if not careful
            'exit_price',
            'exit_timestamp',
            'realized_pnl',
            'future_volatility',
            'next_day_return'
        ]
        
        for suspicious in suspicious_features:
            if suspicious in features:
                logger.warning(f"Suspicious feature detected: {suspicious}")
                return True
        
        return False
    
    def train_new_model_safe(self, days_back: int = 252) -> Dict[str, Any]:
        """
        STRICT: Train model with proper cadence and validation.
        """
        
        # STRICT: Check training cadence - don't retrain too frequently
        if self.last_training_date:
            days_since_training = (datetime.now() - self.last_training_date).days
            if days_since_training < self.min_retrain_interval_days:
                logger.info(f"Skipping retrain - only {days_since_training} days since last training")
                return {'skipped': True, 'days_since_training': days_since_training}
        
        # Build training dataset
        logger.info("Building training dataset...")
        examples = self.dataset_builder.build_training_dataset(
            datetime.now() - timedelta(days=days_back),
            datetime.now()
        )
        
        if len(examples) < 100:
            error_msg = f"Insufficient training data: {len(examples)} examples (minimum 100)"
            logger.error(error_msg)
            return {'error': error_msg}
        
        # STRICT: Validate no leakage in training data
        if self._validate_training_data_no_leakage(examples):
            logger.error("Future data leakage detected in training data")
            return {'error': 'Future data leakage in training data'}
        
        # Save training data
        training_path = "data/training_data.csv"
        self.dataset_builder.save_training_dataset(examples, training_path)
        
        # Train model with strict feature set
        logger.info("Training ML model with strict feature set...")
        results = self.trainer.train_model(training_path, model_type='gradient_boosting')
        
        if 'error' in results:
            logger.error(f"Model training failed: {results['error']}")
            return results
        
        # STRICT: Validate model doesn't overfit
        if not self._validate_model_not_overfitting(results):
            logger.warning("Model may be overfitting - consider regularization")
        
        # Save model
        try:
            self.trainer.save_model(self.model_path)
            self.last_training_date = datetime.now()
            logger.info(f"Model saved and training date updated to {self.last_training_date}")
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            return {'error': str(e)}
        
        return results
    
    def _validate_training_data_no_leakage(self, examples: List) -> bool:
        """Validate training data has no future leakage."""
        
        # Check each example for future data
        for example in examples:
            # Entry timestamp should be before exit
            if hasattr(example, 'entry_timestamp') and hasattr(example, 'exit_timestamp'):
                if example.entry_timestamp >= example.exit_timestamp:
                    logger.error(f"Invalid timestamps: entry >= exit for {example.trade_id}")
                    return True
            
            # Check raw features for future data
            if hasattr(example, 'raw_features'):
                if self._has_future_leakage(example.raw_features):
                    return True
        
        return False
    
    def _validate_model_not_overfitting(self, results: Dict[str, Any]) -> bool:
        """Validate model is not overfitting."""
        
        # Check if accuracy is suspiciously high
        accuracy = results.get('accuracy', 0)
        if accuracy > 0.85:
            logger.warning(f"Suspiciously high accuracy: {accuracy:.3f} - possible overfitting")
            return False
        
        # Check feature importance distribution
        feature_importance = results.get('feature_importance', {})
        if feature_importance:
            max_importance = max(feature_importance.values())
            min_importance = min(feature_importance.values())
            
            # If one feature dominates, possible overfitting
            if max_importance > 0.8:
                logger.warning(f"Feature dominance detected: {max_importance:.3f}")
                return False
        
        return True
    
    def set_threshold(self, threshold: float):
        """
        STRICT: Set win probability threshold - this is the real system edge.
        Test these values: 0.55, 0.60, 0.65, 0.70
        """
        
        if not 0.5 <= threshold <= 0.9:
            raise ValueError("Threshold must be between 0.5 and 0.9")
        
        self.win_probability_threshold = threshold
        logger.info(f"ML threshold set to {threshold}")
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance from trained model."""
        
        if self.trainer is None:
            return {}
        
        model_info = self.trainer.get_model_info()
        return model_info.get('training_metadata', {}).get('feature_importance', {})
    
    def validate_feature_set(self) -> bool:
        """
        STRICT: Validate we're not expanding beyond 8 features.
        """
        
        if len(self.CORE_FEATURES) > 8:
            logger.error(f"Too many features: {len(self.CORE_FEATURES)} > 8")
            return False
        
        logger.info(f"Feature set validated: {len(self.CORE_FEATURES)} features")
        return True
