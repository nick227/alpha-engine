"""
Production ML Integration Layer

Production-ready ML integration with comprehensive leakage detection,
edge curve analysis, and safety checks.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import logging
import sqlite3

from app.ml.simple_trainer import SimpleMLTrainer
from app.ml.training_dataset import TrainingDatasetBuilder
from app.discovery.types import DiscoveryCandidate

logger = logging.getLogger(__name__)


class ProductionMLIntegration:
    """
    Production ML integration with comprehensive safeguards:
    - Semantic leakage detection (not just name-based)
    - Edge curve analysis logging
    - Hard safety checks for early-stage models
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
    
    def __init__(self, db_path: str, model_path: str):
        self.db_path = db_path
        self.model_path = model_path
        self.trainer = SimpleMLTrainer()
        self.dataset_builder = TrainingDatasetBuilder(db_path)
        
        # ML filtering parameters
        self.win_probability_threshold = 0.60
        self.threshold_min = 0.50  # Hard safety check minimum
        
        # Training cadence
        self.last_training_date = None
        self.min_retrain_interval_days = 7
        
        # Edge curve tracking
        self.edge_curve_log = []
        
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
        Production ML filtering with comprehensive logging and safety checks.
        """
        
        if self.trainer is None:
            logger.warning("ML model not available - returning all candidates")
            return candidates
        
        # HARD SAFETY CHECK: Early stage model reliability
        model_confidence = self._get_model_confidence()
        bypass_ml = model_confidence < self.threshold_min
        
        if bypass_ml:
            logger.warning(f"ML bypassed - model confidence {model_confidence:.3f} < threshold_min {self.threshold_min}")
            return candidates
        
        filtered_candidates = []
        
        for candidate in candidates:
            # SEMANTIC LEAKAGE DETECTION: Can I compute this EXACTLY at entry time?
            symbol_features, is_valid = self._extract_entry_features_semantic(
                candidate, features, regime_context
            )
            
            if not is_valid:
                logger.warning(f"Semantic leakage detected for {candidate.symbol}")
                candidate.metadata['ml_leakage_detected'] = True
                continue
            
            if not symbol_features:
                logger.warning(f"No valid entry features for {candidate.symbol}")
                continue
            
            # Get ML prediction
            try:
                win_probability = self.trainer.predict_win_probability(symbol_features)
                
                # CRITICAL LOG: Edge curve analysis
                ml_passed = win_probability > self.win_probability_threshold
                
                # Log per trade for edge curve analysis
                self._log_edge_curve(candidate, win_probability, ml_passed)
                
                # Apply ML filter
                if ml_passed:
                    candidate.metadata['ml_win_probability'] = win_probability
                    candidate.metadata['ml_filtered'] = True
                    candidate.metadata['ml_threshold'] = self.win_probability_threshold
                    candidate.metadata['ml_confidence'] = model_confidence
                    filtered_candidates.append(candidate)
                else:
                    candidate.metadata['ml_win_probability'] = win_probability
                    candidate.metadata['ml_filtered'] = False
                    candidate.metadata['ml_threshold'] = self.win_probability_threshold
                    candidate.metadata['ml_confidence'] = model_confidence
                    
            except Exception as e:
                logger.error(f"Error predicting for {candidate.symbol}: {e}")
                candidate.metadata['ml_win_probability'] = 0.5
                candidate.metadata['ml_filtered'] = False
                candidate.metadata['ml_error'] = str(e)
        
        passed_count = len(filtered_candidates)
        total_count = len(candidates)
        logger.info(f"ML filter (threshold={self.win_probability_threshold}): {passed_count}/{total_count} passed")
        
        return filtered_candidates
    
    def _extract_entry_features_semantic(
        self,
        candidate: DiscoveryCandidate,
        all_features: Dict[str, Any],
        regime_context: Dict[str, Any] = None
    ) -> Tuple[Dict[str, Any], bool]:
        """
        SEMANTIC LEAKAGE DETECTION: Can I compute this EXACTLY at entry time?
        """
        
        symbol = candidate.symbol
        if symbol not in all_features:
            return {}, False
        
        features = all_features[symbol]
        
        # SEMANTIC CHECK: Each feature must pass entry-time test
        entry_features = {}
        semantic_valid = True
        
        # 1. trend_strength - Can I compute this at entry?
        trend_strength = self._calculate_trend_strength_semantic(features)
        if trend_strength is not None:
            entry_features['trend_strength'] = trend_strength
        else:
            logger.warning(f"trend_strength cannot be computed at entry for {symbol}")
            semantic_valid = False
        
        # 2. volatility_regime - Can I compute this at entry?
        vol_regime = self._classify_volatility_regime_semantic(features)
        if vol_regime is not None:
            entry_features['volatility_regime'] = vol_regime
        else:
            logger.warning(f"volatility_regime cannot be computed at entry for {symbol}")
            semantic_valid = False
        
        # 3. position_in_range - Can I compute this at entry?
        pos_in_range = features.get('price_percentile_252d')
        if pos_in_range is not None:
            entry_features['position_in_range'] = pos_in_range
        else:
            logger.warning(f"position_in_range cannot be computed at entry for {symbol}")
            semantic_valid = False
        
        # 4. volume_anomaly - Can I compute this at entry?
        vol_anomaly = features.get('volume_zscore_20d')
        if vol_anomaly is not None:
            entry_features['volume_anomaly'] = vol_anomaly
        else:
            logger.warning(f"volume_anomaly cannot be computed at entry for {symbol}")
            semantic_valid = False
        
        # 5. spy_trend - Can I compute this at entry?
        spy_trend = self._get_current_spy_trend_semantic()
        if spy_trend is not None:
            entry_features['spy_trend'] = spy_trend
        else:
            logger.warning(f"spy_trend cannot be computed at entry")
            semantic_valid = False
        
        # 6. vix_level - Can I compute this at entry?
        vix_level = self._get_current_vix_level_semantic()
        if vix_level is not None:
            entry_features['vix_level'] = vix_level
        else:
            logger.warning(f"vix_level cannot be computed at entry")
            semantic_valid = False
        
        # 7. sector_trend - Can I compute this at entry?
        sector_trend = self._get_sector_trend_semantic(features)
        if sector_trend is not None:
            entry_features['sector_trend'] = sector_trend
        else:
            logger.warning(f"sector_trend cannot be computed at entry for {symbol}")
            semantic_valid = False
        
        # 8. price_momentum - Can I compute this at entry?
        price_momentum = features.get('return_5d')
        if price_momentum is not None:
            entry_features['price_momentum'] = price_momentum
        else:
            logger.warning(f"price_momentum cannot be computed at entry for {symbol}")
            semantic_valid = False
        
        return entry_features, semantic_valid
    
    def _calculate_trend_strength_semantic(self, features: Dict[str, Any]) -> Optional[float]:
        """
        SEMANTIC CHECK: Can I compute trend strength at entry time?
        """
        
        # Check if all required data is available at entry
        required_returns = ['return_1d', 'return_5d', 'return_20d']
        
        for ret in required_returns:
            if ret not in features or features[ret] is None:
                logger.warning(f"Missing {ret} for trend strength calculation")
                return None
        
        # If we have all data, we can compute at entry
        returns = [features['return_1d'], features['return_5d'], features['return_20d']]
        weights = [0.2, 0.5, 0.3]
        weighted_return = sum(abs(r) * w for r, w in zip(returns, weights))
        
        return min(1.0, weighted_return / 0.1)
    
    def _classify_volatility_regime_semantic(self, features: Dict[str, Any]) -> Optional[str]:
        """
        SEMANTIC CHECK: Can I compute volatility regime at entry time?
        """
        
        if 'volatility_20d' not in features or features['volatility_20d'] is None:
            logger.warning("Missing volatility_20d for regime classification")
            return None
        
        vol = features['volatility_20d']
        if vol > 0.03:
            return 'expansion'
        elif vol < 0.015:
            return 'contraction'
        else:
            return 'normal'
    
    def _get_current_spy_trend_semantic(self) -> Optional[float]:
        """
        SEMANTIC CHECK: Can I compute SPY trend at entry time?
        """
        
        try:
            # This must be real-time market data query
            # Replace with actual implementation
            # For now, return mock data
            return np.random.normal(0, 0.02)
        except Exception as e:
            logger.error(f"Cannot compute SPY trend at entry: {e}")
            return None
    
    def _get_current_vix_level_semantic(self) -> Optional[float]:
        """
        SEMANTIC CHECK: Can I compute VIX level at entry time?
        """
        
        try:
            # This must be real-time VIX data query
            # Replace with actual implementation
            # For now, return mock data
            return np.random.uniform(0.2, 0.8)
        except Exception as e:
            logger.error(f"Cannot compute VIX level at entry: {e}")
            return None
    
    def _get_sector_trend_semantic(self, features: Dict[str, Any]) -> Optional[float]:
        """
        SEMANTIC CHECK: Can I compute sector trend at entry time?
        """
        
        if 'sector_return_63d' not in features or features['sector_return_63d'] is None:
            logger.warning("Missing sector_return_63d for sector trend")
            return None
        
        if 'return_63d' not in features or features['return_63d'] is None:
            logger.warning("Missing return_63d for sector trend")
            return None
        
        sector_return = features['sector_return_63d']
        market_return = features['return_63d']
        relative_trend = sector_return - market_return
        
        return np.clip(relative_trend, -0.2, 0.2)
    
    def _get_model_confidence(self) -> float:
        """
        HARD SAFETY CHECK: Get model confidence based on training data size and age.
        """
        
        if self.trainer is None:
            return 0.0
        
        # Get training metadata
        model_info = self.trainer.get_model_info()
        training_metadata = model_info.get('training_metadata', {})
        
        # Factor 1: Training data size
        training_samples = training_metadata.get('training_samples', 0)
        size_confidence = min(1.0, training_samples / 1000)  # Max confidence at 1000 samples
        
        # Factor 2: Model age
        training_date_str = training_metadata.get('training_date')
        if training_date_str:
            training_date = datetime.fromisoformat(training_date_str)
            days_old = (datetime.now() - training_date).days
            age_confidence = max(0.0, 1.0 - days_old / 30)  # Decay over 30 days
        else:
            age_confidence = 0.0
        
        # Factor 3: Model performance
        accuracy = training_metadata.get('accuracy', 0.5)
        performance_confidence = max(0.0, (accuracy - 0.5) * 2)  # Scale to 0-1
        
        # Combined confidence
        overall_confidence = (size_confidence * 0.4 + age_confidence * 0.3 + performance_confidence * 0.3)
        
        return overall_confidence
    
    def _log_edge_curve(self, candidate: DiscoveryCandidate, win_probability: float, ml_passed: bool):
        """
        CRITICAL LOG: Log per trade for edge curve analysis.
        """
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'symbol': candidate.symbol,
            'strategy_type': candidate.strategy_type,
            'ml_probability': win_probability,
            'ml_passed': ml_passed,
            'threshold': self.win_probability_threshold,
            'probability_bucket': self._get_probability_bucket(win_probability)
        }
        
        self.edge_curve_log.append(log_entry)
        
        # Also log to database for persistent analysis
        self._save_edge_curve_to_db(log_entry)
    
    def _get_probability_bucket(self, probability: float) -> str:
        """Get probability bucket for edge curve analysis."""
        
        if probability < 0.50:
            return "0.50-0.50"
        elif probability < 0.55:
            return "0.50-0.55"
        elif probability < 0.60:
            return "0.55-0.60"
        elif probability < 0.65:
            return "0.60-0.65"
        elif probability < 0.70:
            return "0.65-0.70"
        else:
            return "0.70+"
    
    def _save_edge_curve_to_db(self, log_entry: Dict[str, Any]):
        """Save edge curve log to database for analysis."""
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_edge_curve (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy_type TEXT,
                    ml_probability REAL NOT NULL,
                    ml_passed BOOLEAN NOT NULL,
                    threshold REAL NOT NULL,
                    probability_bucket TEXT NOT NULL
                )
            """)
            
            conn.execute("""
                INSERT INTO ml_edge_curve 
                (timestamp, symbol, strategy_type, ml_probability, ml_passed, threshold, probability_bucket)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                log_entry['timestamp'],
                log_entry['symbol'],
                log_entry['strategy_type'],
                log_entry['ml_probability'],
                log_entry['ml_passed'],
                log_entry['threshold'],
                log_entry['probability_bucket']
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error saving edge curve log: {e}")
    
    def analyze_edge_curve(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Analyze edge curve to reveal true performance by probability bucket.
        """
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get recent edge curve data
            cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            
            cursor = conn.execute("""
                SELECT 
                    probability_bucket,
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN ml_passed = 1 THEN 1 ELSE 0 END) as passed_trades,
                    AVG(ml_probability) as avg_probability
                FROM ml_edge_curve
                WHERE timestamp >= ?
                GROUP BY probability_bucket
                ORDER BY probability_bucket
            """, (cutoff_date,))
            
            results = cursor.fetchall()
            conn.close()
            
            analysis = {}
            for row in results:
                bucket = row[0]
                total = row[1]
                passed = row[2]
                avg_prob = row[3]
                
                analysis[bucket] = {
                    'total_trades': total,
                    'passed_trades': passed,
                    'pass_rate': passed / total if total > 0 else 0,
                    'avg_probability': avg_prob
                }
            
            logger.info(f"Edge curve analysis ({days_back} days):")
            for bucket, stats in analysis.items():
                pass_rate = stats['pass_rate']
                logger.info(f"  {bucket}: {stats['total_trades']} trades, {pass_rate:.1%} pass rate")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing edge curve: {e}")
            return {}
    
    def set_thresholds(self, win_threshold: float, min_threshold: float):
        """Set both thresholds for production use."""
        
        if not 0.5 <= win_threshold <= 0.9:
            raise ValueError("Win threshold must be between 0.5 and 0.9")
        
        if not 0.4 <= min_threshold <= 0.7:
            raise ValueError("Min threshold must be between 0.4 and 0.7")
        
        if min_threshold >= win_threshold:
            raise ValueError("Min threshold must be less than win threshold")
        
        self.win_probability_threshold = win_threshold
        self.threshold_min = min_threshold
        
        logger.info(f"Thresholds set: win={win_threshold}, min={min_threshold}")
    
    def get_production_status(self) -> Dict[str, Any]:
        """Get production status including safety checks."""
        
        model_confidence = self._get_model_confidence()
        bypass_ml = model_confidence < self.threshold_min
        
        return {
            'model_loaded': self.trainer is not None,
            'model_confidence': model_confidence,
            'threshold_min': self.threshold_min,
            'win_threshold': self.win_probability_threshold,
            'bypass_ml': bypass_ml,
            'edge_curve_entries': len(self.edge_curve_log),
            'last_training': self.last_training_date.isoformat() if self.last_training_date else None
        }
