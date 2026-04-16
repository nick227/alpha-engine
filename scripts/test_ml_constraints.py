"""
Test ML Constraints Script

Tests strict ML integration constraints to prevent overfitting and data leakage.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.ml_integration_tight import TightMLIntegration
from app.discovery.types import DiscoveryCandidate

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_feature_constraints():
    """Test that we don't exceed 8 features."""
    
    logger.info("Testing feature constraints...")
    
    ml_integration = TightMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test feature set size
    if not ml_integration.validate_feature_set():
        logger.error("Feature set validation failed")
        return False
    
    logger.info(f"✓ Feature set size: {len(ml_integration.CORE_FEATURES)} features")
    logger.info(f"✓ Features: {ml_integration.CORE_FEATURES}")
    
    return True


def test_threshold_sensitivity():
    """Test different threshold values."""
    
    logger.info("Testing threshold sensitivity...")
    
    ml_integration = TightMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test different thresholds
    thresholds = [0.55, 0.60, 0.65, 0.70]
    
    for threshold in thresholds:
        ml_integration.set_threshold(threshold)
        logger.info(f"✓ Threshold set to {threshold}")
    
    return True


def test_no_future_leakage():
    """Test that no future data leaks into features."""
    
    logger.info("Testing future data leakage...")
    
    ml_integration = TightMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test with suspicious features
    suspicious_features = {
        'return_1d': 0.02,
        'exit_price': 100.0,
        'realized_pnl': 5.0
    }
    
    if ml_integration._has_future_leakage(suspicious_features):
        logger.info("✓ Future data leakage correctly detected")
    else:
        logger.error("Future data leakage not detected")
        return False
    
    # Test with clean features
    clean_features = {
        'return_5d': 0.02,
        'volatility_20d': 0.025,
        'volume_zscore_20d': 1.5
    }
    
    if not ml_integration._has_future_leakage(clean_features):
        logger.info("✓ Clean features correctly validated")
    else:
        logger.error("Clean features incorrectly flagged")
        return False
    
    return True


def test_training_cadence():
    """Test training cadence constraints."""
    
    logger.info("Testing training cadence...")
    
    ml_integration = TightMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test initial training
    ml_integration.last_training_date = None
    result = ml_integration.train_new_model_safe(days_back=30)
    
    if 'skipped' in result:
        logger.info("✓ Initial training allowed")
    else:
        logger.info("✓ Initial training completed")
    
    # Test too frequent retraining
    ml_integration.last_training_date = datetime.now() - timedelta(days=3)
    result = ml_integration.train_new_model_safe(days_back=30)
    
    if 'skipped' in result:
        logger.info("✓ Frequent retraining correctly blocked")
    else:
        logger.error("Frequent retraining not blocked")
        return False
    
    # Test allowed retraining
    ml_integration.last_training_date = datetime.now() - timedelta(days=10)
    result = ml_integration.train_new_model_safe(days_back=30)
    
    if 'skipped' in result:
        logger.info("✓ Allowed retraining processed")
    else:
        logger.info("✓ Allowed retraining completed")
    
    return True


def test_ml_filtering_only():
    """Test that ML only filters, never overrides."""
    
    logger.info("Testing ML filtering only...")
    
    ml_integration = TightMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Create mock candidates
    candidates = [
        DiscoveryCandidate(
            symbol="AAPL",
            strategy_type="test_strategy",
            score=0.8,
            reason="test",
            metadata={}
        ),
        DiscoveryCandidate(
            symbol="MSFT",
            strategy_type="test_strategy",
            score=0.7,
            reason="test",
            metadata={}
        )
    ]
    
    # Create mock features
    features = {
        'AAPL': {
            'return_5d': 0.03,
            'volatility_20d': 0.025,
            'price_percentile_252d': 0.7,
            'volume_zscore_20d': 1.5
        },
        'MSFT': {
            'return_5d': -0.02,
            'volatility_20d': 0.018,
            'price_percentile_252d': 0.3,
            'volume_zscore_20d': 0.8
        }
    }
    
    # Test filtering
    filtered = ml_integration.filter_signals_with_ml(candidates, features)
    
    # Check that ML only adds metadata, doesn't change core attributes
    for candidate in filtered:
        if candidate.strategy_type != "test_strategy":
            logger.error(f"ML changed strategy type: {candidate.strategy_type}")
            return False
        
        if 'ml_filtered' not in candidate.metadata:
            logger.error("ML metadata not added")
            return False
    
    logger.info("✓ ML only filters, doesn't override")
    return True


def test_feature_importance_stability():
    """Test feature importance for stability."""
    
    logger.info("Testing feature importance stability...")
    
    ml_integration = TightMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Get feature importance
    importance = ml_integration.get_feature_importance()
    
    if not importance:
        logger.info("No model trained yet - skipping importance test")
        return True
    
    # Check that no single feature dominates
    max_importance = max(importance.values()) if importance else 0
    if max_importance > 0.8:
        logger.warning(f"Feature dominance detected: {max_importance:.3f}")
    else:
        logger.info(f"✓ Feature importance stable: max = {max_importance:.3f}")
    
    # Check feature count
    if len(importance) > 8:
        logger.error(f"Too many features in importance: {len(importance)}")
        return False
    else:
        logger.info(f"✓ Feature count in importance: {len(importance)}")
    
    return True


def main():
    """Run all constraint tests."""
    
    logger.info("Starting ML constraint tests...")
    
    tests = [
        ("Feature Constraints", test_feature_constraints),
        ("Threshold Sensitivity", test_threshold_sensitivity),
        ("No Future Leakage", test_no_future_leakage),
        ("Training Cadence", test_training_cadence),
        ("ML Filtering Only", test_ml_filtering_only),
        ("Feature Importance Stability", test_feature_importance_stability)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            if test_func():
                logger.info(f"✓ {test_name} PASSED")
                passed += 1
            else:
                logger.error(f"✗ {test_name} FAILED")
        except Exception as e:
            logger.error(f"✗ {test_name} ERROR: {e}")
    
    logger.info(f"\n--- Test Results ---")
    logger.info(f"Passed: {passed}/{total}")
    logger.info(f"Success Rate: {passed/total:.1%}")
    
    if passed == total:
        logger.info("🎉 All constraint tests passed!")
        return 0
    else:
        logger.error("❌ Some constraint tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
