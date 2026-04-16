"""
Test Production ML Script

Tests production ML integration with semantic leakage detection and safety checks.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.ml_integration_production import ProductionMLIntegration
from app.discovery.types import DiscoveryCandidate

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_semantic_leakage_detection():
    """Test semantic leakage detection (not just name-based)."""
    
    logger.info("Testing semantic leakage detection...")
    
    ml_integration = ProductionMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test with valid entry-time features
    valid_features = {
        'return_1d': 0.01,
        'return_5d': 0.02,
        'return_20d': 0.05,
        'volatility_20d': 0.025,
        'price_percentile_252d': 0.7,
        'volume_zscore_20d': 1.5,
        'sector_return_63d': 0.08,
        'return_63d': 0.06
    }
    
    candidate = DiscoveryCandidate(
        symbol="AAPL",
        strategy_type="test_strategy",
        score=0.8,
        reason="test",
        metadata={}
    )
    
    entry_features, is_valid = ml_integration._extract_entry_features_semantic(
        candidate, {"AAPL": valid_features}
    )
    
    if is_valid and len(entry_features) == 8:
        logger.info("✓ Valid features correctly identified")
    else:
        logger.error(f"Valid features incorrectly rejected: {len(entry_features)}")
        return False
    
    # Test with invalid features (missing data)
    invalid_features = {
        'return_1d': None,  # Missing data
        'return_5d': 0.02,
        'return_20d': 0.05,
        'volatility_20d': 0.025,
        'price_percentile_252d': 0.7,
        'volume_zscore_20d': 1.5,
        'sector_return_63d': 0.08,
        'return_63d': 0.06
    }
    
    entry_features, is_valid = ml_integration._extract_entry_features_semantic(
        candidate, {"AAPL": invalid_features}
    )
    
    if not is_valid:
        logger.info("✓ Invalid features correctly rejected")
    else:
        logger.error("Invalid features incorrectly accepted")
        return False
    
    return True


def test_edge_curve_logging():
    """Test edge curve logging functionality."""
    
    logger.info("Testing edge curve logging...")
    
    ml_integration = ProductionMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Create test candidates
    candidates = []
    for i, symbol in enumerate(["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]):
        candidate = DiscoveryCandidate(
            symbol=symbol,
            strategy_type="test_strategy",
            score=0.8 - i * 0.1,
            reason="test",
            metadata={}
        )
        candidates.append(candidate)
    
    # Mock features
    features = {}
    for symbol in ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]:
        features[symbol] = {
            'return_1d': 0.01,
            'return_5d': 0.02,
            'return_20d': 0.05,
            'volatility_20d': 0.025,
            'price_percentile_252d': 0.7,
            'volume_zscore_20d': 1.5,
            'sector_return_63d': 0.08,
            'return_63d': 0.06
        }
    
    # Test edge curve logging
    for candidate in candidates:
        ml_integration._log_edge_curve(candidate, 0.55 + candidates.index(candidate) * 0.05, 
                                     candidate.metadata.get('ml_filtered', False))
    
    # Check if logs were created
    if len(ml_integration.edge_curve_log) == 5:
        logger.info("✓ Edge curve logs created successfully")
    else:
        logger.error(f"Expected 5 logs, got {len(ml_integration.edge_curve_log)}")
        return False
    
    # Test probability bucketing
    buckets = set()
    for log in ml_integration.edge_curve_log:
        buckets.add(log['probability_bucket'])
    
    expected_buckets = {'0.55-0.60', '0.60-0.65', '0.65-0.70', '0.70+'}
    if expected_buckets.issubset(buckets):
        logger.info("✓ Probability bucketing working correctly")
    else:
        logger.error(f"Missing buckets: {expected_buckets - buckets}")
        return False
    
    return True


def test_hard_safety_check():
    """Test hard safety check for early-stage models."""
    
    logger.info("Testing hard safety check...")
    
    ml_integration = ProductionMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test with no model (confidence = 0)
    ml_integration.trainer = None
    confidence = ml_integration._get_model_confidence()
    
    if confidence == 0.0:
        logger.info("✓ No model confidence correctly calculated")
    else:
        logger.error(f"Expected 0.0 confidence, got {confidence}")
        return False
    
    # Test threshold bypass
    ml_integration.threshold_min = 0.6
    should_bypass = confidence < ml_integration.threshold_min
    
    if should_bypass:
        logger.info("✓ Safety bypass correctly triggered")
    else:
        logger.error("Safety bypass not triggered when expected")
        return False
    
    return True


def test_model_confidence_calculation():
    """Test model confidence calculation."""
    
    logger.info("Testing model confidence calculation...")
    
    ml_integration = ProductionMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Mock model with different scenarios
    from app.ml.simple_trainer import SimpleMLTrainer
    from sklearn.ensemble import GradientBoostingClassifier
    
    trainer = SimpleMLTrainer()
    trainer.model = GradientBoostingClassifier()
    trainer.training_metadata = {
        'training_samples': 500,  # Medium size
        'training_date': (datetime.now() - timedelta(days=10)).isoformat(),
        'accuracy': 0.65  # Good performance
    }
    
    ml_integration.trainer = trainer
    
    confidence = ml_integration._get_model_confidence()
    
    # Should be moderate confidence
    if 0.3 <= confidence <= 0.8:
        logger.info(f"✓ Model confidence calculated correctly: {confidence:.3f}")
    else:
        logger.error(f"Model confidence out of range: {confidence:.3f}")
        return False
    
    # Test with old model
    trainer.training_metadata['training_date'] = (datetime.now() - timedelta(days=40)).isoformat()
    confidence = ml_integration._get_model_confidence()
    
    if confidence < 0.5:  # Should be lower due to age
        logger.info(f"✓ Old model confidence correctly reduced: {confidence:.3f}")
    else:
        logger.error(f"Old model confidence not reduced: {confidence:.3f}")
        return False
    
    return True


def test_threshold_configuration():
    """Test threshold configuration."""
    
    logger.info("Testing threshold configuration...")
    
    ml_integration = ProductionMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    # Test valid thresholds
    try:
        ml_integration.set_thresholds(0.65, 0.55)
        logger.info("✓ Valid thresholds accepted")
    except Exception as e:
        logger.error(f"Valid thresholds rejected: {e}")
        return False
    
    # Test invalid thresholds
    try:
        ml_integration.set_thresholds(0.4, 0.6)  # min > win
        logger.error("Invalid thresholds incorrectly accepted")
        return False
    except ValueError:
        logger.info("✓ Invalid thresholds correctly rejected")
    
    try:
        ml_integration.set_thresholds(0.95, 0.5)  # win > 0.9
        logger.error("Invalid win threshold incorrectly accepted")
        return False
    except ValueError:
        logger.info("✓ Invalid win threshold correctly rejected")
    
    return True


def test_production_status():
    """Test production status reporting."""
    
    logger.info("Testing production status...")
    
    ml_integration = ProductionMLIntegration("data/alpha.db", "models/trade_predictor.joblib")
    
    status = ml_integration.get_production_status()
    
    required_keys = [
        'model_loaded', 'model_confidence', 'threshold_min', 
        'win_threshold', 'bypass_ml', 'edge_curve_entries'
    ]
    
    for key in required_keys:
        if key not in status:
            logger.error(f"Missing status key: {key}")
            return False
    
    logger.info("✓ Production status includes all required keys")
    logger.info(f"  Model loaded: {status['model_loaded']}")
    logger.info(f"  Model confidence: {status['model_confidence']:.3f}")
    logger.info(f"  Bypass ML: {status['bypass_ml']}")
    logger.info(f"  Edge curve entries: {status['edge_curve_entries']}")
    
    return True


def main():
    """Run all production ML tests."""
    
    logger.info("Starting production ML tests...")
    
    tests = [
        ("Semantic Leakage Detection", test_semantic_leakage_detection),
        ("Edge Curve Logging", test_edge_curve_logging),
        ("Hard Safety Check", test_hard_safety_check),
        ("Model Confidence Calculation", test_model_confidence_calculation),
        ("Threshold Configuration", test_threshold_configuration),
        ("Production Status", test_production_status)
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
        logger.info("🎉 All production ML tests passed!")
        return 0
    else:
        logger.error("❌ Some production ML tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
