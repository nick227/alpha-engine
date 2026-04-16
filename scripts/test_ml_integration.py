"""
Test ML Integration Script

Test script to validate ML integration with existing pipeline.
"""

import sys
import os
from datetime import datetime
import logging

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.ml_integration import MLIntegration
from app.discovery.strategies.registry import score_candidates
from app.ml.feature_builder import FeatureBuilder

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main test script."""
    
    # Configuration
    db_path = "data/alpha.db"
    model_path = "models/trade_predictor.joblib"
    
    logger.info("Testing ML integration...")
    
    try:
        # Initialize ML integration
        ml_integration = MLIntegration(db_path, model_path)
        
        # Check model status
        model_status = ml_integration.get_model_status()
        logger.info(f"Model status: {model_status}")
        
        if not model_status.get('model_loaded', False):
            logger.error("No ML model found - please train model first")
            return 1
        
        # Create mock features for testing
        mock_features = {
            'AAPL': {
                'return_5d': 0.03,
                'return_20d': 0.08,
                'return_63d': 0.15,
                'volatility_20d': 0.025,
                'price_percentile_252d': 0.7,
                'volume_zscore_20d': 1.5,
                'sector': 'TECH'
            },
            'MSFT': {
                'return_5d': -0.02,
                'return_20d': -0.05,
                'return_63d': -0.10,
                'volatility_20d': 0.018,
                'price_percentile_252d': 0.3,
                'volume_zscore_20d': 0.8,
                'sector': 'TECH'
            },
            'SPY': {
                'return_5d': 0.01,
                'return_20d': 0.03,
                'return_63d': 0.06,
                'volatility_20d': 0.015,
                'price_percentile_252d': 0.5,
                'volume_zscore_20d': 0.0,
                'sector': 'ETF'
            }
        }
        
        # Test ML prediction
        logger.info("Testing ML predictions...")
        
        for symbol, features in mock_features.items():
            ml_features = ml_integration._extract_features_for_candidate(
                type('MockCandidate', (), {'symbol': symbol})(), features
            )
            
            win_prob = ml_integration.trainer.predict_win_probability(ml_features)
            logger.info(f"{symbol}: ML win probability = {win_prob:.3f}")
        
        # Test ML scoring
        logger.info("\nTesting ML scoring...")
        ml_scores = ml_integration.get_ml_scores_for_candidates(
            [type('MockCandidate', (), {'symbol': s})() for s in mock_features.keys()],
            mock_features
        )
        
        for symbol, score in ml_scores.items():
            logger.info(f"{symbol}: ML score = {score:.3f}")
        
        # Test model validation (if we have test data)
        test_data_path = "data/test_data.csv"
        if os.path.exists(test_data_path):
            logger.info("\nTesting model validation...")
            validation_results = ml_integration.trainer.validate_model(test_data_path)
            
            if 'error' not in validation_results:
                logger.info("Validation results:")
                logger.info(f"  Accuracy: {validation_results.get('accuracy', 0):.3f}")
                logger.info(f"  Precision: {validation_results.get('precision', 0):.3f}")
                logger.info(f"  Recall: {validation_results.get('recall', 0):.3f}")
                logger.info(f"  F1 Score: {validation_results.get('f1_score', 0):.3f}")
                if validation_results.get('roc_auc'):
                    logger.info(f"  ROC AUC: {validation_results['roc_auc']:.3f}")
            else:
                logger.error(f"Validation failed: {validation_results['error']}")
        else:
            logger.info("No test data found - skipping validation")
        
        logger.info("\nML integration test completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"ML integration test failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
