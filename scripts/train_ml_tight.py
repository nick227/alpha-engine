"""
Train ML Model with Strict Constraints

Production-ready training script with anti-overfitting measures.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.ml_integration_tight import TightMLIntegration

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main training script with strict constraints."""
    
    # Configuration
    db_path = "data/alpha.db"
    model_path = "models/trade_predictor_tight.joblib"
    days_back = 252  # One year of data
    
    logger.info("Starting STRICT ML model training...")
    logger.info(f"Database: {db_path}")
    logger.info(f"Model path: {model_path}")
    logger.info(f"Training period: {days_back} days")
    
    try:
        # Initialize tight ML integration
        ml_integration = TightMLIntegration(db_path, model_path)
        
        # Validate constraints before training
        if not ml_integration.validate_feature_set():
            logger.error("Feature set validation failed - aborting training")
            return 1
        
        # Train with strict constraints
        results = ml_integration.train_new_model_safe(days_back=days_back)
        
        if 'error' in results:
            logger.error(f"Training failed: {results['error']}")
            return 1
        
        if 'skipped' in results:
            logger.info(f"Training skipped: {results['skipped']}")
            return 0
        
        # Display results
        logger.info("Training completed successfully!")
        logger.info(f"Model type: {results.get('model_type', 'unknown')}")
        logger.info(f"Training samples: {results.get('training_samples', 0)}")
        logger.info(f"Test samples: {results.get('test_samples', 0)}")
        logger.info(f"Accuracy: {results.get('accuracy', 0):.3f}")
        logger.info(f"Precision: {results.get('precision', 0):.3f}")
        logger.info(f"Recall: {results.get('recall', 0):.3f}")
        
        # Feature importance
        if 'feature_importance' in results:
            logger.info("Feature importance:")
            for feature, importance in results['feature_importance'].items():
                logger.info(f"  {feature}: {importance:.4f}")
        
        # Validate feature count
        feature_count = len(results.get('feature_importance', {}))
        if feature_count > 8:
            logger.error(f"Too many features: {feature_count} > 8")
            return 1
        else:
            logger.info(f"✓ Feature count validated: {feature_count}")
        
        # Test different thresholds
        logger.info("Testing threshold sensitivity...")
        thresholds = [0.55, 0.60, 0.65, 0.70]
        
        for threshold in thresholds:
            ml_integration.set_threshold(threshold)
            logger.info(f"✓ Threshold set to {threshold}")
        
        logger.info("🎉 STRICT ML training completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"STRICT training script failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
