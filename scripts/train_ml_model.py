"""
Train ML Model Script

Standalone script to train ML model on historical trade data.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.ml_integration import MLIntegration
from app.ml.training_dataset import TrainingDatasetBuilder

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main training script."""
    
    # Configuration
    db_path = "data/alpha.db"
    model_path = "models/trade_predictor.joblib"
    days_back = 252  # One year of data
    
    logger.info("Starting ML model training...")
    logger.info(f"Database: {db_path}")
    logger.info(f"Model path: {model_path}")
    logger.info(f"Training period: {days_back} days")
    
    try:
        # Initialize ML integration
        ml_integration = MLIntegration(db_path, model_path)
        
        # Create training table if needed
        dataset_builder = TrainingDatasetBuilder(db_path)
        dataset_builder.create_training_table()
        
        # Train new model
        results = ml_integration.train_new_model(days_back=days_back)
        
        if 'error' in results:
            logger.error(f"Training failed: {results['error']}")
            return 1
        
        # Display results
        logger.info("Training completed successfully!")
        logger.info(f"Model type: {results.get('model_type', 'unknown')}")
        logger.info(f"Training samples: {results.get('training_samples', 0)}")
        logger.info(f"Test samples: {results.get('test_samples', 0)}")
        logger.info(f"Accuracy: {results.get('accuracy', 0):.3f}")
        logger.info(f"Precision: {results.get('precision', 0):.3f}")
        logger.info(f"Recall: {results.get('recall', 0):.3f}")
        
        if 'feature_importance' in results:
            logger.info("Feature importance:")
            for feature, importance in results['feature_importance'].items():
                logger.info(f"  {feature}: {importance:.4f}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Training script failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
