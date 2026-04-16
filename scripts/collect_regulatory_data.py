"""
SEC Regulatory Data Collection Script

Collects and processes SEC filings for alpha engine.
"""

import os
import sys
import logging
from datetime import datetime

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.regulatory.sec_ingest import run_sec_collection

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main regulatory data collection script."""
    
    # Check for API key
    api_key = os.getenv('SEC_API_KEY')
    if not api_key:
        logger.error("SEC_API_KEY environment variable not set")
        logger.error("Set it with: set SEC_API_KEY=your_api_key")
        logger.error("Or get key from: https://sec-api.io/")
        return 1
    
    # Collection parameters
    days_back = 7  # Default to 7 days
    
    if len(sys.argv) > 1:
        try:
            days_back = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid days_back parameter. Using default 7.")
    
    logger.info(f"Starting SEC data collection for last {days_back} days")
    
    try:
        # Run collection
        results = run_sec_collection(days_back, api_key)
        
        # Report results
        logger.info("SEC Collection Results:")
        logger.info(f"  Form 4 (insider trades): {results['form4']}")
        logger.info(f"  8-K (corporate events): {results['8k']}")
        logger.info(f"  10-Q/10-K (fundamentals): {results['10q_10k']}")
        logger.info(f"  Total events: {results['total']}")
        
        if results['total'] > 0:
            logger.info("✅ Regulatory data collection successful")
            return 0
        else:
            logger.warning("⚠️ No regulatory events collected")
            return 0
            
    except Exception as e:
        logger.error(f"SEC collection failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
