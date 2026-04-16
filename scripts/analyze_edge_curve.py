"""
Analyze Edge Curve Script

Analyzes ML performance by probability bucket to reveal true edge curve.
"""

import sys
import os
from datetime import datetime, timedelta
import logging
import sqlite3
import pandas as pd

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.ml_integration_production import ProductionMLIntegration

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def analyze_edge_curve(days_back: int = 30):
    """Analyze edge curve performance by probability bucket."""
    
    logger.info(f"Analyzing edge curve for last {days_back} days...")
    
    try:
        conn = sqlite3.connect("data/alpha.db")
        
        # Get edge curve data
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        query = """
        SELECT 
            probability_bucket,
            COUNT(*) as total_trades,
            SUM(CASE WHEN ml_passed = 1 THEN 1 ELSE 0 END) as passed_trades,
            AVG(ml_probability) as avg_probability,
            MIN(ml_probability) as min_probability,
            MAX(ml_probability) as max_probability
        FROM ml_edge_curve
        WHERE timestamp >= ?
        GROUP BY probability_bucket
        ORDER BY 
            CASE 
                WHEN probability_bucket = '0.50-0.50' THEN 1
                WHEN probability_bucket = '0.50-0.55' THEN 2
                WHEN probability_bucket = '0.55-0.60' THEN 3
                WHEN probability_bucket = '0.60-0.65' THEN 4
                WHEN probability_bucket = '0.65-0.70' THEN 5
                WHEN probability_bucket = '0.70+' THEN 6
            END
        """
        
        cursor = conn.execute(query, (cutoff_date,))
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            logger.info("No edge curve data found")
            return
        
        # Display results
        logger.info("\n" + "="*60)
        logger.info("EDGE CURVE ANALYSIS")
        logger.info("="*60)
        
        total_trades = sum(row[1] for row in results)
        
        for row in results:
            bucket = row[0]
            total = row[1]
            passed = row[2]
            avg_prob = row[3]
            min_prob = row[4]
            max_prob = row[5]
            
            pass_rate = passed / total if total > 0 else 0
            trade_pct = total / total_trades if total_trades > 0 else 0
            
            logger.info(f"\n{bucket}:")
            logger.info(f"  Trades: {total} ({trade_pct:.1%} of total)")
            logger.info(f"  Passed: {passed} ({pass_rate:.1%} pass rate)")
            logger.info(f"  Probability: {avg_prob:.3f} (min: {min_prob:.3f}, max: {max_prob:.3f})")
            
            # Edge interpretation
            if bucket == '0.50-0.55':
                edge_type = "🔴 LOSING" if pass_rate < 0.3 else "🟡 BREAKEVEN"
            elif bucket == '0.55-0.60':
                edge_type = "🟡 BREAKEVEN" if pass_rate < 0.5 else "🟢 GOOD"
            elif bucket == '0.60-0.65':
                edge_type = "🟢 GOOD" if pass_rate > 0.6 else "🟡 BREAKEVEN"
            elif bucket == '0.65-0.70':
                edge_type = "🟢 GOOD" if pass_rate > 0.7 else "🟡 BREAKEVEN"
            elif bucket == '0.70+':
                edge_type = "🟢 BEST" if pass_rate > 0.8 else "🟢 GOOD"
            else:
                edge_type = "❓ UNKNOWN"
            
            logger.info(f"  Edge: {edge_type}")
        
        # Recommendations
        logger.info("\n" + "="*60)
        logger.info("RECOMMENDATIONS")
        logger.info("="*60)
        
        # Find optimal threshold
        best_bucket = None
        best_pass_rate = 0
        
        for row in results:
            bucket = row[0]
            total = row[1]
            passed = row[2]
            pass_rate = passed / total if total > 0 else 0
            
            if pass_rate > best_pass_rate and total >= 10:  # Minimum trades
                best_pass_rate = pass_rate
                best_bucket = bucket
        
        if best_bucket:
            logger.info(f"Best performing bucket: {best_bucket} ({best_pass_rate:.1%} pass rate)")
            
            # Suggest threshold based on best bucket
            if best_bucket == '0.50-0.55':
                logger.info("Suggestion: Consider lowering threshold to 0.55")
            elif best_bucket == '0.55-0.60':
                logger.info("Suggestion: Keep threshold at 0.60")
            elif best_bucket == '0.60-0.65':
                logger.info("Suggestion: Consider raising threshold to 0.60")
            elif best_bucket == '0.65-0.70':
                logger.info("Suggestion: Consider raising threshold to 0.65")
            elif best_bucket == '0.70+':
                logger.info("Suggestion: Consider raising threshold to 0.70")
        
        # Check for concerning patterns
        low_prob_buckets = [r for r in results if r[0] in ['0.50-0.55', '0.55-0.60']]
        if low_prob_buckets:
            low_prob_pass_rate = sum(r[2] for r in low_prob_buckets) / sum(r[1] for r in low_prob_buckets)
            if low_prob_pass_rate < 0.3:
                logger.warning("⚠️  Low probability buckets showing poor performance")
                logger.warning("   Consider raising minimum threshold")
        
        high_prob_buckets = [r for r in results if r[0] in ['0.65-0.70', '0.70+']]
        if high_prob_buckets:
            high_prob_pass_rate = sum(r[2] for r in high_prob_buckets) / sum(r[1] for r in high_prob_buckets)
            if high_prob_pass_rate < 0.5:
                logger.warning("⚠️  High probability buckets underperforming")
                logger.warning("   Model may be overconfident - consider retraining")
        
        return True
        
    except Exception as e:
        logger.error(f"Error analyzing edge curve: {e}")
        return False


def analyze_probability_distribution(days_back: int = 30):
    """Analyze probability distribution for model calibration."""
    
    logger.info(f"Analyzing probability distribution for last {days_back} days...")
    
    try:
        conn = sqlite3.connect("data/alpha.db")
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        query = """
        SELECT 
            ml_probability,
            ml_passed
        FROM ml_edge_curve
        WHERE timestamp >= ?
        ORDER BY ml_probability
        """
        
        cursor = conn.execute(query, (cutoff_date,))
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            logger.info("No probability data found")
            return
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame(results, columns=['probability', 'passed'])
        
        # Calculate calibration metrics
        df['probability_bucket'] = pd.cut(df['probability'], 
                                        bins=[0.5, 0.55, 0.6, 0.65, 0.7, 1.0],
                                        labels=['0.50-0.55', '0.55-0.60', '0.60-0.65', '0.65-0.70', '0.70+'])
        
        calibration = df.groupby('probability_bucket').agg({
            'probability': 'mean',
            'passed': 'mean',
            'passed': 'count'
        }).rename(columns={'passed': 'pass_rate'})
        
        logger.info("\n" + "="*60)
        logger.info("PROBABILITY CALIBRATION")
        logger.info("="*60)
        
        for bucket, row in calibration.iterrows():
            avg_prob = row['probability']
            pass_rate = row['pass_rate']
            count = row['passed']
            
            logger.info(f"{bucket}:")
            logger.info(f"  Avg Probability: {avg_prob:.3f}")
            logger.info(f"  Pass Rate: {pass_rate:.3f}")
            logger.info(f"  Calibration Error: {abs(avg_prob - pass_rate):.3f}")
            logger.info(f"  Count: {count}")
        
        # Overall calibration
        overall_prob = df['probability'].mean()
        overall_pass_rate = df['passed'].mean()
        calibration_error = abs(overall_prob - overall_pass_rate)
        
        logger.info(f"\nOverall:")
        logger.info(f"  Avg Probability: {overall_prob:.3f}")
        logger.info(f"  Pass Rate: {overall_pass_rate:.3f}")
        logger.info(f"  Calibration Error: {calibration_error:.3f}")
        
        if calibration_error > 0.1:
            logger.warning("⚠️  Poor model calibration detected")
            logger.warning("   Consider retraining with more data")
        elif calibration_error > 0.05:
            logger.info("🟡 Moderate calibration - monitor closely")
        else:
            logger.info("✅ Good calibration")
        
        return True
        
    except Exception as e:
        logger.error(f"Error analyzing probability distribution: {e}")
        return False


def main():
    """Main analysis script."""
    
    logger.info("Starting edge curve analysis...")
    
    # Analyze edge curve
    if not analyze_edge_curve(days_back=30):
        return 1
    
    # Analyze probability distribution
    if not analyze_probability_distribution(days_back=30):
        return 1
    
    logger.info("\n🎉 Edge curve analysis completed!")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
