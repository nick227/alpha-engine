"""
Expectancy Bucket Analysis

Analyzes ML performance by probability bucket to prove ML is working.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import sys
import os
import matplotlib.pyplot as plt

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


def analyze_expectancy_buckets(days_back: int = 30):
    """
    Analyze expectancy per trade bucket.
    
    This proves ML actually separates good from bad trades.
    """
    
    print("🎯 EXPECTANCY BUCKET ANALYSIS")
    print("=" * 60)
    print("Proving ML separates good from bad trades...")
    
    try:
        conn = sqlite3.connect("data/alpha.db")
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        # Get ML predictions and outcomes
        query = """
        SELECT 
            ml_probability,
            ml_passed,
            CASE WHEN outcome_return > 0 THEN 1 ELSE 0 END as is_win,
            outcome_return as trade_return,
            symbol,
            strategy_type
        FROM ml_edge_curve mec
        JOIN trades t ON mec.symbol = t.symbol 
        WHERE mec.timestamp >= ?
        AND mec.ml_probability IS NOT NULL
        AND t.outcome_return IS NOT NULL
        ORDER BY mec.ml_probability
        """
        
        df = pd.read_sql_query(query, conn, params=(cutoff_date,))
        conn.close()
        
        if len(df) < 20:
            print(f"❌ Insufficient data: {len(df)} trades (need 20+)")
            return
        
        print(f"📊 Analyzing {len(df)} trades over {days_back} days")
        
        # Create probability buckets
        buckets = [
            (0.50, 0.55, "0.50–0.55"),
            (0.55, 0.60, "0.55–0.60"),
            (0.60, 0.65, "0.60–0.65"),
            (0.65, 0.70, "0.65–0.70"),
            (0.70, 1.00, "0.70+")
        ]
        
        bucket_analysis = []
        
        for min_prob, max_prob, bucket_name in buckets:
            bucket_data = df[
                (df['ml_probability'] >= min_prob) & 
                (df['ml_probability'] < max_prob)
            ]
            
            if len(bucket_data) == 0:
                continue
            
            # Calculate metrics for this bucket
            total_trades = len(bucket_data)
            win_rate = bucket_data['is_win'].mean()
            avg_return = bucket_data['trade_return'].mean()
            expectancy = win_rate * avg_return - (1 - win_rate) * abs(avg_return)
            
            # Simplified expectancy calculation
            expectancy = bucket_data['trade_return'].mean()
            
            bucket_analysis.append({
                'bucket': bucket_name,
                'min_prob': min_prob,
                'max_prob': max_prob,
                'total_trades': total_trades,
                'win_rate': win_rate,
                'avg_return': avg_return,
                'expectancy': expectancy,
                'avg_probability': bucket_data['ml_probability'].mean()
            })
        
        # Display results
        print(f"\n📈 EXPECTANCY BY PROBABILITY BUCKET:")
        print(f"Bucket     | Trades | Confidence | Win Rate | Avg Return | Expectancy | Interpretation")
        print(f"-----------|---------|------------|-----------|------------|------------|----------------")
        
        for bucket in bucket_analysis:
            bucket_name = bucket['bucket']
            trades = bucket['total_trades']
            confidence = bucket['confidence']
            win_rate = bucket['win_rate']
            avg_return = bucket['avg_return']
            expectancy = bucket['expectancy']
            
            # Interpretation
            if expectancy < -0.01:
                interpretation = "🔴 NEGATIVE"
            elif expectancy < 0.005:
                interpretation = "🟡 FLAT"
            elif expectancy < 0.02:
                interpretation = "🟢 POSITIVE"
            else:
                interpretation = "🚀 STRONG"
            
            # Confidence indicator
            conf_indicator = "🔒" if confidence == 'HIGH' else "🔓"
            
            print(f"{bucket_name:11} | {trades:7d} | {conf_indicator:10} | {win_rate:9.1%} | {avg_return:10.3%} | {expectancy:10.3%} | {interpretation}")
        
        # Overall analysis
        total_trades = sum(b['total_trades'] for b in bucket_analysis)
        overall_expectancy = df['trade_return'].mean()
        
        print(f"\n📋 OVERALL ANALYSIS:")
        print(f"Total trades: {total_trades}")
        print(f"Overall expectancy: {overall_expectancy:.3%}")
        
        # Check if ML is working
        ml_working = False
        monotonicity_score = 0
        
        # MONOTONICITY CHECK: Does expectancy increase as probability increases?
        expectancies = [b['expectancy'] for b in bucket_analysis]
        if len(expectancies) >= 3:
            # Check monotonic improvement
            monotonic_increases = 0
            monotonic_decreases = 0
            
            for i in range(1, len(expectancies)):
                if expectancies[i] > expectancies[i-1]:
                    monotonic_increases += 1
                elif expectancies[i] < expectancies[i-1]:
                    monotonic_decreases += 1
            
            # Score monotonicity (0-1 scale)
            total_comparisons = len(expectancies) - 1
            monotonicity_score = monotonic_increases / total_comparisons
            
            # ML is working if mostly monotonic
            if monotonicity_score >= 0.6:  # At least 60% monotonic
                ml_working = True
        
        # Check separation
        positive_buckets = [b for b in bucket_analysis if b['expectancy'] > 0.01]
        negative_buckets = [b for b in bucket_analysis if b['expectancy'] < -0.01]
        
        if len(positive_buckets) > 0 and len(negative_buckets) > 0:
            # Check if high probability buckets are positive
            high_positive = any(b['expectancy'] > 0.01 for b in bucket_analysis[-2:])
            if high_positive:
                ml_working = True
        
        # MINIMUM BUCKET SIZE: Mark low confidence buckets
        min_bucket_size = 15  # Minimum trades per bucket
        for bucket in bucket_analysis:
            bucket['confidence'] = 'HIGH' if bucket['total_trades'] >= min_bucket_size else 'LOW'
        
        # ML OFF SWITCH: Check if ML is helping
        baseline_expectancy = df['trade_return'].mean()
        filtered_trades = df[df['ml_passed'] == True]
        filtered_expectancy = filtered_trades['trade_return'].mean() if len(filtered_trades) > 0 else baseline_expectancy
        
        ml_is_helping = filtered_expectancy > baseline_expectancy
        ml_off_switch = not ml_is_helping or not ml_working
        
        print(f"\n🎯 ML EFFECTIVENESS:")
        if ml_working:
            print("✅ ML is WORKING - separates good from bad trades")
            print(f"   Monotonicity score: {monotonicity_score:.1%}")
            print("   Higher probability → higher expectancy")
        else:
            print("❌ ML NOT WORKING - no clear separation")
            print(f"   Monotonicity score: {monotonicity_score:.1%}")
            print("   Probability buckets show random performance")
        
        # ML OFF SWITCH STATUS
        print(f"\n🔴 ML OFF SWITCH:")
        if ml_off_switch:
            print("⚠️  ML OFF SWITCH ACTIVATED")
            print(f"   Baseline expectancy: {baseline_expectancy:.3%}")
            print(f"   ML-filtered expectancy: {filtered_expectancy:.3%}")
            print("   ACTION: Consider bypassing ML until fixed")
        else:
            print("✅ ML OFF SWITCH: OFF (ML is helping)")
            print(f"   ML improvement: {filtered_expectancy - baseline_expectancy:+.3%}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        # Find optimal threshold
        best_bucket = max(bucket_analysis, key=lambda x: x['expectancy'])
        print(f"Best performing bucket: {best_bucket['bucket']} ({best_bucket['expectancy']:.3%})")
        
        # Threshold suggestions
        if best_bucket['min_prob'] >= 0.65:
            print("Consider raising threshold to 0.65+")
        elif best_bucket['min_prob'] >= 0.60:
            print("Current threshold (0.60) looks good")
        elif best_bucket['min_prob'] >= 0.55:
            print("Consider lowering threshold to 0.55")
        else:
            print("Consider lowering threshold to 0.50")
        
        # Check for edge cases
        flat_performance = all(abs(b['expectancy']) < 0.005 for b in bucket_analysis)
        if flat_performance:
            print("⚠️  All buckets showing flat performance")
            print("   ML may need retraining or feature adjustment")
        
        negative_high_prob = any(b['expectancy'] < 0 for b in bucket_analysis[-2:])
        if negative_high_prob:
            print("⚠️  High probability buckets showing negative expectancy")
            print("   ML may be overconfident - check calibration")
        
        # Low confidence buckets
        low_conf_buckets = [b for b in bucket_analysis if b['confidence'] == 'LOW']
        if low_conf_buckets:
            print(f"⚠️  {len(low_conf_buckets)} buckets with LOW confidence (< 15 trades)")
            for bucket in low_conf_buckets:
                print(f"   {bucket['bucket']}: {bucket['total_trades']} trades")
        
        # ML OFF recommendation
        if ml_off_switch:
            print("\n🚨 CRITICAL RECOMMENDATION:")
            print("   SET bypass_ml = True until ML is fixed")
            print("   ML is currently hurting performance")
        
        return bucket_analysis
        
    except Exception as e:
        print(f"❌ Error analyzing expectancy buckets: {e}")
        return []


def create_expectancy_chart(bucket_analysis):
    """Create a simple chart showing expectancy by probability bucket."""
    
    try:
        buckets = [b['bucket'] for b in bucket_analysis]
        expectancies = [b['expectancy'] for b in bucket_analysis]
        
        plt.figure(figsize=(10, 6))
        plt.bar(buckets, expectancies, color=['red' if e < 0 else 'green' for e in expectancies])
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        plt.title('Expectancy by Probability Bucket')
        plt.xlabel('Probability Bucket')
        plt.ylabel('Expectancy')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Save chart
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"logs/expectancy_buckets_{timestamp}.png"
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"\n📊 Chart saved to: {filename}")
        
    except Exception as e:
        print(f"Error creating chart: {e}")


def main():
    """Main expectancy bucket analysis."""
    
    days_back = 30
    if len(sys.argv) > 1:
        days_back = int(sys.argv[1])
    
    # Run analysis
    bucket_analysis = analyze_expectancy_buckets(days_back)
    
    if bucket_analysis:
        # Create chart
        create_expectancy_chart(bucket_analysis)
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
