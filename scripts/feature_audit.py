"""
Feature Audit Script

1-minute feature audit to detect drift, redundancy, and useless features.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import logging
import sys
import os

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def feature_audit(df, feature_cols, target_col="is_win"):
    """
    1-minute feature audit to detect drift, redundancy, and useless features.
    
    Args:
        df: DataFrame with features and target
        feature_cols: List of feature column names
        target_col: Target column name
    """
    
    print("\n=== FEATURE AUDIT ===\n")
    
    # Validate inputs
    missing_cols = [col for col in feature_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing feature columns: {missing_cols}")
        return
    
    if target_col not in df.columns:
        print(f"❌ Missing target column: {target_col}")
        return
    
    if len(df) < 10:
        print(f"❌ Insufficient data: {len(df)} rows (minimum 10)")
        return
    
    # 1. Correlation matrix (redundancy)
    print("🔍 Correlation (feature vs feature):")
    corr = df[feature_cols].corr().abs()
    high_corr = []
    moderate_corr = []

    for i in range(len(feature_cols)):
        for j in range(i+1, len(feature_cols)):
            corr_val = corr.iloc[i, j]
            if corr_val > 0.9:
                high_corr.append((feature_cols[i], feature_cols[j], corr_val))
            elif corr_val > 0.8:
                moderate_corr.append((feature_cols[i], feature_cols[j], corr_val))

    if high_corr:
        print("🔴 High correlation pairs (>0.9):")
        for f1, f2, c in high_corr:
            print(f"  {f1} vs {f2}: {c:.2f}")
    
    if moderate_corr:
        print("🟡 Moderate correlation pairs (0.8-0.9):")
        for f1, f2, c in moderate_corr:
            print(f"  {f1} vs {f2}: {c:.2f}")
    
    if not high_corr and not moderate_corr:
        print("✅ No major redundancy")

    # 2. Feature vs target (signal strength)
    print("\n📊 Feature → target correlation:")
    feature_target_corr = {}
    for col in feature_cols:
        try:
            val = np.corrcoef(df[col], df[target_col])[0, 1]
            feature_target_corr[col] = val
            print(f"  {col}: {val:.3f}")
        except:
            print(f"  {col}: error")
            feature_target_corr[col] = None

    # 3. Distribution check
    print("\n📈 Feature variance:")
    feature_variances = {}
    for col in feature_cols:
        std = df[col].std()
        feature_variances[col] = std
        print(f"  {col}: std={std:.4f}")

    # 4. Simple importance proxy (mean difference)
    print("\n🎯 Mean difference (win vs loss):")
    wins = df[df[target_col] == 1]
    losses = df[df[target_col] == 0]

    mean_differences = {}
    for col in feature_cols:
        try:
            diff = wins[col].mean() - losses[col].mean()
            mean_differences[col] = diff
            print(f"  {col}: Δ={diff:.4f}")
        except:
            print(f"  {col}: error")
            mean_differences[col] = None

    # 5. Summary and recommendations
    print("\n📋 SUMMARY & RECOMMENDATIONS:")
    
    # Check for issues
    issues = []
    
    # Redundancy issues
    if high_corr:
        issues.append(f"🔴 {len(high_corr)} redundant feature pairs")
    
    # Weak features
    weak_features = [col for col, corr in feature_target_corr.items() 
                   if corr is not None and abs(corr) < 0.05]
    if weak_features:
        issues.append(f"🟡 {len(weak_features)} weak features (|corr| < 0.05)")
    
    # Low variance features
    low_var_features = [col for col, var in feature_variances.items() 
                        if var < 0.01]
    if low_var_features:
        issues.append(f"🔴 {len(low_var_features)} near-constant features")
    
    # Dominant features
    max_mean_diff = max(abs(diff) for diff in mean_differences.values() if diff is not None)
    dominant_features = [col for col, diff in mean_differences.items() 
                        if diff is not None and abs(diff) > 0.8 * max_mean_diff]
    if len(dominant_features) == 1 and max_mean_diff > 0.1:
        issues.append(f"🟡 Single dominant feature: {dominant_features[0]}")
    
    if issues:
        print("⚠️ Issues detected:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("✅ No major issues detected")
    
    # Feature recommendations
    print("\n💡 RECOMMENDATIONS:")
    
    # Redundancy recommendations
    if high_corr:
        print("🔧 Consider removing redundant features:")
        for f1, f2, c in high_corr:
            # Keep the one with higher target correlation
            corr1 = feature_target_corr.get(f1, 0)
            corr2 = feature_target_corr.get(f2, 0)
            keep = f1 if abs(corr1) > abs(corr2) else f2
            remove = f2 if keep == f1 else f1
            print(f"  Remove '{remove}' (keep '{keep}' - stronger signal)")
    
    # Weak feature recommendations
    if weak_features:
        print("🔧 Consider removing weak features:")
        for col in weak_features:
            print(f"  '{col}' (no signal)")
    
    # Low variance recommendations
    if low_var_features:
        print("🔧 Remove near-constant features:")
        for col in low_var_features:
            print(f"  '{col}' (std ≈ 0)")
    
    print("\n=== END AUDIT ===\n")
    
    return {
        'high_correlations': high_corr,
        'feature_target_corr': feature_target_corr,
        'feature_variances': feature_variances,
        'mean_differences': mean_differences,
        'issues': issues
    }


def load_training_data(days_back: int = 30) -> pd.DataFrame:
    """Load training data for audit."""
    
    try:
        conn = sqlite3.connect("data/alpha.db")
        
        # Get recent training data
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        query = """
        SELECT 
            trend_strength,
            volatility_regime,
            position_in_range,
            volume_anomaly,
            spy_trend,
            vix_level,
            sector_trend,
            price_momentum,
            is_win
        FROM ml_training_dataset
        WHERE created_at >= ?
        AND trend_strength IS NOT NULL
        ORDER BY created_at DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(cutoff_date,))
        conn.close()
        
        # Encode categorical variables
        if 'volatility_regime' in df.columns:
            df['volatility_regime'] = df['volatility_regime'].map({
                'expansion': 1,
                'contraction': -1,
                'normal': 0
            }).fillna(0)
        
        logger.info(f"Loaded {len(df)} training examples for audit")
        return df
        
    except Exception as e:
        logger.error(f"Error loading training data: {e}")
        return pd.DataFrame()


def load_edge_curve_data(days_back: int = 30) -> pd.DataFrame:
    """Load edge curve data for audit."""
    
    try:
        conn = sqlite3.connect("data/alpha.db")
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        query = """
        SELECT 
            ml_probability as trend_strength,
            CASE WHEN ml_passed = 1 THEN 1 ELSE 0 END as is_win
        FROM ml_edge_curve
        WHERE timestamp >= ?
        ORDER BY timestamp DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(cutoff_date,))
        conn.close()
        
        # Add mock features for demonstration
        if len(df) > 0:
            df['volatility_regime'] = np.random.choice([-1, 0, 1], len(df))
            df['position_in_range'] = np.random.uniform(0, 1, len(df))
            df['volume_anomaly'] = np.random.normal(0, 1, len(df))
            df['spy_trend'] = np.random.normal(0, 0.02, len(df))
            df['vix_level'] = np.random.uniform(0.2, 0.8, len(df))
            df['sector_trend'] = np.random.normal(0, 0.05, len(df))
            df['price_momentum'] = np.random.normal(0, 0.03, len(df))
        
        logger.info(f"Loaded {len(df)} edge curve examples for audit")
        return df
        
    except Exception as e:
        logger.error(f"Error loading edge curve data: {e}")
        return pd.DataFrame()


def create_investigation_list(audit_results: dict) -> list:
    """
    Create investigation list based on audit results.
    
    WARNING: This is a DIAGNOSTIC system, not an optimizer.
    Features are only removed after A/B testing confirms no performance degradation.
    
    Returns list of features to investigate with reasons.
    """
    
    investigation_list = []
    
    # Rule 1: Investigate highly correlated features (threshold relaxed from 0.8 to 0.9)
    high_corr = audit_results.get('high_correlations', [])
    for f1, f2, corr in high_corr:
        if corr > 0.9:  # Only investigate very high correlations
            corr1 = audit_results['feature_target_corr'].get(f1, 0)
            corr2 = audit_results['feature_target_corr'].get(f2, 0)
            keep = f1 if abs(corr1) > abs(corr2) else f2
            investigate = f2 if keep == f1 else f1
            investigation_list.append({
                'feature': investigate,
                'reason': f'High correlation with {keep} (corr={corr:.2f})',
                'action': 'investigate',
                'requires_ab_test': True,
                'priority': 'medium'
            })
    
    # Rule 2: Investigate very weak features (threshold tightened from 0.05 to 0.03)
    feature_target_corr = audit_results.get('feature_target_corr', {})
    mean_differences = audit_results.get('mean_differences', {})
    
    for col, corr in feature_target_corr.items():
        if corr is not None and abs(corr) < 0.03:
            # Check if mean difference is also near zero
            mean_diff = mean_differences.get(col, 0)
            if abs(mean_diff) < 0.02:  # Both conditions must be true
                investigation_list.append({
                    'feature': col,
                    'reason': f'Very weak signal (corr={corr:.3f}, Δ={mean_diff:.4f})',
                    'action': 'investigate',
                    'requires_ab_test': True,
                    'priority': 'low'
                })
    
    # Rule 3: Investigate near-constant features (still valid)
    feature_variances = audit_results.get('feature_variances', {})
    for col, var in feature_variances.items():
        if var < 0.01:
            investigation_list.append({
                'feature': col,
                'reason': f'Near-constant (std={var:.4f})',
                'action': 'investigate',
                'requires_ab_test': True,
                'priority': 'high'  # Constant features are usually useless
            })
    
    return investigation_list


def create_ab_test_recommendation(investigation_list: list) -> dict:
    """
    Create A/B test recommendations for investigated features.
    
    This ensures features are only removed after performance validation.
    """
    
    recommendations = {}
    
    for item in investigation_list:
        feature = item['feature']
        priority = item['priority']
        
        recommendations[feature] = {
            'test_type': 'ab_test',
            'hypothesis': f"Removing '{feature}' will not degrade model performance",
            'test_duration': '30 days minimum',
            'success_criteria': [
                'Expectancy within 5% of baseline',
                'Drawdown no worse than baseline',
                'Stability maintained or improved'
            ],
            'priority': priority,
            'reason': item['reason']
        }
    
    return recommendations


def main():
    """Main audit script."""
    
    logger.info("Starting feature audit...")
    
    # Define core features
    CORE_FEATURES = [
        'trend_strength',
        'volatility_regime',
        'position_in_range',
        'volume_anomaly',
        'spy_trend',
        'vix_level',
        'sector_trend',
        'price_momentum'
    ]
    
    # Try to load training data first
    df = load_training_data(days_back=30)
    
    if len(df) < 10:
        logger.info("Insufficient training data, trying edge curve data...")
        df = load_edge_curve_data(days_back=30)
    
    if len(df) < 10:
        logger.error("Insufficient data for audit")
        return 1
    
    # Run audit
    audit_results = feature_audit(df, CORE_FEATURES, target_col="is_win")
    
    # Generate investigation list (NOT kill list)
    investigation_list = create_investigation_list(audit_results)
    
    if investigation_list:
        print("\n� INVESTIGATION LIST (DIAGNOSTIC ONLY):")
        for item in investigation_list:
            priority_emoji = "🔴" if item['priority'] == 'high' else "🟡" if item['priority'] == 'medium' else "🟢"
            print(f"  {priority_emoji} INVESTIGATE: {item['feature']} - {item['reason']}")
        
        print(f"\n💡 Total features to investigate: {len(investigation_list)}")
        print(f"⚠️  DO NOT REMOVE without A/B testing!")
        
        # Generate A/B test recommendations
        ab_recommendations = create_ab_test_recommendation(investigation_list)
        
        print("\n🧪 A/B TEST RECOMMENDATIONS:")
        for feature, rec in ab_recommendations.items():
            print(f"\n  Feature: {feature}")
            print(f"  Hypothesis: {rec['hypothesis']}")
            print(f"  Duration: {rec['test_duration']}")
            print(f"  Success Criteria:")
            for criteria in rec['success_criteria']:
                print(f"    - {criteria}")
            print(f"  Priority: {rec['priority']}")
        
        print(f"\n🔥 REMEMBER: This is a DIAGNOSTIC system, not an optimizer")
        print(f"   Features only removed after A/B test confirms no performance loss")
        
    else:
        print("\n✅ No features require investigation")
    
    logger.info("Feature audit completed!")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
