"""
Quick Feature Check

Ultra-fast feature audit for daily monitoring.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import sys
import os

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


def quick_feature_check():
    """Quick 30-second feature check for daily monitoring."""
    
    print("🚀 QUICK FEATURE CHECK (30 seconds)")
    print("=" * 50)
    
    # Core features
    features = [
        'trend_strength',
        'volatility_regime', 
        'position_in_range',
        'volume_anomaly',
        'spy_trend',
        'vix_level',
        'sector_trend',
        'price_momentum'
    ]
    
    try:
        # Load recent data
        conn = sqlite3.connect("data/alpha.db")
        cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()
        
        query = """
        SELECT trend_strength, volatility_regime, position_in_range,
               volume_anomaly, spy_trend, vix_level, sector_trend,
               price_momentum, is_win
        FROM ml_training_dataset
        WHERE created_at >= ?
        AND trend_strength IS NOT NULL
        LIMIT 100
        """
        
        df = pd.read_sql_query(query, conn, params=(cutoff_date,))
        conn.close()
        
        if len(df) < 10:
            print("❌ Insufficient recent data")
            return
        
        # Encode categorical
        if 'volatility_regime' in df.columns:
            df['volatility_regime'] = df['volatility_regime'].map({
                'expansion': 1, 'contraction': -1, 'normal': 0
            }).fillna(0)
        
        # Quick checks
        print(f"📊 Data points: {len(df)}")
        
        # 1. Feature variance
        print("\n📈 Variance check:")
        low_var = []
        for col in features:
            var = df[col].var()
            status = "✅" if var > 0.01 else "⚠️"
            print(f"  {col}: {var:.4f} {status}")
            if var <= 0.01:
                low_var.append(col)
        
        # 2. Correlation check
        print("\n🔍 Redundancy check:")
        corr_matrix = df[features].corr().abs()
        high_corr_pairs = []
        
        for i in range(len(features)):
            for j in range(i+1, len(features)):
                corr_val = corr_matrix.iloc[i, j]
                if corr_val > 0.8:
                    high_corr_pairs.append((features[i], features[j], corr_val))
        
        if high_corr_pairs:
            print("⚠️ High correlations:")
            for f1, f2, corr in high_corr_pairs:
                print(f"  {f1} vs {f2}: {corr:.2f}")
        else:
            print("✅ No major redundancy")
        
        # 3. Signal check
        print("\n📡 Signal check:")
        weak_signals = []
        for col in features:
            if 'is_win' in df.columns:
                corr = np.corrcoef(df[col], df['is_win'])[0, 1]
                status = "✅" if abs(corr) > 0.05 else "⚠️"
                print(f"  {col}: {corr:.3f} {status}")
                if abs(corr) <= 0.05:
                    weak_signals.append(col)
        
        # Summary
        print("\n📋 SUMMARY:")
        issues = len(low_var) + len(high_corr_pairs) + len(weak_signals)
        
        if issues == 0:
            print("✅ All features look good!")
        else:
            print(f"⚠️ {issues} issue(s) detected:")
            if low_var:
                print(f"  - {len(low_var)} low variance features")
            if high_corr_pairs:
                print(f"  - {len(high_corr_pairs)} redundant pairs")
            if weak_signals:
                print(f"  - {len(weak_signals)} weak signals")
        
        # Recommendation
        if issues > 0:
            print("\n💡 Recommendation: Run full audit with:")
            print("  python scripts/feature_audit.py")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    quick_feature_check()
