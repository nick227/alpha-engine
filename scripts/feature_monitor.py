"""
Feature Monitor

Long-term feature monitoring to detect drift and degradation.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import json
import sys
import os

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


class FeatureMonitor:
    """Monitor features over time to detect drift and degradation."""
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.features = [
            'trend_strength',
            'volatility_regime',
            'position_in_range', 
            'volume_anomaly',
            'spy_trend',
            'vix_level',
            'sector_trend',
            'price_momentum'
        ]
    
    def load_historical_data(self, days_back: int = 90) -> pd.DataFrame:
        """Load historical feature data."""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            
            query = """
            SELECT 
                created_at,
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
            ORDER BY created_at
            """
            
            df = pd.read_sql_query(query, conn, params=(cutoff_date,))
            conn.close()
            
            # Encode categorical
            if 'volatility_regime' in df.columns:
                df['volatility_regime'] = df['volatility_regime'].map({
                    'expansion': 1,
                    'contraction': -1,
                    'normal': 0
                }).fillna(0)
            
            df['created_at'] = pd.to_datetime(df['created_at'])
            return df
            
        except Exception as e:
            print(f"Error loading historical data: {e}")
            return pd.DataFrame()
    
    def detect_drift(self, df: pd.DataFrame, window_days: int = 30) -> dict:
        """Detect feature drift over time."""
        
        if len(df) < 60:
            return {"error": "Insufficient data for drift detection"}
        
        # Split into two periods
        mid_point = len(df) // 2
        early_period = df.iloc[:mid_point]
        late_period = df.iloc[mid_point:]
        
        drift_report = {}
        
        for feature in self.features:
            if feature not in df.columns:
                continue
            
            # Compare distributions
            early_mean = early_period[feature].mean()
            late_mean = late_period[feature].mean()
            early_std = early_period[feature].std()
            late_std = late_period[feature].std()
            
            # Calculate drift metrics
            mean_drift = abs(late_mean - early_mean)
            std_drift = abs(late_std - early_std)
            
            # Signal drift
            if 'is_win' in df.columns:
                early_corr = np.corrcoef(early_period[feature], early_period['is_win'])[0, 1]
                late_corr = np.corrcoef(late_period[feature], late_period['is_win'])[0, 1]
                signal_drift = abs(late_corr - early_corr)
            else:
                signal_drift = 0
            
            drift_report[feature] = {
                'mean_drift': mean_drift,
                'std_drift': std_drift,
                'signal_drift': signal_drift,
                'early_mean': early_mean,
                'late_mean': late_mean,
                'early_std': early_std,
                'late_std': late_std
            }
        
        return drift_report
    
    def generate_monitoring_report(self, days_back: int = 90) -> dict:
        """Generate comprehensive monitoring report."""
        
        print("🔍 GENERATING FEATURE MONITORING REPORT")
        print("=" * 60)
        
        # Load data
        df = self.load_historical_data(days_back)
        
        if len(df) < 60:
            print(f"❌ Insufficient data: {len(df)} records (need 60+)")
            return {"error": "Insufficient data"}
        
        print(f"📊 Analyzing {len(df)} records over {days_back} days")
        
        # Detect drift
        drift_report = self.detect_drift(df)
        
        if "error" in drift_report:
            return drift_report
        
        # Analyze drift
        print("\n📈 FEATURE DRIFT ANALYSIS:")
        
        critical_drift = []
        moderate_drift = []
        
        for feature, metrics in drift_report.items():
            mean_drift = metrics['mean_drift']
            std_drift = metrics['std_drift']
            signal_drift = metrics['signal_drift']
            
            # Classify drift severity
            if mean_drift > 0.1 or std_drift > 0.05 or signal_drift > 0.1:
                severity = "🔴 CRITICAL"
                critical_drift.append(feature)
            elif mean_drift > 0.05 or std_drift > 0.02 or signal_drift > 0.05:
                severity = "🟡 MODERATE"
                moderate_drift.append(feature)
            else:
                severity = "✅ STABLE"
            
            print(f"\n{feature}: {severity}")
            print(f"  Mean drift: {mean_drift:.4f}")
            print(f"  Std drift: {std_drift:.4f}")
            print(f"  Signal drift: {signal_drift:.4f}")
            
            if severity != "✅ STABLE":
                print(f"  Early: μ={metrics['early_mean']:.3f}, σ={metrics['early_std']:.3f}")
                print(f"  Late:  μ={metrics['late_mean']:.3f}, σ={metrics['late_std']:.3f}")
        
        # Summary
        print(f"\n📋 DRIFT SUMMARY:")
        print(f"  Critical drift: {len(critical_drift)} features")
        print(f"  Moderate drift: {len(moderate_drift)} features")
        print(f"  Stable: {len(self.features) - len(critical_drift) - len(moderate_drift)} features")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        if critical_drift:
            print("🔴 CRITICAL ISSUES:")
            for feature in critical_drift:
                print(f"  - {feature}: Investigate immediately")
                print(f"    Consider retraining model or removing feature")
        
        if moderate_drift:
            print("🟡 MODERATE ISSUES:")
            for feature in moderate_drift:
                print(f"  - {feature}: Monitor closely")
                print(f"    May need adjustment in next retraining")
        
        if not critical_drift and not moderate_drift:
            print("✅ All features stable - no action needed")
        
        # Feature health score
        total_features = len(self.features)
        health_score = (total_features - len(critical_drift) * 2 - len(moderate_drift)) / total_features * 100
        
        print(f"\n🏥 FEATURE HEALTH SCORE: {health_score:.1f}/100")
        
        if health_score >= 80:
            print("✅ Excellent feature health")
        elif health_score >= 60:
            print("🟡 Good feature health - monitor")
        else:
            print("🔴 Poor feature health - action required")
        
        return {
            'drift_report': drift_report,
            'critical_drift': critical_drift,
            'moderate_drift': moderate_drift,
            'health_score': health_score,
            'total_records': len(df),
            'analysis_period': days_back
        }
    
    def save_monitoring_report(self, report: dict, filename: str = None):
        """Save monitoring report to file."""
        
        if filename is None:
            filename = f"logs/feature_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"\n💾 Report saved to: {filename}")
        except Exception as e:
            print(f"Error saving report: {e}")


def main():
    """Main monitoring script."""
    
    monitor = FeatureMonitor()
    
    # Generate report
    report = monitor.generate_monitoring_report(days_back=90)
    
    # Save report
    if "error" not in report:
        monitor.save_monitoring_report(report)
    
    return 0 if "error" not in report else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
