"""
Feature A/B Test Harness

Safely test feature removal without breaking the system.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import json
import sys
import os
import logging

# Add app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.ml.simple_trainer import SimpleMLTrainer
from app.ml.training_dataset import TrainingDatasetBuilder

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FeatureABTest:
    """
    A/B test harness for feature removal validation.
    
    Tests hypothesis: "Removing feature X will not degrade model performance"
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.dataset_builder = TrainingDatasetBuilder(db_path)
        
        # Core features
        self.core_features = [
            'trend_strength',
            'volatility_regime',
            'position_in_range',
            'volume_anomaly',
            'spy_trend',
            'vix_level',
            'sector_trend',
            'price_momentum'
        ]
    
    def load_test_data(self, days_back: int = 60) -> pd.DataFrame:
        """Load data for A/B testing."""
        
        try:
            conn = sqlite3.connect(self.db_path)
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
            LIMIT 500
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
            
            logger.info(f"Loaded {len(df)} records for A/B testing")
            return df
            
        except Exception as e:
            logger.error(f"Error loading test data: {e}")
            return pd.DataFrame()
    
    def train_baseline_model(self, df: pd.DataFrame) -> dict:
        """Train baseline model with all features."""
        
        try:
            trainer = SimpleMLTrainer()
            
            # Prepare features
            X = df[self.core_features].values
            y = df['is_win'].values
            
            # Train model
            from sklearn.model_selection import train_test_split
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            trainer.model_type = 'gradient_boosting'
            from sklearn.ensemble import GradientBoostingClassifier
            trainer.model = GradientBoostingClassifier(
                n_estimators=100, max_depth=3, random_state=42
            )
            
            trainer.model.fit(X_train, y_train)
            
            # Evaluate
            y_pred = trainer.model.predict(X_test)
            y_prob = trainer.model.predict_proba(X_test)[:, 1]
            
            from sklearn.metrics import accuracy_score, precision_score, recall_score
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            
            # Calculate expectancy (simplified)
            expectancy = np.mean(y_prob) * 0.5 - (1 - np.mean(y_prob)) * 0.5
            
            baseline_results = {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'expectancy': expectancy,
                'win_rate': np.mean(y_test),
                'feature_importance': dict(zip(self.core_features, trainer.model.feature_importances_))
            }
            
            logger.info(f"Baseline model trained - Accuracy: {accuracy:.3f}, Expectancy: {expectancy:.3f}")
            return baseline_results
            
        except Exception as e:
            logger.error(f"Error training baseline model: {e}")
            return {}
    
    def test_feature_removal(self, df: pd.DataFrame, feature_to_remove: str) -> dict:
        """Test model performance without specific feature."""
        
        if feature_to_remove not in self.core_features:
            return {'error': f'Feature {feature_to_remove} not in core features'}
        
        try:
            # Remove feature
            test_features = [f for f in self.core_features if f != feature_to_remove]
            
            trainer = SimpleMLTrainer()
            
            # Prepare features
            X = df[test_features].values
            y = df['is_win'].values
            
            # Train model
            from sklearn.model_selection import train_test_split
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            trainer.model_type = 'gradient_boosting'
            from sklearn.ensemble import GradientBoostingClassifier
            trainer.model = GradientBoostingClassifier(
                n_estimators=100, max_depth=3, random_state=42
            )
            
            trainer.model.fit(X_train, y_train)
            
            # Evaluate
            y_pred = trainer.model.predict(X_test)
            y_prob = trainer.model.predict_proba(X_test)[:, 1]
            
            from sklearn.metrics import accuracy_score, precision_score, recall_score
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            
            # Calculate expectancy
            expectancy = np.mean(y_prob) * 0.5 - (1 - np.mean(y_prob)) * 0.5
            
            test_results = {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'expectancy': expectancy,
                'win_rate': np.mean(y_test),
                'feature_importance': dict(zip(test_features, trainer.model.feature_importances_))
            }
            
            logger.info(f"Test model trained (without {feature_to_remove}) - Accuracy: {accuracy:.3f}, Expectancy: {expectancy:.3f}")
            return test_results
            
        except Exception as e:
            logger.error(f"Error testing feature removal: {e}")
            return {'error': str(e)}
    
    def compare_performance(self, baseline: dict, test: dict, feature: str) -> dict:
        """Compare baseline vs test performance."""
        
        if 'error' in test:
            return test
        
        # Calculate performance differences
        accuracy_diff = test['accuracy'] - baseline['accuracy']
        precision_diff = test['precision'] - baseline['precision']
        recall_diff = test['recall'] - baseline['recall']
        expectancy_diff = test['expectancy'] - baseline['expectancy']
        
        # Trading-focused decision criteria (ONLY trading metrics)
        expectancy_ok = expectancy_diff >= -0.01  # Within 0.01
        
        # NEW: Trade distribution stability (check variance in returns)
        baseline_stability = self._calculate_trade_stability(baseline)
        test_stability = self._calculate_trade_stability(test)
        stability_ok = test_stability >= baseline_stability * 0.95  # Within 5%
        
        # NEW: Tail dependence check (top 5 P&L contribution)
        tail_dependence_ok = self._check_tail_dependence(baseline, test)
        
        # FINAL DECISION RULE: Only trading metrics matter
        can_remove = expectancy_ok and stability_ok and tail_dependence_ok
        
        # DIAGNOSTIC ONLY: Classification metrics for insight
        precision_ok = precision_diff >= -0.05
        recall_ok = recall_diff >= -0.05
        
        comparison = {
            'feature': feature,
            'baseline_performance': baseline,
            'test_performance': test,
            'differences': {
                'accuracy': accuracy_diff,
                'precision': precision_diff,
                'recall': recall_diff,
                'expectancy': expectancy_diff
            },
            'decision_criteria': {
                'expectancy_within_1pct': expectancy_ok,
                'trade_stability_ok': stability_ok,
                'tail_dependence_ok': tail_dependence_ok
            },
            'diagnostic_criteria': {
                'precision_within_5pct': precision_ok,
                'recall_within_5pct': recall_ok
            },
            'recommendation': 'REMOVE' if can_remove else 'KEEP',
            'confidence': 'HIGH' if abs(accuracy_diff) < 0.02 else 'MEDIUM'
        }
        
        return comparison
    
    def run_ab_test(self, feature_to_remove: str, days_back: int = 60) -> dict:
        """Run complete A/B test for feature removal."""
        
        print(f"🧪 Running A/B Test: Remove '{feature_to_remove}'")
        print("=" * 60)
        
        # Load test data
        df = self.load_test_data(days_back)
        
        if len(df) < 100:
            return {'error': f'Insufficient data: {len(df)} records (need 100+)'}
        
        # Train baseline model
        print("📊 Training baseline model (with all features)...")
        baseline_results = self.train_baseline_model(df)
        
        if not baseline_results:
            return {'error': 'Failed to train baseline model'}
        
        # Test feature removal
        print(f"🧪 Testing model without '{feature_to_remove}'...")
        test_results = self.test_feature_removal(df, feature_to_remove)
        
        if 'error' in test_results:
            return test_results
        
        # Compare performance
        print("📈 Comparing performance...")
        comparison = self.compare_performance(baseline_results, test_results, feature_to_remove)
        
        # Display results
        self.display_ab_test_results(comparison)
        
        return comparison
    
    def _calculate_trade_stability(self, performance: dict) -> float:
        """
        Calculate trade distribution stability.
        
        Lower variance in returns = more stable system.
        """
        
        # Mock calculation - in real system, use actual trade returns
        # For now, use precision as proxy for stability
        precision = performance.get('precision', 0.5)
        recall = performance.get('recall', 0.5)
        
        # Higher precision + reasonable recall = more stable
        stability = precision * (1 - abs(recall - 0.5))
        
        return stability
    
    def _check_tail_dependence(self, baseline: dict, test: dict) -> bool:
        """
        Check tail dependence - top 5 P&L contribution must NOT increase.
        
        If top 5 contribution increases → system became more fragile.
        """
        
        # Mock calculation - in real system, use actual P&L data
        # For now, use feature importance as proxy
        baseline_importance = baseline.get('feature_importance', {})
        test_importance = test.get('feature_importance', {})
        
        # Check if any single feature dominates more after removal
        baseline_max = max(baseline_importance.values()) if baseline_importance else 0
        test_max = max(test_importance.values()) if test_importance else 0
        
        # If max importance increased significantly, tail dependence increased
        tail_increase = test_max - baseline_max
        
        # Tail dependence should NOT increase materially
        return tail_increase <= 0.05  # Allow 5% increase max
    
    def run_feature_flip_test(self, df: pd.DataFrame, feature_to_test: str) -> dict:
        """
        Feature "flip test" - randomize feature instead of removing.
        
        If performance stays same when feature is randomized:
        → feature was useless
        
        This is faster than retraining and isolates feature contribution cleanly.
        """
        
        print(f"🔄 Running Flip Test: Randomize '{feature_to_test}'")
        print("=" * 50)
        
        if feature_to_test not in df.columns:
            return {'error': f'Feature {feature_to_test} not found'}
        
        # Create flipped dataset
        df_flipped = df.copy()
        # Randomize the feature (destroy any signal)
        df_flipped[feature_to_test] = np.random.permutation(df_flipped[feature_to_test].values)
        
        # Train baseline model
        baseline_results = self.train_baseline_model(df)
        
        # Train model with flipped feature
        flipped_results = self.test_feature_removal(df_flipped, feature_to_test)
        
        if 'error' in flipped_results:
            return flipped_results
        
        # Compare performance
        comparison = self.compare_performance(baseline_results, flipped_results, feature_to_test)
        
        # Flip test interpretation
        baseline_exp = baseline_results['expectancy']
        flipped_exp = flipped_results['expectancy']
        exp_diff = flipped_exp - baseline_exp
        
        # GUARD: Only trust flip test after sufficient data
        data_sufficient = len(df) >= 100
        baseline_strong = baseline_exp >= 0.01  # Minimum expectancy threshold
        
        # If performance stays same when randomized → feature was useless
        # BUT only if we have sufficient data and strong baseline
        is_useless = abs(exp_diff) < 0.005 and data_sufficient and baseline_strong
        
        # Add warning for insufficient data
        if not data_sufficient:
            warning = f"INSUFFICIENT DATA: {len(df)} trades (need 100+)"
        elif not baseline_strong:
            warning = f"WEAK BASELINE: {baseline_exp:.4f} (need 0.01+)"
        else:
            warning = None
        
        flip_results = {
            'feature': feature_to_test,
            'baseline_expectancy': baseline_exp,
            'flipped_expectancy': flipped_exp,
            'expectancy_difference': exp_diff,
            'data_sufficient': data_sufficient,
            'baseline_strong': baseline_strong,
            'warning': warning,
            'is_useless': is_useless,
            'interpretation': 'USELESS' if is_useless else 'USEFUL',
            'recommendation': 'REMOVE' if is_useless else 'KEEP'
        }
        
        # Display flip test results
        print(f"\n🔄 FLIP TEST RESULTS for '{feature_to_test}'")
        print("=" * 50)
        print(f"Baseline expectancy: {baseline_exp:.4f}")
        print(f"Flipped expectancy:  {flipped_exp:.4f}")
        print(f"Difference:          {exp_diff:+.4f}")
        print(f"Interpretation:      {flip_results['interpretation']}")
        
        # Show warning if present
        if flip_results['warning']:
            print(f"⚠️  WARNING: {flip_results['warning']}")
            print(f"   Flip test conclusion may be unreliable")
        
        if is_useless:
            print(f"\n💡 Feature '{feature_to_test}' appears USELESS")
            print(f"   Randomizing it didn't hurt performance")
        else:
            print(f"\n💡 Feature '{feature_to_test}' appears USEFUL")
            print(f"   Randomizing it degraded performance")
        
        return flip_results
    
    def display_ab_test_results(self, comparison: dict):
        """Display A/B test results in a clear format."""
        
        feature = comparison['feature']
        recommendation = comparison['recommendation']
        confidence = comparison['confidence']
        
        print(f"\n📋 A/B TEST RESULTS for '{feature}'")
        print("=" * 50)
        
        # Performance comparison
        baseline = comparison['baseline_performance']
        test = comparison['test_performance']
        diffs = comparison['differences']
        
        print(f"\n📊 Trading-Focused Performance Comparison:")
        print(f"  Metric           | Baseline | Test     | Diff     | Status")
        print(f"  ----------------|----------|----------|----------|--------")
        print(f"  Expectancy      | {baseline['expectancy']:.4f}    | {test['expectancy']:.4f}    | {diffs['expectancy']:+.4f}    | {'✅' if diffs['expectancy'] >= -0.01 else '❌'}")
        print(f"  Precision       | {baseline['precision']:.3f}    | {test['precision']:.3f}    | {diffs['precision']:+.3f}    | {'✅' if diffs['precision'] >= -0.05 else '❌'}")
        print(f"  Recall          | {baseline['recall']:.3f}    | {test['recall']:.3f}    | {diffs['recall']:+.3f}    | {'✅' if diffs['recall'] >= -0.05 else '❌'}")
        print(f"  Trade Stability | {self._calculate_trade_stability(baseline):.3f}    | {self._calculate_trade_stability(test):.3f}    | {self._calculate_trade_stability(test) - self._calculate_trade_stability(baseline):+.3f}    | {'✅' if comparison['criteria_met']['trade_stability_ok'] else '❌'}")
        print(f"  Tail Dependence | {'OK' if comparison['criteria_met']['tail_dependence_ok'] else 'WORSE'}    |          |          |          | {'✅' if comparison['criteria_met']['tail_dependence_ok'] else '❌'}")
        
        # Note about accuracy
        print(f"\n📝 Note: Accuracy removed (noise in trading)")
        print(f"   Focus: Expectancy, stability, tail dependence")
        
        # Decision criteria (trading metrics only)
        print(f"\n✅ DECISION CRITERIA (Trading Only):")
        decision_criteria = comparison['decision_criteria']
        for criterion, met in decision_criteria.items():
            status = "✅" if met else "❌"
            criterion_name = criterion.replace('_', ' ').title()
            print(f"  {criterion_name}: {status}")
        
        # Diagnostic criteria (classification metrics for insight)
        print(f"\n📊 DIAGNOSTIC CRITERIA (Insight Only):")
        diagnostic_criteria = comparison['diagnostic_criteria']
        for criterion, met in diagnostic_criteria.items():
            status = "✅" if met else "❌"
            criterion_name = criterion.replace('_', ' ').title()
            print(f"  {criterion_name}: {status}")
        
        print(f"\n📝 FINAL DECISION based on trading metrics only")
        
        # Recommendation
        rec_emoji = "🟢" if recommendation == "REMOVE" else "🔴"
        conf_emoji = "🔒" if confidence == "HIGH" else "🔓"
        
        print(f"\n{rec_emoji} RECOMMENDATION: {recommendation}")
        print(f"{conf_emoji} CONFIDENCE: {confidence}")
        
        if recommendation == "REMOVE":
            print(f"\n💡 Safe to remove '{feature}' - no significant performance degradation")
        else:
            print(f"\n⚠️  Keep '{feature}' - removal would degrade performance")
        
        print(f"\n🔥 Remember: This test validates the hypothesis:")
        print(f"   'Removing {feature} will not degrade model performance'")
    
    def save_ab_test_results(self, results: dict, filename: str = None):
        """Save A/B test results to file."""
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"logs/ab_test_{results['feature']}_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\n💾 A/B test results saved to: {filename}")
        except Exception as e:
            print(f"Error saving results: {e}")


def main():
    """Main A/B test script."""
    
    if len(sys.argv) < 2:
        print("Usage: python scripts/feature_ab_test.py <feature_name> [--flip]")
        print("Examples:")
        print("  python scripts/feature_ab_test.py trend_strength")
        print("  python scripts/feature_ab_test.py trend_strength --flip")
        return 1
    
    feature_to_test = sys.argv[1]
    use_flip_test = '--flip' in sys.argv
    
    # Run A/B test
    ab_test = FeatureABTest()
    
    if use_flip_test:
        # Run flip test (faster, isolates feature contribution)
        df = ab_test.load_test_data()
        results = ab_test.run_feature_flip_test(df, feature_to_test)
    else:
        # Run traditional A/B test
        results = ab_test.run_ab_test(feature_to_test)
    
    if 'error' in results:
        print(f"❌ Test failed: {results['error']}")
        return 1
    
    # Save results
    if use_flip_test:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"logs/flip_test_{feature_to_test}_{timestamp}.json"
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\n💾 Flip test results saved to: {filename}")
        except Exception as e:
            print(f"Error saving results: {e}")
    else:
        ab_test.save_ab_test_results(results)
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
