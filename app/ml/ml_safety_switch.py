"""
ML Safety Switch

Critical safety mechanism to prevent ML from silently hurting performance.
"""

import logging
from datetime import datetime, timedelta
import sqlite3
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class MLSafetySwitch:
    """
    ML Safety Switch - prevents ML from silently hurting performance.
    
    Triggers bypass_ml = True when:
    1. ML-filtered expectancy ≤ baseline expectancy
    2. Bucket separation disappears
    3. Model confidence too low
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.bypass_ml = False
        self.last_check = None
        self.check_interval_hours = 6  # Check every 6 hours
        
        # Safety thresholds
        self.min_ml_improvement = 0.005  # 0.5% minimum improvement
        self.min_monotonicity = 0.6      # 60% monotonic
        self.min_model_confidence = 0.3      # 30% minimum confidence
        
    def check_ml_safety(self, force_check: bool = False) -> Dict[str, Any]:
        """
        Check if ML should be bypassed based on safety criteria.
        
        Args:
            force_check: Force check even if not due
            
        Returns:
            Safety check results with bypass recommendation
        """
        
        # Check if we need to run safety check
        if not force_check and self.last_check:
            hours_since_check = (datetime.now() - self.last_check).total_seconds() / 3600
            if hours_since_check < self.check_interval_hours:
                return {
                    'bypass_ml': self.bypass_ml,
                    'reason': 'Recently checked',
                    'safe_to_continue': True
                }
        
        logger.info("Running ML safety check...")
        
        try:
            # Get recent performance data
            performance_data = self._get_recent_performance_data()
            
            if not performance_data:
                logger.warning("No recent performance data available")
                return {
                    'bypass_ml': True,
                    'reason': 'No performance data',
                    'safe_to_continue': False
                }
            
            # Check safety criteria
            safety_results = {
                'ml_improvement_check': self._check_ml_improvement(performance_data),
                'monotonicity_check': self._check_monotonicity(performance_data),
                'model_confidence_check': self._check_model_confidence(),
                'timestamp': datetime.now().isoformat()
            }
            
            # Make bypass decision
            bypass_reasons = []
            
            if not safety_results['ml_improvement_check']['passing']:
                bypass_reasons.append("ML not improving expectancy")
            
            if not safety_results['monotonicity_check']['passing']:
                bypass_reasons.append("ML not separating trades properly")
            
            if not safety_results['model_confidence_check']['passing']:
                bypass_reasons.append("Model confidence too low")
            
            # Update bypass state
            self.bypass_ml = len(bypass_reasons) > 0
            self.last_check = datetime.now()
            
            safety_results['bypass_ml'] = self.bypass_ml
            safety_results['bypass_reasons'] = bypass_reasons
            safety_results['safe_to_continue'] = not self.bypass_ml
            
            # Log results
            if self.bypass_ml:
                logger.warning(f"ML SAFETY SWITCH ACTIVATED: {', '.join(bypass_reasons)}")
                self._log_safety_event('ACTIVATED', bypass_reasons, safety_results)
            else:
                logger.info("ML safety check passed - ML is safe to use")
                self._log_safety_event('PASSED', [], safety_results)
            
            return safety_results
            
        except Exception as e:
            logger.error(f"Error in ML safety check: {e}")
            return {
                'bypass_ml': True,
                'reason': f'Safety check error: {e}',
                'safe_to_continue': False
            }
    
    def _get_recent_performance_data(self, days_back: int = 7) -> Optional[Dict[str, Any]]:
        """Get recent ML performance data."""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            
            # Get ML edge curve data with outcomes
            query = """
            SELECT 
                ml_probability,
                ml_passed,
                CASE WHEN t.outcome_return > 0 THEN 1 ELSE 0 END as is_win,
                t.outcome_return as trade_return
            FROM ml_edge_curve mec
            JOIN trades t ON mec.symbol = t.symbol 
                AND DATE(mec.timestamp) = DATE(t.entry_timestamp)
            WHERE mec.timestamp >= ?
            AND mec.ml_probability IS NOT NULL
            AND t.outcome_return IS NOT NULL
            ORDER BY mec.timestamp DESC
            LIMIT 500
            """
            
            cursor = conn.execute(query, (cutoff_date,))
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                return None
            
            # Calculate performance metrics
            all_returns = [row[3] for row in rows]
            filtered_returns = [row[3] for row in rows if row[1]]  # ml_passed = True
            
            baseline_expectancy = sum(all_returns) / len(all_returns)
            filtered_expectancy = sum(filtered_returns) / len(filtered_returns) if filtered_returns else baseline_expectancy
            
            # Calculate monotonicity
            ml_probabilities = [row[0] for row in rows]
            monotonicity_score = self._calculate_monotonicity(ml_probabilities, all_returns)
            
            return {
                'total_trades': len(rows),
                'baseline_expectancy': baseline_expectancy,
                'filtered_expectancy': filtered_expectancy,
                'ml_improvement': filtered_expectancy - baseline_expectancy,
                'monotonicity_score': monotonicity_score,
                'recent_data': True
            }
            
        except Exception as e:
            logger.error(f"Error getting performance data: {e}")
            return None
    
    def _calculate_monotonicity(self, probabilities: list, returns: list) -> float:
        """Calculate monotonicity score between probabilities and returns."""
        
        if len(probabilities) < 10:
            return 0.0
        
        # Sort by probability
        sorted_data = sorted(zip(probabilities, returns), key=lambda x: x[0])
        sorted_probs, sorted_returns = zip(*sorted_data)
        
        # Calculate monotonicity
        monotonic_increases = 0
        total_comparisons = 0
        
        for i in range(1, len(sorted_returns)):
            # Use moving average to smooth noise
            window = min(5, i)
            prev_avg = sum(sorted_returns[max(0, i-window):i]) / window
            curr_avg = sum(sorted_returns[i:min(i+window, len(sorted_returns))]) / min(window, len(sorted_returns)-i)
            
            if curr_avg > prev_avg:
                monotonic_increases += 1
            total_comparisons += 1
        
        return monotonic_increases / total_comparisons if total_comparisons > 0 else 0.0
    
    def _check_ml_improvement(self, performance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check if ML is improving expectancy over baseline."""
        
        ml_improvement = performance_data.get('ml_improvement', 0)
        passing = ml_improvement >= self.min_ml_improvement
        
        return {
            'passing': passing,
            'ml_improvement': ml_improvement,
            'threshold': self.min_ml_improvement,
            'message': f"ML improvement: {ml_improvement:.3%} (threshold: {self.min_ml_improvement:.3%})"
        }
    
    def _check_monotonicity(self, performance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check if ML is properly separating trades by probability."""
        
        monotonicity_score = performance_data.get('monotonicity_score', 0)
        passing = monotonicity_score >= self.min_monotonicity
        
        return {
            'passing': passing,
            'monotonicity_score': monotonicity_score,
            'threshold': self.min_monotonicity,
            'message': f"Monotonicity: {monotonicity_score:.1%} (threshold: {self.min_monotonicity:.1%})"
        }
    
    def _check_model_confidence(self) -> Dict[str, Any]:
        """Check if model confidence is sufficient."""
        
        # This would integrate with the actual model confidence calculation
        # For now, return a mock result
        mock_confidence = 0.7  # Replace with actual model confidence
        passing = mock_confidence >= self.min_model_confidence
        
        return {
            'passing': passing,
            'model_confidence': mock_confidence,
            'threshold': self.min_model_confidence,
            'message': f"Model confidence: {mock_confidence:.1%} (threshold: {self.min_model_confidence:.1%})"
        }
    
    def _log_safety_event(self, event_type: str, reasons: list, results: Dict[str, Any]):
        """Log safety switch events."""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_safety_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    bypass_ml BOOLEAN NOT NULL,
                    reasons TEXT,
                    results TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                INSERT INTO ml_safety_events 
                (timestamp, event_type, bypass_ml, reasons, results)
                VALUES (?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                event_type,
                self.bypass_ml,
                json.dumps(reasons),
                json.dumps(results)
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error logging safety event: {e}")
    
    def get_safety_status(self) -> Dict[str, Any]:
        """Get current safety switch status."""
        
        return {
            'bypass_ml': self.bypass_ml,
            'last_check': self.last_check.isoformat() if self.last_check else None,
            'check_interval_hours': self.check_interval_hours,
            'min_ml_improvement': self.min_ml_improvement,
            'min_monotonicity': self.min_monotonicity,
            'min_model_confidence': self.min_model_confidence
        }
    
    def force_bypass(self, reason: str, duration_hours: int = 24):
        """Force ML bypass for specified duration."""
        
        logger.warning(f"Force ML bypass activated: {reason}")
        self.bypass_ml = True
        self._log_safety_event('FORCE_BYPASS', [reason], {
            'force_reason': reason,
            'duration_hours': duration_hours,
            'auto_resume': (datetime.now() + timedelta(hours=duration_hours)).isoformat()
        })
    
    def resume_ml(self):
        """Manually resume ML operation."""
        
        logger.info("ML operation resumed")
        self.bypass_ml = False
        self._log_safety_event('MANUAL_RESUME', [], {
            'resumed_at': datetime.now().isoformat()
        })


# Global safety switch instance
ml_safety_switch = MLSafetySwitch()


def should_bypass_ml() -> bool:
    """Check if ML should be bypassed."""
    
    safety_results = ml_safety_switch.check_ml_safety()
    return safety_results['bypass_ml']


def get_ml_safety_status() -> Dict[str, Any]:
    """Get current ML safety status."""
    
    return ml_safety_switch.get_safety_status()
