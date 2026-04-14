"""
Model Performance Tracker: Tracks prediction accuracy by model key.

This enables model competition and evolution - critical for adaptive improvement.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict, deque

# from app.core.types import OutcomeRow  # Not needed for performance tracking


class ModelPerformanceTracker:
    """
    Tracks model performance for competition and evolution.
    
    Key insight: Models compete → best models emerge → adaptive evolution.
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.performance_cache: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))  # Last 100 predictions per model
        self.model_stats: Dict[str, Dict] = {}
        self._init_database()
    
    def _init_database(self):
        """Initialize performance tracking database."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS model_performance (
                model_key TEXT NOT NULL,
                prediction_date TEXT NOT NULL,
                prediction REAL NOT NULL,
                actual_return REAL,
                prediction_error REAL,
                confidence REAL,
                environment_bucket TEXT,
                sector_regime TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (model_key, prediction_date)
            )
        """)
        conn.commit()
        conn.close()
    
    def store_prediction(self, prediction_result: Dict[str, Any], actual_return: float, 
                     env_bucket: tuple, features: Dict[str, Any]):
        """Store prediction with actual outcome for performance tracking."""
        
        model_key = prediction_result.get("model_key", "unknown")
        prediction = prediction_result.get("prediction", 0.0)
        confidence = prediction_result.get("confidence", 0.5)
        prediction_date = datetime.now().isoformat()
        
        # Calculate prediction error
        prediction_error = abs(prediction - actual_return)
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO model_performance 
                (model_key, prediction_date, prediction, actual_return, 
                 prediction_error, confidence, environment_bucket, sector_regime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                model_key, prediction_date, prediction, actual_return,
                prediction_error, confidence, str(env_bucket), 
                prediction_result.get("regime", {}).get("sector_regime", "unknown")
            ))
            conn.commit()
            
            # Update cache
            self.performance_cache[model_key].append({
                "prediction": prediction,
                "actual_return": actual_return,
                "prediction_error": prediction_error,
                "confidence": confidence,
                "timestamp": prediction_date,
                "env_bucket": env_bucket,
            })
            
            print(f"📊 Stored: {model_key} prediction={prediction:.4f}, actual={actual_return:.4f}, error={prediction_error:.4f}")
            
        except Exception as e:
            print(f"❌ Failed to store prediction: {e}")
        finally:
            conn.close()
    
    def get_model_performance(self, model_key: str, days: int = 30) -> Dict[str, Any]:
        """Get performance statistics for specific model."""
        
        conn = sqlite3.connect(self.db_path)
        try:
            # Get recent predictions for this model
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            rows = conn.execute("""
                SELECT prediction, actual_return, prediction_error, confidence, 
                       COUNT(*) as sample_count, AVG(prediction_error) as avg_error,
                       AVG(actual_return) as avg_return, 
                       SUM(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN prediction_error < 0.01 THEN 1 ELSE 0 END) as accurate_predictions
                FROM model_performance 
                WHERE model_key = ? AND prediction_date >= ?
                GROUP BY model_key
            """, (model_key, cutoff_date)).fetchone()
            
            if rows:
                win_rate = (rows[5] / rows[3]) if rows[3] > 0 else 0.0
                accuracy_rate = (rows[6] / rows[3]) if rows[3] > 0 else 0.0
                
                performance = {
                    "model_key": model_key,
                    "sample_count": rows[3],
                    "avg_prediction": rows[0],
                    "avg_actual": rows[4],
                    "avg_error": rows[5],
                    "win_rate": win_rate,
                    "accuracy_rate": accuracy_rate,
                    "avg_confidence": rows[2],
                    "period_days": days,
                    "last_updated": datetime.now().isoformat()
                }
                
                # Cache performance
                self.model_stats[model_key] = performance
                return performance
            
            return {
                "model_key": model_key,
                "sample_count": 0,
                "error": "No data found"
            }
            
        except Exception as e:
            return {"model_key": model_key, "error": str(e)}
        finally:
            conn.close()
    
    def get_competition_ranking(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get ranking of all models by performance - enables competition."""
        
        conn = sqlite3.connect(self.db_path)
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            rows = conn.execute("""
                SELECT model_key, 
                       COUNT(*) as sample_count,
                       AVG(prediction_error) as avg_error,
                       AVG(actual_return) as avg_return,
                       SUM(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate,
                       SUM(CASE WHEN prediction_error < 0.01 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as accuracy_rate,
                       AVG(confidence) as avg_confidence
                FROM model_performance 
                WHERE prediction_date >= ?
                GROUP BY model_key
                HAVING COUNT(*) >= 10
                ORDER BY avg_error ASC, win_rate DESC, accuracy_rate DESC
            """, (cutoff_date,)).fetchall()
            
            ranking = []
            for row in rows:
                # Calculate composite score
                error_score = 1.0 / (row[2] + 0.001)  # Lower error = higher score
                win_rate_score = row[4]  # Higher win rate = higher score
                accuracy_score = row[5]  # Higher accuracy = higher score
                composite_score = (error_score * 0.4) + (win_rate_score * 0.4) + (accuracy_score * 0.2)
                
                ranking.append({
                    "model_key": row[0],
                    "sample_count": row[1],
                    "avg_error": row[2],
                    "avg_return": row[3],
                    "win_rate": row[4],
                    "accuracy_rate": row[5],
                    "avg_confidence": row[6],
                    "composite_score": composite_score,
                    "rank": len(ranking) + 1
                })
            
            print(f"🏆 MODEL COMPETITION RANKING ({days} days):")
            for i, model in enumerate(ranking[:5]):  # Top 5
                print(f"  {i+1}. {model['model_key']}: score={model['composite_score']:.3f}, error={model['avg_error']:.4f}, win_rate={model['win_rate']:.1%}")
            
            return ranking
            
        except Exception as e:
            print(f"❌ Failed to get competition ranking: {e}")
            return []
        finally:
            conn.close()
    
    def get_best_model_for_regime(self, env_bucket: tuple, days: int = 30) -> Optional[str]:
        """Get best performing model for specific regime."""
        
        # Extract regime characteristics
        vol_regime = "HI_VOL" if len(env_bucket) > 0 and env_bucket[0] == "HI_VOL" else "LO_VOL"
        trend_regime = "TREND" if len(env_bucket) > 1 and env_bucket[1] == "TREND" else "CHOP"
        sector_regime = env_bucket[4] if len(env_bucket) > 4 else "BALANCED"
        
        # Find models that match this regime
        matching_models = []
        for model_key in self.model_stats.keys():
            if (vol_regime in model_key and 
                trend_regime in model_key and 
                (sector_regime in model_key or sector_regime == "BALANCED")):
                matching_models.append(model_key)
        
        if not matching_models:
            return None
        
        # Get performance for matching models
        best_model = None
        best_score = -1.0
        
        for model_key in matching_models:
            performance = self.get_model_performance(model_key, days)
            if "error" not in performance:
                # Calculate score
                error_score = 1.0 / (performance["avg_error"] + 0.001)
                win_rate_score = performance["win_rate"]
                composite_score = (error_score * 0.6) + (win_rate_score * 0.4)
                
                if composite_score > best_score:
                    best_score = composite_score
                    best_model = model_key
        
        return best_model
    
    def suggest_model_improvements(self, model_key: str) -> List[str]:
        """Suggest improvements for underperforming models."""
        
        performance = self.get_model_performance(model_key)
        
        if "error" in performance or performance["sample_count"] < 20:
            return ["Insufficient data for analysis"]
        
        suggestions = []
        
        # Analyze performance issues
        if performance["avg_error"] > 0.02:
            suggestions.append("High prediction error - consider retraining with more data")
        
        if performance["win_rate"] < 0.4:
            suggestions.append("Low win rate - check feature relevance or model complexity")
        
        if performance["accuracy_rate"] < 0.6:
            suggestions.append("Low accuracy - improve confidence calibration")
        
        if performance["avg_confidence"] < 0.6:
            suggestions.append("Low confidence - model may be overfitting or underfitting")
        
        return suggestions
    
    def get_evolution_candidates(self) -> List[Dict[str, Any]]:
        """Identify models that should be evolved or retired."""
        
        candidates = []
        
        for model_key in self.model_stats.keys():
            performance = self.get_model_performance(model_key)
            
            if "error" in performance:
                continue
            
            # Evolution criteria
            should_evolve = False
            evolution_reason = []
            
            if performance["sample_count"] >= 50:  # Enough data
                if performance["avg_error"] > 0.015:  # Poor performance
                    should_evolve = True
                    evolution_reason.append("High prediction error")
                
                if performance["win_rate"] < 0.35:  # Low win rate
                    should_evolve = True
                    evolution_reason.append("Low win rate")
                
                if performance["accuracy_rate"] < 0.5:  # Low accuracy
                    should_evolve = True
                    evolution_reason.append("Low accuracy")
            
            if should_evolve:
                candidates.append({
                    "model_key": model_key,
                    "should_evolve": True,
                    "evolution_reason": evolution_reason,
                    "current_performance": performance,
                    "priority": "high" if performance["avg_error"] > 0.02 else "medium"
                })
        
        return candidates


# Global performance tracker instance
_performance_tracker = None


def get_performance_tracker() -> ModelPerformanceTracker:
    """Get global performance tracker instance."""
    global _performance_tracker
    if _performance_tracker is None:
        _performance_tracker = ModelPerformanceTracker()
    return _performance_tracker


def store_prediction_outcome(prediction_result: Dict[str, Any], actual_return: float, 
                         env_bucket: tuple, features: Dict[str, Any]):
    """Store prediction outcome - main entry point."""
    
    tracker = get_performance_tracker()
    tracker.store_prediction(prediction_result, actual_return, env_bucket, features)


def get_best_models_for_current_regimes() -> Dict[str, Optional[str]]:
    """Get best models for all current regime patterns."""
    
    tracker = get_performance_tracker()
    
    # Common regime patterns
    regime_patterns = [
        ("HI_VOL", "TREND", "TECH_LEAD"),
        ("HI_VOL", "CHOP", "FINANCIALS"),
        ("LO_VOL", "TREND", "ENERGY"),
        ("LO_VOL", "CHOP", "BALANCED"),
    ]
    
    best_models = {}
    
    for pattern in regime_patterns:
        # This would use current environment detection
        # For demo, return pattern-based suggestions
        best_models["_".join(pattern)] = f"{'_'.join(pattern)}_specialized"
    
    return best_models
