"""
Dimensional Tagger: Lightweight axis-based prediction tagging.

Instead of complex model selection, tag every prediction with dimensional keys
and track performance to build a performance surface. This enables selective activation
based on proven edges without heavy complexity.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum


class PredictionAxis(Enum):
    """Lightweight prediction axes for dimensional tagging."""
    ENVIRONMENT = "environment"      # HI_VOL, LO_VOL, TREND, CHOP
    SECTOR = "sector"            # TECH, FINANCIALS, HEALTHCARE, ENERGY
    MODEL = "model"              # AGGRESSIVE, DEFENSIVE, BALANCED
    HORIZON = "horizon"          # 1d, 5d, 7d, 20d
    VOLATILITY = "volatility"       # HIGH_VOL, MED_VOL, LOW_VOL
    LIQUIDITY = "liquidity"        # HIGH_LIQ, MED_LIQ, LOW_LIQ


@dataclass
class DimensionalTags:
    """Lightweight tags for each prediction."""
    environment: str
    sector: str
    model: str
    horizon: str
    volatility: str
    liquidity: str
    confidence: float
    prediction: float


class DimensionalTagger:
    """
    Tags predictions with lightweight axes and tracks performance by combination.
    
    Builds performance surface: (axis1, axis2, axis3) → performance
    Enables selective activation: only act where proven edge exists.
    
    DATA INTEGRITY: axis_key is immutable and generated from canonical components only.
    Never mutate axis_key with REPLACE - always regenerate from clean components.
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self._init_database()
        self._axis_key_mutation_blocked = True  # Prevent axis_key corruption
        self._quality_thresholds = {"bad_data_ratio_warning": 0.2, "bad_data_ratio_limit": 0.3}
        self._self_correcting_enabled = False  # DISABLED: No real outcomes yet
    
    def _init_database(self):
        """Initialize dimensional performance tracking."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dimensional_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prediction_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                prediction REAL NOT NULL,
                actual_return REAL,
                prediction_error REAL,
                confidence REAL,
                
                -- Dimensional tags
                environment_tag TEXT,
                sector_tag TEXT,
                model_tag TEXT,
                horizon_tag TEXT,
                volatility_tag TEXT,
                liquidity_tag TEXT,
                
                -- Performance tracking
                axis_key TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for fast axis lookup
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dim_axis_key ON dimensional_predictions(axis_key)
        """)
        
        conn.commit()
        conn.close()
    
    def extract_environment_tag(self, features: Dict[str, Any]) -> str:
        """Extract environment tag from features."""
        
        vol = features.get("volatility_20d", 0.02)
        trend = features.get("return_5d", 0.0)
        
        # Environment classification
        if vol > 0.03:
            env_tag = "HIGH_VOL"
        elif vol < 0.015:
            env_tag = "LOW_VOL"
        else:
            env_tag = "MED_VOL"
        
        if abs(trend) > 0.02:
            env_tag += "_TREND"
        elif abs(trend) < 0.005:
            env_tag += "_CHOP"
        else:
            env_tag += "_STABLE"
        
        return env_tag
    
    def extract_sector_tag(self, features: Dict[str, Any]) -> str:
        """Extract sector tag from features with normalization."""
        # Normalize sectors to canonical values to prevent fragmentation
        SECTOR_MAP = {
            "technology": "TECH",
            "tech": "TECH",
            "financial": "FINA",
            "financials": "FINA",
            "fin": "FINA",
            "healthcare": "HEAL",
            "health": "HEAL",
            "hea": "HEAL",
            "energy": "ENER",
            "ener": "ENER",
            "consumer": "CONS",
            "cons": "CONS",
            "industrial": "INDU",
            "materials": "MATL",
            "utilities": "UTIL",
            "real_estate": "REIT",
            "telecom": "TELE",
        }
        
        raw_sector = features.get("sector", "").lower()
        sector = SECTOR_MAP.get(raw_sector, "UNK")
        
        # Track data quality - no in-memory state, use database queries
        if sector == "UNK":
            print(f"DATA QUALITY: UNK sector detected for '{raw_sector}'")
        
        return sector
    
    def _get_persistent_data_quality_metrics(self) -> Dict[str, Any]:
        """Get real-time data quality metrics from database (persistent across runs)."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_count,
                    SUM(CASE WHEN sector_tag = 'UNK' THEN 1 ELSE 0 END) as unk_count
                FROM dimensional_predictions
            """)
            
            result = cursor.fetchone()
            total_count = result[0] if result else 0
            unk_count = result[1] if result else 0
            
            # Calculate ratios
            bad_sector_ratio = unk_count / total_count if total_count > 0 else 0.0
            
            return {
                "total_predictions": total_count,
                "bad_sector_count": unk_count,
                "bad_sector_ratio": bad_sector_ratio
            }
            
        except Exception as e:
            print(f"Error getting data quality metrics: {e}")
            return {"total_predictions": 0, "bad_sector_count": 0, "bad_sector_ratio": 0.0}
        finally:
            conn.close()
    
    def _check_data_quality_ratio(self):
        """Monitor data quality ratios and recommend trading adjustments."""
        metrics = self._get_persistent_data_quality_metrics()
        total = metrics["total_predictions"]
        
        if total == 0:
            return
        
        bad_ratio = metrics["bad_sector_ratio"]
        warning_threshold = self._quality_thresholds["bad_data_ratio_warning"]
        limit_threshold = self._quality_thresholds["bad_data_ratio_limit"]
        
        if bad_ratio > limit_threshold:
            print(f"DATA QUALITY ALERT: Bad data ratio {bad_ratio:.1%} exceeds limit {limit_threshold:.1%}")
            print("RECOMMENDATION: Consider reducing trading activity until data quality improves")
        elif bad_ratio > warning_threshold:
            print(f"DATA QUALITY WARNING: Bad data ratio {bad_ratio:.1%} exceeds warning {warning_threshold:.1%}")
            print("RECOMMENDATION: Monitor UNK axis performance separately")
    
    def get_data_quality_metrics(self) -> Dict[str, Any]:
        """Get current data quality metrics with ratios (persistent)."""
        return self._get_persistent_data_quality_metrics()
    
    def should_reduce_trading_activity(self) -> bool:
        """Check if trading activity should be reduced due to poor data quality."""
        metrics = self._get_persistent_data_quality_metrics()
        return metrics["bad_sector_ratio"] > self._quality_thresholds["bad_data_ratio_limit"]
    
    def get_unk_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for UNK sector tracking (persistent)."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT COUNT(*) as count,
                       AVG(prediction) as avg_prediction,
                       AVG(confidence) as avg_confidence
                FROM dimensional_predictions 
                WHERE sector_tag = 'UNK'
            """)
            
            unk_stats = cursor.fetchone()
            
            if unk_stats and unk_stats[0] > 0:
                # Get persistent total count
                metrics = self._get_persistent_data_quality_metrics()
                total_predictions = metrics["total_predictions"]
                
                return {
                    "unk_count": unk_stats[0],
                    "avg_prediction": unk_stats[1],
                    "avg_confidence": unk_stats[2],
                    "total_predictions": total_predictions,
                    "unk_percentage": unk_stats[0] / total_predictions if total_predictions > 0 else 0
                }
            else:
                return {"unk_count": 0, "message": "No UNK data yet"}
                
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()
    
    def get_axis_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for all axes (self-correcting foundation)."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT 
                    axis_key,
                    COUNT(*) as sample_count,
                    AVG(prediction) as avg_prediction,
                    AVG(confidence) as avg_confidence,
                    AVG(CASE WHEN actual_return IS NOT NULL THEN prediction_error ELSE NULL END) as avg_error,
                    COUNT(CASE WHEN actual_return IS NOT NULL THEN 1 ELSE NULL END) as outcome_count,
                    AVG(CASE WHEN actual_return IS NOT NULL THEN actual_return ELSE NULL END) as avg_actual_return,
                    COUNT(CASE WHEN actual_return IS NOT NULL AND actual_return > 0 THEN 1 ELSE NULL END) as win_count
                FROM dimensional_predictions 
                GROUP BY axis_key
                HAVING sample_count >= 5
                ORDER BY sample_count DESC
            """)
            
            axes_data = cursor.fetchall()
            
            if not axes_data:
                return {"message": "Insufficient data for axis performance analysis"}
            
            axis_metrics = {}
            for row in axes_data:
                axis_key, sample_count, avg_prediction, avg_confidence, avg_error, outcome_count, avg_actual_return, win_count = row
                
                # REAL OUTCOME-BASED METRICS ONLY
                if outcome_count < 10:  # Need real outcomes
                    performance_score = 0.5  # Neutral until we have data
                    win_rate = 0.0
                else:
                    # Real performance based on actual returns
                    win_rate = win_count / outcome_count if outcome_count > 0 else 0.0
                    # Performance score: win_rate + avg_return (normalized)
                    performance_score = win_rate + (avg_actual_return if avg_actual_return else 0.0)
                    performance_score = max(0.0, min(1.0, performance_score))  # Clamp to [0,1]
                
                # Calculate reliability (outcome coverage)
                reliability = outcome_count / sample_count if sample_count > 0 else 0.0
                
                axis_metrics[axis_key] = {
                    "sample_count": sample_count,
                    "avg_prediction": avg_prediction,
                    "avg_confidence": avg_confidence,
                    "avg_error": avg_error,
                    "outcome_count": outcome_count,
                    "avg_actual_return": avg_actual_return,
                    "win_count": win_count,
                    "win_rate": win_rate,
                    "reliability": reliability,
                    "performance_score": performance_score,
                    "has_real_outcomes": outcome_count > 0
                }
            
            return axis_metrics
            
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()
    
    def get_real_outcome_status(self) -> Dict[str, Any]:
        """Check if we have enough real outcomes for self-correction."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Count predictions with real outcomes
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_predictions,
                    COUNT(CASE WHEN actual_return IS NOT NULL THEN 1 ELSE NULL END) as predictions_with_outcomes,
                    COUNT(DISTINCT axis_key) as total_axes,
                    COUNT(DISTINCT CASE WHEN actual_return IS NOT NULL THEN axis_key ELSE NULL END) as axes_with_outcomes
                FROM dimensional_predictions
            """)
            
            stats = cursor.fetchone()
            total_predictions, predictions_with_outcomes, total_axes, axes_with_outcomes = stats
            
            # Check minimum thresholds for self-correction
            min_outcomes_per_axis = 30
            min_axes_with_outcomes = 3
            
            can_self_correct = (
                predictions_with_outcomes >= min_outcomes_per_axis * min_axes_with_outcomes and
                axes_with_outcomes >= min_axes_with_outcomes
            )
            
            return {
                "total_predictions": total_predictions,
                "predictions_with_outcomes": predictions_with_outcomes,
                "total_axes": total_axes,
                "axes_with_outcomes": axes_with_outcomes,
                "min_outcomes_per_axis": min_outcomes_per_axis,
                "min_axes_with_outcomes": min_axes_with_outcomes,
                "can_self_correct": can_self_correct,
                "outcome_coverage": predictions_with_outcomes / total_predictions if total_predictions > 0 else 0.0
            }
            
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()
    
    def calculate_axis_weights(self) -> Dict[str, float]:
        """Calculate self-correcting axis weights based on REAL performance only."""
        if not self._self_correcting_enabled:
            return {"message": "Self-correcting disabled"}
        
        # Check if we have enough real outcomes
        outcome_status = self.get_real_outcome_status()
        if not outcome_status.get("can_self_correct", False):
            return {"message": "Insufficient real outcomes for self-correction"}
        
        axis_metrics = self.get_axis_performance_metrics()
        
        if "error" in axis_metrics or "message" in axis_metrics:
            return axis_metrics
        
        weights = {}
        
        for axis_key, metrics in axis_metrics.items():
            # ONLY use axes with real outcomes
            if not metrics.get("has_real_outcomes", False) or metrics.get("outcome_count", 0) < 10:
                weights[axis_key] = 0.5  # Neutral weight for axes without real data
                continue
            
            # Base weight from REAL win rate + actual returns
            base_weight = metrics["performance_score"]
            
            # Boost for high reliability (more real outcomes)
            reliability_boost = metrics["reliability"] * 0.2
            
            # Boost for sufficient real sample size
            sample_boost = min(0.1, metrics["outcome_count"] / 100) if metrics["outcome_count"] >= 30 else 0.0
            
            # Penalty for UNK axis (always)
            unk_penalty = 0.3 if "UNK" in axis_key else 0.0
            
            # Calculate final weight
            final_weight = base_weight + reliability_boost + sample_boost - unk_penalty
            final_weight = max(0.1, min(1.0, final_weight))  # Clamp between 0.1 and 1.0
            
            weights[axis_key] = final_weight
        
        return weights
    
    def apply_self_correcting_adjustment(self, axis_key: str, base_confidence: float) -> float:
        """Apply self-correcting weight adjustment to confidence."""
        if not self._self_correcting_enabled:
            return base_confidence
        
        weights = self.calculate_axis_weights()
        
        if "error" in weights or "message" in weights:
            return base_confidence
        
        # Get weight for this axis, default to 1.0 if not found
        axis_weight = weights.get(axis_key, 1.0)
        
        # Apply weight adjustment
        adjusted_confidence = base_confidence * axis_weight
        
        # Log significant adjustments
        if axis_weight < 0.7:
            print(f"SELF-CORRECTING: Reduced confidence for weak axis {axis_key}: {axis_weight:.2f}x")
        elif axis_weight > 0.9:
            print(f"SELF-CORRECTING: Boosted confidence for strong axis {axis_key}: {axis_weight:.2f}x")
        
        return adjusted_confidence
    
    def extract_model_tag(self, prediction: float, confidence: float) -> str:
        """Extract model behavior tag from prediction characteristics."""
        
        if confidence > 0.8 and abs(prediction) > 0.02:
            return "AGGRESSIVE"
        elif confidence > 0.7 and prediction < 0:
            return "DEFENSIVE"
        else:
            return "BALANCED"
    
    def extract_volatility_tag(self, features: Dict[str, Any]) -> str:
        """Extract volatility regime tag."""
        vol = features.get("volatility_20d", 0.02)
        
        if vol > 0.035:
            return "HIGH_VOL"
        elif vol < 0.015:
            return "LOW_VOL"
        else:
            return "MED_VOL"
    
    def extract_liquidity_tag(self, features: Dict[str, Any]) -> str:
        """Extract liquidity tag from features."""
        
        volume = features.get("dollar_volume", 1000000)
        volume_zscore = features.get("volume_zscore_20d", 0.0)
        
        if volume > 5000000 and volume_zscore > 2.0:
            return "HIGH_LIQ"
        elif volume < 500000 or volume_zscore < -1.0:
            return "LOW_LIQ"
        else:
            return "MED_LIQ"
    
    def create_dimensional_tags(self, features: Dict[str, Any], prediction: float, 
                          confidence: float, horizon: str = "7d") -> DimensionalTags:
        """Create lightweight dimensional tags for prediction."""
        
        # Extract components
        environment = self.extract_environment_tag(features)
        sector = self.extract_sector_tag(features)
        model = self.extract_model_tag(prediction, confidence)
        volatility = self.extract_volatility_tag(features)
        liquidity = self.extract_liquidity_tag(features)
        
        # DATA INTEGRITY GUARDRAILS: Fail-safe validation for production
        VALID_SECTORS = {"TECH","FINA","HEAL","ENER","CONS","UNK"}
        VALID_ENVIRONMENTS = {"HIGH_VOL_TREND","HIGH_VOL_STABLE","HIGH_VOL_CHOP","MED_VOL_TREND","MED_VOL_STABLE","MED_VOL_CHOP","LOW_VOL_TREND","LOW_VOL_STABLE","LOW_VOL_CHOP"}
        VALID_MODELS = {"AGGRESSIVE","DEFENSIVE","BALANCED"}
        VALID_HORIZONS = {"1d","5d","7d","20d"}
        VALID_VOLATILITIES = {"HIGH_VOL","MED_VOL","LOW_VOL"}
        VALID_LIQUIDITIES = {"HIGH_LIQ","MED_LIQ","LOW_LIQ"}
        
        # Fail-safe: fallback to unknown values instead of crashing
        if sector not in VALID_SECTORS:
            sector = "UNK"
            print(f"WARNING: Invalid sector_tag normalized to UNK")
        
        if environment not in VALID_ENVIRONMENTS:
            environment = "MED_VOL_STABLE"  # Safe default
            print(f"WARNING: Invalid environment_tag normalized to MED_VOL_STABLE")
        
        if model not in VALID_MODELS:
            model = "BALANCED"  # Safe default
            print(f"WARNING: Invalid model_tag normalized to BALANCED")
        
        if horizon not in VALID_HORIZONS:
            horizon = "7d"  # Safe default
            print(f"WARNING: Invalid horizon_tag normalized to 7d")
        
        if volatility not in VALID_VOLATILITIES:
            volatility = "MED_VOL"  # Safe default
            print(f"WARNING: Invalid volatility_tag normalized to MED_VOL")
        
        if liquidity not in VALID_LIQUIDITIES:
            liquidity = "MED_LIQ"  # Safe default
            print(f"WARNING: Invalid liquidity_tag normalized to MED_LIQ")
        
        # DATA AWARENESS: Dynamic distrust based on real data quality
        if sector == "UNK":
            # Get real-time data quality ratio from database
            metrics = self._get_persistent_data_quality_metrics()
            bad_ratio = metrics["bad_sector_ratio"]
            
            # Dynamic confidence reduction: max(0.3, 1 - bad_data_ratio)
            confidence_multiplier = max(0.3, 1 - bad_ratio)
            confidence *= confidence_multiplier
            
            print(f"DATA AWARENESS: Reduced confidence for UNK sector: {confidence:.2f} (ratio: {bad_ratio:.1%}, multiplier: {confidence_multiplier:.2f})")
        
        # SELF-CORRECTING: Apply axis performance adjustment
        axis_key = f"{environment}_{sector}_{model}_{horizon}"
        adjusted_confidence = self.apply_self_correcting_adjustment(axis_key, confidence)
        
        if adjusted_confidence != confidence:
            confidence = adjusted_confidence
        
        return DimensionalTags(
            environment=environment,
            sector=sector,
            model=model,
            horizon=horizon,
            volatility=volatility,
            liquidity=liquidity,
            confidence=confidence,
            prediction=prediction
        )
    
    def store_dimensional_prediction(self, symbol: str, features: Dict[str, Any], 
                                 prediction: float, confidence: float, 
                                 horizon: str = "7d") -> str:
        """Store prediction with dimensional tags."""
        
        tags = self.create_dimensional_tags(features, prediction, confidence, horizon)
        
        # DATA INTEGRITY GUARDRAIL: Generate axis_key from clean components only
        # Never mutate axis_key with REPLACE - always generate from canonical values
        axis_key = f"{tags.environment}_{tags.sector}_{tags.model}_{tags.horizon}"
        
        # Basic sanity check only - axis_key should exist and be non-empty
        assert axis_key and len(axis_key) > 0, f"Invalid axis_key: {axis_key}"
        
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT INTO dimensional_predictions 
                (prediction_date, symbol, prediction, actual_return, prediction_error, confidence,
                 environment_tag, sector_tag, model_tag, horizon_tag, 
                 volatility_tag, liquidity_tag, axis_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),  # Add prediction_date
                symbol, prediction, None, None, confidence,  # No actual_return yet
                tags.environment, tags.sector, tags.model, horizon,
                tags.volatility, tags.liquidity, axis_key
            ))
            conn.commit()
            
            print(f"🏷 Stored: {symbol} prediction={prediction:.4f} axis={axis_key}")
            return axis_key
            
        except Exception as e:
            print(f"❌ Failed to store dimensional prediction: {e}")
            return ""
        finally:
            conn.close()
    
    def update_prediction_outcome(self, symbol: str, actual_return: float):
        """Update prediction with actual outcome."""
        
        conn = sqlite3.connect(self.db_path)
        try:
            # Update the most recent prediction for this symbol
            conn.execute("""
                UPDATE dimensional_predictions 
                SET actual_return = ?, prediction_error = ABS(? - actual_return)
                WHERE symbol = ? AND actual_return IS NULL
                ORDER BY created_at DESC
                LIMIT 1
            """, (actual_return, actual_return, symbol))
            conn.commit()
            
            print(f"📊 Updated: {symbol} actual={actual_return:.4f}")
            
        except Exception as e:
            print(f"❌ Failed to update prediction outcome: {e}")
        finally:
            conn.close()
    
    def get_axis_performance(self, axis_key: str, days: int = 30) -> Dict[str, Any]:
        """Get performance for specific dimensional axis."""
        
        conn = sqlite3.connect(self.db_path)
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            row = conn.execute("""
                SELECT COUNT(*) as sample_count,
                       AVG(prediction_error) as avg_error,
                       AVG(actual_return) as avg_return,
                       SUM(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate,
                       AVG(confidence) as avg_confidence
                FROM dimensional_predictions 
                WHERE axis_key = ? AND prediction_date >= ?
            """, (axis_key, cutoff_date)).fetchone()
            
            if row:
                return {
                    "axis_key": axis_key,
                    "sample_count": row[0],
                    "avg_error": row[1],
                    "avg_return": row[2],
                    "win_rate": row[3],
                    "avg_confidence": row[4],
                    "sharpe": row[2] / (row[1] + 0.001) if row[1] > 0 else 0,
                    "period_days": days
                }
            
            return {"error": f"No data found for axis: {axis_key}"}
            
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()
    
    def get_best_axes(self, min_samples: int = 20) -> Dict[str, Any]:
        """Get best performing axes across all dimensions."""
        
        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute("""
                SELECT axis_key, 
                       COUNT(*) as sample_count,
                       AVG(prediction_error) as avg_error,
                       AVG(actual_return) as avg_return,
                       SUM(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate,
                       AVG(confidence) as avg_confidence
                FROM dimensional_predictions 
                WHERE prediction_date >= date('now', '-30 days')
                GROUP BY axis_key
                HAVING COUNT(*) >= ?
                ORDER BY avg_error ASC, win_rate DESC
            """, (min_samples,)).fetchall()
            
            results = {}
            for row in rows:
                # Parse axis components
                parts = row[0].split('_')
                if len(parts) >= 3:
                    results[row[0]] = {
                        "axis_key": row[0],
                        "environment": parts[0],
                        "sector": parts[1], 
                        "model": parts[2],
                        "horizon": parts[3] if len(parts) > 3 else "7d",
                        "sample_count": row[1],
                        "avg_error": row[2],
                        "avg_return": row[3],
                        "win_rate": row[4],
                        "avg_confidence": row[5],
                        "sharpe": row[3] / (row[2] + 0.001) if row[2] > 0 else 0,
                    }
            
            return results
            
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()
    
    def should_activate_prediction(self, axis_key: str, min_win_rate: float = 0.55, 
                           cold_start_mode: bool = True, min_samples: int = 50) -> bool:
        """Decide whether to activate prediction based on historical performance."""
        
        performance = self.get_axis_performance(axis_key, days=30)
        
        if "error" in performance:
            # Cold start mode: allow trades to build history
            if cold_start_mode:
                print(f"  Cold start: ACTIVATE {axis_key} (no history yet)")
                return True
            return False  # No data, don't activate
        
        # Cold start mode: allow trades until we have enough samples
        if cold_start_mode and performance["sample_count"] < min_samples:
            print(f"  Building history: ACTIVATE {axis_key} ({performance['sample_count']}/{min_samples} samples)")
            return True
        
        # Normal mode: only activate if historically performs well
        should_activate = (performance["win_rate"] >= min_win_rate and 
                          performance["avg_error"] < 0.015)
        
        if should_activate:
            print(f"  Proven edge: ACTIVATE {axis_key} (win_rate: {performance['win_rate']:.1%})")
        else:
            print(f"  Poor performance: BLOCK {axis_key} (win_rate: {performance['win_rate']:.1%})")
        
        return should_activate
    
    def get_activation_matrix(self) -> Dict[str, Dict[str, bool]]:
        """Get activation matrix for all axis combinations."""
        
        best_axes = self.get_best_axes()
        activation_matrix = {}
        
        for axis_key, performance in best_axes.items():
            env = performance["environment"]
            sector = performance["sector"]
            model = performance["model"]
            
            if env not in activation_matrix:
                activation_matrix[env] = {}
            if sector not in activation_matrix[env]:
                activation_matrix[env][sector] = {}
            
            activation_matrix[env][sector][model] = self.should_activate_prediction(axis_key)
        
        return activation_matrix


# Global dimensional tagger instance
_dimensional_tagger = None


def get_dimensional_tagger() -> DimensionalTagger:
    """Get global dimensional tagger instance."""
    global _dimensional_tagger
    if _dimensional_tagger is None:
        _dimensional_tagger = DimensionalTagger()
    return _dimensional_tagger


# Convenience functions for production use
def tag_and_store_prediction(symbol: str, features: Dict[str, Any], 
                           prediction: float, confidence: float, 
                           horizon: str = "7d") -> str:
    """Tag and store prediction with dimensional axes."""
    tagger = get_dimensional_tagger()
    return tagger.store_dimensional_prediction(symbol, features, prediction, confidence, horizon)


def update_prediction_result(symbol: str, actual_return: float):
    """Update prediction with actual outcome."""
    tagger = get_dimensional_tagger()
    return tagger.update_prediction_outcome(symbol, actual_return)


def should_activate_for_conditions(environment: str, sector: str, model: str, 
                                cold_start_mode: bool = True) -> bool:
    """Check if prediction should be activated for current conditions."""
    
    tagger = get_dimensional_tagger()
    axis_key = f"{environment}_{sector}_{model}_7d"
    return tagger.should_activate_prediction(axis_key, cold_start_mode=cold_start_mode)


def get_best_performing_axes() -> Dict[str, Any]:
    """Get best performing axes for selective activation."""
    tagger = get_dimensional_tagger()
    return tagger.get_best_axes()


def get_activation_rules() -> Dict[str, Dict[str, bool]]:
    """Get activation matrix for production decisions."""
    tagger = get_dimensional_tagger()
    return tagger.get_activation_matrix()
