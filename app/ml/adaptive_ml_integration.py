"""
Adaptive ML Integration: Connects regime-aware ML with our adaptive infrastructure.

This creates the specialization → stronger signals → portfolio integration pipeline.
"""

from __future__ import annotations

from typing import Dict, List, Any
from collections import defaultdict

from app.core.environment import build_env_snapshot_v3
from app.core.environment_v3 import bucket_env_v3
from app.ml.regime_aware_ml import get_regime_aware_ml, RegimeAwareML
from app.discovery.adaptive_stats import store_adaptive_stats, lookup_best_config
from app.discovery.adaptive_selection import enable_adaptive_globally


class AdaptiveMLIntegration:
    """
    Integrates regime-aware ML with adaptive discovery pipeline.
    
    Creates specialization → stronger signals → portfolio integration.
    """
    
    def __init__(self):
        self.regime_ml = get_regime_aware_ml()
        self.adaptive_stats = defaultdict(list)
        
    def run_adaptive_ml_discovery(self, features: Dict[str, Any], db_path: str, as_of: str) -> Dict[str, Any]:
        """Run discovery with adaptive ML selection."""
        
        # Detect current environment
        env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
        env_bucket = bucket_env_v3(env)
        
        print(f"=== ADAPTIVE ML DISCOVERY ===")
        print(f"Environment: {env_bucket}")
        print(f"Market vol: {env.market_vol_pct:.2f}")
        print(f"Sector regime: {env.sector_regime}")
        print(f"Industry dispersion: {env.industry_dispersion:.2f}")
        
        # Get ML prediction for current regime
        ml_result = self.regime_ml.predict(features, db_path, as_of)
        
        print(f"ML Prediction: {ml_result['prediction']:.4f}")
        print(f"Model Type: {ml_result['model_type']}")
        print(f"Confidence: {ml_result['confidence']:.2f}")
        
        # Store adaptive stats for ML model selection
        self.store_ml_adaptive_stats(env_bucket, ml_result, as_of)
        
        # Generate candidates based on ML signals
        ml_candidates = self.generate_ml_candidates(features, ml_result, env_bucket)
        
        return {
            "environment": {
                "env_bucket": env_bucket,
                "market_vol_pct": env.market_vol_pct,
                "trend_strength": env.trend_strength,
                "cross_sectional_disp": env.cross_sectional_disp,
                "liquidity_regime": env.liquidity_regime,
                "sector_regime": env.sector_regime,
                "industry_dispersion": env.industry_dispersion,
                "size_regime": env.size_regime,
            },
            "ml_prediction": ml_result,
            "candidates": ml_candidates,
            "adaptive_stats": dict(self.adaptive_stats),
        }
    
    def generate_ml_candidates(self, features: Dict[str, Any], ml_result: Dict, env_bucket: tuple) -> List[Dict]:
        """Generate discovery candidates based on ML signals."""
        
        candidates = []
        prediction = ml_result["prediction"]
        confidence = ml_result["confidence"]
        regime_name = ml_result["regime_name"]
        
        # Filter and rank stocks based on ML prediction
        for symbol, feature_dict in features.items():
            # Calculate ML score for this stock
            ml_score = self.calculate_ml_score(feature_dict, prediction, confidence)
            
            if ml_score > 0.3:  # Minimum ML threshold
                candidates.append({
                    "symbol": symbol,
                    "ml_score": ml_score,
                    "ml_prediction": prediction,
                    "ml_confidence": confidence,
                    "regime": regime_name,
                    "features": feature_dict,
                    "strategy_type": "adaptive_ml",
                })
        
        # Sort by ML score
        candidates.sort(key=lambda x: x["ml_score"], reverse=True)
        
        print(f"Generated {len(candidates)} ML candidates")
        print(f"Top 3: {[c['symbol'] for c in candidates[:3]]}")
        
        return candidates[:50]  # Top 50 candidates
    
    def calculate_ml_score(self, feature_dict: Dict, prediction: float, confidence: float) -> float:
        """Calculate ML score for individual stock."""
        
        # Base score from prediction strength
        base_score = abs(prediction) * confidence
        
        # Adjust for stock-specific factors
        volatility = feature_dict.get("volatility_20d", 0.02)
        volume = feature_dict.get("dollar_volume", 1000000)
        
        # Volatility adjustment
        if abs(prediction) > 0.01:  # Strong prediction
            vol_multiplier = 1.2 if volatility > 0.025 else 0.8
        else:
            vol_multiplier = 1.0
        
        # Volume adjustment (liquidity filter)
        vol_multiplier = 1.1 if volume > 5000000 else 0.9
        
        # Sector adjustment (if available)
        sector = feature_dict.get("sector")
        sector_multiplier = 1.0
        if sector:
            # Different sectors respond differently to ML signals
            sector_multipliers = {
                "technology": 1.2,
                "financials": 1.1,
                "healthcare": 0.9,
                "energy": 1.15,
                "consumer": 1.0,
                "industrial": 1.05,
            }
            sector_multiplier = sector_multipliers.get(sector, 1.0)
        
        final_score = base_score * vol_multiplier * vol_multiplier * sector_multiplier
        
        return min(1.0, final_score)  # Cap at 1.0
    
    def store_ml_adaptive_stats(self, env_bucket: tuple, ml_result: Dict, as_of: str):
        """Store adaptive stats for ML model performance."""
        
        # Create mock outcome for stats tracking
        # In production, this would be real performance data
        mock_outcome = {
            "return": ml_result["prediction"] * 0.5,  # Simplified
            "success": ml_result["confidence"] > 0.6,
            "regime": env_bucket,
            "model_type": ml_result["model_type"],
            "as_of": as_of,
        }
        
        # Store in adaptive stats
        self.adaptive_stats[env_bucket].append(mock_outcome)
        
        # Also store in database for persistence
        try:
            from app.discovery.outcomes import OutcomeRow
            
            outcome = OutcomeRow(
                symbol="ML_MODEL",
                horizon_days=7,
                entry_date=as_of,
                exit_date=as_of,
                entry_close=100.0,
                exit_close=100.0 * (1 + mock_outcome["return"]),
                return_pct=mock_outcome["return"],
                overlap_count=1,
                days_seen=7,
                strategies=["adaptive_ml"],
            )
            
            config = {
                "config_name": f"ml_{ml_result['regime_name']}",
                "model_type": ml_result["model_type"],
                "regime": ml_result["regime"],
                "confidence_threshold": 0.6,
            }
            
            store_adaptive_stats(
                db_path="data/alpha.db",
                strategy="adaptive_ml",
                config=config,
                env_bucket=env_bucket,
                outcomes=[outcome],
            )
            
        except Exception as e:
            print(f"Failed to store adaptive stats: {e}")
    
    def analyze_ml_performance(self) -> Dict[str, Any]:
        """Analyze ML performance across regimes."""
        
        print("=== ML PERFORMANCE ANALYSIS ===")
        
        performance_by_regime = {}
        
        for regime, outcomes in self.adaptive_stats.items():
            if not outcomes:
                continue
            
            # Calculate performance metrics
            returns = [o["return"] for o in outcomes]
            successes = [o for o in outcomes if o["success"]]
            
            avg_return = sum(returns) / len(returns)
            success_rate = len(successes) / len(outcomes)
            
            performance_by_regime[regime] = {
                "samples": len(outcomes),
                "avg_return": avg_return,
                "success_rate": success_rate,
                "model_types": list(set(o["model_type"] for o in outcomes)),
            }
            
            print(f"{regime}:")
            print(f"  Samples: {len(outcomes)}")
            print(f"  Avg return: {avg_return:.4f}")
            print(f"  Success rate: {success_rate:.1%}")
        
        # Get model summary
        model_summary = self.regime_ml.get_model_summary()
        
        return {
            "performance_by_regime": performance_by_regime,
            "model_summary": model_summary,
            "total_regimes": len(performance_by_regime),
        }


# Global adaptive ML integration instance
_adaptive_ml_integration = None


def get_adaptive_ml_integration() -> AdaptiveMLIntegration:
    """Get global adaptive ML integration instance."""
    global _adaptive_ml_integration
    if _adaptive_ml_integration is None:
        _adaptive_ml_integration = AdaptiveMLIntegration()
    return _adaptive_ml_integration
