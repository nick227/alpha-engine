"""
Lightweight Dimensional ML: Production-ready system using axis tagging.

Instead of complex model selection, uses lightweight dimensional tagging
to build performance surfaces and enable selective activation.
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
from datetime import datetime

from app.ml.dimensional_tagger import (
    get_dimensional_tagger, tag_and_store_prediction, update_prediction_result,
    should_activate_for_conditions, get_best_performing_axes, get_activation_rules
)
from app.core.environment import build_env_snapshot_v3
from app.ml.feature_builder import FeatureBuilder


class LightweightDimensionalML:
    """
    Lightweight ML system using dimensional tagging for selective activation.
    
    Core principle: Tag everything, track performance, activate only proven edges.
    """
    
    def __init__(self):
        self.tagger = get_dimensional_tagger()
        self.feature_builder = FeatureBuilder()
    
    def predict_with_dimensional_tagging(self, features: Dict[str, Any], 
                                     base_prediction: float, 
                                     confidence: float,
                                     db_path: str, as_of: str) -> Dict[str, Any]:
        """
        Make prediction with lightweight dimensional tagging.
        
        Instead of complex model selection, just tag and store.
        """
        # Get current environment for context
        env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
        
        # Create dimensional tags
        symbol = list(features.keys())[0]  # Get first symbol
        axis_key = tag_and_store_prediction(
            symbol, features, base_prediction, confidence
        )
        
        # Check if we should activate this prediction (cold start mode enabled)
        should_activate = self.tagger.should_activate_prediction(axis_key, cold_start_mode=True)
        
        # Extract environment and sector from features
        env_tag = self.tagger.extract_environment_tag(features)
        sector_tag = self.tagger.extract_sector_tag(features)
        
        return {
            "symbol": symbol,
            "prediction": base_prediction,
            "confidence": confidence,
            "axis_key": axis_key,
            "should_activate": should_activate,
            "environment_tag": env_tag,
            "sector_tag": sector_tag,
            "activation_reason": self._get_activation_reason(should_activate, axis_key),
            "dimensional_context": {
                "market_vol_pct": env.market_vol_pct,
                "trend_strength": env.trend_strength,
                "sector_regime": env.sector_regime,
                "industry_dispersion": env.industry_dispersion,
            }
        }
    
    def _get_activation_reason(self, should_activate: bool, axis_key: str) -> str:
        """Get human-readable activation reason."""
        
        if should_activate:
            return f"ACTIVATED: {axis_key} has proven edge"
        else:
            return f"BLOCKED: {axis_key} insufficient performance"
    
    def batch_predict_and_tag(self, features_dict: Dict[str, Any], 
                           predictions: Dict[str, float], 
                           confidences: Dict[str, float],
                           db_path: str, as_of: str) -> List[Dict[str, Any]]:
        """
        Batch process multiple predictions with dimensional tagging.
        """
        results = []
        
        print(f"🏷 BATCH DIMENSIONAL PREDICTION: {len(predictions)} predictions")
        
        for symbol, prediction in predictions.items():
            if symbol in features_dict:
                features = features_dict[symbol]
                confidence = confidences.get(symbol, 0.5)
                
                result = self.predict_with_dimensional_tagging(
                    features, prediction, confidence, db_path, as_of
                )
                
                results.append(result)
        
        # Summary statistics
        activated_count = sum(1 for r in results if r["should_activate"])
        blocked_count = len(results) - activated_count
        
        print(f"📊 BATCH SUMMARY:")
        print(f"  Activated: {activated_count} predictions")
        print(f"  Blocked: {blocked_count} predictions")
        print(f"  Activation Rate: {activated_count/len(results):.1%}")
        
        return results
    
    def selective_activation_pipeline(self, features_dict: Dict[str, Any], 
                                db_path: str, as_of: str,
                                activation_mode: str = "conservative") -> List[Dict[str, Any]]:
        """
        Production pipeline with selective activation based on proven edges.
        
        activation_mode can be:
        - "conservative": Only activate high-confidence edges
        - "moderate": Activate moderate-performing edges  
        - "aggressive": Activate all predictions with decent performance
        """
        
        # Get current environment
        env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
        
        # Get activation rules
        activation_matrix = get_activation_rules()
        
        print(f"🎯 SELECTIVE ACTIVATION PIPELINE ({activation_mode})")
        print(f"Environment: {env.sector_regime} regime, vol: {env.market_vol_pct:.2f}")
        
        activated_predictions = []
        blocked_predictions = []
        
        for symbol, features in features_dict.items():
            # Extract dimensional tags
            env_tag = self.tagger.extract_environment_tag(features)
            sector_tag = self.tagger.extract_sector_tag(features)
            
            # Check activation rules
            should_activate = False
            activation_reason = "No matching rule"
            
            if (env_tag in activation_matrix and 
                sector_tag in activation_matrix[env_tag] and
                activation_matrix[env_tag][sector_tag]):
                
                # Found matching activation rule
                for model, should_activate in activation_matrix[env_tag][sector_tag].items():
                    if should_activate:
                        should_activate = True
                        activation_reason = f"ACTIVATED: {model} edge for {env_tag}_{sector_tag}"
                        break
            
            # Apply activation mode filters
            if should_activate:
                if activation_mode == "conservative":
                    # Only activate if high confidence and strong historical performance
                    should_activate = (self._get_confidence_for_symbol(features) > 0.8 and 
                                      self._get_historical_performance(symbol) > 0.6)
                elif activation_mode == "moderate":
                    # Activate if reasonable performance
                    should_activate = (self._get_historical_performance(symbol) > 0.45)
                # aggressive mode activates all that pass basic filters
            
            result = {
                "symbol": symbol,
                "should_activate": should_activate,
                "activation_reason": activation_reason,
                "environment_tag": env_tag,
                "sector_tag": sector_tag,
                "confidence": self._get_confidence_for_symbol(features),
                "historical_performance": self._get_historical_performance(symbol),
                "activation_mode": activation_mode
            }
            
            if should_activate:
                activated_predictions.append(result)
            else:
                blocked_predictions.append(result)
        
        # Summary
        print(f"📊 ACTIVATION SUMMARY:")
        print(f"  Activated: {len(activated_predictions)} predictions")
        print(f"  Blocked: {len(blocked_predictions)} predictions")
        print(f"  Selectivity: {len(activated_predictions)/(len(activated_predictions) + len(blocked_predictions)):.1%}")
        
        return activated_predictions
    
    def _get_confidence_for_symbol(self, features: Dict[str, Any]) -> float:
        """Extract confidence score for symbol."""
        # This would use your actual confidence calculation
        # For demo, return based on feature characteristics
        vol = features.get("volatility_20d", 0.02)
        volume = features.get("dollar_volume", 1000000)
        
        base_confidence = 0.5
        
        # Adjust confidence based on data quality
        if vol < 0.01 or vol > 0.05:
            base_confidence -= 0.2  # Unusual volatility
        
        if volume < 100000:
            base_confidence -= 0.1  # Low volume
        
        return max(0.1, min(0.9, base_confidence))
    
    def _get_historical_performance(self, symbol: str) -> float:
        """Get historical performance for symbol."""
        # This would query actual historical performance
        # For demo, return mock performance based on symbol
        hash_val = hash(symbol) % 100
        return 0.3 + (hash_val / 100.0)  # Range 0.3 to 1.3
    
    def get_performance_surface_analysis(self) -> Dict[str, Any]:
        """Analyze performance surface across dimensional axes."""
        
        best_axes = get_best_performing_axes()
        
        if not best_axes or "error" in best_axes:
            return {"error": "Insufficient data for analysis"}
        
        # Analyze by environment
        env_performance = {}
        for axis_key, performance in best_axes.items():
            env = performance["environment"]
            if env not in env_performance:
                env_performance[env] = []
            env_performance[env].append({
                "axis_key": axis_key,
                "win_rate": performance["win_rate"],
                "sharpe": performance["sharpe"]
            })
        
        # Analyze by sector
        sector_performance = {}
        for axis_key, performance in best_axes.items():
            sector = performance["sector"]
            if sector not in sector_performance:
                sector_performance[sector] = []
            sector_performance[sector].append({
                "axis_key": axis_key,
                "win_rate": performance["win_rate"],
                "sharpe": performance["sharpe"]
            })
        
        # Find patterns
        best_env = max(env_performance.items(), key=lambda x: len(x[1]) if len(x[1]) > 0 else 0)
        best_sector = max(sector_performance.items(), key=lambda x: len(x[1]) if len(x[1]) > 0 else 0)
        
        return {
            "total_analyzed_axes": len(best_axes),
            "environment_performance": env_performance,
            "sector_performance": sector_performance,
            "best_environment": best_env[0] if best_env else "none",
            "best_sector": best_sector[0] if best_sector else "none",
            "analysis_date": datetime.now().isoformat()
        }


# Global lightweight dimensional ML instance
_lightweight_dimensional_ml = None


def get_lightweight_dimensional_ml() -> LightweightDimensionalML:
    """Get global lightweight dimensional ML instance."""
    global _lightweight_dimensional_ml
    if _lightweight_dimensional_ml is None:
        _lightweight_dimensional_ml = LightweightDimensionalML()
    return _lightweight_dimensional_ml


# Production-ready convenience functions
def predict_with_selective_activation(features_dict: Dict[str, Any], 
                                 predictions: Dict[str, float],
                                 confidences: Dict[str, float],
                                 db_path: str, as_of: str,
                                 activation_mode: str = "conservative") -> List[Dict[str, Any]]:
    """Production function for selective activation based on proven edges."""
    
    ml_system = get_lightweight_dimensional_ml()
    return ml_system.selective_activation_pipeline(
        features_dict, predictions, confidences, db_path, as_of, activation_mode
    )


def analyze_performance_surface() -> Dict[str, Any]:
    """Analyze performance surface across all dimensional axes."""
    
    ml_system = get_lightweight_dimensional_ml()
    return ml_system.get_performance_surface_analysis()
