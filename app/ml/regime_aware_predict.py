"""
Regime-Aware Prediction: Fixed architecture using pre-trained models.

This replaces the broken runtime training with proper model selection.
"""

from __future__ import annotations

from typing import Dict, Any, Tuple, Optional

from app.core.environment import build_env_snapshot_v3
from app.ml.regime_model_loader import get_model, get_fallback_model


def predict_regime_aware(features: Dict[str, Any], db_path: str, as_of: str) -> Dict[str, Any]:
    """
    Make prediction using pre-trained regime-specific model.
    
    Fixed architecture: no runtime training, just model selection.
    """
    # Detect current environment
    env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
    
    # Get appropriate model for current regime
    model, model_key = get_model(env, horizon="7d")
    
    if model is None:
        print(f"No model found for {model_key}, using fallback")
        model, model_key = get_fallback_model(horizon="7d")
    
    if model is None:
        return {
            "prediction": 0.0,
            "confidence": 0.1,
            "model_key": "none",
            "regime": "unknown",
            "error": "No suitable model available"
        }
    
    # Prepare features for prediction
    try:
        # This would use your actual feature builder
        # For demo, create simple feature vector
        feature_vector = [
            features.get("volatility_20d", 0.02),
            features.get("return_5d", 0.0),
            features.get("return_63d", 0.0),
            features.get("dollar_volume", 1000000) / 1000000,  # Normalize
            features.get("volume_zscore_20d", 0.0),
            features.get("price_percentile_252d", 0.5),
        ]
        
        # Make prediction
        prediction = model.predict([feature_vector])
        prediction_value = float(prediction[0]) if len(prediction) > 0 else 0.0
        
        # Calculate confidence based on model match
        confidence = calculate_confidence(env, model_key)
        
        return {
            "prediction": prediction_value,
            "confidence": confidence,
            "model_key": model_key,
            "regime": {
                "vol_regime": "HI_VOL" if env.market_vol_pct >= 0.7 else "LO_VOL",
                "trend_regime": "TREND" if abs(env.trend_strength) >= 0.2 else "CHOP",
                "sector_regime": env.sector_regime,
                "industry_dispersion": env.industry_dispersion,
                "size_regime": env.size_regime,
            },
            "environment": {
                "market_vol_pct": env.market_vol_pct,
                "trend_strength": env.trend_strength,
                "cross_sectional_disp": env.cross_sectional_disp,
                "liquidity_regime": env.liquidity_regime,
                "sector_regime": env.sector_regime,
                "industry_dispersion": env.industry_dispersion,
                "size_regime": env.size_regime,
            },
            "model_type": "regime_aware_pretrained",
            "feature_vector": feature_vector,
        }
        
    except Exception as e:
        return {
            "prediction": 0.0,
            "confidence": 0.1,
            "model_key": model_key,
            "error": f"Prediction failed: {e}",
            "regime": "error"
        }


def calculate_confidence(env, model_key: str) -> float:
    """Calculate confidence based on environment-model match."""
    
    # Base confidence
    base_confidence = 0.7
    
    # Perfect match bonus
    if "HI_VOL" in model_key and env.market_vol_pct >= 0.7:
        base_confidence += 0.2
    elif "LO_VOL" in model_key and env.market_vol_pct < 0.7:
        base_confidence += 0.2
    
    if "TREND" in model_key and abs(env.trend_strength) >= 0.2:
        base_confidence += 0.1
    elif "CHOP" in model_key and abs(env.trend_strength) < 0.2:
        base_confidence += 0.1
    
    # Sector match bonus
    if env.sector_regime != "BALANCED" and env.sector_regime in model_key:
        base_confidence += 0.1
    
    return min(0.95, base_confidence)


# Global prediction function
def predict_regime_aware_ml(features: Dict[str, Any], db_path: str, as_of: str) -> Dict[str, Any]:
    """
    Main entry point for regime-aware ML prediction.
    
    This is the function that should be used in production.
    """
    return predict_regime_aware(features, db_path, as_of)
