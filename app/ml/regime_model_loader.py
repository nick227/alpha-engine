"""
Regime Model Loader: Pre-trained model selection for runtime.

This fixes the architectural error - no runtime training, just model selection.
"""

import os
import joblib
from typing import Tuple, Optional, Any

from app.core.environment import build_env_snapshot_v3
from app.core.environment_v3 import bucket_env_v3


MODEL_DIR = "models/regime_aware"


def _model_path(key: str) -> str:
    """Get model file path."""
    return os.path.join(MODEL_DIR, f"{key}.pkl")


def load_model(key: str):
    """Load pre-trained model by key."""
    path = _model_path(key)
    if os.path.exists(path):
        return joblib.load(path)
    return None


def get_regime_key(env, horizon: str = "7d") -> str:
    """Generate model key from environment and horizon."""
    
    # Simple regime classification for model selection
    vol_regime = "HI_VOL" if env.market_vol_pct >= 0.7 else "LO_VOL"
    trend_regime = "TREND" if abs(env.trend_strength) >= 0.2 else "CHOP"
    
    # Add sector leadership for meaningful expansion
    sector_suffix = f"_{env.sector_regime}" if env.sector_regime != "BALANCED" else ""
    
    # Add industry dispersion for even more granular selection
    industry_suffix = ""
    if env.industry_dispersion >= 0.5:
        industry_suffix = "_HIDISP"
    else:
        industry_suffix = "_LODISP"
    
    return f"{vol_regime}_{trend_regime}{sector_suffix}{industry_suffix}_{horizon}"


def get_model(env, horizon: str = "7d") -> Tuple[Optional[Any], Optional[str]]:
    """
    Get pre-trained model for current environment.
    
    Returns: (model, model_key)
    """
    key = get_regime_key(env, horizon)
    model = load_model(key)
    
    if model:
        return model, key
    return None, None


def get_available_models() -> list[str]:
    """Get list of available pre-trained models."""
    if not os.path.exists(MODEL_DIR):
        return []
    
    models = []
    for file in os.listdir(MODEL_DIR):
        if file.endswith('.pkl'):
            models.append(file[:-4])  # Remove .pkl extension
    
    return sorted(models)


def get_fallback_model(horizon: str = "7d"):
    """Get fallback model when no regime-specific model available."""
    fallback_keys = [
        f"default_{horizon}",
        f"baseline_{horizon}",
        f"ensemble_{horizon}",
    ]
    
    for key in fallback_keys:
        model = load_model(key)
        if model:
            return model, key
    
    return None, None


def model_info() -> dict[str, Any]:
    """Get information about available models."""
    models = get_available_models()
    
    info = {
        "total_models": len(models),
        "available_models": models,
        "model_directory": MODEL_DIR,
        "model_types": {}
    }
    
    # Classify model types
    for model_key in models:
        if "HI_VOL" in model_key:
            info["model_types"]["high_volatility"] = info["model_types"].get("high_volatility", 0) + 1
        elif "LO_VOL" in model_key:
            info["model_types"]["low_volatility"] = info["model_types"].get("low_volatility", 0) + 1
        elif "TREND" in model_key:
            info["model_types"]["trend_following"] = info["model_types"].get("trend_following", 0) + 1
        elif "CHOP" in model_key:
            info["model_types"]["mean_reversion"] = info["model_types"].get("mean_reversion", 0) + 1
    
    return info
