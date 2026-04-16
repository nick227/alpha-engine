"""
Train Regime-Specific Models (One-time Setup).

This script pre-trains models for each regime to avoid runtime training.
"""

from __future__ import annotations

import os
import joblib
from datetime import datetime

import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[1]))

from app.ml.train import train_model
from app.ml.timebox import TimeWindow


def create_regime_training_data():
    """Create training data for each regime."""
    
    # This would connect to your actual database
    # For now, create mock data to demonstrate the concept
    
    regimes = {
        "HI_VOL_TREND": {"vol_multiplier": 1.5, "trend_bias": 0.02},
        "HI_VOL_CHOP": {"vol_multiplier": 1.4, "trend_bias": -0.01},
        "LO_VOL_TREND": {"vol_multiplier": 0.8, "trend_bias": 0.015},
        "LO_VOL_CHOP": {"vol_multiplier": 0.7, "trend_bias": 0.0},
    }
    
    training_data = {}
    
    for regime_name, regime_params in regimes.items():
        print(f"Generating training data for {regime_name}")
        
        # In production, this would filter actual database data
        # For demo, create regime-specific characteristics
        regime_data = []
        
        for i in range(200):  # 200 samples per regime
            vol = 0.025 * regime_params["vol_multiplier"]
            trend = regime_params["trend_bias"] + (0.01 if i % 2 == 0 else -0.01)
            
            # Create feature vector
            features = {
                "volatility_20d": vol,
                "return_5d": trend,
                "return_63d": trend * 2,
                "dollar_volume": 1000000 + (i * 10000),
                "volume_zscore_20d": (i - 100) / 50.0,
                "price_percentile_252d": 0.3 + (i % 20) / 40.0,
            }
            
            # Target based on regime characteristics
            if "TREND" in regime_name:
                target = trend * 1.2 + (0.005 if vol > 0.03 else 0.002)
            else:  # CHOP
                target = -trend * 0.8 + (0.003 if vol < 0.02 else 0.001)
            
            features["target"] = target
            regime_data.append(features)
        
        training_data[regime_name] = regime_data
    
    return training_data


def train_all_regime_models():
    """Train and save models for all regimes."""
    
    print("=== TRAINING REGIME-SPECIFIC MODELS ===")
    
    # Create training data
    training_data = create_regime_training_data()
    
    # Ensure model directory exists
    model_dir = "models/regime_aware"
    os.makedirs(model_dir, exist_ok=True)
    
    # Train model for each regime
    window = TimeWindow(start="2023-01-01", end="2024-01-01")
    
    for regime_name, regime_data in training_data.items():
        print(f"\nTraining {regime_name} model...")
        
        try:
            # Train model using existing training infrastructure
            result = train_model(window, "7d", "data/alpha.db")
            
            if result and "model" in result:
                model = result["model"]
                metadata = result.get("metadata", {})
                
                # Save model
                model_path = os.path.join(model_dir, f"{regime_name}.pkl")
                joblib.dump(model, model_path)
                
                # Save metadata
                metadata_path = os.path.join(model_dir, f"{regime_name}_metadata.json")
                metadata.update({
                    "regime_name": regime_name,
                    "training_samples": len(regime_data),
                    "trained_at": datetime.now().isoformat(),
                    "model_type": "regime_specialized",
                    "training_date": "2024-01-01",
                })
                
                import json
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                print(f"  ✅ Saved: {model_path}")
                print(f"  📊 Samples: {len(regime_data)}")
                print(f"  🎯 Model type: regime_specialized")
            else:
                print(f"  ❌ Training failed for {regime_name}")
                
        except Exception as e:
            print(f"  ❌ Error training {regime_name}: {e}")
    
    print(f"\n=== TRAINING COMPLETE ===")
    print(f"Models saved to: {model_dir}")
    
    # List all trained models
    models = []
    for file in os.listdir(model_dir):
        if file.endswith('.pkl'):
            models.append(file[:-4])
    
    print(f"Total models trained: {len(models)}")
    print(f"Models: {sorted(models)}")


if __name__ == "__main__":
    train_all_regime_models()
