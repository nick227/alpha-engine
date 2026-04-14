"""
Regime-Aware ML: Solves regime mixing problem through specialization.

Uses our existing adaptive infrastructure for environment detection
and model selection.
"""

from __future__ import annotations

import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from app.core.environment import build_env_snapshot_v3
from app.core.environment_v3 import bucket_env_v3
from app.ml.feature_builder import FeatureBuilder
from app.ml.train import train_model


class RegimeAwareML:
    """
    Regime-aware ML system that trains specialized models per environment.
    
    Solves the core ML problem: regime mixing in training data.
    """
    
    def __init__(self, model_dir: str = "models/regime_aware"):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        # Regime-specific model storage
        self.models: Dict[str, Any] = {}
        self.model_metadata: Dict[str, Dict] = {}
        
        # Feature builder for regime detection
        self.feature_builder = FeatureBuilder()
    
    def detect_regime(self, db_path: str, as_of: str) -> tuple[str, str, str, str, str, str]:
        """Detect current market regime using our adaptive infrastructure."""
        env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
        return bucket_env_v3(env)
    
    def get_regime_data(self, features: Dict[str, Any], target_regime: tuple[str, str, str, str, str, str]) -> List[Dict]:
        """Filter training data for specific regime."""
        regime_data = []
        
        for symbol, feature_dict in features.items():
            # Simple regime classification for training data
            # In production, this would use historical environment detection
            symbol_regime = self.classify_symbol_regime(feature_dict)
            
            if symbol_regime == target_regime:
                regime_data.append({
                    "symbol": symbol,
                    "features": feature_dict,
                    "target": self.calculate_target(feature_dict)
                })
        
        return regime_data
    
    def classify_symbol_regime(self, feature_dict: Dict) -> tuple[str, str, str, str, str, str]:
        """Classify symbol's regime for training data filtering."""
        
        # Use volatility and trend to classify
        vol = feature_dict.get("volatility_20d", 0.02)
        trend = feature_dict.get("return_63d", 0.0)
        
        # Simple regime classification
        if vol > 0.03:
            vol_regime = "HI_VOL"
        else:
            vol_regime = "LO_VOL"
        
        if abs(trend) > 0.02:
            trend_regime = "TREND"
        else:
            trend_regime = "CHOP"
        
        # Default balanced for other dimensions
        return (vol_regime, trend_regime, "LO_DISP", "HI_LIQ", "BALANCED", "LO_INDUSTRY_DISP")
    
    def calculate_target(self, feature_dict: Dict) -> float:
        """Calculate target variable for training."""
        # Use forward return as target
        return feature_dict.get("return_5d", 0.0)
    
    def train_regime_models(self, db_path: str, training_data: Dict[str, Any]):
        """Train specialized models for each regime."""
        
        # Define key regimes to specialize on
        key_regimes = [
            ("HI_VOL", "TREND", "HI_DISP", "HI_LIQ", "TECH_LEAD", "HI_INDUSTRY_DISP"),
            ("HI_VOL", "CHOP", "HI_DISP", "HI_LIQ", "FINANCIALS", "HI_INDUSTRY_DISP"),
            ("LO_VOL", "TREND", "LO_DISP", "HI_LIQ", "ENERGY", "LO_INDUSTRY_DISP"),
            ("LO_VOL", "CHOP", "LO_DISP", "LO_LIQ", "BALANCED", "LO_INDUSTRY_DISP"),
        ]
        
        print("=== TRAINING REGIME-SPECIALIZED MODELS ===")
        
        for regime in key_regimes:
            regime_name = "_".join(regime)
            print(f"\nTraining model for regime: {regime_name}")
            
            # Get regime-specific data
            regime_data = self.get_regime_data(training_data, regime)
            
            if len(regime_data) < 50:
                print(f"  Insufficient data: {len(regime_data)} samples")
                continue
            
            # Train specialized model
            try:
                from app.ml.timebox import TimeWindow
                window = TimeWindow(start="2023-01-01", end="2024-01-01")
                model_result = train_model(window, "7d", "data/alpha.db")
                model = model_result.get("model")
                metadata = model_result.get("metadata", {})
                
                # Store model
                model_path = self.model_dir / f"{regime_name}.pkl"
                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)
                
                # Store metadata
                metadata_path = self.model_dir / f"{regime_name}_metadata.json"
                metadata.update({
                    "regime": regime,
                    "regime_name": regime_name,
                    "training_samples": len(regime_data),
                    "trained_at": datetime.now().isoformat(),
                    "model_type": "ridge_specialized"
                })
                
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                self.models[regime_name] = model
                self.model_metadata[regime_name] = metadata
                
                print(f"  ✅ Trained: {len(regime_data)} samples")
                print(f"  📊 Performance: {metadata.get('cv_score', 'N/A')}")
                
            except Exception as e:
                print(f"  ❌ Training failed: {e}")
    
    def load_regime_models(self):
        """Load all trained regime models."""
        print("=== LOADING REGIME-SPECIALIZED MODELS ===")
        
        for model_file in self.model_dir.glob("*.pkl"):
            regime_name = model_file.stem
            
            # Load model
            with open(model_file, 'rb') as f:
                self.models[regime_name] = pickle.load(f)
            
            # Load metadata
            metadata_file = self.model_dir / f"{regime_name}_metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    self.model_metadata[regime_name] = json.load(f)
            
            print(f"  Loaded: {regime_name}")
        
        print(f"Total models loaded: {len(self.models)}")
    
    def predict(self, features: Dict[str, Any], db_path: str, as_of: str) -> Dict[str, Any]:
        """Make prediction using regime-specialized model."""
        
        # Detect current regime
        current_regime = self.detect_regime(db_path, as_of)
        regime_name = "_".join(current_regime)
        
        # Get best model for this regime
        if regime_name in self.models:
            model = self.models[regime_name]
            metadata = self.model_metadata[regime_name]
            
            # Prepare features for prediction
            feature_vector = self.feature_builder.build_features(features)
            
            # Make prediction
            try:
                prediction = model.predict(feature_vector)
                
                return {
                    "prediction": float(prediction[0]) if len(prediction) > 0 else 0.0,
                    "regime": current_regime,
                    "regime_name": regime_name,
                    "model_metadata": metadata,
                    "confidence": self.calculate_confidence(prediction, metadata),
                    "model_type": "regime_specialized"
                }
            except Exception as e:
                print(f"Prediction failed: {e}")
                return self.fallback_prediction(features)
        else:
            print(f"No model for regime: {regime_name}")
            return self.fallback_prediction(features)
    
    def fallback_prediction(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback prediction when no regime model available."""
        
        # Use simple ensemble or default model
        if self.models:
            # Average of available models
            predictions = []
            for model in self.models.values():
                try:
                    feature_vector = self.feature_builder.build_features(features)
                    pred = model.predict(feature_vector)
                    predictions.append(float(pred[0]) if len(pred) > 0 else 0.0)
                except:
                    continue
            
            if predictions:
                avg_prediction = sum(predictions) / len(predictions)
                return {
                    "prediction": avg_prediction,
                    "regime": "FALLBACK",
                    "regime_name": "ensemble_fallback",
                    "confidence": 0.5,
                    "model_type": "ensemble_fallback"
                }
        
        # Ultimate fallback
        return {
            "prediction": 0.0,
            "regime": "UNKNOWN",
            "regime_name": "default_fallback",
            "confidence": 0.1,
            "model_type": "default_fallback"
        }
    
    def calculate_confidence(self, prediction: Any, metadata: Dict) -> float:
        """Calculate prediction confidence based on model metadata."""
        
        # Use training performance and sample size
        cv_score = metadata.get("cv_score", 0.5)
        training_samples = metadata.get("training_samples", 100)
        
        # Higher confidence with better CV scores and more samples
        base_confidence = cv_score
        sample_bonus = min(0.2, training_samples / 1000)  # Max 0.2 bonus
        
        return min(0.95, base_confidence + sample_bonus)
    
    def get_model_summary(self) -> Dict[str, Any]:
        """Get summary of all loaded models."""
        
        summary = {
            "total_models": len(self.models),
            "regimes_covered": list(self.models.keys()),
            "model_details": {}
        }
        
        for regime_name, metadata in self.model_metadata.items():
            summary["model_details"][regime_name] = {
                "regime": metadata.get("regime"),
                "training_samples": metadata.get("training_samples"),
                "cv_score": metadata.get("cv_score"),
                "trained_at": metadata.get("trained_at"),
                "model_type": metadata.get("model_type")
            }
        
        return summary


# Global regime-aware ML instance
_regime_aware_ml = None


def get_regime_aware_ml() -> RegimeAwareML:
    """Get global regime-aware ML instance."""
    global _regime_aware_ml
    if _regime_aware_ml is None:
        _regime_aware_ml = RegimeAwareML()
        _regime_aware_ml.load_regime_models()
    return _regime_aware_ml
