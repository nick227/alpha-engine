# Alpha Engine ML Training Layer Implementation

## Overview

This document provides a focused implementation guide for the missing ML training layer in Alpha Engine. The system already generates signals and tracks outcomes - we need to formalize the learning structure.

## The Gap

### Current System
```
features → strategies → trades → outcomes → adaptive weights
```

### Target System
```
features → ML model → probability → trade filter → outcomes → retrain
```

## What We're Building

### 1. Clean ML Training Dataset

**Single Table Structure:**
```sql
CREATE TABLE ml_training_dataset (
    trade_id VARCHAR(36) PRIMARY KEY,
    symbol VARCHAR(10),
    entry_timestamp TIMESTAMP,
    exit_timestamp TIMESTAMP,
    
    -- Entry features snapshot (JSON)
    entry_features_snapshot JSON,
    
    -- Outcomes
    return_5d DECIMAL(8,4),
    return_20d DECIMAL(8,4),
    is_win BOOLEAN,
    r_multiple DECIMAL(8,4),
    
    -- Context
    regime_at_entry VARCHAR(20),
    sector_at_entry VARCHAR(10),
    volatility_regime VARCHAR(20),
    market_volatility DECIMAL(8,4),
    
    -- Metadata
    strategy_type VARCHAR(50),
    entry_price DECIMAL(10,2),
    exit_price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Simple Model Layer

**Logistic Regression / Light GBM** trained on trade outcomes producing probabilities.

### 3. Feature Compression

**Distill raw features into signal-level inputs:**
- `trend_strength` (instead of return_5d, return_20d, return_63d)
- `trend_direction` (bull/bear/sideways)
- `volatility_regime` (expansion/contraction/normal)
- `position_in_range` (0-1 normalized)

### 4. Cross-Asset Context

**High-value features:**
- SPY trend (market direction)
- VIX/volatility index (fear gauge)
- Oil/Gold/Dollar context (macro signals)

## Implementation

### Step 1: Training Dataset Builder

```python
# app/ml/training_dataset.py
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import json
import sqlite3
from datetime import datetime
import numpy as np

@dataclass
class TradeTrainingExample:
    """Single training example for ML model."""
    trade_id: str
    symbol: str
    entry_timestamp: datetime
    
    # Compressed features (what we actually feed to model)
    trend_strength: float
    trend_direction: str  # 'bull', 'bear', 'sideways'
    volatility_regime: str  # 'expansion', 'contraction', 'normal'
    position_in_range: float  # 0-1 normalized
    volume_anomaly: float  # z-score
    sector: str
    
    # Cross-asset context
    spy_trend: float  # SPY 5d return
    vix_level: float  # VIX percentile
    macro_context: str  # 'risk_on', 'risk_off', 'neutral'
    
    # Outcomes
    return_5d: float
    is_win: bool
    r_multiple: float
    
    # Raw features snapshot (for debugging)
    raw_features: Dict[str, Any]

class TrainingDatasetBuilder:
    """Build clean training dataset from completed trades."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def build_training_dataset(
        self,
        start_date: datetime,
        end_date: datetime,
        min_return_period: int = 5
    ) -> List[TradeTrainingExample]:
        """
        Build training dataset from completed trades.
        
        Returns:
            List of training examples with compressed features
        """
        
        conn = sqlite3.connect(self.db_path)
        
        # Get completed trades with outcomes
        query = """
        SELECT 
            t.id as trade_id,
            t.symbol,
            t.entry_timestamp,
            t.exit_timestamp,
            t.entry_price,
            t.exit_price,
            t.realized_pnl,
            t.strategy_type,
            fr.*,
            r.volatility_regime,
            r.trend_regime
        FROM trades t
        JOIN feature_rows fr ON t.symbol = fr.symbol AND DATE(t.entry_timestamp) = fr.as_of_date
        LEFT JOIN regime_classifications r ON DATE(t.entry_timestamp) = r.classification_date
        WHERE t.exit_timestamp IS NOT NULL
        AND t.entry_timestamp BETWEEN ? AND ?
        ORDER BY t.entry_timestamp
        """
        
        cursor = conn.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        conn.close()
        
        training_examples = []
        
        for row in rows:
            example = self._convert_row_to_training_example(row, min_return_period)
            if example:
                training_examples.append(example)
        
        return training_examples
    
    def _convert_row_to_training_example(
        self,
        row: sqlite3.Row,
        min_return_period: int
    ) -> Optional[TradeTrainingExample]:
        """Convert database row to training example with feature compression."""
        
        try:
            # Calculate outcomes
            entry_price = row['entry_price']
            exit_price = row['exit_price']
            return_5d = (exit_price - entry_price) / entry_price
            is_win = return_5d > 0
            r_multiple = return_5d / 0.02  # Assuming 2% risk per trade
            
            # Compress features
            trend_strength = self._calculate_trend_strength(row)
            trend_direction = self._classify_trend_direction(row)
            volatility_regime = row['volatility_regime'] or 'normal'
            position_in_range = row['price_percentile_252d'] or 0.5
            volume_anomaly = row['volume_zscore_20d'] or 0.0
            
            # Get cross-asset context
            spy_trend = self._get_spy_trend(row['entry_timestamp'])
            vix_level = self._get_vix_percentile(row['entry_timestamp'])
            macro_context = self._classify_macro_context(spy_trend, vix_level)
            
            # Build raw features snapshot
            raw_features = {
                'return_1d': row['return_1d'],
                'return_5d': row['return_5d'],
                'return_20d': row['return_20d'],
                'return_63d': row['return_63d'],
                'volatility_20d': row['volatility_20d'],
                'price_percentile_252d': row['price_percentile_252d'],
                'volume_zscore_20d': row['volume_zscore_20d'],
                'sector': row['sector']
            }
            
            return TradeTrainingExample(
                trade_id=row['trade_id'],
                symbol=row['symbol'],
                entry_timestamp=row['entry_timestamp'],
                trend_strength=trend_strength,
                trend_direction=trend_direction,
                volatility_regime=volatility_regime,
                position_in_range=position_in_range,
                volume_anomaly=volume_anomaly,
                sector=row['sector'] or 'UNK',
                spy_trend=spy_trend,
                vix_level=vix_level,
                macro_context=macro_context,
                return_5d=return_5d,
                is_win=is_win,
                r_multiple=r_multiple,
                raw_features=raw_features
            )
            
        except Exception as e:
            print(f"Error converting row to training example: {e}")
            return None
    
    def _calculate_trend_strength(self, row: sqlite3.Row) -> float:
        """Calculate compressed trend strength from multiple returns."""
        
        returns = [
            row['return_5d'] or 0,
            row['return_20d'] or 0,
            row['return_63d'] or 0
        ]
        
        # Use absolute returns and weight recent more heavily
        weights = [0.5, 0.3, 0.2]
        weighted_return = sum(abs(r) * w for r, w in zip(returns, weights))
        
        return min(1.0, weighted_return / 0.1)  # Normalize to 0-1
    
    def _classify_trend_direction(self, row: sqlite3.Row) -> str:
        """Classify trend direction from returns."""
        
        return_5d = row['return_5d'] or 0
        return_20d = row['return_20d'] or 0
        
        if return_5d > 0.02 and return_20d > 0.05:
            return 'bull'
        elif return_5d < -0.02 and return_20d < -0.05:
            return 'bear'
        else:
            return 'sideways'
    
    def _get_spy_trend(self, timestamp: datetime) -> float:
        """Get SPY trend around trade entry."""
        # This would query SPY data for 5-day return
        # For now, return mock data
        return np.random.normal(0, 0.02)  # Replace with actual SPY query
    
    def _get_vix_percentile(self, timestamp: datetime) -> float:
        """Get VIX percentile at trade entry."""
        # This would query VIX data and calculate percentile
        # For now, return mock data
        return np.random.uniform(0.2, 0.8)  # Replace with actual VIX query
    
    def _classify_macro_context(self, spy_trend: float, vix_level: float) -> str:
        """Classify macro context from SPY and VIX."""
        
        if spy_trend > 0.02 and vix_level < 0.5:
            return 'risk_on'
        elif spy_trend < -0.02 and vix_level > 0.7:
            return 'risk_off'
        else:
            return 'neutral'
    
    def save_training_dataset(
        self,
        examples: List[TradeTrainingExample],
        output_path: str
    ):
        """Save training dataset to CSV for model training."""
        
        import pandas as pd
        
        # Convert to DataFrame
        data = []
        for example in examples:
            data.append({
                'trade_id': example.trade_id,
                'symbol': example.symbol,
                'trend_strength': example.trend_strength,
                'trend_direction': example.trend_direction,
                'volatility_regime': example.volatility_regime,
                'position_in_range': example.position_in_range,
                'volume_anomaly': example.volume_anomaly,
                'sector': example.sector,
                'spy_trend': example.spy_trend,
                'vix_level': example.vix_level,
                'macro_context': example.macro_context,
                'return_5d': example.return_5d,
                'is_win': example.is_win,
                'r_multiple': example.r_multiple
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
        print(f"Saved {len(examples)} training examples to {output_path}")
```

### Step 2: Simple Model Trainer

```python
# app/ml/simple_trainer.py
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib

class SimpleMLTrainer:
    """Simple ML trainer for trade outcome prediction."""
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.encoders = {}
        self.feature_columns = []
        
    def prepare_features(
        self,
        df: pd.DataFrame
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare features for ML training."""
        
        # Define feature columns
        self.feature_columns = [
            'trend_strength',
            'volatility_regime',
            'position_in_range',
            'volume_anomaly',
            'spy_trend',
            'vix_level',
            'macro_context'
        ]
        
        # Encode categorical variables
        df_encoded = df.copy()
        
        # Encode volatility_regime
        if 'volatility_regime' not in self.encoders:
            self.encoders['volatility_regime'] = LabelEncoder()
        df_encoded['volatility_regime'] = self.encoders['volatility_regime'].fit_transform(
            df_encoded['volatility_regime']
        )
        
        # Encode macro_context
        if 'macro_context' not in self.encoders:
            self.encoders['macro_context'] = LabelEncoder()
        df_encoded['macro_context'] = self.encoders['macro_context'].fit_transform(
            df_encoded['macro_context']
        )
        
        # Extract features
        X = df_encoded[self.feature_columns].values
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Target variable
        y = df_encoded['is_win'].values
        
        return X_scaled, y
    
    def train_model(
        self,
        training_data_path: str,
        model_type: str = 'logistic'
    ) -> Dict[str, Any]:
        """Train simple ML model on training data."""
        
        # Load training data
        df = pd.read_csv(training_data_path)
        print(f"Loaded {len(df)} training examples")
        
        # Prepare features
        X, y = self.prepare_features(df)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Select model
        if model_type == 'logistic':
            self.model = LogisticRegression(random_state=42, max_iter=1000)
        elif model_type == 'gradient_boosting':
            self.model = GradientBoostingClassifier(
                n_estimators=100, 
                max_depth=3, 
                random_state=42
            )
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train model
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        # Get feature importance
        if hasattr(self.model, 'feature_importances_'):
            feature_importance = dict(zip(self.feature_columns, self.model.feature_importances_))
        elif hasattr(self.model, 'coef_'):
            feature_importance = dict(zip(self.feature_columns, self.model.coef_[0]))
        else:
            feature_importance = {}
        
        print(f"Model trained. Accuracy: {accuracy:.3f}")
        print("Feature importance:", feature_importance)
        
        return {
            'accuracy': accuracy,
            'feature_importance': feature_importance,
            'training_samples': len(X_train),
            'test_samples': len(X_test)
        }
    
    def predict_win_probability(
        self,
        features: Dict[str, Any]
    ) -> float:
        """Predict win probability for new trade."""
        
        if self.model is None:
            raise ValueError("Model not trained yet")
        
        # Prepare features
        feature_vector = []
        for col in self.feature_columns:
            if col in features:
                feature_vector.append(features[col])
            else:
                feature_vector.append(0.0)  # Default value
        
        # Encode categorical features
        if 'volatility_regime' in self.encoders:
            vol_regime_idx = self.feature_columns.index('volatility_regime')
            # This is simplified - in production, handle unseen categories
            try:
                feature_vector[vol_regime_idx] = self.encoders['volatility_regime'].transform(
                    [features['volatility_regime']]
                )[0]
            except:
                feature_vector[vol_regime_idx] = 0
        
        if 'macro_context' in self.encoders:
            macro_idx = self.feature_columns.index('macro_context')
            try:
                feature_vector[macro_idx] = self.encoders['macro_context'].transform(
                    [features['macro_context']]
                )[0]
            except:
                feature_vector[macro_idx] = 0
        
        # Scale features
        X = np.array(feature_vector).reshape(1, -1)
        X_scaled = self.scaler.transform(X)
        
        # Predict probability
        if hasattr(self.model, 'predict_proba'):
            probabilities = self.model.predict_proba(X_scaled)[0]
            win_probability = probabilities[1]  # Probability of class 1 (win)
        else:
            # For models without predict_proba, use decision function
            decision = self.model.decision_function(X_scaled)[0]
            win_probability = 1 / (1 + np.exp(-decision))  # Sigmoid
        
        return win_probability
    
    def save_model(self, model_path: str):
        """Save trained model and preprocessing objects."""
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'encoders': self.encoders,
            'feature_columns': self.feature_columns
        }
        
        joblib.dump(model_data, model_path)
        print(f"Model saved to {model_path}")
    
    def load_model(self, model_path: str):
        """Load trained model and preprocessing objects."""
        
        model_data = joblib.load(model_path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.encoders = model_data['encoders']
        self.feature_columns = model_data['feature_columns']
        print(f"Model loaded from {model_path}")
```

### Step 3: Integration with Current Pipeline

```python
# app/ml/ml_integration.py
from typing import Dict, List, Any
from app.ml.simple_trainer import SimpleMLTrainer
from app.ml.training_dataset import TrainingDatasetBuilder

class MLIntegration:
    """Integrate simple ML model with existing pipeline."""
    
    def __init__(self, db_path: str, model_path: str):
        self.db_path = db_path
        self.model_path = model_path
        self.trainer = SimpleMLTrainer()
        self.dataset_builder = TrainingDatasetBuilder(db_path)
        
        # Load existing model if available
        try:
            self.trainer.load_model(model_path)
            print("ML model loaded successfully")
        except:
            print("No existing model found - will train new one")
            self.trainer = None
    
    def train_new_model(self, days_back: int = 252):
        """Train new model on recent trade data."""
        
        from datetime import datetime, timedelta
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Build training dataset
        print("Building training dataset...")
        examples = self.dataset_builder.build_training_dataset(start_date, end_date)
        
        if len(examples) < 100:
            print(f"Insufficient training data: {len(examples)} examples")
            return
        
        # Save training data
        training_path = "data/training_data.csv"
        self.dataset_builder.save_training_dataset(examples, training_path)
        
        # Train model
        print("Training ML model...")
        results = self.trainer.train_model(training_path, model_type='gradient_boosting')
        
        # Save model
        self.trainer.save_model(self.model_path)
        
        return results
    
    def filter_signals_with_ml(
        self,
        candidates: List[DiscoveryCandidate],
        features: Dict[str, Any]
    ) -> List[DiscoveryCandidate]:
        """Filter discovery candidates using ML model."""
        
        if self.trainer is None:
            print("ML model not available - returning all candidates")
            return candidates
        
        filtered_candidates = []
        
        for candidate in candidates:
            # Extract features for this candidate
            symbol_features = self._extract_features_for_candidate(candidate, features)
            
            # Get ML prediction
            win_probability = self.trainer.predict_win_probability(symbol_features)
            
            # Apply ML filter (only keep high-probability trades)
            if win_probability > 0.6:  # Threshold can be tuned
                candidate.metadata['ml_win_probability'] = win_probability
                candidate.metadata['ml_filtered'] = True
                filtered_candidates.append(candidate)
            else:
                candidate.metadata['ml_win_probability'] = win_probability
                candidate.metadata['ml_filtered'] = False
        
        print(f"ML filter: {len(filtered_candidates)}/{len(candidates)} candidates passed")
        return filtered_candidates
    
    def _extract_features_for_candidate(
        self,
        candidate: DiscoveryCandidate,
        all_features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract ML features for discovery candidate."""
        
        symbol = candidate.symbol
        if symbol not in all_features:
            return {}
        
        features = all_features[symbol]
        
        # Extract compressed features
        return {
            'trend_strength': self._calculate_trend_strength(features),
            'volatility_regime': self._classify_volatility_regime(features),
            'position_in_range': features.get('price_percentile_252d', 0.5),
            'volume_anomaly': features.get('volume_zscore_20d', 0.0),
            'spy_trend': self._get_spy_trend(),  # Current market trend
            'vix_level': self._get_vix_level(),   # Current VIX level
            'macro_context': self._classify_macro_context()
        }
    
    def _calculate_trend_strength(self, features: Dict[str, Any]) -> float:
        """Calculate trend strength from features."""
        returns = [
            features.get('return_5d', 0),
            features.get('return_20d', 0),
            features.get('return_63d', 0)
        ]
        weights = [0.5, 0.3, 0.2]
        return min(1.0, sum(abs(r) * w for r, w in zip(returns, weights)) / 0.1)
    
    def _classify_volatility_regime(self, features: Dict[str, Any]) -> str:
        """Classify volatility regime."""
        vol = features.get('volatility_20d', 0.02)
        if vol > 0.03:
            return 'expansion'
        elif vol < 0.015:
            return 'contraction'
        else:
            return 'normal'
    
    def _get_spy_trend(self) -> float:
        """Get current SPY trend."""
        # This would query current SPY data
        return 0.01  # Replace with actual query
    
    def _get_vix_level(self) -> float:
        """Get current VIX level."""
        # This would query current VIX data
        return 0.6  # Replace with actual query
    
    def _classify_macro_context(self) -> str:
        """Classify current macro context."""
        spy_trend = self._get_spy_trend()
        vix_level = self._get_vix_level()
        
        if spy_trend > 0.02 and vix_level < 0.5:
            return 'risk_on'
        elif spy_trend < -0.02 and vix_level > 0.7:
            return 'risk_off'
        else:
            return 'neutral'
```

### Step 4: Usage in Production

```python
# Example usage in existing pipeline
from app.ml.ml_integration import MLIntegration

# Initialize ML integration
ml_integration = MLIntegration(
    db_path="data/alpha.db",
    model_path="models/trade_predictor.joblib"
)

# Train new model (run periodically)
ml_integration.train_new_model(days_back=252)

# Use in existing signal generation
def generate_signals_with_ml_filter():
    # Get features
    features = feature_builder.build_features_for_universe(symbols, as_of_date)
    
    # Generate discovery candidates
    strategy_results = strategy_executor.execute_all_strategies(features)
    merged_candidates = consensus_engine.merge_strategy_results(strategy_results)
    
    # Apply ML filter
    filtered_candidates = ml_integration.filter_signals_with_ml(merged_candidates, features)
    
    return filtered_candidates
```

## Implementation Checklist

### Phase 1: Dataset Building
- [ ] Implement `TrainingDatasetBuilder`
- [ ] Create `ml_training_dataset` table
- [ ] Build historical training dataset
- [ ] Validate data quality

### Phase 2: Model Training
- [ ] Implement `SimpleMLTrainer`
- [ ] Train initial model on historical data
- [ ] Validate model performance
- [ ] Set up model versioning

### Phase 3: Integration
- [ ] Implement `MLIntegration`
- [ ] Integrate with existing signal pipeline
- [ ] Add ML scoring to candidate metadata
- [ ] Test end-to-end pipeline

### Phase 4: Production
- [ ] Set up periodic model retraining
- [ ] Add model performance monitoring
- [ ] Implement model rollback procedures
- [ ] Document ML model lifecycle

## Expected Benefits

1. **Clean Learning Structure**: One table, clear outcomes, direct feedback
2. **Simple Model**: Logistic regression or light GBM, interpretable
3. **Feature Compression**: High-value signals instead of noise
4. **Cross-Asset Context**: Market regime awareness
5. **Minimal Complexity**: Focus on what works, not what's fancy

## Bottom Line

You already have an ML system - you just need to formalize the learning layer. This implementation provides exactly that: clean dataset, simple model, and direct integration with your existing pipeline.

---

*Document Version: 1.0*
*Last Updated: 2026-04-16*
*Next Review: 2026-05-16*
