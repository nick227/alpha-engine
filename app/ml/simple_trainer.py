"""
Simple ML Trainer

Lightweight ML trainer for trade outcome prediction using logistic regression or gradient boosting.
"""

from typing import Dict, List, Tuple, Any
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import logging

logger = logging.getLogger(__name__)


class SimpleMLTrainer:
    """Simple ML trainer for trade outcome prediction."""
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.encoders = {}
        self.feature_columns = []
        self.model_type = None
        self.training_metadata = {}
        
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
        
        # Make copy to avoid SettingWithCopyWarning
        df_encoded = df.copy()
        
        # Handle missing values
        for col in self.feature_columns:
            if col not in df_encoded.columns:
                df_encoded[col] = 0.0
            else:
                df_encoded[col] = df_encoded[col].fillna(0.0)
        
        # Encode categorical variables
        # Encode volatility_regime
        if 'volatility_regime' not in self.encoders:
            self.encoders['volatility_regime'] = LabelEncoder()
            # Fit on all possible values
            possible_values = ['expansion', 'contraction', 'normal', 'unknown']
            self.encoders['volatility_regime'].fit(possible_values)
        
        df_encoded['volatility_regime'] = self.encoders['volatility_regime'].transform(
            df_encoded['volatility_regime'].astype(str)
        )
        
        # Encode macro_context
        if 'macro_context' not in self.encoders:
            self.encoders['macro_context'] = LabelEncoder()
            # Fit on all possible values
            possible_values = ['risk_on', 'risk_off', 'neutral', 'unknown']
            self.encoders['macro_context'].fit(possible_values)
        
        df_encoded['macro_context'] = self.encoders['macro_context'].transform(
            df_encoded['macro_context'].astype(str)
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
        model_type: str = 'gradient_boosting'
    ) -> Dict[str, Any]:
        """Train simple ML model on training data."""
        
        # Load training data
        try:
            df = pd.read_csv(training_data_path)
            logger.info(f"Loaded {len(df)} training examples from {training_data_path}")
        except Exception as e:
            logger.error(f"Error loading training data: {e}")
            return {'error': str(e)}
        
        if len(df) < 50:
            error_msg = f"Insufficient training data: {len(df)} examples (minimum 50)"
            logger.error(error_msg)
            return {'error': error_msg}
        
        # Prepare features
        X, y = self.prepare_features(df)
        
        # Check class balance
        win_rate = np.mean(y)
        logger.info(f"Training data win rate: {win_rate:.2%}")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Select model
        self.model_type = model_type
        if model_type == 'logistic':
            self.model = LogisticRegression(
                random_state=42, 
                max_iter=1000,
                class_weight='balanced'
            )
        elif model_type == 'gradient_boosting':
            self.model = GradientBoostingClassifier(
                n_estimators=100, 
                max_depth=3, 
                random_state=42,
                learning_rate=0.1
            )
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train model
        logger.info(f"Training {model_type} model...")
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
        
        # Additional metrics
        cm = confusion_matrix(y_test, y_pred)
        tn, fp, fn, tp = cm.ravel()
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        # Store training metadata
        self.training_metadata = {
            'model_type': model_type,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'win_rate': win_rate,
            'feature_importance': feature_importance,
            'training_date': pd.Timestamp.now().isoformat()
        }
        
        logger.info(f"Model trained successfully. Accuracy: {accuracy:.3f}")
        logger.info(f"Precision: {precision:.3f}, Recall: {recall:.3f}")
        logger.info(f"Feature importance: {feature_importance}")
        
        return self.training_metadata
    
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
            value = features.get(col, 0.0)
            feature_vector.append(value)
        
        # Encode categorical features
        if 'volatility_regime' in self.encoders:
            vol_regime_idx = self.feature_columns.index('volatility_regime')
            vol_value = str(features.get('volatility_regime', 'normal'))
            try:
                if vol_value in self.encoders['volatility_regime'].classes_:
                    feature_vector[vol_regime_idx] = self.encoders['volatility_regime'].transform([vol_value])[0]
                else:
                    feature_vector[vol_regime_idx] = 0  # Default for unknown
            except:
                feature_vector[vol_regime_idx] = 0
        
        if 'macro_context' in self.encoders:
            macro_idx = self.feature_columns.index('macro_context')
            macro_value = str(features.get('macro_context', 'neutral'))
            try:
                if macro_value in self.encoders['macro_context'].classes_:
                    feature_vector[macro_idx] = self.encoders['macro_context'].transform([macro_value])[0]
                else:
                    feature_vector[macro_idx] = 0  # Default for unknown
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
        
        return float(win_probability)
    
    def predict_batch(
        self,
        features_list: List[Dict[str, Any]]
    ) -> List[float]:
        """Predict win probabilities for multiple trades."""
        
        if self.model is None:
            raise ValueError("Model not trained yet")
        
        probabilities = []
        for features in features_list:
            prob = self.predict_win_probability(features)
            probabilities.append(prob)
        
        return probabilities
    
    def save_model(self, model_path: str):
        """Save trained model and preprocessing objects."""
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'encoders': self.encoders,
            'feature_columns': self.feature_columns,
            'model_type': self.model_type,
            'training_metadata': self.training_metadata
        }
        
        try:
            joblib.dump(model_data, model_path)
            logger.info(f"Model saved to {model_path}")
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            raise
    
    def load_model(self, model_path: str):
        """Load trained model and preprocessing objects."""
        
        try:
            model_data = joblib.load(model_path)
            self.model = model_data['model']
            self.scaler = model_data['scaler']
            self.encoders = model_data['encoders']
            self.feature_columns = model_data['feature_columns']
            self.model_type = model_data.get('model_type', 'unknown')
            self.training_metadata = model_data.get('training_metadata', {})
            logger.info(f"Model loaded from {model_path}")
            logger.info(f"Model type: {self.model_type}")
            if self.training_metadata:
                logger.info(f"Training accuracy: {self.training_metadata.get('accuracy', 'unknown')}")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the trained model."""
        
        if self.model is None:
            return {'error': 'Model not trained yet'}
        
        return {
            'model_type': self.model_type,
            'feature_columns': self.feature_columns,
            'training_metadata': self.training_metadata,
            'is_trained': True
        }
    
    def validate_model(self, test_data_path: str) -> Dict[str, Any]:
        """Validate model on separate test data."""
        
        if self.model is None:
            return {'error': 'Model not trained yet'}
        
        try:
            # Load test data
            df = pd.read_csv(test_data_path)
            
            # Prepare features
            X, y = self.prepare_features(df)
            
            # Make predictions
            y_pred = self.model.predict(X)
            y_prob = self.model.predict_proba(X)[:, 1] if hasattr(self.model, 'predict_proba') else None
            
            # Calculate metrics
            accuracy = accuracy_score(y, y_pred)
            cm = confusion_matrix(y, y_pred)
            tn, fp, fn, tp = cm.ravel()
            
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            
            # Calculate ROC AUC if probabilities available
            roc_auc = None
            if y_prob is not None:
                from sklearn.metrics import roc_auc_score
                roc_auc = roc_auc_score(y, y_prob)
            
            results = {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1_score': f1,
                'roc_auc': roc_auc,
                'confusion_matrix': cm.tolist(),
                'test_samples': len(df)
            }
            
            logger.info(f"Model validation results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error validating model: {e}")
            return {'error': str(e)}
