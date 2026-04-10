"""
Feature Integration Layer

Integrates the comprehensive feature engine with existing Alpha Engine pipeline.
Ensures backward compatibility while providing rich predictive features.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

from app.core.feature_engine import FeatureEngine
from app.core.price_context import build_price_context_for_event


class FeatureIntegration:
    """
    Integration layer that bridges comprehensive features with existing pipeline.
    
    Responsibilities:
    - Maintain backward compatibility with existing price_context
    - Enrich existing features with new predictive state features
    - Ensure strict separation of features vs outcomes
    - Provide migration path for strategies
    """
    
    def __init__(self):
        self.feature_engine = FeatureEngine()
        
    def build_enhanced_context(
        self,
        ticker_bars: pd.DataFrame,
        event_ts: datetime,
        cross_asset_data: Optional[Dict[str, pd.DataFrame]] = None,
        legacy_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Build enhanced price context with comprehensive features.
        
        Args:
            ticker_bars: OHLCV bars for the ticker
            event_ts: Event timestamp
            cross_asset_data: Optional cross-asset DataFrames
            legacy_mode: If True, returns legacy-compatible format
            
        Returns:
            Enhanced context with features and separated outcomes
        """
        # Get comprehensive features
        features, outcomes = self.feature_engine.build_feature_set(
            ticker_bars, event_ts, cross_asset_data
        )
        
        if legacy_mode:
            # Return legacy format for backward compatibility
            return self._build_legacy_context(features, outcomes)
        else:
            # Return enhanced format with clear separation
            return {
                "features": features,
                "outcomes": outcomes,
                "metadata": {
                    "feature_version": "v2.0",
                    "generated_at": datetime.utcnow().isoformat(),
                    "total_features": len(features),
                    "outcome_count": len(outcomes)
                }
            }
    
    def _build_legacy_context(self, features: Dict[str, Any], outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build legacy-compatible context format.
        Merges features and outcomes for existing strategies.
        """
        # Start with existing price_context structure
        legacy_context = {}
        
        # Map comprehensive features to legacy keys
        feature_mapping = {
            # Basic price info
            'entry_price': 'entry_price',
            'entry_volume': 'entry_volume',
            
            # Returns (legacy uses specific keys)
            'return_1m': 'return_1m',
            'return_5m': 'return_5m',
            'return_15m': 'return_15m',
            'return_1h': 'return_1h',
            'return_1d': 'return_1d',
            
            # Volatility
            'realized_vol_20': 'realized_volatility',
            'vol_regime': 'volatility_regime',
            
            # Volume
            'volume_ratio_20': 'volume_ratio',
            'volume_anomaly': 'volume_anomaly',
            
            # VWAP (computed from bars)
            'vwap_distance': 'vwap_distance',
            
            # Range expansion
            'range_expansion': 'range_expansion',
            
            # Trend and momentum
            'adx_14': 'adx_value',
            'trend_strength': 'trend_strength',
            'momentum_10': 'momentum_score',
            
            # Mean reversion
            'bb_position': 'bb_position',
            'mean_reversion_score': 'mean_reversion_score',
            
            # Microstructure
            'gap_up': 'gap_up',
            'gap_down': 'gap_down',
            'gap_size': 'gap_size',
        }
        
        # Map features
        for new_key, legacy_key in feature_mapping.items():
            if new_key in features:
                legacy_context[legacy_key] = features[new_key]
        
        # Add historical volatility window
        if 'realized_vol_5' in features:
            legacy_context['historical_volatility_window'] = [
                features.get('realized_vol_5', 0),
                features.get('realized_vol_10', 0),
                features.get('realized_vol_20', 0),
                features.get('realized_vol_50', 0),
            ] + [0.0] * 16  # Pad to 20 elements
        
        # Add outcomes for evaluation (legacy includes them)
        legacy_context.update(outcomes)
        
        # Add computed legacy features
        legacy_context.update(self._compute_legacy_features(features))
        
        return legacy_context
    
    def _compute_legacy_features(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Compute legacy-specific features from comprehensive features."""
        legacy = {}
        
        # Short trend (5m lookback)
        if 'return_5m' in features:
            legacy['short_trend'] = features['return_5m']
        
        # Continuation slope and pullback depth (from recent price action)
        if 'momentum_5' in features and 'momentum_15' in features:
            legacy['continuation_slope'] = features['momentum_5']
            legacy['pullback_depth'] = abs(features['momentum_5'] - features['momentum_15'])
        
        # Z-score (20 bars)
        if 'distance_from_sma_20' in features:
            legacy['zscore_20'] = features['distance_from_sma_20']
        
        # RSI (if available)
        if 'rsi_momentum' in features:
            legacy['rsi_14'] = features['rsi_momentum']
        
        # VWAP reclaim/reject (simplified)
        if 'gap_up' in features and 'gap_down' in features:
            legacy['vwap_reclaim'] = features['gap_up']
            legacy['vwap_reject'] = features['gap_down']
        
        return legacy
    
    def migrate_strategy_features(self, strategy_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate strategy configuration to use new features.
        
        Args:
            strategy_config: Existing strategy configuration
            
        Returns:
            Updated configuration with new feature mappings
        """
        migrated_config = strategy_config.copy()
        
        # Update feature references
        feature_updates = {
            # Volume enhancements
            'volume_ratio': 'volume_ratio_20',
            'volume_anomaly': 'volume_anomaly',
            'volume_trend_5': 'volume_trend_5',
            
            # Volatility enhancements
            'realized_volatility': 'realized_vol_20',
            'volatility_regime': 'vol_regime',
            'parkinson_vol_20': 'parkinson_vol_20',
            'gk_vol_20': 'gk_vol_20',
            
            # Trend enhancements
            'adx_value': 'adx_14',
            'trend_strength': 'trend_strength',
            'trend_direction': 'trend_direction',
            'plus_di_14': 'plus_di_14',
            'minus_di_14': 'minus_di_14',
            
            # Momentum enhancements
            'momentum_3': 'momentum_3',
            'momentum_5': 'momentum_5',
            'momentum_10': 'momentum_10',
            'momentum_20': 'momentum_20',
            'momentum_accel_10': 'momentum_accel_10',
            'momentum_rank_50': 'momentum_rank_50',
            
            # Mean reversion enhancements
            'distance_from_sma_5': 'distance_from_sma_5',
            'distance_from_sma_10': 'distance_from_sma_10',
            'distance_from_sma_20': 'distance_from_sma_20',
            'distance_from_ema_12': 'distance_from_ema_12',
            'distance_from_ema_26': 'distance_from_ema_26',
            'bb_position': 'bb_position',
            'bb_width': 'bb_width',
            'mean_reversion_score': 'mean_reversion_score',
            
            # Gap detection
            'gap_up': 'gap_up',
            'gap_down': 'gap_down',
            'gap_size': 'gap_size',
            'gap_fill_probability': 'gap_fill_probability',
            
            # Cross-asset signals
            'vix_price': 'vix_price',
            'vix_return_5': 'vix_return_5',
            'dxy_price': 'dxy_price',
            'dxy_return_5': 'dxy_return_5',
            'btc_price': 'btc_price',
            'btc_return_5': 'btc_return_5',
            'oil_price': 'oil_price',
            'oil_return_5': 'oil_return_5',
            'cross_asset_regime': 'cross_asset_regime',
            
            # Microstructure
            'intraday_return': 'intraday_return',
            'overnight_gap': 'overnight_gap',
            'range_expansion': 'range_expansion',
            'close_position': 'close_position',
        }
        
        # Update strategy config
        if 'config' in migrated_config:
            config = migrated_config['config'].copy()
            
            # Update feature references in strategy logic
            for old_feature, new_feature in feature_updates.items():
                if old_feature in config:
                    config[new_feature] = config[old_feature]
                    # Optionally keep old feature for backward compatibility
                    # del config[old_feature]
            
            migrated_config['config'] = config
        
        # Add feature version metadata
        migrated_config['feature_version'] = 'v2.0'
        migrated_config['migrated_at'] = datetime.utcnow().isoformat()
        
        return migrated_config
    
    def get_feature_importance_report(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate feature importance report for analysis.
        
        Args:
            features: Feature dictionary from feature engine
            
        Returns:
            Report with feature categories and importance scores
        """
        report = {
            'total_features': len(features),
            'categories': {},
            'high_importance_features': [],
            'missing_features': []
        }
        
        # Categorize features
        categories = {
            'returns': [k for k in features.keys() if k.startswith('return_')],
            'volatility': [k for k in features.keys() if 'vol' in k],
            'trend': [k for k in features.keys() if any(x in k for x in ['trend', 'adx', 'momentum'])],
            'volume': [k for k in features.keys() if 'volume' in k],
            'mean_reversion': [k for k in features.keys() if any(x in k for x in ['distance', 'bb_', 'reversion'])],
            'gap': [k for k in features.keys() if 'gap' in k],
            'cross_asset': [k for k in features.keys() if any(x in k for x in ['vix', 'dxy', 'btc', 'oil'])],
            'microstructure': [k for k in features.keys() if any(x in k for x in ['intraday', 'overnight', 'range'])],
        }
        
        for category, feature_list in categories.items():
            report['categories'][category] = {
                'count': len(feature_list),
                'features': feature_list
            }
        
        # Identify high importance features
        high_importance = [
            'adx_14', 'trend_strength', 'vol_regime', 'momentum_10',
            'mean_reversion_score', 'bb_position', 'volume_anomaly',
            'gap_size', 'cross_asset_regime'
        ]
        
        report['high_importance_features'] = [
            f for f in high_importance if f in features
        ]
        
        # Check for missing critical features
        critical_features = [
            'return_1m', 'return_5m', 'return_15m', 'return_1h',
            'realized_vol_20', 'adx_14', 'volume_ratio_20',
            'momentum_10', 'bb_position'
        ]
        
        report['missing_features'] = [
            f for f in critical_features if f not in features
        ]
        
        return report
    
    def validate_feature_completeness(self, features: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate that required features are present.
        
        Args:
            features: Feature dictionary to validate
            
        Returns:
            Tuple of (is_valid, missing_features)
        """
        required_features = {
            # Basic price features
            'entry_price', 'entry_volume',
            
            # Multi-timeframe returns
            'return_1m', 'return_5m', 'return_15m', 'return_1h',
            
            # Volatility
            'realized_vol_20', 'vol_regime',
            
            # Trend
            'adx_14', 'trend_strength',
            
            # Volume
            'volume_ratio_20',
            
            # Momentum
            'momentum_10',
            
            # Mean reversion
            'bb_position',
        }
        
        missing = required_features - set(features.keys())
        return len(missing) == 0, list(missing)
    
    def enrich_price_context(
        self,
        existing_context: Dict[str, Any],
        enhanced_features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich existing price context with new features.
        
        Args:
            existing_context: Legacy price context
            enhanced_features: New comprehensive features
            
        Returns:
            Enriched context with both legacy and new features
        """
        enriched = existing_context.copy()
        
        # Add new features without overwriting existing ones
        for key, value in enhanced_features.items():
            if key not in enriched:
                enriched[key] = value
        
        # Add feature metadata
        enriched['_feature_metadata'] = {
            'legacy_keys': list(existing_context.keys()),
            'new_keys': list(enhanced_features.keys()),
            'total_features': len(enriched),
            'enriched_at': datetime.utcnow().isoformat()
        }
        
        return enriched
