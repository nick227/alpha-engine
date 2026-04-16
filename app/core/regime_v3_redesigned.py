"""
Regime-Aware Quality Scoring - Redesigned Architecture

Key changes:
1. Separate gating from ranking
2. Rank within passed set only
3. Signal-specific features
4. Use ranks, not averages
5. Two-view validation
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List
import numpy as np
import pandas as pd

from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime, SignalGating


@dataclass
class SignalFeatures:
    """Signal-specific features for ranking"""
    
    # Volatility breakout features
    breakout_distance: Optional[float] = None  # Distance above trigger
    volume_expansion_percentile: Optional[float] = None
    atr_expansion_percentile: Optional[float] = None
    price_vs_vwap: Optional[float] = None
    
    # Momentum features
    trend_persistence: Optional[float] = None  # How long trend has persisted
    momentum_strength: Optional[float] = None
    price_acceleration: Optional[float] = None
    
    # Mean reversion features
    reversal_stretch: Optional[float] = None  # How stretched the move is
    support_distance: Optional[float] = None
    oversold_level: Optional[float] = None
    
    # Common features
    signal_strength_raw: float = 0.0
    volume_confirmation: float = 0.0
    liquidity_score: float = 0.5


class RegimeAwareRankerV2:
    """
    Redesigned quality ranking system.
    
    Architecture:
    1. Gate by regime first (pass/fail)
    2. Extract signal-specific features
    3. Rank within passed set only
    4. Use percentile ranks, not averages
    """
    
    def __init__(self):
        self.feature_history = {}  # Store feature distributions for ranking
        
    def gate_and_rank(
        self,
        signal_type: str,
        regime: RegimeClassification,
        features: SignalFeatures,
        all_signals: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[float]:
        """
        Main entry point: Gate first, then rank within passed set.
        """
        
        # Step 1: Gate by regime
        passed, reason = SignalGating.gate_signal(signal_type, regime)
        
        if not passed:
            return None  # Don't assign score to failed trades
        
        # Step 2: Rank within passed set
        score = self._rank_signal(signal_type, regime, features, all_signals)
        
        return score
    
    def _rank_signal(
        self,
        signal_type: str,
        regime: RegimeClassification,
        features: SignalFeatures,
        all_signals: Optional[List[Dict[str, Any]]] = None
    ) -> float:
        """
        Rank signal using signal-specific features and percentile ranking.
        """
        
        # Extract signal-specific features
        signal_features = self._extract_signal_features(signal_type, features)
        
        # Convert to percentile ranks (0-1)
        ranked_features = self._percentile_rank_features(
            signal_features, signal_type, all_signals
        )
        
        # Combine ranked features with regime context
        combined_score = self._combine_ranked_features(
            ranked_features, signal_type, regime
        )
        
        # Final percentile rank within passed set
        final_score = self._final_percentile_rank(combined_score, all_signals)
        
        return final_score
    
    def _extract_signal_features(
        self, signal_type: str, features: SignalFeatures
    ) -> Dict[str, float]:
        """Extract relevant features for each signal type"""
        
        if signal_type in ['volatility_breakout', 'breakout']:
            return {
                'breakout_distance': features.breakout_distance or 0.0,
                'volume_expansion': features.volume_expansion_percentile or 0.0,
                'atr_expansion': features.atr_expansion_percentile or 0.0,
                'price_vs_vwap': features.price_vs_vwap or 0.0,
                'signal_strength': features.signal_strength_raw,
                'volume_confirmation': features.volume_confirmation
            }
        
        elif signal_type in ['momentum', 'trend_following']:
            return {
                'trend_persistence': features.trend_persistence or 0.0,
                'momentum_strength': features.momentum_strength or 0.0,
                'price_acceleration': features.price_acceleration or 0.0,
                'signal_strength': features.signal_strength_raw,
                'volume_confirmation': features.volume_confirmation,
                'liquidity_score': features.liquidity_score
            }
        
        elif signal_type in ['mean_reversion', 'reversal']:
            return {
                'reversal_stretch': features.reversal_stretch or 0.0,
                'support_distance': features.support_distance or 0.0,
                'oversold_level': features.oversold_level or 0.0,
                'signal_strength': features.signal_strength_raw,
                'volume_confirmation': features.volume_confirmation,
                'liquidity_score': features.liquidity_score
            }
        
        else:
            # Default features for unknown signal types
            return {
                'signal_strength': features.signal_strength_raw,
                'volume_confirmation': features.volume_confirmation,
                'liquidity_score': features.liquidity_score
            }
    
    def _percentile_rank_features(
        self,
        features: Dict[str, float],
        signal_type: str,
        all_signals: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, float]:
        """
        Convert raw features to percentile ranks (0-1).
        Higher values = better rank.
        """
        
        ranked_features = {}
        
        # If we have historical data, use it for ranking
        if all_signals and len(all_signals) > 10:
            # Extract same signal type
            same_type_signals = [
                s for s in all_signals 
                if s.get('signal_type') == signal_type and s.get('passed', False)
            ]
            
            if len(same_type_signals) > 5:
                for feature_name, feature_value in features.items():
                    # Get feature values from same signal type
                    feature_values = [
                        s.get('features', {}).get(feature_name, 0.0)
                        for s in same_type_signals
                        if s.get('features', {}).get(feature_name) is not None
                    ]
                    
                    if feature_values:
                        # Calculate percentile rank
                        ranked_features[feature_name] = self._calculate_percentile_rank(
                            feature_value, feature_values
                        )
                    else:
                        ranked_features[feature_name] = 0.5  # Default middle rank
            else:
                # Not enough data for ranking
                for feature_name, feature_value in features.items():
                    ranked_features[feature_name] = 0.5
        else:
            # No historical data - use simple normalization
            for feature_name, feature_value in features.items():
                # Simple sigmoid normalization for demo
                ranked_features[feature_name] = 1 / (1 + np.exp(-feature_value * 10))
        
        return ranked_features
    
    def _calculate_percentile_rank(self, value: float, values: List[float]) -> float:
        """Calculate percentile rank (0-1)"""
        if not values:
            return 0.5
        
        sorted_values = sorted(values)
        rank = sum(1 for v in sorted_values if v <= value)
        percentile = rank / len(sorted_values)
        
        return max(0.0, min(1.0, percentile))
    
    def _combine_ranked_features(
        self,
        ranked_features: Dict[str, float],
        signal_type: str,
        regime: RegimeClassification
    ) -> float:
        """
        Combine ranked features with regime context.
        Uses weighted sum of top features.
        """
        
        # Signal-specific feature weights
        if signal_type in ['volatility_breakout', 'breakout']:
            weights = {
                'breakout_distance': 0.25,
                'volume_expansion': 0.20,
                'atr_expansion': 0.15,
                'signal_strength': 0.20,
                'volume_confirmation': 0.10,
                'price_vs_vwap': 0.10
            }
        elif signal_type in ['momentum', 'trend_following']:
            weights = {
                'trend_persistence': 0.25,
                'momentum_strength': 0.25,
                'signal_strength': 0.20,
                'price_acceleration': 0.15,
                'volume_confirmation': 0.10,
                'liquidity_score': 0.05
            }
        elif signal_type in ['mean_reversion', 'reversal']:
            weights = {
                'reversal_stretch': 0.25,
                'support_distance': 0.20,
                'oversold_level': 0.20,
                'signal_strength': 0.15,
                'volume_confirmation': 0.10,
                'liquidity_score': 0.10
            }
        else:
            # Default weights
            weights = {
                'signal_strength': 0.5,
                'volume_confirmation': 0.3,
                'liquidity_score': 0.2
            }
        
        # Calculate weighted sum
        score = 0.0
        for feature_name, weight in weights.items():
            if feature_name in ranked_features:
                score += weight * ranked_features[feature_name]
        
        # Add regime alignment bonus (small, since already gated)
        regime_bonus = self._calculate_regime_alignment_bonus(signal_type, regime)
        score += regime_bonus * 0.1  # Small bonus
        
        return score
    
    def _calculate_regime_alignment_bonus(
        self, signal_type: str, regime: RegimeClassification
    ) -> float:
        """Calculate small regime alignment bonus for fine-tuning"""
        
        if signal_type in ['volatility_breakout', 'breakout']:
            if regime.volatility_regime == VolatilityRegime.EXPANSION:
                return 1.0  # Ideal
            elif regime.volatility_regime == VolatilityRegime.NORMAL:
                return 0.7  # Good
            else:
                return 0.3  # Poor
        
        elif signal_type in ['momentum', 'trend_following']:
            if regime.volatility_regime == VolatilityRegime.EXPANSION:
                return 1.0  # Ideal
            elif regime.volatility_regime == VolatilityRegime.NORMAL:
                return 0.7  # Good
            else:
                return 0.3  # Poor
        
        elif signal_type in ['mean_reversion', 'reversal']:
            if regime.volatility_regime == VolatilityRegime.COMPRESSION:
                return 1.0  # Ideal
            elif regime.volatility_regime == VolatilityRegime.NORMAL:
                return 0.7  # Good
            else:
                return 0.3  # Poor
        
        return 0.5  # Neutral
    
    def _final_percentile_rank(
        self, score: float, all_signals: Optional[List[Dict[str, Any]]] = None
    ) -> float:
        """Final percentile rank within passed set"""
        
        if not all_signals:
            # No reference set - return normalized score
            return max(0.0, min(1.0, score))
        
        # Get scores from passed signals only
        passed_scores = [
            s.get('ranked_score', 0.0)
            for s in all_signals
            if s.get('passed', False) and s.get('ranked_score') is not None
        ]
        
        if len(passed_scores) < 2:
            return 0.5
        
        # Calculate percentile rank
        final_rank = self._calculate_percentile_rank(score, passed_scores)
        
        return final_rank


class SignalFeatureExtractor:
    """Extract signal-specific features from market data"""
    
    @staticmethod
    def extract_volatility_breakout_features(
        current_price: float,
        atr: float,
        volume: float,
        vwap: Optional[float] = None,
        historical_volume: Optional[List[float]] = None,
        historical_atr: Optional[List[float]] = None
    ) -> SignalFeatures:
        """Extract features for volatility breakout signals"""
        
        # Distance from trigger (simplified)
        breakout_distance = min(atr / current_price, 0.05)  # Cap at 5%
        
        # Volume expansion percentile
        volume_expansion = 0.5  # Default
        if historical_volume and len(historical_volume) > 20:
            volume_expansion = sum(1 for v in historical_volume if v <= volume) / len(historical_volume)
        
        # ATR expansion percentile
        atr_expansion = 0.5  # Default
        if historical_atr and len(historical_atr) > 20:
            atr_expansion = sum(1 for a in historical_atr if a <= atr) / len(historical_atr)
        
        # Price vs VWAP
        price_vs_vwap = 0.0
        if vwap:
            price_vs_vwap = (current_price - vwap) / vwap
        
        # Volume confirmation (z-score)
        volume_confirmation = 0.5  # Simplified
        
        return SignalFeatures(
            breakout_distance=breakout_distance,
            volume_expansion_percentile=volume_expansion,
            atr_expansion_percentile=atr_expansion,
            price_vs_vwap=price_vs_vwap,
            signal_strength_raw=breakout_distance,
            volume_confirmation=volume_confirmation,
            liquidity_score=0.5  # Simplified
        )
    
    @staticmethod
    def extract_momentum_features(
        current_price: float,
        ma50: float,
        ma200: float,
        returns_5d: float,
        returns_20d: float,
        volume: float
    ) -> SignalFeatures:
        """Extract features for momentum signals"""
        
        # Trend persistence (how long above MAs)
        price_vs_ma50 = (current_price - ma50) / ma50
        ma50_vs_ma200 = (ma50 - ma200) / ma200
        trend_persistence = (price_vs_ma50 + ma50_vs_ma200) / 2
        
        # Momentum strength
        momentum_strength = abs(returns_5d) + abs(returns_20d) / 2
        
        # Price acceleration
        price_acceleration = returns_5d - returns_20d if returns_20d != 0 else 0
        
        return SignalFeatures(
            trend_persistence=trend_persistence,
            momentum_strength=momentum_strength,
            price_acceleration=price_acceleration,
            signal_strength_raw=momentum_strength,
            volume_confirmation=0.5,
            liquidity_score=0.5
        )
    
    @staticmethod
    def extract_mean_reversion_features(
        current_price: float,
        ma50: float,
        lowest_20d: float,
        volume: float,
        rsi: Optional[float] = None
    ) -> SignalFeatures:
        """Extract features for mean reversion signals"""
        
        # Reversal stretch (how far from mean)
        reversal_stretch = abs(current_price - ma50) / ma50
        
        # Support distance
        support_distance = (current_price - lowest_20d) / current_price
        
        # Oversold level
        oversold_level = 0.5  # Default
        if rsi:
            oversold_level = (30 - rsi) / 30 if rsi < 30 else 0
        else:
            # Simplified oversold using price
            oversold_level = max(0, (lowest_20d - current_price) / current_price)
        
        return SignalFeatures(
            reversal_stretch=reversal_stretch,
            support_distance=support_distance,
            oversold_level=oversold_level,
            signal_strength_raw=reversal_stretch,
            volume_confirmation=0.5,
            liquidity_score=0.5
        )
