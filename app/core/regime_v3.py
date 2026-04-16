"""
Regime-Aware Signal Classification (v3)

Implements 2-axis regime classification:
- Trend axis: BULL/BEAR/CHOP based on moving averages
- Volatility axis: COMPRESSION/NORMAL/EXPANSION based on ATR percentiles
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple
import numpy as np
import pandas as pd

from app.core.regime_manager import RegimeManager, RegimeSnapshot


class TrendRegime(str):
    BULL = "BULL"
    BEAR = "BEAR"
    CHOP = "CHOP"


class VolatilityRegime(str):
    COMPRESSION = "COMPRESSION"
    NORMAL = "NORMAL"
    EXPANSION = "EXPANSION"


@dataclass(frozen=True)
class RegimeClassification:
    """2-axis regime classification"""
    trend_regime: TrendRegime
    volatility_regime: VolatilityRegime
    combined_regime: str  # e.g., "(BULL, EXPANSION)"
    
    # Raw values for debugging
    price_vs_ma50: float
    ma50_vs_ma200: float
    atr_percentile: float
    
    # Legacy compatibility
    volatility_value: float
    adx_value: Optional[float] = None


class RegimeClassifierV3:
    """
    Regime classifier implementing the 2-axis design:
    
    Trend Axis:
    - BULL: price > MA(50) & MA(50) > MA(200)
    - BEAR: price < MA(50) & MA(50) < MA(200)
    - CHOP: everything else
    
    Volatility Axis:
    - COMPRESSION: ATR/price < p20
    - NORMAL: p20-p80
    - EXPANSION: > p80
    """
    
    def __init__(self, lookback_period: int = 252):
        self.lookback_period = lookback_period
        self.atr_history: Dict[str, list] = {}
    
    def classify_market(
        self,
        ticker: str,
        current_price: float,
        ma50: float,
        ma200: float,
        atr: float,
        historical_atr: Optional[list[float]] = None
    ) -> RegimeClassification:
        """Classify market regime for a single ticker"""
        
        # Trend classification
        price_vs_ma50 = (current_price - ma50) / ma50
        ma50_vs_ma200 = (ma50 - ma200) / ma200
        
        if price_vs_ma50 > 0 and ma50_vs_ma200 > 0:
            trend_regime = TrendRegime.BULL
        elif price_vs_ma50 < 0 and ma50_vs_ma200 < 0:
            trend_regime = TrendRegime.BEAR
        else:
            trend_regime = TrendRegime.CHOP
        
        # Volatility classification
        if historical_atr and len(historical_atr) >= 20:
            atr_percentile = np.percentile(historical_atr, [20, 50, 80])
            
            atr_ratio = atr / current_price
            if atr_ratio < atr_percentile[0] / current_price:
                volatility_regime = VolatilityRegime.COMPRESSION
            elif atr_ratio > atr_percentile[2] / current_price:
                volatility_regime = VolatilityRegime.EXPANSION
            else:
                volatility_regime = VolatilityRegime.NORMAL
            
            # For percentile calculation
            atr_percentile_value = np.searchsorted(
                np.sort(historical_atr), atr
            ) / len(historical_atr)
        else:
            volatility_regime = VolatilityRegime.NORMAL
            atr_percentile_value = 0.5
        
        combined_regime = f"({trend_regime.value}, {volatility_regime.value})"
        
        return RegimeClassification(
            trend_regime=trend_regime,
            volatility_regime=volatility_regime,
            combined_regime=combined_regime,
            price_vs_ma50=price_vs_ma50,
            ma50_vs_ma200=ma50_vs_ma200,
            atr_percentile=atr_percentile_value,
            volatility_value=atr / current_price,
            adx_value=None
        )
    
    def classify_batch(
        self,
        market_data: Dict[str, Dict[str, float]]
    ) -> Dict[str, RegimeClassification]:
        """Classify regime for multiple tickers"""
        results = {}
        
        for ticker, data in market_data.items():
            required_fields = ['price', 'ma50', 'ma200', 'atr']
            if not all(field in data for field in required_fields):
                continue
            
            classification = self.classify_market(
                ticker=ticker,
                current_price=data['price'],
                ma50=data['ma50'],
                ma200=data['ma200'],
                atr=data['atr'],
                historical_atr=data.get('atr_history')
            )
            
            results[ticker] = classification
        
        return results


class SignalGating:
    """
    Implements signal gating based on regime conditions.
    
    Volatility Breakout: Only in (BULL, EXPANSION) or (BEAR, EXPANSION)
    Momentum: Only in (BULL, NORMAL/EXPANSION) or (BEAR, NORMAL/EXPANSION)
    Mean Reversion: Only in (CHOP, COMPRESSION)
    """
    
    @staticmethod
    def allow_volatility_breakout(regime: RegimeClassification) -> bool:
        """Allow volatility breakout signals in expansion and normal regimes with trend"""
        return (
            regime.volatility_regime in [VolatilityRegime.EXPANSION, VolatilityRegime.NORMAL] and
            regime.trend_regime in [TrendRegime.BULL, TrendRegime.BEAR]
        )
    
    @staticmethod
    def allow_momentum(regime: RegimeClassification) -> bool:
        """Allow momentum signals in trending markets with normal/expansion volatility"""
        return (
            regime.trend_regime in [TrendRegime.BULL, TrendRegime.BEAR] and
            regime.volatility_regime in [VolatilityRegime.NORMAL, VolatilityRegime.EXPANSION]
        )
    
    @staticmethod
    def allow_mean_reversion(regime: RegimeClassification) -> bool:
        """Allow mean reversion signals in choppy markets (compression or normal)"""
        return (
            regime.trend_regime == TrendRegime.CHOP and
            regime.volatility_regime in [VolatilityRegime.COMPRESSION, VolatilityRegime.NORMAL]
        )
    
    @staticmethod
    def gate_signal(
        strategy_type: str,
        regime: RegimeClassification
    ) -> Tuple[bool, str]:
        """Gate signal based on strategy type and regime"""
        
        if strategy_type.lower() in ['volatility_breakout', 'breakout']:
            allowed = SignalGating.allow_volatility_breakout(regime)
            reason = "expansion with trend" if allowed else "requires expansion + trend"
            return allowed, reason
        
        elif strategy_type.lower() in ['momentum', 'trend_following']:
            allowed = SignalGating.allow_momentum(regime)
            reason = "trending market" if allowed else "requires trending market"
            return allowed, reason
        
        elif strategy_type.lower() in ['mean_reversion', 'reversal']:
            allowed = SignalGating.allow_mean_reversion(regime)
            reason = "choppy compression" if allowed else "requires choppy compression"
            return allowed, reason
        
        # Default: allow all signals for unknown strategies
        return True, "unknown strategy - allowed by default"


class QualityScoreV3:
    """
    Enhanced quality scoring with regime awareness.
    
    Q = 0.30 * signal_strength
      + 0.25 * regime_alignment
      + 0.20 * volatility_quality
      + 0.15 * agreement_score
      + 0.10 * liquidity_confidence
    """
    
    @staticmethod
    def calculate_quality_score(
        signal_strength: float,
        regime: RegimeClassification,
        strategy_type: str,
        agreement_score: float = 0.5,
        liquidity_confidence: float = 0.5,
        volatility_quality: Optional[float] = None
    ) -> float:
        """Calculate enhanced quality score (0-1 normalized)"""
        
        # 1. Signal strength (0-1)
        signal_strength = max(0.0, min(1.0, signal_strength))
        
        # 2. Regime alignment (0-1) - DOMINANT WEIGHT
        regime_alignment = QualityScoreV3._calculate_regime_alignment(
            regime, strategy_type
        )
        
        # Hard filter: kill trades with zero regime alignment
        if regime_alignment == 0.0:
            return 0.0
        
        # 3. Volatility quality (0-1)
        if volatility_quality is None:
            volatility_quality = QualityScoreV3._calculate_volatility_quality(regime)
        
        # 4. Agreement score (0-1)
        agreement_score = max(0.0, min(1.0, agreement_score))
        
        # REBALANCED WEIGHTS - regime alignment dominant
        quality = (
            0.45 * regime_alignment +      # DOMINANT
            0.30 * signal_strength +       # Real edge
            0.15 * volatility_quality +   # Context
            0.10 * agreement_score         # Confirmation
        )
        
        # NON-LINEAR SEPARATION - force spread
        quality = quality ** 1.5
        
        return max(0.0, min(1.0, quality))
    
    @staticmethod
    def _calculate_regime_alignment(regime: RegimeClassification, strategy_type: str) -> float:
        """Calculate regime alignment score with stronger differentiation"""
        
        allowed, _ = SignalGating.gate_signal(strategy_type, regime)
        
        if not allowed:
            return 0.0
        
        # Stronger differentiation for ideal vs acceptable regimes
        if strategy_type.lower() in ['volatility_breakout', 'breakout']:
            if regime.volatility_regime == VolatilityRegime.EXPANSION:
                return 1.0  # Ideal
            elif regime.volatility_regime == VolatilityRegime.NORMAL:
                return 0.7  # Acceptable
            return 0.3  # Poor
        
        elif strategy_type.lower() in ['momentum', 'trend_following']:
            if regime.volatility_regime == VolatilityRegime.EXPANSION:
                return 1.0  # Ideal
            elif regime.volatility_regime == VolatilityRegime.NORMAL:
                return 0.7  # Acceptable
            return 0.3  # Poor
        
        elif strategy_type.lower() in ['mean_reversion', 'reversal']:
            if regime.volatility_regime == VolatilityRegime.COMPRESSION:
                return 1.0  # Ideal
            elif regime.volatility_regime == VolatilityRegime.NORMAL:
                return 0.7  # Acceptable
            return 0.3  # Poor
        
        return 0.5  # Neutral for unknown strategies
    
    @staticmethod
    def _calculate_volatility_quality(regime: RegimeClassification) -> float:
        """Calculate volatility quality score"""
        
        if regime.volatility_regime == VolatilityRegime.EXPANSION:
            # Reward expansion for breakout strategies
            return 0.8
        elif regime.volatility_regime == VolatilityRegime.COMPRESSION:
            # Reward compression for mean reversion
            return 0.7
        else:
            # Normal volatility gets baseline score
            return 0.5


class PositionSizerV3:
    """
    Enhanced position sizing based on quality scores.
    
    position_size = base_size * (Q^2)
    
    This pushes capital into top decile signals only.
    """
    
    @staticmethod
    def calculate_position_size(
        base_size: float,
        quality_score: float,
        use_squared: bool = True
    ) -> float:
        """Calculate position size based on quality score"""
        
        if use_squared:
            # Square the quality to heavily weight top deciles
            adjusted_quality = quality_score ** 2
        else:
            adjusted_quality = quality_score
        
        return base_size * adjusted_quality
    
    @staticmethod
    def calculate_portfolio_allocation(
        signals: list[Dict[str, Any]],
        total_capital: float,
        base_position_size: float = 0.02  # 2% base position
    ) -> Dict[str, float]:
        """Calculate portfolio allocation across multiple signals"""
        
        allocations = {}
        total_weight = 0.0
        
        # Calculate weighted allocations
        for signal in signals:
            quality_score = signal.get('quality_score', 0.5)
            position_size = PositionSizerV3.calculate_position_size(
                base_position_size, quality_score
            )
            
            allocations[signal['ticker']] = position_size
            total_weight += position_size
        
        # Normalize to ensure total doesn't exceed 100%
        if total_weight > 1.0:
            scale_factor = 1.0 / total_weight
            for ticker in allocations:
                allocations[ticker] *= scale_factor * total_capital
        else:
            for ticker in allocations:
                allocations[ticker] *= total_capital
        
        return allocations
