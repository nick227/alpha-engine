"""
Advanced Position Sizing Models

Comprehensive position sizing system that converts predictions to actual trade sizes.
Integrates confidence, stability, volatility, drawdown, and regime factors.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import math
import logging

logger = logging.getLogger(__name__)


class PositionSizingMethod(Enum):
    """Position sizing methodologies."""
    FIXED_PERCENTAGE = "fixed_percentage"
    KELLY_CRITERION = "kelly_criterion"
    VOLATILITY_TARGET = "volatility_target"
    RISK_PARITY = "risk_parity"
    CONFIDENCE_SCALED = "confidence_scaled"
    ADAPTIVE_KELLY = "adaptive_kelly"


@dataclass
class PositionSizeResult:
    """Result of position sizing calculation."""
    size_shares: float
    size_value: float
    risk_amount: float
    risk_pct: float
    confidence_adj: float
    volatility_adj: float
    stability_adj: float
    regime_adj: float
    drawdown_adj: float
    method_used: str
    metadata: Dict[str, Any]


@dataclass
class SizingContext:
    """Context for position sizing calculations."""
    portfolio_value: float
    available_cash: float
    current_positions: Dict[str, float]  # ticker -> quantity
    current_exposure: Dict[str, float]  # ticker -> market value
    sector_exposure: Dict[str, float]   # sector -> market value
    strategy_exposure: Dict[str, float] # strategy -> market value
    daily_pnl: float
    max_drawdown: float
    current_drawdown: float
    volatility_regime: str
    market_regime: str


class PositionSizer:
    """
    Advanced position sizing engine.
    
    Converts prediction confidence and market conditions to optimal position sizes.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_config = config.get('position_sizing', {})
        
        # Default parameters
        self.base_position_pct = self.base_config.get('base_position_pct', 0.01)  # 1%
        self.max_position_pct = self.base_config.get('max_position_pct', 0.02)      # 2%
        self.confidence_threshold = self.base_config.get('confidence_threshold', 0.5)
        self.volatility_cap = self.base_config.get('volatility_cap', 0.05)         # 5%
        self.max_leverage = self.base_config.get('max_leverage', 1.0)
        
        # Kelly criterion parameters
        self.kelly_fraction = self.base_config.get('kelly_fraction', 0.25)  # Fractional Kelly
        self.kelly_confidence_weight = self.base_config.get('kelly_confidence_weight', 2.0)
        
        # Volatility targeting
        self.volatility_target = self.base_config.get('volatility_target', 0.02)  # 2% daily vol
        self.volatility_lookback = self.base_config.get('volatility_lookback', 20)
        
        # Risk management
        self.max_risk_per_trade = self.base_config.get('max_risk_per_trade', 0.02)  # 2% max risk
        self.drawdown_scaling = self.base_config.get('drawdown_scaling', True)
        self.regime_adjustment = self.base_config.get('regime_adjustment', True)
        
    def calculate_position_size(
        self,
        ticker: str,
        direction: str,  # 'long' or 'short'
        entry_price: float,
        confidence: float,
        stability: float,
        volatility: float,
        regime: str,
        strategy_id: str,
        sector: Optional[str] = None,
        historical_returns: Optional[List[float]] = None,
        context: Optional[SizingContext] = None
    ) -> PositionSizeResult:
        """
        Calculate optimal position size using comprehensive model.
        
        Args:
            ticker: Stock ticker
            direction: Trade direction ('long' or 'short')
            entry_price: Entry price
            confidence: Signal confidence (0-1)
            stability: Strategy stability (0-1)
            volatility: Market volatility (annualized)
            regime: Market regime
            strategy_id: Strategy identifier
            sector: Sector classification
            historical_returns: Historical return series for Kelly
            context: Portfolio context
            
        Returns:
            PositionSizeResult with detailed sizing information
        """
        # Validate inputs
        if confidence < self.confidence_threshold:
            return self._create_zero_size("Below confidence threshold")
        
        if context and context.portfolio_value <= 0:
            return self._create_zero_size("Invalid portfolio value")
        
        # Choose sizing method
        method = self._select_sizing_method(confidence, stability, volatility, context)
        
        # Calculate base size using selected method
        if method == PositionSizingMethod.FIXED_PERCENTAGE:
            base_size = self._fixed_percentage_sizing(confidence, context)
        elif method == PositionSizingMethod.KELLY_CRITERION:
            base_size = self._kelly_criterion_sizing(
                confidence, stability, historical_returns, context
            )
        elif method == PositionSizingMethod.VOLATILITY_TARGET:
            base_size = self._volatility_target_sizing(volatility, context)
        elif method == PositionSizingMethod.RISK_PARITY:
            base_size = self._risk_parity_sizing(volatility, context)
        elif method == PositionSizingMethod.CONFIDENCE_SCALED:
            base_size = self._confidence_scaled_sizing(confidence, stability, context)
        elif method == PositionSizingMethod.ADAPTIVE_KELLY:
            base_size = self._adaptive_kelly_sizing(
                confidence, stability, volatility, historical_returns, context
            )
        else:
            base_size = self.base_position_pct
        
        # Apply adjustments
        adjusted_size = self._apply_adjustments(
            base_size, confidence, stability, volatility, regime, context
        )
        
        # Apply risk limits
        final_size = self._apply_risk_limits(
            adjusted_size, ticker, direction, entry_price, context
        )
        
        # Convert to shares and value
        if context:
            size_value = final_size * context.portfolio_value
            size_shares = size_value / entry_price
            risk_amount = self._calculate_risk_amount(size_shares, entry_price, volatility)
            risk_pct = risk_amount / context.portfolio_value if context.portfolio_value > 0 else 0
        else:
            size_value = final_size * 100000  # Default portfolio value
            size_shares = size_value / entry_price
            risk_amount = 0
            risk_pct = 0
        
        # Calculate adjustments for reporting
        adjustments = self._calculate_adjustments(
            confidence, stability, volatility, regime, context
        )
        
        return PositionSizeResult(
            size_shares=size_shares,
            size_value=size_value,
            risk_amount=risk_amount,
            risk_pct=risk_pct,
            confidence_adj=adjustments['confidence'],
            volatility_adj=adjustments['volatility'],
            stability_adj=adjustments['stability'],
            regime_adj=adjustments['regime'],
            drawdown_adj=adjustments['drawdown'],
            method_used=method.value,
            metadata={
                'base_size': base_size,
                'adjusted_size': adjusted_size,
                'final_size': final_size,
                'direction': direction,
                'entry_price': entry_price,
                'volatility_regime': context.volatility_regime if context else 'UNKNOWN'
            }
        )
    
    def _select_sizing_method(
        self,
        confidence: float,
        stability: float,
        volatility: float,
        context: Optional[SizingContext]
    ) -> PositionSizingMethod:
        """Select optimal sizing method based on conditions."""
        # High confidence + high stability = Kelly
        if confidence > 0.8 and stability > 0.7:
            return PositionSizingMethod.KELLY_CRITERION
        
        # High volatility = volatility targeting
        if volatility > 0.03:
            return PositionSizingMethod.VOLATILITY_TARGET
        
        # Low confidence = confidence scaled
        if confidence < 0.6:
            return PositionSizingMethod.CONFIDENCE_SCALED
        
        # Drawdown conditions = adaptive
        if context and context.current_drawdown > 0.05:
            return PositionSizingMethod.ADAPTIVE_KELLY
        
        # Default to confidence scaled
        return PositionSizingMethod.CONFIDENCE_SCALED
    
    def _fixed_percentage_sizing(
        self,
        confidence: float,
        context: Optional[SizingContext]
    ) -> float:
        """Fixed percentage sizing with confidence scaling."""
        base_size = self.base_position_pct
        
        # Scale by confidence
        confidence_factor = confidence / 0.8  # Normalize to 80% = 1.0x
        confidence_factor = max(0.5, min(2.0, confidence_factor))  # Cap between 0.5x and 2.0x
        
        return base_size * confidence_factor
    
    def _kelly_criterion_sizing(
        self,
        confidence: float,
        stability: float,
        historical_returns: Optional[List[float]],
        context: Optional[SizingContext]
    ) -> float:
        """Kelly criterion position sizing."""
        if not historical_returns or len(historical_returns) < 10:
            # Fallback to confidence-based sizing
            return self._confidence_scaled_sizing(confidence, stability, context)
        
        # Calculate win rate and average win/loss
        wins = [r for r in historical_returns if r > 0]
        losses = [r for r in historical_returns if r < 0]
        
        win_rate = len(wins) / len(historical_returns) if historical_returns else 0.5
        avg_win = sum(wins) / len(wins) if wins else 0.01
        avg_loss = abs(sum(losses) / len(losses)) if losses else 0.01
        
        # Kelly formula: f = (p * b - q) / b
        # where p = win rate, b = win/loss ratio, q = loss rate
        win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 1.0
        kelly_fraction = (win_rate * win_loss_ratio - (1 - win_rate)) / win_loss_ratio
        
        # Apply fractional Kelly and confidence scaling
        kelly_size = max(0, kelly_fraction) * self.kelly_fraction
        
        # Scale by confidence and stability
        confidence_factor = confidence ** self.kelly_confidence_weight
        stability_factor = stability
        
        final_size = kelly_size * confidence_factor * stability_factor
        
        # Cap at maximum
        return min(final_size, self.max_position_pct)
    
    def _volatility_target_sizing(
        self,
        volatility: float,
        context: Optional[SizingContext]
    ) -> float:
        """Volatility targeting position sizing."""
        # Target constant volatility exposure
        if volatility <= 0:
            return self.base_position_pct
        
        # Scale position to achieve target volatility
        vol_factor = self.volatility_target / volatility
        vol_factor = max(0.1, min(5.0, vol_factor))  # Cap between 0.1x and 5.0x
        
        size = self.base_position_pct * vol_factor
        
        # Apply volatility cap
        if volatility > self.volatility_cap:
            size *= 0.5  # Reduce size by half for high volatility
        
        return size
    
    def _risk_parity_sizing(
        self,
        volatility: float,
        context: Optional[SizingContext]
    ) -> float:
        """Risk parity position sizing."""
        # Equalize risk contribution across positions
        if volatility <= 0:
            return self.base_position_pct
        
        # Inverse volatility scaling
        vol_weight = 1.0 / volatility
        
        # Normalize (simplified - would use all positions in practice)
        target_weight = vol_weight / (1.0 + vol_weight)  # Assume equal weight to cash
        
        size = target_weight * self.max_position_pct
        
        return size
    
    def _confidence_scaled_sizing(
        self,
        confidence: float,
        stability: float,
        context: Optional[SizingContext]
    ) -> float:
        """Confidence-scaled position sizing."""
        base_size = self.base_position_pct
        
        # Confidence scaling (non-linear)
        confidence_factor = confidence ** 1.5  # Convex scaling
        
        # Stability scaling
        stability_factor = stability
        
        # Combined scaling
        size = base_size * confidence_factor * stability_factor
        
        # Apply minimum/maximum
        return max(self.base_position_pct * 0.1, min(size, self.max_position_pct))
    
    def _adaptive_kelly_sizing(
        self,
        confidence: float,
        stability: float,
        volatility: float,
        historical_returns: Optional[List[float]],
        context: Optional[SizingContext]
    ) -> float:
        """Adaptive Kelly that adjusts for market conditions."""
        # Start with Kelly
        kelly_size = self._kelly_criterion_sizing(confidence, stability, historical_returns, context)
        
        # Volatility adjustment
        if volatility > 0.02:  # High volatility
            kelly_size *= 0.7  # Reduce by 30%
        elif volatility < 0.01:  # Low volatility
            kelly_size *= 1.3  # Increase by 30%
        
        # Drawdown adjustment
        if context and context.current_drawdown > 0.05:
            drawdown_factor = max(0.3, 1.0 - context.current_drawdown * 2)
            kelly_size *= drawdown_factor
        
        # Regime adjustment
        if context:
            if context.market_regime == 'RISK_OFF':
                kelly_size *= 0.5
            elif context.market_regime == 'RISK_ON':
                kelly_size *= 1.2
        
        return kelly_size
    
    def _apply_adjustments(
        self,
        base_size: float,
        confidence: float,
        stability: float,
        volatility: float,
        regime: str,
        context: Optional[SizingContext]
    ) -> float:
        """Apply comprehensive adjustments to base position size."""
        adjusted_size = base_size
        
        # Confidence adjustment
        confidence_adj = confidence / 0.8  # Normalize
        confidence_adj = max(0.5, min(2.0, confidence_adj))
        adjusted_size *= confidence_adj
        
        # Stability adjustment
        stability_adj = stability
        adjusted_size *= stability_adj
        
        # Volatility adjustment (inverse)
        volatility_adj = min(0.02 / max(volatility, 0.001), 2.0)  # Cap at 2x
        adjusted_size *= volatility_adj
        
        # Regime adjustment
        regime_adj = self._get_regime_adjustment(regime, context)
        adjusted_size *= regime_adj
        
        # Drawdown adjustment
        drawdown_adj = self._get_drawdown_adjustment(context)
        adjusted_size *= drawdown_adj
        
        return adjusted_size
    
    def _apply_risk_limits(
        self,
        size: float,
        ticker: str,
        direction: str,
        entry_price: float,
        context: Optional[SizingContext]
    ) -> float:
        """Apply risk management limits."""
        if not context:
            return size
        
        # Maximum position size
        size = min(size, self.max_position_pct)
        
        # Ticker exposure limit
        current_exposure = context.current_exposure.get(ticker, 0)
        max_ticker_exposure = self.base_config.get('max_ticker_exposure', 0.10)
        
        proposed_exposure = size * context.portfolio_value
        if direction == 'short':
            proposed_exposure = abs(proposed_exposure)
        
        total_exposure = current_exposure + proposed_exposure
        if total_exposure > max_ticker_exposure * context.portfolio_value:
            # Reduce size to stay within limit
            available_exposure = max_ticker_exposure * context.portfolio_value - current_exposure
            size = available_exposure / context.portfolio_value
            size = max(0, size)
        
        # Daily loss limit
        daily_loss_limit = self.base_config.get('daily_loss_limit_pct', 0.02)
        if context.daily_pnl < -daily_loss_limit * context.portfolio_value:
            size = 0  # No new positions if daily loss limit exceeded
        
        # Cash availability
        if direction == 'long':
            max_affordable = context.available_cash / context.portfolio_value
            size = min(size, max_affordable)
        
        return size
    
    def _calculate_risk_amount(self, shares: float, entry_price: float, volatility: float) -> float:
        """Calculate risk amount based on position and volatility."""
        position_value = shares * entry_price
        
        # Risk = position value * volatility * sqrt(days) * confidence interval
        # Using 2-day risk horizon and 95% confidence (1.96 std)
        risk_horizon_days = 2
        confidence_interval = 1.96
        
        daily_vol = volatility / math.sqrt(252)  # Convert annual to daily
        risk_amount = position_value * daily_vol * math.sqrt(risk_horizon_days) * confidence_interval
        
        return risk_amount
    
    def _calculate_adjustments(
        self,
        confidence: float,
        stability: float,
        volatility: float,
        regime: str,
        context: Optional[SizingContext]
    ) -> Dict[str, float]:
        """Calculate individual adjustment factors for reporting."""
        adjustments = {
            'confidence': confidence / 0.8,
            'volatility': min(0.02 / max(volatility, 0.001), 2.0),
            'stability': stability,
            'regime': self._get_regime_adjustment(regime, context),
            'drawdown': self._get_drawdown_adjustment(context)
        }
        
        # Cap adjustments for reporting
        for key, value in adjustments.items():
            adjustments[key] = max(0.1, min(3.0, value))
        
        return adjustments
    
    def _get_regime_adjustment(self, regime: str, context: Optional[SizingContext]) -> float:
        """Get regime-based adjustment factor."""
        regime_adjustments = {
            'BULLISH_MODERATE': 1.2,
            'BULLISH_STRONG': 1.5,
            'BEARISH_MODERATE': 0.8,
            'BEARISH_STRONG': 0.6,
            'SIDEWAYS': 1.0,
            'HIGH_VOLATILITY': 0.7,
            'LOW_VOLATILITY': 1.1,
            'RISK_ON': 1.3,
            'RISK_OFF': 0.5,
            'NEUTRAL': 1.0
        }
        
        base_adj = regime_adjustments.get(regime, 1.0)
        
        # Additional context-based adjustments
        if context:
            if context.volatility_regime == 'HIGH':
                base_adj *= 0.8
            elif context.volatility_regime == 'LOW':
                base_adj *= 1.1
            
            if context.current_drawdown > 0.10:
                base_adj *= 0.5
        
        return base_adj
    
    def _get_drawdown_adjustment(self, context: Optional[SizingContext]) -> float:
        """Get drawdown-based adjustment factor."""
        if not context or not self.drawdown_scaling:
            return 1.0
        
        drawdown = context.current_drawdown
        
        # Gradual reduction based on drawdown
        if drawdown < 0.02:
            return 1.0
        elif drawdown < 0.05:
            return 0.9
        elif drawdown < 0.10:
            return 0.7
        elif drawdown < 0.15:
            return 0.5
        elif drawdown < 0.20:
            return 0.3
        else:
            return 0.1
    
    def _create_zero_size(self, reason: str) -> PositionSizeResult:
        """Create zero-size result with reason."""
        return PositionSizeResult(
            size_shares=0.0,
            size_value=0.0,
            risk_amount=0.0,
            risk_pct=0.0,
            confidence_adj=0.0,
            volatility_adj=0.0,
            stability_adj=0.0,
            regime_adj=0.0,
            drawdown_adj=0.0,
            method_used="zero_size",
            metadata={'reason': reason}
        )
    
    def get_position_size_summary(self, results: List[PositionSizeResult]) -> Dict[str, Any]:
        """Get summary statistics for position sizing results."""
        if not results:
            return {'total_positions': 0}
        
        total_value = sum(r.size_value for r in results)
        total_risk = sum(r.risk_amount for r in results)
        avg_confidence = sum(r.confidence_adj for r in results) / len(results)
        avg_volatility_adj = sum(r.volatility_adj for r in results) / len(results)
        
        method_counts = {}
        for result in results:
            method_counts[result.method_used] = method_counts.get(result.method_used, 0) + 1
        
        return {
            'total_positions': len(results),
            'total_value': total_value,
            'total_risk': total_risk,
            'average_confidence_adj': avg_confidence,
            'average_volatility_adj': avg_volatility_adj,
            'method_distribution': method_counts,
            'largest_position': max(r.size_value for r in results),
            'smallest_position': min(r.size_value for r in results if r.size_value > 0)
        }
