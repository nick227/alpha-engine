"""
Regime-First Trading System v2

Fixed entry location logic:
- BULL EXPANSION: Enter on pullbacks (30-60% range)
- BEAR EXPANSION: Enter on breakdowns (<40% range)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import uuid
import logging
import numpy as np
from datetime import datetime, timezone

from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime, SignalGating

logger = logging.getLogger(__name__)


class TradeDirection(Enum):
    LONG = "long"
    SHORT = "short"


class TradeStatus(Enum):
    PENDING = "pending"
    EXECUTED = "executed"
    FILLED = "filled"
    CLOSED = "closed"
    CANCELLED = "cancelled"


@dataclass
class Position:
    """Clean position tracking for regime-first system"""
    ticker: str
    direction: TradeDirection
    entry_price: float
    entry_time: datetime
    quantity: float
    stop_loss: float
    target_price: Optional[float] = None
    
    # Current state
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    
    # Risk management
    atr_at_entry: float = 0.0
    regime_at_entry: str = ""
    position_in_range_at_entry: float = 0.0  # NEW: Track entry location
    
    # Status
    status: TradeStatus = TradeStatus.PENDING
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    
    def update_price(self, current_price: float):
        """Update position with current price"""
        self.current_price = current_price
        
        # Calculate unrealized P&L
        if self.direction == TradeDirection.LONG:
            self.unrealized_pnl = self.quantity * (current_price - self.entry_price)
        else:
            self.unrealized_pnl = self.quantity * (self.entry_price - current_price)
    
    def should_exit(self, current_price: float) -> Tuple[bool, str]:
        """Check if position should be closed"""
        
        # Stop loss check
        if self.direction == TradeDirection.LONG and current_price <= self.stop_loss:
            return True, "stop_loss"
        elif self.direction == TradeDirection.SHORT and current_price >= self.stop_loss:
            return True, "stop_loss"
        
        # Target check
        if self.target_price:
            if self.direction == TradeDirection.LONG and current_price >= self.target_price:
                return True, "target_reached"
            elif self.direction == TradeDirection.SHORT and current_price <= self.target_price:
                return True, "target_reached"
        
        return False, ""
    
    def close_position(self, exit_price: float, exit_time: datetime, reason: str):
        """Close position and calculate realized P&L"""
        self.exit_price = exit_price
        self.exit_time = exit_time
        self.exit_reason = reason
        self.status = TradeStatus.CLOSED
        
        # Calculate realized P&L
        if self.direction == TradeDirection.LONG:
            self.realized_pnl = self.quantity * (exit_price - self.entry_price)
        else:
            self.realized_pnl = self.quantity * (self.entry_price - exit_price)


@dataclass
class RiskParameters:
    """Risk management parameters for regime-first system"""
    base_risk_per_trade: float = 0.02  # 2% base risk
    max_portfolio_heat: float = 0.20   # 20% max portfolio exposure
    max_positions_per_regime: int = 5   # Max positions in same regime
    atr_stop_multiplier: float = 2.0    # Stop at 2x ATR
    atr_target_multiplier: float = 3.0  # Target at 3x ATR
    max_concurrent_positions: int = 10   # Max total positions


class RegimeFirstTraderV2:
    """
    Regime-First Trading System v2
    
    FIXED: Entry location logic based on regime analysis.
    """
    
    def __init__(self, initial_capital: float = 100000.0, risk_params: Optional[RiskParameters] = None):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_params = risk_params or RiskParameters()
        
        # Portfolio state
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []
        
        # Regime tracking
        self.regime_positions: Dict[str, List[str]] = {}  # Track positions per regime
        self.regime_performance: Dict[str, Dict[str, Any]] = {}  # Track performance per regime
        
        # Trading statistics
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        
        logger.info(f"Regime-First Trader V2 initialized with ${initial_capital:,.0f}")
    
    def is_regime_tradeable(self, regime: RegimeClassification) -> bool:
        """
        Primary decision: Is this regime tradeable?
        """
        
        # High-edge regimes
        if (regime.trend_regime == TrendRegime.BULL and 
            regime.volatility_regime == VolatilityRegime.EXPANSION):
            return True
        
        if (regime.trend_regime == TrendRegime.BEAR and 
            regime.volatility_regime == VolatilityRegime.EXPANSION):
            return True
        
        # Secondary regime (strong performance)
        if (regime.trend_regime == TrendRegime.BEAR and 
            regime.volatility_regime == VolatilityRegime.COMPRESSION):
            return True
        
        return False
    
    def should_enter_trade(
        self, 
        regime: RegimeClassification, 
        position_in_range: float
    ) -> bool:
        """
        FIXED: Entry location logic based on regime analysis.
        
        BULL EXPANSION: Enter on pullbacks (30-60% range)
        BEAR EXPANSION: Enter on breakdowns (<40% range)
        """
        
        if (regime.trend_regime == TrendRegime.BULL and 
            regime.volatility_regime == VolatilityRegime.EXPANSION):
            # BULL EXPANSION: Wait for pullback zone
            return 0.3 < position_in_range < 0.6
        
        elif (regime.trend_regime == TrendRegime.BEAR and 
              regime.volatility_regime == VolatilityRegime.EXPANSION):
            # BEAR EXPANSION: Early breakdown entries
            return position_in_range < 0.4
        
        elif (regime.trend_regime == TrendRegime.BEAR and 
              regime.volatility_regime == VolatilityRegime.COMPRESSION):
            # BEAR COMPRESSION: Enter near lows
            return position_in_range < 0.4
        
        return False
    
    def calculate_position_size(self, ticker: str, regime: RegimeClassification, atr: float) -> float:
        """
        Calculate position size based on risk parameters.
        Equal risk sizing with regime adjustments.
        """
        
        # Base position size from risk per trade and ATR
        base_size = (self.current_capital * self.risk_params.base_risk_per_trade) / (atr * self.risk_params.atr_stop_multiplier)
        
        # Regime adjustment (optional - can be removed for pure equal sizing)
        regime_multiplier = 1.0
        if regime.volatility_regime == VolatilityRegime.EXPANSION:
            regime_multiplier = 1.2  # Slightly larger in expansion
        elif regime.volatility_regime == VolatilityRegime.COMPRESSION:
            regime_multiplier = 0.8  # Smaller in compression
        
        adjusted_size = base_size * regime_multiplier
        
        # Portfolio heat check
        current_exposure = sum(abs(pos.quantity) for pos in self.positions.values())
        available_heat = (self.current_capital * self.risk_params.max_portfolio_heat) - current_exposure
        
        if adjusted_size > available_heat:
            adjusted_size = available_heat
        
        return max(0, adjusted_size)
    
    def enter_trade(
        self,
        ticker: str,
        direction: TradeDirection,
        entry_price: float,
        regime: RegimeClassification,
        atr: float,
        position_in_range: float,
        entry_time: Optional[datetime] = None
    ) -> Optional[str]:
        """
        Enter a trade with FIXED entry location logic.
        """
        
        # Check if regime is tradeable
        if not self.is_regime_tradeable(regime):
            return None
        
        # FIXED: Check entry location
        if not self.should_enter_trade(regime, position_in_range):
            return None
        
        # Check portfolio constraints
        if len(self.positions) >= self.risk_params.max_concurrent_positions:
            return None
        
        # Check regime position limit
        regime_key = regime.combined_regime
        regime_positions = self.regime_positions.get(regime_key, [])
        if len(regime_positions) >= self.risk_params.max_positions_per_regime:
            return None
        
        # Calculate position size
        position_size = self.calculate_position_size(ticker, regime, atr)
        
        if position_size <= 0:
            return None
        
        # Calculate stop loss and target
        if direction == TradeDirection.LONG:
            stop_loss = entry_price - (atr * self.risk_params.atr_stop_multiplier)
            target_price = entry_price + (atr * self.risk_params.atr_target_multiplier)
        else:
            stop_loss = entry_price + (atr * self.risk_params.atr_stop_multiplier)
            target_price = entry_price - (atr * self.risk_params.atr_target_multiplier)
        
        # Create position
        position_id = str(uuid.uuid4())
        position = Position(
            ticker=ticker,
            direction=direction,
            entry_price=entry_price,
            entry_time=entry_time or datetime.now(timezone.utc),
            quantity=position_size,
            stop_loss=stop_loss,
            target_price=target_price,
            atr_at_entry=atr,
            regime_at_entry=regime.combined_regime,
            position_in_range_at_entry=position_in_range,  # NEW: Track entry location
            status=TradeStatus.EXECUTED
        )
        
        # Update portfolio
        self.positions[position_id] = position
        
        # Track regime positions
        if regime_key not in self.regime_positions:
            self.regime_positions[regime_key] = []
        self.regime_positions[regime_key].append(position_id)
        
        # Initialize regime performance tracking
        if regime_key not in self.regime_performance:
            self.regime_performance[regime_key] = {
                'trades': 0,
                'wins': 0,
                'losses': 0,
                'total_pnl': 0.0,
                'win_rate': 0.0
            }
        
        logger.info(f"FIXED ENTRY: {ticker} {direction.value} at {entry_price:.2f}, range_pos: {position_in_range:.1%}, stop: {stop_loss:.2f}")
        
        return position_id
    
    def update_positions(self, market_data: Dict[str, Dict[str, Any]]):
        """
        Update all positions with current market data.
        Handle exits based on stop loss/target.
        """
        
        positions_to_close = []
        
        for position_id, position in self.positions.items():
            ticker = position.ticker
            
            if ticker not in market_data:
                continue
            
            current_price = market_data[ticker]['price']
            position.update_price(current_price)
            
            # Check for exit
            should_exit, reason = position.should_exit(current_price)
            
            if should_exit:
                positions_to_close.append((position_id, current_price, reason))
        
        # Close positions
        for position_id, exit_price, reason in positions_to_close:
            self.close_position(position_id, exit_price, reason)
    
    def close_position(self, position_id: str, exit_price: float, reason: str):
        """Close a position and update statistics"""
        
        if position_id not in self.positions:
            return
        
        position = self.positions[position_id]
        position.close_position(exit_price, datetime.now(timezone.utc), reason)
        
        # Update portfolio
        self.current_capital += position.realized_pnl
        del self.positions[position_id]
        
        # Update regime tracking
        regime_key = position.regime_at_entry
        if regime_key in self.regime_positions:
            self.regime_positions[regime_key].remove(position_id)
        
        # Update statistics
        self.total_trades += 1
        if position.realized_pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        
        # Update regime performance
        if regime_key in self.regime_performance:
            perf = self.regime_performance[regime_key]
            perf['trades'] += 1
            perf['total_pnl'] += position.realized_pnl
            
            if position.realized_pnl > 0:
                perf['wins'] += 1
            else:
                perf['losses'] += 1
            
            perf['win_rate'] = perf['wins'] / perf['trades'] if perf['trades'] > 0 else 0
        
        # Store in closed positions
        self.closed_positions.append(position)
        
        logger.info(f"Closed {position.ticker} {position.direction.value}: {position.realized_pnl:+.2f} ({reason})")
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get comprehensive portfolio summary"""
        
        # Calculate current P&L
        unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        total_pnl = sum(pos.realized_pnl for pos in self.closed_positions) + unrealized_pnl
        
        # Current exposure
        current_exposure = sum(abs(pos.quantity) for pos in self.positions.values())
        portfolio_heat = current_exposure / self.current_capital if self.current_capital > 0 else 0
        
        # Performance metrics
        win_rate = self.winning_trades / self.total_trades if self.total_trades > 0 else 0
        
        return {
            'capital': {
                'initial': self.initial_capital,
                'current': self.current_capital,
                'total_pnl': total_pnl,
                'pnl_percentage': (total_pnl / self.initial_capital) * 100
            },
            'positions': {
                'open_count': len(self.positions),
                'closed_count': len(self.closed_positions),
                'current_exposure': current_exposure,
                'portfolio_heat': portfolio_heat
            },
            'performance': {
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'losing_trades': self.losing_trades,
                'win_rate': win_rate,
                'avg_trade_pnl': total_pnl / self.total_trades if self.total_trades > 0 else 0
            },
            'regime_performance': self.regime_performance,
            'risk_parameters': {
                'base_risk_per_trade': self.risk_params.base_risk_per_trade,
                'max_portfolio_heat': self.risk_params.max_portfolio_heat,
                'max_concurrent_positions': self.risk_params.max_concurrent_positions
            }
        }
    
    def get_entry_location_stats(self) -> Dict[str, Any]:
        """Get entry location statistics for analysis"""
        
        bull_expansion_entries = []
        bear_expansion_entries = []
        
        for pos in self.closed_positions:
            if pos.regime_at_entry == "(BULL, EXPANSION)":
                bull_expansion_entries.append(pos.position_in_range_at_entry)
            elif pos.regime_at_entry == "(BEAR, EXPANSION)":
                bear_expansion_entries.append(pos.position_in_range_at_entry)
        
        return {
            'bull_expansion': {
                'count': len(bull_expansion_entries),
                'avg_position_in_range': np.mean(bull_expansion_entries) if bull_expansion_entries else 0,
                'entries': bull_expansion_entries
            },
            'bear_expansion': {
                'count': len(bear_expansion_entries),
                'avg_position_in_range': np.mean(bear_expansion_entries) if bear_expansion_entries else 0,
                'entries': bear_expansion_entries
            }
        }
