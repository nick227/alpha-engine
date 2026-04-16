"""
Bear Expansion Single-Regime Strategy

Isolated strategy focusing ONLY on the validated edge:
(BEAR, EXPANSION) + position_in_range < 0.4

This is the first validated edge - no blending, no portfolio.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import uuid
import logging
import numpy as np
from datetime import datetime, timezone

from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime

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
    """Position tracking for bear expansion strategy"""
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
    position_in_range_at_entry: float = 0.0
    
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
class BearExpansionConfig:
    """Configuration for bear expansion strategy"""
    
    # Entry criteria
    max_position_in_range: float = 0.4  # Enter below 40% of range
    
    # Risk management
    base_risk_per_trade: float = 0.02  # 2% risk per trade
    atr_stop_multiplier: float = 2.0    # Stop at 2x ATR
    atr_target_multiplier: float = 3.0  # Target at 3x ATR
    
    # Portfolio constraints
    max_concurrent_positions: int = 5
    max_portfolio_heat: float = 0.15   # 15% max exposure
    
    # Exit options
    use_trailing_stop: bool = False
    trailing_stop_atr: float = 1.5     # Trail stop at 1.5x ATR
    max_hold_days: int = 10             # Max hold period


class BearExpansionTrader:
    """
    Single-Regime Strategy: Bear Expansion Only
    
    Focus on the validated edge without noise from other regimes.
    """
    
    def __init__(self, initial_capital: float = 100000.0, config: Optional[BearExpansionConfig] = None):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.config = config or BearExpansionConfig()
        
        # Portfolio state
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []
        
        # Performance tracking
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        
        # Entry tracking
        self.entry_attempts = 0
        self.entry_rejections = 0
        
        logger.info(f"Bear Expansion Trader initialized with ${initial_capital:,.0f}")
    
    def is_bear_expansion(self, regime: RegimeClassification) -> bool:
        """
        Single-regime filter: ONLY (BEAR, EXPANSION)
        """
        return (
            regime.trend_regime == TrendRegime.BEAR and 
            regime.volatility_regime == VolatilityRegime.EXPANSION
        )
    
    def should_enter_trade(
        self, 
        regime: RegimeClassification, 
        position_in_range: float
    ) -> bool:
        """
        Entry criteria for bear expansion strategy.
        
        Enter on breakdowns: position_in_range < 0.4
        """
        
        if not self.is_bear_expansion(regime):
            self.entry_rejections += 1
            return False
        
        if position_in_range >= self.config.max_position_in_range:
            self.entry_rejections += 1
            return False
        
        self.entry_attempts += 1
        return True
    
    def calculate_position_size(self, atr: float) -> float:
        """
        Calculate position size based on ATR and risk parameters.
        """
        
        # Base position from risk per trade and ATR
        base_size = (self.current_capital * self.config.base_risk_per_trade) / (atr * self.config.atr_stop_multiplier)
        
        # Portfolio heat check
        current_exposure = sum(abs(pos.quantity) for pos in self.positions.values())
        available_heat = (self.current_capital * self.config.max_portfolio_heat) - current_exposure
        
        if base_size > available_heat:
            base_size = available_heat
        
        return max(0, base_size)
    
    def enter_trade(
        self,
        ticker: str,
        entry_price: float,
        regime: RegimeClassification,
        atr: float,
        position_in_range: float,
        entry_time: Optional[datetime] = None
    ) -> Optional[str]:
        """
        Enter a bear expansion trade.
        """
        
        # Check entry criteria
        if not self.should_enter_trade(regime, position_in_range):
            return None
        
        # Check portfolio constraints
        if len(self.positions) >= self.config.max_concurrent_positions:
            return None
        
        # Calculate position size
        position_size = self.calculate_position_size(atr)
        
        if position_size <= 0:
            return None
        
        # Calculate stop loss and target (SHORT positions)
        stop_loss = entry_price + (atr * self.config.atr_stop_multiplier)
        target_price = entry_price - (atr * self.config.atr_target_multiplier)
        
        # Create position
        position_id = str(uuid.uuid4())
        position = Position(
            ticker=ticker,
            direction=TradeDirection.SHORT,
            entry_price=entry_price,
            entry_time=entry_time or datetime.now(timezone.utc),
            quantity=position_size,
            stop_loss=stop_loss,
            target_price=target_price,
            atr_at_entry=atr,
            position_in_range_at_entry=position_in_range,
            status=TradeStatus.EXECUTED
        )
        
        # Update portfolio
        self.positions[position_id] = position
        
        logger.info(f"BEAR EXPANSION: {ticker} SHORT at {entry_price:.2f}, range_pos: {position_in_range:.1%}, stop: {stop_loss:.2f}")
        
        return position_id
    
    def update_positions(self, market_data: Dict[str, Dict[str, Any]]):
        """
        Update positions and handle exits.
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
            
            # Check max hold period
            if position.entry_time:
                # Use current_date from market data context instead of now()
                # For now, skip max hold period check
                pass
        
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
        
        # Update statistics
        self.total_trades += 1
        if position.realized_pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        
        # Store in closed positions
        self.closed_positions.append(position)
        
        logger.info(f"Closed {position.ticker} SHORT: {position.realized_pnl:+.2f} ({reason})")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        
        # Calculate metrics
        total_pnl = sum(pos.realized_pnl for pos in self.closed_positions)
        win_rate = self.winning_trades / self.total_trades if self.total_trades > 0 else 0
        avg_trade_pnl = total_pnl / self.total_trades if self.total_trades > 0 else 0
        
        # Calculate drawdown
        equity_curve = []
        running_capital = self.initial_capital
        
        for pos in self.closed_positions:
            running_capital += pos.realized_pnl
            equity_curve.append(running_capital)
        
        max_drawdown = 0
        peak = self.initial_capital
        
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # Calculate Sharpe (simplified)
        if len(equity_curve) > 1:
            returns = np.diff(equity_curve) / equity_curve[:-1]
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
        else:
            sharpe = 0
        
        return {
            'capital': {
                'initial': self.initial_capital,
                'current': self.current_capital,
                'total_pnl': total_pnl,
                'pnl_percentage': (total_pnl / self.initial_capital) * 100
            },
            'performance': {
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'losing_trades': self.losing_trades,
                'win_rate': win_rate,
                'avg_trade_pnl': avg_trade_pnl,
                'max_drawdown': max_drawdown * 100,
                'sharpe_ratio': sharpe
            },
            'entry_stats': {
                'entry_attempts': self.entry_attempts,
                'entry_rejections': self.entry_rejections,
                'acceptance_rate': self.entry_attempts / (self.entry_attempts + self.entry_rejections) if (self.entry_attempts + self.entry_rejections) > 0 else 0
            },
            'risk_metrics': {
                'base_risk_per_trade': self.config.base_risk_per_trade,
                'max_concurrent_positions': self.config.max_concurrent_positions,
                'max_portfolio_heat': self.config.max_portfolio_heat
            }
        }
    
    def get_trade_details(self) -> List[Dict[str, Any]]:
        """Get detailed trade information for analysis"""
        
        trades = []
        
        for pos in self.closed_positions:
            trades.append({
                'ticker': pos.ticker,
                'entry_date': pos.entry_time.date(),
                'entry_price': pos.entry_price,
                'exit_date': pos.exit_time.date(),
                'exit_price': pos.exit_price,
                'realized_pnl': pos.realized_pnl,
                'win': pos.realized_pnl > 0,
                'hold_days': (pos.exit_time.date() - pos.entry_time.date()).days,
                'position_in_range': pos.position_in_range_at_entry,
                'atr_at_entry': pos.atr_at_entry,
                'exit_reason': pos.exit_reason
            })
        
        return trades
