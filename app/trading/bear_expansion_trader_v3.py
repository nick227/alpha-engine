"""
Bear Expansion Strategy V3 - Quality Over Quantity

Key improvements:
1. Trade quality filters (ONE additional constraint)
2. Anti-clustering controls
3. Reduced aggressiveness
4. Smoother equity curve focus
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import uuid
import logging
import numpy as np
from datetime import datetime, timezone, timedelta

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
    volatility_confirmation: bool = False
    breakout_confirmation: bool = False
    
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
class BearExpansionConfigV3:
    """Conservative configuration for bear expansion strategy"""
    
    # PRECISION ENTRY BAND
    min_position_in_range: float = 0.25   # Shifted to 0.25-0.35 based on sweep
    max_position_in_range: float = 0.35
    
    # CONSERVATIVE RISK MANAGEMENT
    base_risk_per_trade: float = 0.01     # 1% risk per trade (reduced)
    atr_stop_multiplier: float = 1.25     # Tighter stop
    atr_target_multiplier: float = 2.5   # Faster target
    
    # ANTI-CLUSTERING CONTROLS
    max_concurrent_positions: int = 2     # Reduced from 3
    max_portfolio_heat: float = 0.08      # Reduced to 8%
    
    # ENTRY CONFIRMATION (Option A: Volatility timing)
    use_volatility_timing: bool = True
    volatility_lookback_days: int = 5
    min_atr_increase_pct: float = 0.05    # 5% increase over lookback
    
    # ENTRY CONFIRMATION (Option B: Break confirmation)
    use_breakout_confirmation: bool = False
    breakout_lookback_days: int = 10
    
    # ENTRY CONFIRMATION (Option C: Delay entry)
    use_delayed_entry: bool = False
    entry_delay_days: int = 1
    
    # Correlation control
    max_positions_per_sector: int = 1
    
    # Exit options
    use_trailing_stop: bool = False
    trailing_stop_atr: float = 1.25
    max_hold_days: int = 7                # Reduced hold period


class BearExpansionTraderV3:
    """
    Bear Expansion Strategy V3 - Quality Over Quantity
    
    Key changes:
    - ONE additional entry confirmation constraint
    - Anti-clustering controls
    - Reduced aggressiveness
    - Focus on smoother equity curve
    """
    
    def __init__(self, initial_capital: float = 100000.0, config: Optional[BearExpansionConfigV3] = None):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.config = config or BearExpansionConfigV3()
        
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
        
        # Sector tracking
        self.sector_positions: Dict[str, List[str]] = {}
        
        # Historical data for confirmations
        self.price_history: Dict[str, List[Tuple[datetime, float, float]]] = {}  # (date, price, atr)
        
        logger.info(f"Bear Expansion Trader V3 initialized with ${initial_capital:,.0f}")
    
    def is_bear_expansion(self, regime: RegimeClassification) -> bool:
        """Single-regime filter: ONLY (BEAR, EXPANSION)"""
        return (
            regime.trend_regime == TrendRegime.BEAR and 
            regime.volatility_regime == VolatilityRegime.EXPANSION
        )
    
    def check_volatility_timing(self, ticker: str, current_atr: float, current_date: datetime) -> bool:
        """
        Option A: Volatility timing confirmation
        Enter only if ATR is rising vs last N days
        """
        
        if not self.config.use_volatility_timing:
            return True
        
        if ticker not in self.price_history:
            return False
        
        # Get recent ATR values
        recent_data = [
            (date, atr) for date, price, atr in self.price_history[ticker]
            if (current_date - date).days <= self.config.volatility_lookback_days
        ]
        
        if len(recent_data) < 3:
            return False
        
        # Calculate average ATR over lookback period
        avg_atr = np.mean([atr for date, atr in recent_data])
        
        # Check if current ATR is significantly higher
        return current_atr > avg_atr * (1 + self.config.min_atr_increase_pct)
    
    def check_breakout_confirmation(self, ticker: str, current_price: float, current_date: datetime) -> bool:
        """
        Option B: Break confirmation
        Enter only if today breaks previous N-day low
        """
        
        if not self.config.use_breakout_confirmation:
            return True
        
        if ticker not in self.price_history:
            return False
        
        # Get recent price data
        recent_data = [
            (date, price) for date, price, atr in self.price_history[ticker]
            if (current_date - date).days <= self.config.breakout_lookback_days
        ]
        
        if len(recent_data) < self.config.breakout_lookback_days:
            return False
        
        # Find lowest price in lookback period
        lowest_price = min(price for date, price in recent_data)
        
        # Check if current price breaks below recent low
        return current_price < lowest_price
    
    def check_delayed_entry(self, ticker: str, current_date: datetime) -> bool:
        """
        Option C: Delay entry
        Enter 1 day AFTER breakdown
        """
        
        if not self.config.use_delayed_entry:
            return True
        
        # This would require tracking breakdown signals
        # For now, return True (will implement if chosen)
        return True
    
    def should_enter_trade(
        self, 
        regime: RegimeClassification, 
        position_in_range: float,
        ticker: str,
        current_price: float,
        current_atr: float,
        current_date: datetime
    ) -> Tuple[bool, str]:
        """
        Entry criteria with ONE additional confirmation constraint.
        """
        
        if not self.is_bear_expansion(regime):
            self.entry_rejections += 1
            return False, "not_bear_expansion"
        
        # PRECISION BAND FILTER
        if not (self.config.min_position_in_range <= position_in_range <= self.config.max_position_in_range):
            self.entry_rejections += 1
            return False, "position_out_of_range"
        
        # ENTRY CONFIRMATION (choose ONE)
        confirmation_passed = False
        confirmation_type = ""
        
        if self.config.use_volatility_timing:
            confirmation_passed = self.check_volatility_timing(ticker, current_atr, current_date)
            confirmation_type = "volatility_timing"
        
        elif self.config.use_breakout_confirmation:
            confirmation_passed = self.check_breakout_confirmation(ticker, current_price, current_date)
            confirmation_type = "breakout_confirmation"
        
        elif self.config.use_delayed_entry:
            confirmation_passed = self.check_delayed_entry(ticker, current_date)
            confirmation_type = "delayed_entry"
        
        if not confirmation_passed:
            self.entry_rejections += 1
            return False, f"confirmation_failed_{confirmation_type}"
        
        self.entry_attempts += 1
        return True, confirmation_type
    
    def get_ticker_sector(self, ticker: str) -> str:
        """Simple sector classification based on ticker patterns"""
        
        # Tech sector
        if ticker in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'ADBE', 'CRM']:
            return 'TECH'
        
        # Financial sector
        elif ticker in ['JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'AIG']:
            return 'FINANCIAL'
        
        # Healthcare sector
        elif ticker in ['JNJ', 'PFE', 'UNH', 'ABT', 'MRK', 'CVS', 'MDT']:
            return 'HEALTHCARE'
        
        # Consumer sector
        elif ticker in ['WMT', 'HD', 'MCD', 'NKE', 'KO', 'PEP', 'COST']:
            return 'CONSUMER'
        
        # Industrial sector
        elif ticker in ['BA', 'CAT', 'GE', 'MMM', 'UPS', 'HON']:
            return 'INDUSTRIAL'
        
        # Energy sector
        elif ticker in ['XOM', 'CVX', 'COP', 'SLB', 'HAL']:
            return 'ENERGY'
        
        # Telecom sector
        elif ticker in ['T', 'VZ', 'TMUS']:
            return 'TELECOM'
        
        # Default
        else:
            return 'OTHER'
    
    def calculate_position_size(self, atr: float) -> float:
        """
        Calculate position size with conservative risk management.
        """
        
        # Base position from reduced risk per trade
        base_size = (self.current_capital * self.config.base_risk_per_trade) / (atr * self.config.atr_stop_multiplier)
        
        # Portfolio heat check (reduced)
        current_exposure = sum(abs(pos.quantity) for pos in self.positions.values())
        available_heat = (self.current_capital * self.config.max_portfolio_heat) - current_exposure
        
        if base_size > available_heat:
            base_size = available_heat
        
        return max(0, base_size)
    
    def update_price_history(self, ticker: str, date: datetime, price: float, atr: float):
        """Update price history for confirmation checks"""
        
        if ticker not in self.price_history:
            self.price_history[ticker] = []
        
        # Add new data point
        self.price_history[ticker].append((date, price, atr))
        
        # Keep only recent data (last 30 days)
        cutoff_date = date - timedelta(days=30)
        self.price_history[ticker] = [
            (d, p, a) for d, p, a in self.price_history[ticker]
            if d > cutoff_date
        ]
    
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
        Enter a bear expansion trade with quality controls.
        """
        
        current_time = entry_time or datetime.now(timezone.utc)
        
        # Check entry criteria
        should_enter, confirmation_reason = self.should_enter_trade(
            regime, position_in_range, ticker, entry_price, atr, current_time
        )
        
        if not should_enter:
            return None
        
        # Check portfolio constraints (anti-clustering)
        if len(self.positions) >= self.config.max_concurrent_positions:
            return None
        
        # Check sector clustering
        sector = self.get_ticker_sector(ticker)
        sector_positions = self.sector_positions.get(sector, [])
        if len(sector_positions) >= self.config.max_positions_per_sector:
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
            entry_time=current_time,
            quantity=position_size,
            stop_loss=stop_loss,
            target_price=target_price,
            atr_at_entry=atr,
            position_in_range_at_entry=position_in_range,
            status=TradeStatus.EXECUTED
        )
        
        # Update portfolio
        self.positions[position_id] = position
        
        # Track sector positions
        if sector not in self.sector_positions:
            self.sector_positions[sector] = []
        self.sector_positions[sector].append(position_id)
        
        logger.info(f"QUALITY ENTRY: {ticker} SHORT at {entry_price:.2f}, range_pos: {position_in_range:.1%}, "
                   f"sector: {sector}, stop: {stop_loss:.2f}, confirmation: {confirmation_reason}")
        
        return position_id
    
    def update_positions(self, market_data: Dict[str, Dict[str, Any]], current_date: datetime):
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
        
        # Close positions
        for position_id, exit_price, reason in positions_to_close:
            self.close_position(position_id, exit_price, current_date, reason)
    
    def close_position(self, position_id: str, exit_price: float, exit_time: datetime, reason: str):
        """Close a position and update statistics"""
        
        if position_id not in self.positions:
            return
        
        position = self.positions[position_id]
        position.close_position(exit_price, exit_time, reason)
        
        # Update portfolio
        self.current_capital += position.realized_pnl
        del self.positions[position_id]
        
        # Update sector tracking
        sector = self.get_ticker_sector(position.ticker)
        if sector in self.sector_positions:
            self.sector_positions[sector] = [
                pos_id for pos_id in self.sector_positions[sector] 
                if pos_id != position_id
            ]
        
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
        
        # Calculate trade quality metrics
        trade_pnls = [pos.realized_pnl for pos in self.closed_positions]
        if trade_pnls:
            profit_factor = sum(pnl for pnl in trade_pnls if pnl > 0) / abs(sum(pnl for pnl in trade_pnls if pnl < 0))
            largest_win = max(trade_pnls)
            largest_loss = min(trade_pnls)
        else:
            profit_factor = 0
            largest_win = 0
            largest_loss = 0
        
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
                'sharpe_ratio': sharpe,
                'profit_factor': profit_factor,
                'largest_win': largest_win,
                'largest_loss': largest_loss
            },
            'entry_stats': {
                'entry_attempts': self.entry_attempts,
                'entry_rejections': self.entry_rejections,
                'acceptance_rate': self.entry_attempts / (self.entry_attempts + self.entry_rejections) if (self.entry_attempts + self.entry_rejections) > 0 else 0
            },
            'risk_metrics': {
                'base_risk_per_trade': self.config.base_risk_per_trade,
                'max_concurrent_positions': self.config.max_concurrent_positions,
                'max_portfolio_heat': self.config.max_portfolio_heat,
                'entry_band': f"{self.config.min_position_in_range:.1f}-{self.config.max_position_in_range:.1f}",
                'stop_multiplier': self.config.atr_stop_multiplier,
                'target_multiplier': self.config.atr_target_multiplier
            },
            'sector_stats': {
                sector: len(positions) for sector, positions in self.sector_positions.items()
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
                'exit_reason': pos.exit_reason,
                'sector': self.get_ticker_sector(pos.ticker)
            })
        
        return trades
