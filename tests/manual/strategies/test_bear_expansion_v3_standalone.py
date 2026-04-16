"""
Test Bear Expansion Strategy V3 - Standalone Version

Key improvements:
1. Trade quality filters (ONE additional constraint)
2. Anti-clustering controls  
3. Reduced aggressiveness
4. Smoother equity curve focus
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import uuid
import logging
from dataclasses import dataclass

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrendRegime(Enum):
    BULL = "BULL"
    BEAR = "BEAR"
    CHOP = "CHOP"


class VolatilityRegime(Enum):
    EXPANSION = "EXPANSION"
    NORMAL = "NORMAL"
    COMPRESSION = "COMPRESSION"


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
    
    def is_bear_expansion(self, trend_regime: TrendRegime, volatility_regime: VolatilityRegime) -> bool:
        """Single-regime filter: ONLY (BEAR, EXPANSION)"""
        return (
            trend_regime == TrendRegime.BEAR and 
            volatility_regime == VolatilityRegime.EXPANSION
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
        trend_regime: TrendRegime,
        volatility_regime: VolatilityRegime,
        position_in_range: float,
        ticker: str,
        current_price: float,
        current_atr: float,
        current_date: datetime
    ) -> Tuple[bool, str]:
        """
        Entry criteria with ONE additional confirmation constraint.
        """
        
        if not self.is_bear_expansion(trend_regime, volatility_regime):
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
        trend_regime: TrendRegime,
        volatility_regime: VolatilityRegime,
        atr: float,
        position_in_range: float,
        entry_time: Optional[datetime] = None
    ) -> Optional[str]:
        """
        Enter a bear expansion trade with quality controls.
        """
        
        current_time = entry_time or datetime.now()
        
        # Check entry criteria
        should_enter, confirmation_reason = self.should_enter_trade(
            trend_regime, volatility_regime, position_in_range, ticker, entry_price, atr, current_time
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


def get_historical_data():
    """Get historical data with technical indicators"""
    
    print("Loading historical data...")
    
    conn = sqlite3.connect("data/alpha.db")
    
    # Get raw price data
    query = """
    SELECT 
        ticker, 
        date, 
        close,
        volume,
        open,
        high,
        low
    FROM price_data 
    ORDER BY ticker, date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if len(df) == 0:
        print("ERROR: No historical data found")
        return None
    
    df['date'] = pd.to_datetime(df['date'])
    
    # Calculate technical indicators for each ticker
    result_dfs = []
    
    for ticker in df['ticker'].unique():
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        if len(ticker_data) < 200:
            continue
        
        # Calculate moving averages
        ticker_data['ma50'] = ticker_data['close'].rolling(window=50, min_periods=50).mean()
        ticker_data['ma200'] = ticker_data['close'].rolling(window=200, min_periods=200).mean()
        
        # Calculate ATR
        ticker_data['prev_close'] = ticker_data['close'].shift(1)
        ticker_data['tr'] = np.maximum.reduce([
            ticker_data['high'] - ticker_data['low'],
            np.abs(ticker_data['high'] - ticker_data['prev_close']),
            np.abs(ticker_data['low'] - ticker_data['prev_close'])
        ])
        ticker_data['atr'] = ticker_data['tr'].rolling(window=14, min_periods=14).mean()
        
        # Calculate 20-day high/low for position in range
        ticker_data['high_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).max()
        ticker_data['low_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).min()
        
        result_dfs.append(ticker_data)
    
    if not result_dfs:
        print("ERROR: No data after processing")
        return None
    
    # Combine all data
    combined_df = pd.concat(result_dfs, ignore_index=True)
    
    # Filter out rows with missing indicators
    combined_df = combined_df.dropna(subset=['ma50', 'ma200', 'atr'])
    
    print(f"Prepared {len(combined_df)} rows of data with indicators")
    
    return combined_df


def test_bear_expansion_v3(df, confirmation_type="volatility_timing"):
    """Test quality-focused bear expansion strategy"""
    
    print(f"\n=== BEAR EXPANSION V3 TEST - {confirmation_type.upper()} ===")
    print("Quality over quantity: Anti-clustering + reduced aggressiveness")
    
    # Initialize trader with quality-focused config
    config = BearExpansionConfigV3(
        min_position_in_range=0.25,         # Shifted to 0.25-0.35
        max_position_in_range=0.35,
        base_risk_per_trade=0.01,          # 1% risk per trade (reduced)
        atr_stop_multiplier=1.25,           # Tighter stop
        atr_target_multiplier=2.5,          # Faster target
        max_concurrent_positions=2,         # Reduced from 3
        max_portfolio_heat=0.08,            # Reduced to 8%
        max_positions_per_sector=1,         # Prevent sector clustering
        max_hold_days=7,                    # Reduced hold period
        
        # ENTRY CONFIRMATION (choose ONE)
        use_volatility_timing=(confirmation_type == "volatility_timing"),
        volatility_lookback_days=5,
        min_atr_increase_pct=0.05,
        
        use_breakout_confirmation=(confirmation_type == "breakout_confirmation"),
        breakout_lookback_days=10,
        
        use_delayed_entry=(confirmation_type == "delayed_entry"),
        entry_delay_days=1
    )
    
    trader = BearExpansionTraderV3(initial_capital=100000.0, config=config)
    
    # Simulate trading day by day
    trading_days = sorted(df['date'].unique())
    
    print(f"Simulating {len(trading_days)} trading days...")
    
    for day_idx, current_date in enumerate(trading_days):
        if day_idx % 100 == 0:
            print(f"  Processing day {day_idx + 1}/{len(trading_days)}: {current_date.strftime('%Y-%m-%d')}")
        
        # Get current day's data
        day_data = df[df['date'] == current_date]
        
        # Update price history for confirmations
        for _, row in day_data.iterrows():
            trader.update_price_history(row['ticker'], current_date, row['close'], row['atr'])
        
        # Update existing positions
        market_data = {}
        for _, row in day_data.iterrows():
            market_data[row['ticker']] = {
                'price': row['close'],
                'atr': row['atr']
            }
        
        trader.update_positions(market_data, current_date)
        
        # Look for new trade opportunities
        for _, row in day_data.iterrows():
            ticker = row['ticker']
            
            # Skip if already in position
            if any(pos.ticker == ticker for pos in trader.positions.values()):
                continue
            
            # Calculate regime
            try:
                # Get historical ATR for regime calculation
                ticker_history = df[df['ticker'] == ticker]
                current_idx = ticker_history[ticker_history['date'] == current_date].index[0]
                
                if current_idx < 200:
                    continue
                
                historical_atr = ticker_history.iloc[:current_idx]['atr'].dropna().tolist()
                
                if len(historical_atr) < 20:
                    continue
                
                # Calculate regime
                price_vs_ma50 = (row['close'] - row['ma50']) / row['ma50']
                ma50_vs_ma200 = (row['ma50'] - row['ma200']) / row['ma200']
                
                # Trend regime
                if price_vs_ma50 > 0.02 and ma50_vs_ma200 > 0.02:
                    trend_regime = TrendRegime.BULL
                elif price_vs_ma50 < -0.02 and ma50_vs_ma200 < -0.02:
                    trend_regime = TrendRegime.BEAR
                else:
                    trend_regime = TrendRegime.CHOP
                
                # Volatility regime
                atr_percentile = sum(1 for x in historical_atr if x <= row['atr']) / len(historical_atr)
                
                if atr_percentile >= 0.8:
                    volatility_regime = VolatilityRegime.EXPANSION
                elif atr_percentile <= 0.2:
                    volatility_regime = VolatilityRegime.COMPRESSION
                else:
                    volatility_regime = VolatilityRegime.NORMAL
                
                # Calculate position in range
                position_in_range = (row['close'] - row['low_20d']) / (row['high_20d'] - row['low_20d'])
                
                # Enter trade (only if bear expansion criteria met)
                trader.enter_trade(
                    ticker=ticker,
                    entry_price=row['close'],
                    trend_regime=trend_regime,
                    volatility_regime=volatility_regime,
                    atr=row['atr'],
                    position_in_range=position_in_range,
                    entry_time=current_date
                )
                
            except Exception as e:
                continue
    
    # Get final results
    summary = trader.get_performance_summary()
    trade_details = trader.get_trade_details()
    
    # Print results
    print(f"\n=== BEAR EXPANSION V3 RESULTS ({confirmation_type.upper()}) ===")
    
    print(f"\nPortfolio Performance:")
    print(f"  Initial Capital: ${summary['capital']['initial']:,.0f}")
    print(f"  Final Capital: ${summary['capital']['current']:,.0f}")
    print(f"  Total P&L: ${summary['capital']['total_pnl']:,.0f} ({summary['capital']['pnl_percentage']:+.1f}%)")
    print(f"  Total Trades: {summary['performance']['total_trades']}")
    print(f"  Win Rate: {summary['performance']['win_rate']:.1%}")
    print(f"  Avg P&L per Trade: ${summary['performance']['avg_trade_pnl']:+.2f}")
    print(f"  Max Drawdown: {summary['performance']['max_drawdown']:.1f}%")
    print(f"  Sharpe Ratio: {summary['performance']['sharpe_ratio']:.2f}")
    print(f"  Profit Factor: {summary['performance']['profit_factor']:.2f}")
    print(f"  Largest Win: ${summary['performance']['largest_win']:+,.0f}")
    print(f"  Largest Loss: ${summary['performance']['largest_loss']:+,.0f}")
    
    print(f"\nEntry Statistics:")
    print(f"  Entry Attempts: {summary['entry_stats']['entry_attempts']}")
    print(f"  Entry Rejections: {summary['entry_stats']['entry_rejections']}")
    print(f"  Acceptance Rate: {summary['entry_stats']['acceptance_rate']:.1%}")
    print(f"  Entry Band: {summary['risk_metrics']['entry_band']}")
    
    print(f"\nRisk Management:")
    print(f"  Base Risk per Trade: {summary['risk_metrics']['base_risk_per_trade']:.1%}")
    print(f"  Max Concurrent Positions: {summary['risk_metrics']['max_concurrent_positions']}")
    print(f"  Max Portfolio Heat: {summary['risk_metrics']['max_portfolio_heat']:.1%}")
    print(f"  Stop Multiplier: {summary['risk_metrics']['stop_multiplier']:.2f}x ATR")
    print(f"  Target Multiplier: {summary['risk_metrics']['target_multiplier']:.2f}x ATR")
    
    print(f"\nSector Distribution:")
    for sector, count in summary['sector_stats'].items():
        print(f"  {sector}: {count} positions")
    
    # Trade analysis
    if trade_details:
        trades_df = pd.DataFrame(trade_details)
        
        print(f"\nTrade Analysis:")
        print(f"  Average Position in Range: {trades_df['position_in_range'].mean():.1%}")
        print(f"  Average Hold Days: {trades_df['hold_days'].mean():.1f}")
        print(f"  Exit Reasons:")
        for reason, count in trades_df['exit_reason'].value_counts().items():
            print(f"    {reason}: {count}")
        
        # Performance by sector
        print(f"\nPerformance by Sector:")
        for sector in trades_df['sector'].unique():
            sector_trades = trades_df[trades_df['sector'] == sector]
            if len(sector_trades) > 0:
                win_rate = sector_trades['win'].mean()
                avg_pnl = sector_trades['realized_pnl'].mean()
                print(f"  {sector}: {len(sector_trades)} trades, {win_rate:.1%} win rate, {avg_pnl:+.2f} avg P&L")
        
        # Trade quality analysis
        print(f"\nTrade Quality Analysis:")
        winning_trades = trades_df[trades_df['win']]
        losing_trades = trades_df[~trades_df['win']]
        
        if len(winning_trades) > 0:
            print(f"  Winning Trades: {len(winning_trades)}")
            print(f"    Avg Win: ${winning_trades['realized_pnl'].mean():+,.0f}")
            print(f"    Avg Hold: {winning_trades['hold_days'].mean():.1f} days")
        
        if len(losing_trades) > 0:
            print(f"  Losing Trades: {len(losing_trades)}")
            print(f"    Avg Loss: ${losing_trades['realized_pnl'].mean():+,.0f}")
            print(f"    Avg Hold: {losing_trades['hold_days'].mean():.1f} days")
    
    # Assessment
    print(f"\n=== ASSESSMENT ===")
    
    total_return = summary['capital']['pnl_percentage']
    win_rate = summary['performance']['win_rate']
    sharpe = summary['performance']['sharpe_ratio']
    max_dd = summary['performance']['max_drawdown']
    profit_factor = summary['performance']['profit_factor']
    
    # Performance rating
    if total_return > 10:
        perf_rating = "EXCELLENT"
    elif total_return > 5:
        perf_rating = "GOOD"
    elif total_return > 0:
        perf_rating = "POSITIVE"
    else:
        perf_rating = "NEGATIVE"
    
    # Risk rating
    if max_dd < 10:
        risk_rating = "LOW"
    elif max_dd < 15:
        risk_rating = "MODERATE"
    elif max_dd < 20:
        risk_rating = "HIGH"
    else:
        risk_rating = "VERY HIGH"
    
    # Quality rating
    if profit_factor > 2.0:
        quality_rating = "EXCELLENT"
    elif profit_factor > 1.5:
        quality_rating = "GOOD"
    elif profit_factor > 1.0:
        quality_rating = "ACCEPTABLE"
    else:
        quality_rating = "POOR"
    
    # Overall rating
    if total_return > 0 and max_dd < 15 and sharpe > 0.5 and profit_factor > 1.2:
        overall_rating = "TRADEABLE EDGE"
    elif total_return > 0 and max_dd < 20:
        overall_rating = "PROMISING"
    else:
        overall_rating = "NEEDS WORK"
    
    print(f"  Performance: {perf_rating} ({total_return:+.1f}% return)")
    print(f"  Risk: {risk_rating} ({max_dd:.1f}% max drawdown)")
    print(f"  Quality: {quality_rating} (profit factor: {profit_factor:.2f})")
    print(f"  Win Rate: {win_rate:.1%}")
    print(f"  Sharpe: {sharpe:.2f}")
    print(f"  Overall: {overall_rating}")
    
    # Compare to V2
    print(f"\n=== V2 vs V3 COMPARISON ===")
    v2_return = 2.6  # From previous test
    v2_dd = 20.7
    v2_sharpe = 0.84
    v2_trades = 38
    
    print(f"  V2: {v2_return:+.1f}% return, {v2_dd:.1f}% DD, {v2_sharpe:.2f} Sharpe, {v2_trades} trades")
    print(f"  V3: {total_return:+.1f}% return, {max_dd:.1f}% DD, {sharpe:.2f} Sharpe, {summary['performance']['total_trades']} trades")
    print(f"  Improvement: {total_return - v2_return:+.1f}% return, {v2_dd - max_dd:+.1f}% DD reduction")
    
    # Compare to baseline
    print(f"\n=== BASELINE COMPARISON ===")
    
    if len(df) > 0:
        first_price = df.groupby('ticker')['close'].first().mean()
        last_price = df.groupby('ticker')['close'].last().mean()
        baseline_return = ((last_price - first_price) / first_price) * 100
        
        print(f"  Buy-and-Hold Return: {baseline_return:+.1f}%")
        print(f"  Bear Expansion V3 Return: {total_return:+.1f}%")
        print(f"  Alpha: {total_return - baseline_return:+.1f}%")
    
    return {
        'summary': summary,
        'trade_details': trade_details,
        'total_return': total_return,
        'win_rate': win_rate,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'overall_rating': overall_rating,
        'confirmation_type': confirmation_type
    }


def main():
    """Main test function"""
    
    print("Bear Expansion Strategy V3 Test")
    print("Quality over quantity: Anti-clustering + reduced aggressiveness\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Test all three confirmation types
    confirmation_types = ["volatility_timing", "breakout_confirmation", "delayed_entry"]
    results = []
    
    for confirmation_type in confirmation_types:
        print(f"\n{'='*60}")
        print(f"Testing {confirmation_type.upper()} confirmation")
        print(f"{'='*60}")
        
        result = test_bear_expansion_v3(df, confirmation_type)
        results.append(result)
    
    # Compare results
    print(f"\n{'='*60}")
    print("CONFIRMATION TYPE COMPARISON")
    print(f"{'='*60}")
    
    print(f"\n{'Confirmation Type':<20} {'Return':<10} {'DD':<8} {'Sharpe':<8} {'Trades':<8} {'Win Rate':<10} {'Rating':<15}")
    print("-" * 80)
    
    for result in results:
        print(f"{result['confirmation_type']:<20} "
              f"{result['total_return']:+.1f}%{'':<6} "
              f"{result['max_drawdown']:.1f}%{'':<5} "
              f"{result['sharpe']:.2f}{'':<5} "
              f"{result['summary']['performance']['total_trades']:<8} "
              f"{result['win_rate']:.1%}{'':<4} "
              f"{result['overall_rating']:<15}")
    
    # Find best performer
    best_result = max(results, key=lambda x: (
        x['total_return'] > 0,  # Must be positive
        x['max_drawdown'] < 20,  # Reasonable drawdown
        x['sharpe']  # Higher Sharpe
    ))
    
    print(f"\n=== BEST PERFORMER ===")
    print(f"Confirmation Type: {best_result['confirmation_type'].upper()}")
    print(f"Return: {best_result['total_return']:+.1f}%")
    print(f"Drawdown: {best_result['max_drawdown']:.1f}%")
    print(f"Sharpe: {best_result['sharpe']:.2f}")
    print(f"Trades: {best_result['summary']['performance']['total_trades']}")
    print(f"Win Rate: {best_result['win_rate']:.1%}")
    print(f"Overall Rating: {best_result['overall_rating']}")
    
    return results


if __name__ == "__main__":
    results = main()
