"""
Edge Structure Analysis

Tests to determine what kind of edge we actually have:
1. Remove top trades test
2. Median trade test  
3. Time robustness test
4. Position cap experiment

Goal: Decide between concentration control vs convexity embrace.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EdgeStructureAnalyzer:
    """
    Analyze edge structure to determine strategy type.
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        self.base_trades = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for edge structure analysis...")
        
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
            return False
        
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
            
            # Calculate position in range
            ticker_data['position_in_range'] = (
                (ticker_data['close'] - ticker_data['low_20d']) / 
                (ticker_data['high_20d'] - ticker_data['low_20d'])
            )
            
            result_dfs.append(ticker_data)
        
        if not result_dfs:
            print("ERROR: No data after processing")
            return False
        
        # Combine all data
        self.df = pd.concat(result_dfs, ignore_index=True)
        
        # Filter out rows with missing indicators
        self.df = self.df.dropna(subset=['ma50', 'ma200', 'atr', 'position_in_range'])
        
        print(f"Prepared {len(self.df)} rows of data with indicators")
        
        return True
    
    def calculate_regimes(self):
        """Calculate regimes for all data"""
        
        print("Calculating regimes...")
        
        regime_data = []
        
        for ticker in self.df['ticker'].unique():
            ticker_data = self.df[self.df['ticker'] == ticker].copy()
            
            for idx, row in ticker_data.iterrows():
                if idx < 200:
                    continue
                
                # Get historical ATR for percentile calculation
                historical_atr = ticker_data.iloc[:idx]['atr'].dropna().tolist()
                
                if len(historical_atr) < 20:
                    continue
                
                # Calculate regime
                price_vs_ma50 = (row['close'] - row['ma50']) / row['ma50']
                ma50_vs_ma200 = (row['ma50'] - row['ma200']) / row['ma200']
                
                # Trend regime
                if price_vs_ma50 > 0.02 and ma50_vs_ma200 > 0.02:
                    trend_regime = "BULL"
                elif price_vs_ma50 < -0.02 and ma50_vs_ma200 < -0.02:
                    trend_regime = "BEAR"
                else:
                    trend_regime = "CHOP"
                
                # Volatility regime
                atr_percentile = sum(1 for x in historical_atr if x <= row['atr']) / len(historical_atr)
                
                if atr_percentile >= 0.8:
                    volatility_regime = "EXPANSION"
                elif atr_percentile <= 0.2:
                    volatility_regime = "COMPRESSION"
                else:
                    volatility_regime = "NORMAL"
                
                # Store regime data
                regime_data.append({
                    'ticker': ticker,
                    'date': row['date'],
                    'close': row['close'],
                    'high': row['high'],
                    'low': row['low'],
                    'atr': row['atr'],
                    'position_in_range': row['position_in_range'],
                    'trend_regime': trend_regime,
                    'volatility_regime': volatility_regime,
                    'is_bear_expansion': (
                        trend_regime == "BEAR" and 
                        volatility_regime == "EXPANSION"
                    )
                })
        
        self.regime_data = pd.DataFrame(regime_data)
        print(f"Calculated regimes for {len(self.regime_data)} data points")
        
        return True
    
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
    
    def check_volatility_timing(self, ticker: str, current_atr: float, current_date: datetime, 
                              price_history: Dict[str, List[Dict]], lookback_days: int = 5, 
                              min_increase_pct: float = 0.05) -> bool:
        """
        Check volatility timing confirmation.
        """
        
        if ticker not in price_history:
            return False
        
        # Get recent ATR values
        recent_data = [
            entry for entry in price_history[ticker]
            if (current_date - entry['date']).days <= lookback_days
        ]
        
        if len(recent_data) < 3:
            return False
        
        # Calculate average ATR over lookback period
        avg_atr = np.mean([entry['atr'] for entry in recent_data])
        
        # Check if current ATR is significantly higher
        return current_atr > avg_atr * (1 + min_increase_pct)
    
    def generate_base_trades(self):
        """Generate base trades with winning parameters"""
        
        print("Generating base trades with winning parameters...")
        
        # Use winning parameters from validation
        min_range = 0.30
        max_range = 0.40
        stop_multiplier = 1.25
        target_multiplier = 2.5
        max_positions = 2
        portfolio_heat = 0.08
        volatility_lookback = 5
        volatility_increase = 0.05
        
        # Filter for bear expansion entries
        entries = self.regime_data[self.regime_data['is_bear_expansion']].copy()
        
        # Apply position in range filter
        entries = entries[
            (entries['position_in_range'] >= min_range) &
            (entries['position_in_range'] <= max_range)
        ]
        
        # Sort by date
        entries = entries.sort_values('date')
        
        # Build price history for volatility timing
        price_history = {}
        for _, row in self.regime_data.iterrows():
            ticker = row['ticker']
            if ticker not in price_history:
                price_history[ticker] = []
            
            price_history[ticker].append({
                'date': row['date'],
                'price': row['close'],
                'atr': row['atr']
            })
            
            # Keep only recent data (last 30 days)
            cutoff_date = row['date'] - pd.Timedelta(days=30)
            price_history[ticker] = [
                entry for entry in price_history[ticker]
                if entry['date'] > cutoff_date
            ]
        
        # Simulation variables
        capital = 100000.0
        positions = {}
        trades = []
        
        # Risk parameters
        risk_per_trade = portfolio_heat / max_positions
        
        # Simulate day by day
        trading_days = sorted(self.regime_data['date'].unique())
        
        for current_date in trading_days:
            # Get current day's data
            day_data = self.regime_data[self.regime_data['date'] == current_date]
            
            # Update existing positions
            positions_to_close = []
            
            for pos_id, position in positions.items():
                ticker = position['ticker']
                
                # Get current price
                current_price_data = day_data[day_data['ticker'] == ticker]
                if len(current_price_data) == 0:
                    continue
                
                current_price = current_price_data.iloc[0]['close']
                high_price = current_price_data.iloc[0]['high']
                low_price = current_price_data.iloc[0]['low']
                
                # Check exit conditions
                entry_price = position['entry_price']
                stop_loss = position['stop_loss']
                target_price = position['target_price']
                
                # For short positions
                should_exit = False
                exit_reason = ""
                exit_price = current_price
                
                # Stop loss hit
                if high_price >= stop_loss:
                    should_exit = True
                    exit_reason = "stop_loss"
                    exit_price = stop_loss
                
                # Target hit
                elif low_price <= target_price:
                    should_exit = True
                    exit_reason = "target_reached"
                    exit_price = target_price
                
                # Max hold period
                elif (current_date - position['entry_date']).days >= 7:
                    should_exit = True
                    exit_reason = "max_hold"
                    exit_price = current_price
                
                if should_exit:
                    positions_to_close.append((pos_id, exit_price, exit_reason))
            
            # Close positions
            for pos_id, exit_price, exit_reason in positions_to_close:
                position = positions[pos_id]
                
                # Calculate P&L for short position
                pnl = position['quantity'] * (position['entry_price'] - exit_price)
                capital += pnl
                
                # Record trade
                trades.append({
                    'ticker': position['ticker'],
                    'sector': position['sector'],
                    'entry_date': position['entry_date'],
                    'entry_price': position['entry_price'],
                    'exit_date': current_date,
                    'exit_price': exit_price,
                    'quantity': position['quantity'],
                    'pnl': pnl,
                    'exit_reason': exit_reason,
                    'hold_days': (current_date - position['entry_date']).days,
                    'position_in_range': position['position_in_range']
                })
                
                del positions[pos_id]
            
            # Look for new entries
            if len(positions) < max_positions:
                # Get potential entries for current date
                potential_entries = entries[entries['date'] == current_date]
                
                for _, entry in potential_entries.iterrows():
                    if len(positions) >= max_positions:
                        break
                    
                    ticker = entry['ticker']
                    
                    # Skip if already in position
                    if any(pos['ticker'] == ticker for pos in positions.values()):
                        continue
                    
                    # Check volatility timing
                    if not self.check_volatility_timing(
                        ticker, entry['atr'], current_date, 
                        price_history, volatility_lookback, volatility_increase
                    ):
                        continue
                    
                    # Check sector clustering
                    sector = self.get_ticker_sector(ticker)
                    if any(pos.get('sector') == sector for pos in positions.values()):
                        continue
                    
                    # Calculate position size
                    atr = entry['atr']
                    entry_price = entry['close']
                    
                    # For short positions
                    stop_loss = entry_price + (atr * stop_multiplier)
                    target_price = entry_price - (atr * target_multiplier)
                    
                    # Calculate quantity based on risk
                    risk_amount = capital * risk_per_trade
                    risk_per_share = stop_loss - entry_price
                    quantity = risk_amount / risk_per_share if risk_per_share > 0 else 0
                    
                    if quantity <= 0:
                        continue
                    
                    # Create position
                    position = {
                        'ticker': ticker,
                        'sector': sector,
                        'entry_date': current_date,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'stop_loss': stop_loss,
                        'target_price': target_price,
                        'atr': atr,
                        'position_in_range': entry['position_in_range']
                    }
                    
                    positions[str(len(positions))] = position
        
        self.base_trades = trades
        print(f"Generated {len(trades)} base trades")
        
        return trades
    
    def test_remove_top_trades(self):
        """Test system robustness by removing top trades"""
        
        print("\n=== TEST 1: REMOVE TOP TRADES ===")
        
        if not self.base_trades:
            print("No base trades to analyze")
            return
        
        trades_df = pd.DataFrame(self.base_trades)
        total_pnl = trades_df['pnl'].sum()
        
        # Sort trades by absolute P&L
        trades_df['pnl_abs'] = abs(trades_df['pnl'])
        trades_sorted = trades_df.sort_values('pnl_abs', ascending=False)
        
        # Test scenarios
        scenarios = [
            ("Remove Top 1", 1),
            ("Remove Top 3", 3),
            ("Remove Top 5", 5)
        ]
        
        results = {}
        
        for scenario_name, remove_count in scenarios:
            # Remove top trades
            if remove_count >= len(trades_sorted):
                remaining_pnl = 0
            else:
                remaining_trades = trades_sorted.iloc[remove_count:]
                remaining_pnl = remaining_trades['pnl'].sum()
            
            remaining_return = (remaining_pnl / 100000.0) * 100
            contribution_pct = (remaining_pnl / total_pnl) * 100
            
            results[scenario_name] = {
                'remaining_trades': len(trades_sorted) - remove_count,
                'remaining_pnl': remaining_pnl,
                'remaining_return': remaining_return,
                'contribution_pct': contribution_pct,
                'still_positive': remaining_pnl > 0
            }
            
            print(f"{scenario_name}:")
            print(f"  Remaining trades: {len(trades_sorted) - remove_count}")
            print(f"  Remaining P&L: ${remaining_pnl:,.0f}")
            print(f"  Return: {remaining_return:+.1%}")
            print(f"  Contribution: {contribution_pct:.1f}%")
            print(f"  Still positive: {'Yes' if remaining_pnl > 0 else 'No'}")
        
        return results
    
    def test_median_trade_quality(self):
        """Test median trade statistics"""
        
        print("\n=== TEST 2: MEDIAN TRADE QUALITY ===")
        
        if not self.base_trades:
            print("No base trades to analyze")
            return
        
        trades_df = pd.DataFrame(self.base_trades)
        
        # Calculate median statistics
        median_pnl = trades_df['pnl'].median()
        median_winner = trades_df[trades_df['pnl'] > 0]['pnl'].median() if len(trades_df[trades_df['pnl'] > 0]) > 0 else 0
        median_loser = trades_df[trades_df['pnl'] < 0]['pnl'].median() if len(trades_df[trades_df['pnl'] < 0]) > 0 else 0
        
        # Calculate other statistics
        mean_pnl = trades_df['pnl'].mean()
        std_pnl = trades_df['pnl'].std()
        
        # Calculate expectancy
        win_rate = len(trades_df[trades_df['pnl'] > 0]) / len(trades_df)
        avg_winner = trades_df[trades_df['pnl'] > 0]['pnl'].mean() if len(trades_df[trades_df['pnl'] > 0]) > 0 else 0
        avg_loser = trades_df[trades_df['pnl'] < 0]['pnl'].mean() if len(trades_df[trades_df['pnl'] < 0]) > 0 else 0
        expectancy = (win_rate * avg_winner) + ((1 - win_rate) * avg_loser)
        
        # Calculate excluding top decile
        trades_df['pnl_abs'] = abs(trades_df['pnl'])
        top_decile_threshold = trades_df['pnl_abs'].quantile(0.9)
        excluding_top_decile = trades_df[trades_df['pnl_abs'] <= top_decile_threshold]
        expectancy_excluding_top = (excluding_top_decile['pnl'].mean())
        
        results = {
            'median_pnl': median_pnl,
            'mean_pnl': mean_pnl,
            'std_pnl': std_pnl,
            'median_winner': median_winner,
            'median_loser': median_loser,
            'win_rate': win_rate,
            'avg_winner': avg_winner,
            'avg_loser': avg_loser,
            'expectancy': expectancy,
            'expectancy_excluding_top': expectancy_excluding_top,
            'tail_dependent': median_pnl < 0 or expectancy_excluding_top < 0
        }
        
        print(f"Median P&L: ${median_pnl:,.0f}")
        print(f"Mean P&L: ${mean_pnl:,.0f}")
        print(f"Std P&L: ${std_pnl:,.0f}")
        print(f"Median Winner: ${median_winner:,.0f}")
        print(f"Median Loser: ${median_loser:,.0f}")
        print(f"Win Rate: {win_rate:.1%}")
        print(f"Expectancy: ${expectancy:,.0f}")
        print(f"Expectancy (excluding top decile): ${expectancy_excluding_top:,.0f}")
        print(f"Tail Dependent: {'Yes' if results['tail_dependent'] else 'No'}")
        
        return results
    
    def test_time_robustness(self):
        """Test robustness by removing best time periods"""
        
        print("\n=== TEST 3: TIME ROBUSTNESS ===")
        
        if not self.base_trades:
            print("No base trades to analyze")
            return
        
        trades_df = pd.DataFrame(self.base_trades)
        trades_df['entry_date'] = pd.to_datetime(trades_df['entry_date'])
        trades_df['month'] = trades_df['entry_date'].dt.to_period('M')
        trades_df['quarter'] = trades_df['entry_date'].dt.to_period('Q')
        
        total_pnl = trades_df['pnl'].sum()
        
        # Find best month and quarter
        monthly_pnl = trades_df.groupby('month')['pnl'].sum()
        quarterly_pnl = trades_df.groupby('quarter')['pnl'].sum()
        
        best_month = monthly_pnl.idxmax()
        best_quarter = quarterly_pnl.idxmax()
        
        # Test scenarios
        scenarios = [
            ("Remove Best Month", best_month, 'month'),
            ("Remove Best Quarter", best_quarter, 'quarter')
        ]
        
        results = {}
        
        for scenario_name, best_period, period_type in scenarios:
            # Remove best period
            if period_type == 'month':
                remaining_trades = trades_df[trades_df['month'] != best_period]
            else:
                remaining_trades = trades_df[trades_df['quarter'] != best_period]
            
            remaining_pnl = remaining_trades['pnl'].sum()
            remaining_return = (remaining_pnl / 100000.0) * 100
            contribution_pct = (remaining_pnl / total_pnl) * 100
            
            results[scenario_name] = {
                'best_period': str(best_period),
                'best_period_pnl': monthly_pnl[best_period] if period_type == 'month' else quarterly_pnl[best_quarter],
                'remaining_trades': len(remaining_trades),
                'remaining_pnl': remaining_pnl,
                'remaining_return': remaining_return,
                'contribution_pct': contribution_pct,
                'still_positive': remaining_pnl > 0
            }
            
            print(f"{scenario_name}:")
            print(f"  Best period: {best_period}")
            print(f"  Best period P&L: ${monthly_pnl[best_period]:,.0f}" if period_type == 'month' else f"  Best period P&L: ${quarterly_pnl[best_quarter]:,.0f}")
            print(f"  Remaining trades: {len(remaining_trades)}")
            print(f"  Remaining P&L: ${remaining_pnl:,.0f}")
            print(f"  Return: {remaining_return:+.1%}")
            print(f"  Contribution: {contribution_pct:.1f}%")
            print(f"  Still positive: {'Yes' if remaining_pnl > 0 else 'No'}")
        
        return results
    
    def test_position_cap_experiment(self):
        """Test with max_positions = 1"""
        
        print("\n=== TEST 4: POSITION CAP EXPERIMENT ===")
        
        # Re-run simulation with max_positions = 1
        min_range = 0.30
        max_range = 0.40
        stop_multiplier = 1.25
        target_multiplier = 2.5
        max_positions = 1  # Changed to 1
        portfolio_heat = 0.08
        volatility_lookback = 5
        volatility_increase = 0.05
        
        # Filter for bear expansion entries
        entries = self.regime_data[self.regime_data['is_bear_expansion']].copy()
        
        # Apply position in range filter
        entries = entries[
            (entries['position_in_range'] >= min_range) &
            (entries['position_in_range'] <= max_range)
        ]
        
        # Sort by date
        entries = entries.sort_values('date')
        
        # Build price history for volatility timing
        price_history = {}
        for _, row in self.regime_data.iterrows():
            ticker = row['ticker']
            if ticker not in price_history:
                price_history[ticker] = []
            
            price_history[ticker].append({
                'date': row['date'],
                'price': row['close'],
                'atr': row['atr']
            })
            
            # Keep only recent data (last 30 days)
            cutoff_date = row['date'] - pd.Timedelta(days=30)
            price_history[ticker] = [
                entry for entry in price_history[ticker]
                if entry['date'] > cutoff_date
            ]
        
        # Simulation variables
        capital = 100000.0
        positions = {}
        trades = []
        
        # Risk parameters
        risk_per_trade = portfolio_heat / max_positions
        
        # Simulate day by day
        trading_days = sorted(self.regime_data['date'].unique())
        
        for current_date in trading_days:
            # Get current day's data
            day_data = self.regime_data[self.regime_data['date'] == current_date]
            
            # Update existing positions
            positions_to_close = []
            
            for pos_id, position in positions.items():
                ticker = position['ticker']
                
                # Get current price
                current_price_data = day_data[day_data['ticker'] == ticker]
                if len(current_price_data) == 0:
                    continue
                
                current_price = current_price_data.iloc[0]['close']
                high_price = current_price_data.iloc[0]['high']
                low_price = current_price_data.iloc[0]['low']
                
                # Check exit conditions
                entry_price = position['entry_price']
                stop_loss = position['stop_loss']
                target_price = position['target_price']
                
                # For short positions
                should_exit = False
                exit_reason = ""
                exit_price = current_price
                
                # Stop loss hit
                if high_price >= stop_loss:
                    should_exit = True
                    exit_reason = "stop_loss"
                    exit_price = stop_loss
                
                # Target hit
                elif low_price <= target_price:
                    should_exit = True
                    exit_reason = "target_reached"
                    exit_price = target_price
                
                # Max hold period
                elif (current_date - position['entry_date']).days >= 7:
                    should_exit = True
                    exit_reason = "max_hold"
                    exit_price = current_price
                
                if should_exit:
                    positions_to_close.append((pos_id, exit_price, exit_reason))
            
            # Close positions
            for pos_id, exit_price, exit_reason in positions_to_close:
                position = positions[pos_id]
                
                # Calculate P&L for short position
                pnl = position['quantity'] * (position['entry_price'] - exit_price)
                capital += pnl
                
                # Record trade
                trades.append({
                    'ticker': position['ticker'],
                    'sector': position['sector'],
                    'entry_date': position['entry_date'],
                    'entry_price': position['entry_price'],
                    'exit_date': current_date,
                    'exit_price': exit_price,
                    'quantity': position['quantity'],
                    'pnl': pnl,
                    'exit_reason': exit_reason,
                    'hold_days': (current_date - position['entry_date']).days,
                    'position_in_range': position['position_in_range']
                })
                
                del positions[pos_id]
            
            # Look for new entries (only 1 position allowed)
            if len(positions) < max_positions:
                # Get potential entries for current date
                potential_entries = entries[entries['date'] == current_date]
                
                for _, entry in potential_entries.iterrows():
                    if len(positions) >= max_positions:
                        break
                    
                    ticker = entry['ticker']
                    
                    # Skip if already in position
                    if any(pos['ticker'] == ticker for pos in positions.values()):
                        continue
                    
                    # Check volatility timing
                    if not self.check_volatility_timing(
                        ticker, entry['atr'], current_date, 
                        price_history, volatility_lookback, volatility_increase
                    ):
                        continue
                    
                    # Calculate position size
                    atr = entry['atr']
                    entry_price = entry['close']
                    
                    # For short positions
                    stop_loss = entry_price + (atr * stop_multiplier)
                    target_price = entry_price - (atr * target_multiplier)
                    
                    # Calculate quantity based on risk
                    risk_amount = capital * risk_per_trade
                    risk_per_share = stop_loss - entry_price
                    quantity = risk_amount / risk_per_share if risk_per_share > 0 else 0
                    
                    if quantity <= 0:
                        continue
                    
                    # Create position
                    position = {
                        'ticker': ticker,
                        'sector': self.get_ticker_sector(ticker),
                        'entry_date': current_date,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'stop_loss': stop_loss,
                        'target_price': target_price,
                        'atr': atr,
                        'position_in_range': entry['position_in_range']
                    }
                    
                    positions[str(len(positions))] = position
        
        # Calculate results
        total_pnl = sum(trade['pnl'] for trade in trades)
        total_return = (total_pnl / 100000.0) * 100
        
        # Calculate concentration
        trades_df = pd.DataFrame(trades)
        if len(trades_df) > 0:
            trades_df['pnl_abs'] = abs(trades_df['pnl'])
            top_5_contribution = trades_df.nlargest(5, 'pnl_abs')['pnl_abs'].sum() / abs(total_pnl) * 100
        else:
            top_5_contribution = 0
        
        result = {
            'total_trades': len(trades),
            'total_pnl': total_pnl,
            'total_return': total_return,
            'top_5_contribution': top_5_contribution,
            'concentration_reduced': top_5_contribution < 50
        }
        
        print(f"Max Positions = 1 Results:")
        print(f"  Total trades: {len(trades)}")
        print(f"  Total P&L: ${total_pnl:,.0f}")
        print(f"  Return: {total_return:+.1%}")
        print(f"  Top 5 contribution: {top_5_contribution:.1f}%")
        print(f"  Concentration reduced: {'Yes' if result['concentration_reduced'] else 'No'}")
        
        return result
    
    def analyze_edge_structure(self):
        """Analyze overall edge structure"""
        
        print("\n=== EDGE STRUCTURE ANALYSIS ===")
        
        # Run all tests
        remove_top_results = self.test_remove_top_trades()
        median_results = self.test_median_trade_quality()
        time_results = self.test_time_robustness()
        position_cap_results = self.test_position_cap_experiment()
        
        # Determine edge type
        tail_dependent = median_results.get('tail_dependent', False)
        concentration_risk = not position_cap_results.get('concentration_reduced', False)
        time_fragile = not any(result['still_positive'] for result in time_results.values())
        
        # Edge classification
        if tail_dependent and concentration_risk:
            edge_type = "CONVEX/EVENT-DRIVEN"
            recommendation = "Embrace convexity with proper sizing"
        elif tail_dependent and not concentration_risk:
            edge_type = "TAIL-DEPENDENT"
            recommendation = "Focus on base trade quality"
        elif not tail_dependent and not concentration_risk:
            edge_type = "STABLE DISTRIBUTION"
            recommendation = "Scale with confidence"
        else:
            edge_type = "MIXED/HYBRID"
            recommendation = "Further analysis needed"
        
        print(f"\n=== EDGE CLASSIFICATION ===")
        print(f"Edge Type: {edge_type}")
        print(f"Recommendation: {recommendation}")
        
        print(f"\n=== KEY INDICATORS ===")
        print(f"Tail Dependent: {'Yes' if tail_dependent else 'No'}")
        print(f"Concentration Risk: {'High' if concentration_risk else 'Low'}")
        print(f"Time Fragile: {'Yes' if time_fragile else 'No'}")
        
        # Path recommendation
        print(f"\n=== RECOMMENDED PATH ===")
        
        if edge_type == "CONVEX/EVENT-DRIVEN":
            print("PATH B: Embrace Convexity")
            print("  - Keep winners large")
            print("  - Accept many small losses")
            print("  - Size smaller overall")
            print("  - Treat like optionality")
        elif edge_type == "TAIL-DEPENDENT":
            print("PATH A: Control Concentration")
            print("  - Cap position sizes")
            print("  - Tighten stops")
            print("  - Limit sector exposure")
            print("  - Reduce tail dominance")
        else:
            print("PATH A: Control Concentration")
            print("  - Focus on consistency")
            print("  - Scale gradually")
        
        return {
            'edge_type': edge_type,
            'recommendation': recommendation,
            'tail_dependent': tail_dependent,
            'concentration_risk': concentration_risk,
            'time_fragile': time_fragile,
            'remove_top_results': remove_top_results,
            'median_results': median_results,
            'time_results': time_results,
            'position_cap_results': position_cap_results
        }
    
    def run_analysis(self):
        """Run complete edge structure analysis"""
        
        print("Edge Structure Analysis")
        print("Determining strategy type: stable vs convex\n")
        
        # Load data
        if not self.load_data():
            return None
        
        # Calculate regimes
        if not self.calculate_regimes():
            return None
        
        # Generate base trades
        self.generate_base_trades()
        
        # Analyze edge structure
        analysis = self.analyze_edge_structure()
        
        return analysis


def main():
    """Main analysis function"""
    
    analyzer = EdgeStructureAnalyzer()
    analysis = analyzer.run_analysis()
    
    return analyzer, analysis


if __name__ == "__main__":
    analyzer, analysis = main()
