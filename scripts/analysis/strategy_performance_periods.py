"""
Strategy Performance Periods Analyzer

Identifies periods when strategies performed best and tracks longest winning/losing runs.
Provides insights into temporal performance patterns and streak analysis.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

from app.core.regime_v3 import RegimeClassifierV3, TrendRegime, VolatilityRegime


class StrategyPerformanceAnalyzer:
    """
    Analyzes strategy performance across different time periods and identifies streaks.
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.data = None
        self.trades = None
        self.performance_periods = {}
        
    def load_historical_data(self) -> bool:
        """Load historical price data with technical indicators."""
        
        print("Loading historical data...")
        
        conn = sqlite3.connect(self.db_path)
        
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
            
            # Calculate 20-day high/low
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
        self.data = pd.concat(result_dfs, ignore_index=True)
        self.data = self.data.dropna(subset=['ma50', 'ma200', 'atr', 'position_in_range'])
        
        print(f"Prepared {len(self.data)} rows of data with indicators")
        return True
    
    def calculate_regimes(self):
        """Calculate regimes for all data points."""
        
        print("Calculating regimes...")
        
        regime_data = []
        
        for ticker in self.data['ticker'].unique():
            ticker_data = self.data[self.data['ticker'] == ticker].copy()
            
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
                
                regime_str = f"({trend_regime}, {volatility_regime})"
                
                regime_data.append({
                    'ticker': ticker,
                    'date': row['date'],
                    'close': row['close'],
                    'high': row['high'],
                    'low': row['low'],
                    'atr': row['atr'],
                    'position_in_range': row['position_in_range'],
                    'regime': regime_str,
                    'trend_regime': trend_regime,
                    'volatility_regime': volatility_regime
                })
        
        self.regime_data = pd.DataFrame(regime_data)
        print(f"Calculated regimes for {len(self.regime_data)} data points")
        
        return True
    
    def simulate_bear_expansion_strategy(self):
        """Simulate the bear expansion strategy to generate trade history."""
        
        print("Simulating bear expansion strategy...")
        
        # Filter for bear expansion entries
        entries = self.regime_data[
            (self.regime_data['regime'] == '(BEAR, EXPANSION)') &
            (self.regime_data['position_in_range'] >= 0.30) &
            (self.regime_data['position_in_range'] <= 0.40)
        ].copy()
        
        entries = entries.sort_values('date')
        
        # Simulation parameters
        initial_capital = 100000.0
        max_positions = 2
        portfolio_heat = 0.08
        stop_multiplier = 1.25
        target_multiplier = 2.5
        
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
            
            # Keep only recent data
            cutoff_date = row['date'] - pd.Timedelta(days=30)
            price_history[ticker] = [
                entry for entry in price_history[ticker]
                if entry['date'] > cutoff_date
            ]
        
        # Simulation variables
        capital = initial_capital
        positions = {}
        trades = []
        
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
                    'entry_date': position['entry_date'],
                    'exit_date': current_date,
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'quantity': position['quantity'],
                    'pnl': pnl,
                    'return_pct': (pnl / (position['quantity'] * position['entry_price'])) * 100,
                    'exit_reason': exit_reason,
                    'hold_days': (current_date - position['entry_date']).days,
                    'regime': position['regime'],
                    'position_in_range': position['position_in_range']
                })
                
                del positions[pos_id]
            
            # Look for new entries
            if len(positions) < max_positions:
                potential_entries = entries[entries['date'] == current_date]
                
                for _, entry in potential_entries.iterrows():
                    if len(positions) >= max_positions:
                        break
                    
                    ticker = entry['ticker']
                    
                    # Skip if already in position
                    if any(pos['ticker'] == ticker for pos in positions.values()):
                        continue
                    
                    # Calculate position size
                    atr = entry['atr']
                    entry_price = entry['close']
                    
                    # For short positions
                    stop_loss = entry_price + (atr * stop_multiplier)
                    target_price = entry_price - (atr * target_multiplier)
                    
                    # Calculate quantity based on risk
                    risk_per_trade = portfolio_heat / max_positions
                    risk_amount = capital * risk_per_trade
                    risk_per_share = stop_loss - entry_price
                    quantity = risk_amount / risk_per_share if risk_per_share > 0 else 0
                    
                    if quantity <= 0:
                        continue
                    
                    # Create position
                    position = {
                        'ticker': ticker,
                        'entry_date': current_date,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'stop_loss': stop_loss,
                        'target_price': target_price,
                        'regime': entry['regime'],
                        'position_in_range': entry['position_in_range']
                    }
                    
                    positions[str(len(positions))] = position
        
        self.trades = pd.DataFrame(trades)
        print(f"Generated {len(trades)} trades")
        
        return self.trades
    
    def analyze_performance_periods(self, window_days: int = 30) -> Dict[str, Any]:
        """Analyze performance across rolling time windows."""
        
        if self.trades is None or len(self.trades) == 0:
            print("No trades to analyze")
            return {}
        
        print(f"\n=== PERFORMANCE PERIODS ANALYSIS ({window_days}-day windows) ===")
        
        # Sort trades by entry date
        trades_sorted = self.trades.sort_values('entry_date')
        
        # Calculate rolling performance
        performance_periods = []
        
        start_date = trades_sorted['entry_date'].min()
        end_date = trades_sorted['entry_date'].max()
        
        current_date = start_date
        
        while current_date <= end_date:
            window_end = current_date + pd.Timedelta(days=window_days)
            
            # Get trades in window
            window_trades = trades_sorted[
                (trades_sorted['entry_date'] >= current_date) &
                (trades_sorted['entry_date'] < window_end)
            ]
            
            if len(window_trades) > 0:
                total_pnl = window_trades['pnl'].sum()
                total_return = window_trades['return_pct'].sum()
                win_rate = (window_trades['pnl'] > 0).mean()
                trade_count = len(window_trades)
                
                performance_periods.append({
                    'start_date': current_date,
                    'end_date': window_end,
                    'total_pnl': total_pnl,
                    'total_return': total_return,
                    'win_rate': win_rate,
                    'trade_count': trade_count,
                    'avg_pnl_per_trade': total_pnl / trade_count if trade_count > 0 else 0
                })
            
            current_date += pd.Timedelta(days=7)  # Weekly steps
        
        performance_df = pd.DataFrame(performance_periods)
        
        if len(performance_df) == 0:
            return {}
        
        # Find best and worst periods
        best_period = performance_df.loc[performance_df['total_pnl'].idxmax()]
        worst_period = performance_df.loc[performance_df['total_pnl'].idxmin()]
        
        # Find highest win rate period
        best_win_rate_period = performance_df.loc[performance_df['win_rate'].idxmax()]
        
        # Find most active period
        most_active_period = performance_df.loc[performance_df['trade_count'].idxmax()]
        
        results = {
            'performance_periods': performance_df,
            'best_period': best_period.to_dict(),
            'worst_period': worst_period.to_dict(),
            'best_win_rate_period': best_win_rate_period.to_dict(),
            'most_active_period': most_active_period.to_dict(),
            'window_days': window_days
        }
        
        # Print results
        print(f"\nBEST PERIOD ({best_period['start_date'].strftime('%Y-%m-%d')} to {best_period['end_date'].strftime('%Y-%m-%d')}):")
        print(f"  Total P&L: ${best_period['total_pnl']:,.0f}")
        print(f"  Total Return: {best_period['total_return']:+.1f}%")
        print(f"  Win Rate: {best_period['win_rate']:.1%}")
        print(f"  Trade Count: {best_period['trade_count']}")
        
        print(f"\nWORST PERIOD ({worst_period['start_date'].strftime('%Y-%m-%d')} to {worst_period['end_date'].strftime('%Y-%m-%d')}):")
        print(f"  Total P&L: ${worst_period['total_pnl']:,.0f}")
        print(f"  Total Return: {worst_period['total_return']:+.1f}%")
        print(f"  Win Rate: {worst_period['win_rate']:.1%}")
        print(f"  Trade Count: {worst_period['trade_count']}")
        
        print(f"\nHIGHEST WIN RATE PERIOD ({best_win_rate_period['start_date'].strftime('%Y-%m-%d')} to {best_win_rate_period['end_date'].strftime('%Y-%m-%d')}):")
        print(f"  Win Rate: {best_win_rate_period['win_rate']:.1%}")
        print(f"  Total P&L: ${best_win_rate_period['total_pnl']:,.0f}")
        print(f"  Trade Count: {best_win_rate_period['trade_count']}")
        
        self.performance_periods = results
        return results
    
    def analyze_streaks(self) -> Dict[str, Any]:
        """Analyze winning and losing streaks."""
        
        if self.trades is None or len(self.trades) == 0:
            print("No trades to analyze")
            return {}
        
        print(f"\n=== STREAK ANALYSIS ===")
        
        # Sort trades by entry date
        trades_sorted = self.trades.sort_values('entry_date').copy()
        trades_sorted['win'] = trades_sorted['pnl'] > 0
        
        # Find all streaks
        streaks = []
        current_streak = {
            'start_date': trades_sorted.iloc[0]['entry_date'],
            'start_idx': 0,
            'is_winning': trades_sorted.iloc[0]['win'],
            'length': 1,
            'pnl': trades_sorted.iloc[0]['pnl'],
            'trades': [trades_sorted.iloc[0]]
        }
        
        for i in range(1, len(trades_sorted)):
            trade = trades_sorted.iloc[i]
            
            if trade['win'] == current_streak['is_winning']:
                # Continue current streak
                current_streak['end_date'] = trade['entry_date']
                current_streak['end_idx'] = i
                current_streak['length'] += 1
                current_streak['pnl'] += trade['pnl']
                current_streak['trades'].append(trade)
            else:
                # End current streak and start new one
                current_streak['avg_daily_pnl'] = current_streak['pnl'] / current_streak['length']
                streaks.append(current_streak.copy())
                
                # Start new streak
                current_streak = {
                    'start_date': trade['entry_date'],
                    'start_idx': i,
                    'is_winning': trade['win'],
                    'length': 1,
                    'pnl': trade['pnl'],
                    'trades': [trade]
                }
        
        # Add final streak
        current_streak['avg_daily_pnl'] = current_streak['pnl'] / current_streak['length']
        streaks.append(current_streak)
        
        # Separate winning and losing streaks
        winning_streaks = [s for s in streaks if s['is_winning']]
        losing_streaks = [s for s in streaks if not s['is_winning']]
        
        # Find longest streaks
        longest_winning = max(winning_streaks, key=lambda x: x['length']) if winning_streaks else None
        longest_losing = max(losing_streaks, key=lambda x: x['length']) if losing_streaks else None
        
        # Find most profitable streak
        most_profitable = max(winning_streaks, key=lambda x: x['pnl']) if winning_streaks else None
        
        # Find worst losing streak
        worst_losing = min(losing_streaks, key=lambda x: x['pnl']) if losing_streaks else None
        
        results = {
            'all_streaks': streaks,
            'winning_streaks': winning_streaks,
            'losing_streaks': losing_streaks,
            'longest_winning': longest_winning,
            'longest_losing': longest_losing,
            'most_profitable': most_profitable,
            'worst_losing': worst_losing,
            'total_winning_streaks': len(winning_streaks),
            'total_losing_streaks': len(losing_streaks),
            'avg_winning_streak_length': np.mean([s['length'] for s in winning_streaks]) if winning_streaks else 0,
            'avg_losing_streak_length': np.mean([s['length'] for s in losing_streaks]) if losing_streaks else 0
        }
        
        # Print results
        print(f"Total streaks: {len(streaks)}")
        print(f"Winning streaks: {len(winning_streaks)}")
        print(f"Losing streaks: {len(losing_streaks)}")
        
        if longest_winning:
            print(f"\nLONGEST WINNING STREAK: {longest_winning['length']} trades")
            print(f"  Period: {longest_winning['start_date'].strftime('%Y-%m-%d')} to {longest_winning['end_date'].strftime('%Y-%m-%d')}")
            print(f"  Total P&L: ${longest_winning['pnl']:,.0f}")
            print(f"  Average per trade: ${longest_winning['avg_daily_pnl']:,.0f}")
        
        if longest_losing:
            print(f"\nLONGEST LOSING STREAK: {longest_losing['length']} trades")
            print(f"  Period: {longest_losing['start_date'].strftime('%Y-%m-%d')} to {longest_losing['end_date'].strftime('%Y-%m-%d')}")
            print(f"  Total P&L: ${longest_losing['pnl']:,.0f}")
            print(f"  Average per trade: ${longest_losing['avg_daily_pnl']:,.0f}")
        
        if most_profitable:
            print(f"\nMOST PROFITABLE STREAK: ${most_profitable['pnl']:,.0f} over {most_profitable['length']} trades")
            print(f"  Period: {most_profitable['start_date'].strftime('%Y-%m-%d')} to {most_profitable['end_date'].strftime('%Y-%m-%d')}")
            print(f"  Average per trade: ${most_profitable['avg_daily_pnl']:,.0f}")
        
        if worst_losing:
            print(f"\nWORST LOSING STREAK: ${worst_losing['pnl']:,.0f} over {worst_losing['length']} trades")
            print(f"  Period: {worst_losing['start_date'].strftime('%Y-%m-%d')} to {worst_losing['end_date'].strftime('%Y-%m-%d')}")
            print(f"  Average per trade: ${worst_losing['avg_daily_pnl']:,.0f}")
        
        return results
    
    def analyze_monthly_patterns(self) -> Dict[str, Any]:
        """Analyze performance patterns by month and quarter."""
        
        if self.trades is None or len(self.trades) == 0:
            print("No trades to analyze")
            return {}
        
        print(f"\n=== MONTHLY PATTERNS ANALYSIS ===")
        
        # Add time-based columns
        trades_copy = self.trades.copy()
        trades_copy['entry_date'] = pd.to_datetime(trades_copy['entry_date'])
        trades_copy['month'] = trades_copy['entry_date'].dt.month
        trades_copy['quarter'] = trades_copy['entry_date'].dt.quarter
        trades_copy['year'] = trades_copy['entry_date'].dt.year
        trades_copy['month_name'] = trades_copy['entry_date'].dt.strftime('%B')
        
        # Monthly analysis
        monthly_stats = trades_copy.groupby('month').agg({
            'pnl': ['sum', 'mean', 'count'],
            'return_pct': ['sum', 'mean'],
            'entry_date': 'count'
        }).round(2)
        
        monthly_stats.columns = ['total_pnl', 'avg_pnl', 'trade_count', 'total_return', 'avg_return', 'trade_count2']
        monthly_stats = monthly_stats.drop('trade_count2', axis=1)
        monthly_stats['win_rate'] = trades_copy[trades_copy['pnl'] > 0].groupby('month').size() / trades_copy.groupby('month').size()
        
        # Quarterly analysis
        quarterly_stats = trades_copy.groupby('quarter').agg({
            'pnl': ['sum', 'mean', 'count'],
            'return_pct': ['sum', 'mean'],
            'entry_date': 'count'
        }).round(2)
        
        quarterly_stats.columns = ['total_pnl', 'avg_pnl', 'trade_count', 'total_return', 'avg_return', 'trade_count2']
        quarterly_stats = quarterly_stats.drop('trade_count2', axis=1)
        quarterly_stats['win_rate'] = trades_copy[trades_copy['pnl'] > 0].groupby('quarter').size() / trades_copy.groupby('quarter').size()
        
        # Find best and worst months
        best_month = monthly_stats.loc[monthly_stats['total_pnl'].idxmax()]
        worst_month = monthly_stats.loc[monthly_stats['total_pnl'].idxmin()]
        
        best_quarter = quarterly_stats.loc[quarterly_stats['total_pnl'].idxmax()]
        worst_quarter = quarterly_stats.loc[quarterly_stats['total_pnl'].idxmin()]
        
        results = {
            'monthly_stats': monthly_stats,
            'quarterly_stats': quarterly_stats,
            'best_month': best_month.to_dict(),
            'worst_month': worst_month.to_dict(),
            'best_quarter': best_quarter.to_dict(),
            'worst_quarter': worst_quarter.to_dict()
        }
        
        # Print results
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        print(f"\nMONTHLY PERFORMANCE:")
        print("Month | Total P&L | Avg P&L | Trades | Win Rate | Return")
        print("-" * 55)
        
        for i, month in enumerate(month_names, 1):
            if i in monthly_stats.index:
                stats = monthly_stats.loc[i]
                print(f"{month:>5} | ${stats['total_pnl']:>8.0f} | ${stats['avg_pnl']:>7.0f} | {stats['trade_count']:>6} | {stats['win_rate']:>8.1%} | {stats['total_return']:>6.1f}%")
        
        print(f"\nBEST MONTH: {month_names[int(best_month.name)-1]}")
        print(f"  Total P&L: ${best_month['total_pnl']:,.0f}")
        print(f"  Win Rate: {best_month['win_rate']:.1%}")
        
        print(f"\nWORST MONTH: {month_names[int(worst_month.name)-1]}")
        print(f"  Total P&L: ${worst_month['total_pnl']:,.0f}")
        print(f"  Win Rate: {worst_month['win_rate']:.1%}")
        
        return results
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        
        print("\n" + "="*60)
        print("COMPREHENSIVE STRATEGY PERFORMANCE REPORT")
        print("="*60)
        
        # Run all analyses
        performance_periods = self.analyze_performance_periods()
        streaks = self.analyze_streaks()
        monthly_patterns = self.analyze_monthly_patterns()
        
        # Summary statistics
        if self.trades is not None and len(self.trades) > 0:
            total_trades = len(self.trades)
            total_pnl = self.trades['pnl'].sum()
            total_return = self.trades['return_pct'].sum()
            win_rate = (self.trades['pnl'] > 0).mean()
            avg_win = self.trades[self.trades['pnl'] > 0]['pnl'].mean()
            avg_loss = self.trades[self.trades['pnl'] < 0]['pnl'].mean()
            
            print(f"\nOVERALL PERFORMANCE:")
            print(f"  Total Trades: {total_trades}")
            print(f"  Total P&L: ${total_pnl:,.0f}")
            print(f"  Total Return: {total_return:+.1f}%")
            print(f"  Win Rate: {win_rate:.1%}")
            print(f"  Average Win: ${avg_win:,.0f}")
            print(f"  Average Loss: ${avg_loss:,.0f}")
            
            # Calculate expectancy
            expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
            print(f"  Expectancy per Trade: ${expectancy:.0f}")
            
            # Performance consistency
            if 'performance_periods' in performance_periods:
                periods_df = performance_periods['performance_periods']
                positive_periods = (periods_df['total_pnl'] > 0).sum()
                total_periods = len(periods_df)
                consistency = positive_periods / total_periods if total_periods > 0 else 0
                
                print(f"  Positive Periods: {positive_periods}/{total_periods} ({consistency:.1%})")
        
        return {
            'performance_periods': performance_periods,
            'streaks': streaks,
            'monthly_patterns': monthly_patterns,
            'summary': {
                'total_trades': len(self.trades) if self.trades is not None else 0,
                'total_pnl': self.trades['pnl'].sum() if self.trades is not None else 0,
                'win_rate': (self.trades['pnl'] > 0).mean() if self.trades is not None else 0
            }
        }


def main():
    """Main function to run the analysis."""
    
    analyzer = StrategyPerformanceAnalyzer()
    
    # Load data
    if not analyzer.load_historical_data():
        return
    
    # Calculate regimes
    if not analyzer.calculate_regimes():
        return
    
    # Simulate strategy
    analyzer.simulate_bear_expansion_strategy()
    
    # Generate comprehensive report
    report = analyzer.generate_performance_report()
    
    return analyzer, report


if __name__ == "__main__":
    analyzer, report = main()
