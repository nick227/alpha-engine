"""
Minimal Trade Ledger

One row per closed trade only.
Hard sanity checks before any summary.
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


class MinimalTradeLedger:
    """
    Minimal trade ledger with one row per trade and hard sanity checks.
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for minimal trade ledger...")
        
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
            
            # Calculate position in range
            ticker_data['high_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).max()
            ticker_data['low_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).min()
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
    
    def generate_minimal_trades(self):
        """
        Generate minimal trade ledger - one row per closed trade only.
        """
        
        print("Generating minimal trade ledger...")
        
        # Strategy parameters
        min_range = 0.30
        max_range = 0.40
        stop_multiplier = 1.25
        target_multiplier = 2.5
        max_positions_total = 2
        max_positions_per_sector = 1
        portfolio_heat = 0.08
        volatility_lookback = 5
        volatility_increase = 0.05
        cooldown_days = 2
        
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
        positions = {}  # Active positions
        minimal_trades = []  # Minimal trade ledger
        trade_id_counter = 0
        last_entry_dates = {}  # For cooldown tracking
        
        # Risk parameters
        risk_per_trade = portfolio_heat / max_positions_total
        
        # Simulate day by day
        trading_days = sorted(self.regime_data['date'].unique())
        
        for current_date in trading_days:
            # Get current day's data
            day_data = self.regime_data[self.regime_data['date'] == current_date]
            
            # Update existing positions
            positions_to_close = []
            
            for pos_id in list(positions.keys()):
                position = positions[pos_id]
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
                atr = position['atr']
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
                
                # Add to minimal trade ledger (ONE ROW PER TRADE)
                trade_id_counter += 1
                minimal_trades.append({
                    'trade_id': trade_id_counter,
                    'ticker': position['ticker'],
                    'sector': position['sector'],
                    'entry_time': position['entry_date'],
                    'exit_time': current_date,
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'quantity': position['quantity'],
                    'realized_pnl': pnl
                })
                
                del positions[pos_id]
            
            # Look for new entries
            if len(positions) < max_positions_total:
                # Get potential entries for current date
                potential_entries = entries[entries['date'] == current_date]
                
                for _, entry in potential_entries.iterrows():
                    if len(positions) >= max_positions_total:
                        break
                    
                    ticker = entry['ticker']
                    
                    # Skip if already in position
                    if any(pos['ticker'] == ticker for pos in positions.values()):
                        continue
                    
                    # Check cooldown
                    if ticker in last_entry_dates:
                        days_since_entry = (current_date - last_entry_dates[ticker]).days
                        if days_since_entry < cooldown_days:
                            continue
                    
                    # Check sector caps
                    sector = self.get_ticker_sector(ticker)
                    sector_positions = [pos for pos in positions.values() if pos.get('sector') == sector]
                    if len(sector_positions) >= max_positions_per_sector:
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
                    last_entry_dates[ticker] = current_date
        
        print(f"Generated {len(minimal_trades)} minimal trades")
        
        return minimal_trades
    
    def run_hard_sanity_checks(self, trades: List[Dict[str, Any]]) -> bool:
        """
        Run hard sanity checks before any summary.
        """
        
        print("Running hard sanity checks...")
        
        if not trades:
            print("ERROR: No trades to validate")
            return False
        
        # Convert to DataFrame
        trades_df = pd.DataFrame(trades)
        
        # Check 1: Unique trade_id count
        unique_trade_ids = trades_df['trade_id'].nunique()
        total_trades = len(trades_df)
        if unique_trade_ids != total_trades:
            print(f"ERROR: Trade ID mismatch - unique: {unique_trade_ids}, total: {total_trades}")
            return False
        
        # Check 2: Calculate concentration metrics
        total_pnl = trades_df['realized_pnl'].sum()
        if total_pnl == 0:
            print("ERROR: Total P&L is zero")
            return False
        
        # Top trades
        trades_df['pnl_abs'] = abs(trades_df['realized_pnl'])
        trades_sorted = trades_df.sort_values('pnl_abs', ascending=False)
        
        top_1_pnl = trades_sorted.iloc[0]['pnl_abs']
        top_5_pnl = trades_sorted.head(5)['pnl_abs'].sum()
        top_10_pnl = trades_sorted.head(10)['pnl_abs'].sum()
        
        top_1_contribution = (top_1_pnl / abs(total_pnl)) * 100
        top_5_contribution = (top_5_pnl / abs(total_pnl)) * 100
        top_10_contribution = (top_10_pnl / abs(total_pnl)) * 100
        
        # Check 3: Top 5 <= 100%
        if top_5_contribution > 100:
            print(f"ERROR: Top 5 contribution > 100%: {top_5_contribution:.1f}%")
            return False
        
        # Check 4: Top 10 <= 100%
        if top_10_contribution > 100:
            print(f"ERROR: Top 10 contribution > 100%: {top_10_contribution:.1f}%")
            return False
        
        # Check 5: Sector contributions
        sector_pnl = trades_df.groupby('sector')['realized_pnl'].sum()
        sector_contribution = (sector_pnl.abs() / abs(total_pnl) * 100)
        sector_sum = sector_contribution.sum()
        
        if sector_sum < 95 or sector_sum > 105:
            print(f"ERROR: Sector sum not ~100%: {sector_sum:.1f}%")
            return False
        
        print("All sanity checks passed!")
        return True
    
    def calculate_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate metrics from validated trades.
        """
        
        trades_df = pd.DataFrame(trades)
        
        # Basic P&L calculation
        total_pnl = trades_df['realized_pnl'].sum()
        total_return = (total_pnl / 100000.0) * 100
        
        # Trade quality metrics
        median_pnl = trades_df['realized_pnl'].median()
        win_rate = len(trades_df[trades_df['realized_pnl'] > 0]) / len(trades_df)
        
        # Top trade contributions
        trades_df['pnl_abs'] = abs(trades_df['realized_pnl'])
        trades_sorted = trades_df.sort_values('pnl_abs', ascending=False)
        
        top_1_pnl = trades_sorted.iloc[0]['pnl_abs']
        top_5_pnl = trades_sorted.head(5)['pnl_abs'].sum()
        top_10_pnl = trades_sorted.head(10)['pnl_abs'].sum()
        
        top_1_contribution = (top_1_pnl / abs(total_pnl)) * 100
        top_5_contribution = (top_5_pnl / abs(total_pnl)) * 100
        top_10_contribution = (top_10_pnl / abs(total_pnl)) * 100
        
        # Sector contributions
        sector_pnl = trades_df.groupby('sector')['realized_pnl'].sum()
        sector_contribution = (sector_pnl.abs() / abs(total_pnl) * 100).round(1)
        
        # Drawdown calculation
        equity_curve = []
        running_capital = 100000.0
        
        for _, trade in trades_df.iterrows():
            running_capital += trade['realized_pnl']
            equity_curve.append(running_capital)
        
        max_drawdown = 0
        peak = 100000.0
        
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # Sharpe ratio
        if len(equity_curve) > 1:
            returns = np.diff(equity_curve) / equity_curve[:-1]
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0.0
        else:
            sharpe = 0.0
        
        return {
            'total_trades': len(trades),
            'total_pnl': total_pnl,
            'total_return': total_return,
            'max_drawdown': max_drawdown * 100,
            'sharpe_ratio': sharpe,
            'win_rate': win_rate,
            'median_pnl': median_pnl,
            'top_1_contribution': top_1_contribution,
            'top_5_contribution': top_5_contribution,
            'top_10_contribution': top_10_contribution,
            'sector_contribution': sector_contribution,
            'equity_curve': equity_curve,
            'trades_df': trades_df
        }
    
    def run_minimal_ledger(self):
        """
        Run minimal trade ledger with hard sanity checks.
        """
        
        print("Minimal Trade Ledger")
        print("One row per trade, hard sanity checks\n")
        
        # Load data
        if not self.load_data():
            return None
        
        # Calculate regimes
        if not self.calculate_regimes():
            return None
        
        # Generate minimal trades
        trades = self.generate_minimal_trades()
        
        # Run hard sanity checks
        if not self.run_hard_sanity_checks(trades):
            print("SANITY CHECKS FAILED - ABORTING REPORT")
            return None
        
        # Calculate metrics
        metrics = self.calculate_metrics(trades)
        
        # Print results
        self.print_minimal_results(metrics)
        
        return metrics
    
    def print_minimal_results(self, metrics: Dict[str, Any]):
        """
        Print minimal results with validation confirmation.
        """
        
        print(f"\n=== MINIMAL TRADE LEDGER RESULTS ===")
        
        print(f"\nCORE PERFORMANCE:")
        print(f"  Total Return: {metrics['total_return']:+.1%}")
        print(f"  Max Drawdown: {metrics['max_drawdown']:.1f}%")
        print(f"  Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        print(f"  Win Rate: {metrics['win_rate']:.1%}")
        print(f"  Total Trades: {metrics['total_trades']}")
        print(f"  Median P&L: ${metrics['median_pnl']:,.0f}")
        
        print(f"\nCONCENTRATION ANALYSIS:")
        print(f"  Top 1 Contribution: {metrics['top_1_contribution']:.1f}%")
        print(f"  Top 5 Contribution: {metrics['top_5_contribution']:.1f}%")
        print(f"  Top 10 Contribution: {metrics['top_10_contribution']:.1f}%")
        
        print(f"\nSECTOR BREAKDOWN:")
        for sector, contribution in metrics['sector_contribution'].items():
            print(f"  {sector}: {contribution:.1f}%")
        
        print(f"\n=== VALIDATION CONFIRMED ===")
        
        sector_total = metrics['sector_contribution'].sum()
        
        print(f"  Top 5 <= 100%: {'PASS' if metrics['top_5_contribution'] <= 100 else 'FAIL'}")
        print(f"  Top 10 <= 100%: {'PASS' if metrics['top_10_contribution'] <= 100 else 'FAIL'}")
        print(f"  Sector sum: {sector_total:.1f}%")
        print(f"  Trade IDs unique: {'PASS' if len(metrics['trades_df']['trade_id'].unique()) == len(metrics['trades_df']) else 'FAIL'}")
        
        print(f"\n=== FINAL ASSESSMENT ===")
        
        if metrics['top_5_contribution'] <= 50:
            print("CONTROLLED EDGE - Risk structure validated")
        elif metrics['top_5_contribution'] <= 80:
            print("MODERATE CONCENTRATION - Acceptable risk structure")
        else:
            print("HIGH CONCENTRATION - Risk structure needs attention")
        
        print(f"\nEdge Classification:")
        if metrics['top_5_contribution'] <= 50:
            print("  Type: STABLE DISTRIBUTION")
        elif metrics['top_5_contribution'] <= 80:
            print("  Type: CONTROLLED CONVEX")
        else:
            print("  Type: UNCONTROLLED CONVEX")
        
        print(f"\nACCOUNTING VALIDATION COMPLETE")
        print(f"   Risk structure is now trustworthy")


def main():
    """Main minimal ledger function"""
    
    ledger = MinimalTradeLedger()
    metrics = ledger.run_minimal_ledger()
    
    return ledger, metrics


if __name__ == "__main__":
    ledger, metrics = main()
