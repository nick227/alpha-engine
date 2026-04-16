"""
Controlled Convex Edge Implementation

Transform hybrid edge with uncontrolled spikes into:
controlled convex edge with repeatable distribution.

Goal: Reduce Top 5 contribution to 30-50% while keeping:
- median P&L > 0
- expectancy > 0
- DD ≤ current or lower
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


class ControlledConvexEdge:
    """
    Controlled convex edge with concentration management.
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        self.trades = []
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for controlled convex edge...")
        
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
            
            # Calculate additional indicators for position scaling
            ticker_data['atr_5d'] = ticker_data['atr'].rolling(window=5, min_periods=5).mean()
            ticker_data['atr_20d'] = ticker_data['atr'].rolling(window=20, min_periods=20).mean()
            
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
        self.df = self.df.dropna(subset=['ma50', 'ma200', 'atr', 'position_in_range', 'atr_5d', 'atr_20d'])
        
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
                    'atr_5d': row['atr_5d'],
                    'atr_20d': row['atr_20d'],
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
    
    def calculate_position_scaling(self, row: Dict[str, Any]) -> float:
        """
        Calculate position scaling factor based on market conditions.
        """
        
        # Volatility factor (expansion speed)
        vol_factor = np.clip(row['atr_5d'] / row['atr_20d'], 0.8, 1.6)  # >1 = expanding fast
        
        # Stretch factor (how late in the move)
        stretch = np.clip(row['position_in_range'], 0.3, 0.5)  # entry band ~0.30-0.40
        late_penalty = 1 + 2 * (stretch - 0.3)  # 1→1.2 as you get "late"
        
        # Size multiplier
        size_mult = 1 / (vol_factor * late_penalty)
        
        # Cap shrink at 50-100%
        return np.clip(size_mult, 0.5, 1.0)
    
    def simulate_controlled_edge(self):
        """
        Simulate controlled convex edge with concentration management.
        """
        
        print("\n=== SIMULATING CONTROLLED CONVEX EDGE ===")
        
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
        positions = {}
        trades = []
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
            
            for pos_id, position in positions.items():
                ticker = position['ticker']
                
                # Get current price
                current_price_data = day_data[day_data['ticker'] == ticker]
                if len(current_price_data) == 0:
                    continue
                
                current_price = current_price_data.iloc[0]['close']
                high_price = current_price_data.iloc[0]['high']
                low_price = current_price_data.iloc[0]['low']
                
                # Calculate current P&L in R multiples
                entry_price = position['entry_price']
                atr = position['atr']
                current_r = (entry_price - current_price) / atr  # For short positions
                
                # Check profit scaling
                should_exit = False
                exit_reason = ""
                exit_price = current_price
                
                # Stop loss
                if high_price >= position['stop_loss']:
                    should_exit = True
                    exit_reason = "stop_loss"
                    exit_price = position['stop_loss']
                
                # Profit scaling: TP1 at +1R
                elif current_r >= 1.0 and not position.get('tp1_done', False):
                    should_exit = True
                    exit_reason = "tp1_partial"
                    exit_price = entry_price - (1.0 * atr)  # Exit half at +1R
                
                # Full target
                elif low_price <= position['target_price']:
                    should_exit = True
                    exit_reason = "target_reached"
                    exit_price = position['target_price']
                
                # Max hold period
                elif (current_date - position['entry_date']).days >= 7:
                    should_exit = True
                    exit_reason = "max_hold"
                    exit_price = current_price
                
                if should_exit:
                    positions_to_close.append((pos_id, exit_price, exit_reason, current_r))
            
            # Close positions
            for pos_id, exit_price, exit_reason, current_r in positions_to_close:
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
                    'position_in_range': position['position_in_range'],
                    'size_multiplier': position['size_multiplier'],
                    'tp1_done': position.get('tp1_done', False)
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
                    
                    # Calculate position size with scaling
                    atr = entry['atr']
                    entry_price = entry['close']
                    
                    # Position scaling
                    size_multiplier = self.calculate_position_scaling(entry)
                    scaled_risk_per_trade = risk_per_trade * size_multiplier
                    
                    # For short positions
                    stop_loss = entry_price + (atr * stop_multiplier)
                    target_price = entry_price - (atr * target_multiplier)
                    
                    # Calculate quantity based on scaled risk
                    risk_amount = capital * scaled_risk_per_trade
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
                        'position_in_range': entry['position_in_range'],
                        'size_multiplier': size_multiplier,
                        'tp1_done': False
                    }
                    
                    positions[str(len(positions))] = position
                    last_entry_dates[ticker] = current_date
        
        self.trades = trades
        print(f"Generated {len(trades)} controlled trades")
        
        return trades
    
    def analyze_concentration(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze concentration metrics.
        """
        
        if not trades:
            return {}
        
        trades_df = pd.DataFrame(trades)
        total_pnl = trades_df['pnl'].sum()
        
        # Top trade contributions
        trades_df['pnl_abs'] = abs(trades_df['pnl'])
        top_1_contribution = trades_df.nlargest(1, 'pnl_abs')['pnl_abs'].sum() / abs(total_pnl) * 100
        top_5_contribution = trades_df.nlargest(5, 'pnl_abs')['pnl_abs'].sum() / abs(total_pnl) * 100
        top_10_contribution = trades_df.nlargest(10, 'pnl_abs')['pnl_abs'].sum() / abs(total_pnl) * 100
        
        # Sector contributions
        sector_pnl = trades_df.groupby('sector')['pnl'].sum()
        sector_contribution = (sector_pnl.abs() / abs(total_pnl) * 100).round(1)
        
        # Exit reason analysis
        exit_reasons = trades_df['exit_reason'].value_counts()
        tp1_trades = trades_df[trades_df['exit_reason'] == 'tp1_partial']
        
        return {
            'total_trades': len(trades),
            'total_pnl': total_pnl,
            'total_return': (total_pnl / 100000.0) * 100,
            'top_1_contribution': top_1_contribution,
            'top_5_contribution': top_5_contribution,
            'top_10_contribution': top_10_contribution,
            'sector_contribution': sector_contribution.to_dict(),
            'exit_reasons': exit_reasons.to_dict(),
            'tp1_trades': len(tp1_trades),
            'tp1_avg_pnl': tp1_trades['pnl'].mean() if len(tp1_trades) > 0 else 0
        }
    
    def calculate_performance_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate comprehensive performance metrics.
        """
        
        if not trades:
            return {}
        
        trades_df = pd.DataFrame(trades)
        
        # Basic metrics
        total_pnl = trades_df['pnl'].sum()
        total_return = (total_pnl / 100000.0) * 100
        win_rate = len(trades_df[trades_df['pnl'] > 0]) / len(trades_df)
        
        # Trade quality metrics
        median_pnl = trades_df['pnl'].median()
        mean_pnl = trades_df['pnl'].mean()
        std_pnl = trades_df['pnl'].std()
        
        median_winner = trades_df[trades_df['pnl'] > 0]['pnl'].median() if len(trades_df[trades_df['pnl'] > 0]) > 0 else 0
        median_loser = trades_df[trades_df['pnl'] < 0]['pnl'].median() if len(trades_df[trades_df['pnl'] < 0]) > 0 else 0
        
        # Expectancy
        avg_winner = trades_df[trades_df['pnl'] > 0]['pnl'].mean() if len(trades_df[trades_df['pnl'] > 0]) > 0 else 0
        avg_loser = trades_df[trades_df['pnl'] < 0]['pnl'].mean() if len(trades_df[trades_df['pnl'] < 0]) > 0 else 0
        expectancy = (win_rate * avg_winner) + ((1 - win_rate) * avg_loser)
        
        # Drawdown calculation
        equity_curve = []
        running_capital = 100000.0
        
        for _, trade in trades_df.iterrows():
            running_capital += trade['pnl']
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
        
        # Profit factor
        winning_pnl = trades_df[trades_df['pnl'] > 0]['pnl'].sum()
        losing_pnl = abs(trades_df[trades_df['pnl'] < 0]['pnl'].sum())
        profit_factor = winning_pnl / losing_pnl if losing_pnl > 0 else 0.0
        
        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown * 100,
            'sharpe_ratio': sharpe,
            'win_rate': win_rate,
            'median_pnl': median_pnl,
            'mean_pnl': mean_pnl,
            'std_pnl': std_pnl,
            'median_winner': median_winner,
            'median_loser': median_loser,
            'expectancy': expectancy,
            'profit_factor': profit_factor
        }
    
    def run_controlled_edge_test(self):
        """
        Run complete controlled edge test and analysis.
        """
        
        print("Controlled Convex Edge Test")
        print("Implementing concentration controls while preserving convexity\n")
        
        # Load data
        if not self.load_data():
            return None
        
        # Calculate regimes
        if not self.calculate_regimes():
            return None
        
        # Simulate controlled edge
        trades = self.simulate_controlled_edge()
        
        # Analyze concentration
        concentration = self.analyze_concentration(trades)
        
        # Calculate performance metrics
        performance = self.calculate_performance_metrics(trades)
        
        # Print results
        self.print_results(concentration, performance)
        
        return {
            'trades': trades,
            'concentration': concentration,
            'performance': performance
        }
    
    def print_results(self, concentration: Dict[str, Any], performance: Dict[str, Any]):
        """
        Print comprehensive results.
        """
        
        print(f"\n=== CONTROLLED CONVEX EDGE RESULTS ===")
        
        print(f"\nCORE PERFORMANCE:")
        print(f"  Total Return: {performance['total_return']:+.1%}")
        print(f"  Max Drawdown: {performance['max_drawdown']:.1%}")
        print(f"  Sharpe Ratio: {performance['sharpe_ratio']:.2f}")
        print(f"  Win Rate: {performance['win_rate']:.1%}")
        print(f"  Profit Factor: {performance['profit_factor']:.2f}")
        
        print(f"\nTRADE QUALITY:")
        print(f"  Median P&L: ${performance['median_pnl']:,.0f}")
        print(f"  Mean P&L: ${performance['mean_pnl']:,.0f}")
        print(f"  Std P&L: ${performance['std_pnl']:,.0f}")
        print(f"  Median Winner: ${performance['median_winner']:,.0f}")
        print(f"  Median Loser: ${performance['median_loser']:,.0f}")
        print(f"  Expectancy: ${performance['expectancy']:,.0f}")
        
        print(f"\nCONCENTRATION ANALYSIS:")
        print(f"  Total Trades: {concentration['total_trades']}")
        print(f"  Top 1 Contribution: {concentration['top_1_contribution']:.1f}%")
        print(f"  Top 5 Contribution: {concentration['top_5_contribution']:.1f}%")
        print(f"  Top 10 Contribution: {concentration['top_10_contribution']:.1f}%")
        print(f"  TP1 Partial Trades: {concentration['tp1_trades']}")
        print(f"  TP1 Avg P&L: ${concentration['tp1_avg_pnl']:,.0f}")
        
        print(f"\nSECTOR BREAKDOWN:")
        for sector, contribution in concentration['sector_contribution'].items():
            print(f"  {sector}: {contribution:.1f}%")
        
        print(f"\nEXIT REASONS:")
        for reason, count in concentration['exit_reasons'].items():
            print(f"  {reason}: {count}")
        
        print(f"\n=== ACCEPTANCE CRITERIA CHECK ===")
        
        # Check criteria
        top_5_ok = concentration['top_5_contribution'] <= 50
        return_positive = performance['total_return'] > 0
        median_positive = performance['median_pnl'] > 0
        expectancy_positive = performance['expectancy'] > 0
        
        # Check sector dominance
        max_sector_contribution = max(concentration['sector_contribution'].values()) if concentration['sector_contribution'] else 0
        sector_dominance_ok = max_sector_contribution <= 60
        
        print(f"  Top 5 Contribution ≤ 50%: {'✅' if top_5_ok else '❌'} ({concentration['top_5_contribution']:.1f}%)")
        print(f"  Return Positive: {'✅' if return_positive else '❌'} ({performance['total_return']:+.1%})")
        print(f"  Median P&L Positive: {'✅' if median_positive else '❌'} (${performance['median_pnl']:,.0f})")
        print(f"  Expectancy Positive: {'✅' if expectancy_positive else '❌'} (${performance['expectancy']:,.0f})")
        print(f"  No Sector Dominance: {'✅' if sector_dominance_ok else '❌'} (max: {max_sector_contribution:.1f}%)")
        
        # Overall assessment
        criteria_met = sum([
            top_5_ok,
            return_positive,
            median_positive,
            expectancy_positive,
            sector_dominance_ok
        ])
        
        print(f"\n=== OVERALL ASSESSMENT ===")
        print(f"Criteria Met: {criteria_met}/5")
        
        if criteria_met >= 4:
            print("🎯 CONTROLLED CONVEX EDGE ACHIEVED")
            print("   Concentration controlled while preserving edge")
        elif criteria_met >= 3:
            print("⚠️  PARTIAL SUCCESS")
            print("   Some criteria met, needs refinement")
        else:
            print("❌ NEEDS MORE WORK")
            print("   Multiple criteria not met")
        
        return criteria_met >= 4


def main():
    """Main test function"""
    
    controller = ControlledConvexEdge()
    results = controller.run_controlled_edge_test()
    
    return controller, results


if __name__ == "__main__":
    controller, results = main()
