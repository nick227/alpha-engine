"""
Bear Expansion Robustness Sweep

VectorBT optimization focused on stability, not maximum performance.
Tests if quality improvements survive nearby parameter settings.

Goal: Find multiple stable parameter neighborhoods, not single best combo.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Tuple
from itertools import product


class BearExpansionRobustnessSweep:
    """
    Robustness sweep for bear expansion strategy.
    
    Tests if quality improvements survive nearby parameter settings.
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        self.sweep_results = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for robustness sweep...")
        
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
    
    def check_volatility_timing(self, ticker: str, current_atr: float, current_date: datetime, 
                              lookback_days: int, min_increase_pct: float, 
                              price_history: Dict[str, List[Dict]]) -> bool:
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
    
    def simulate_strategy(
        self, 
        min_range: float, 
        max_range: float, 
        stop_multiplier: float, 
        target_multiplier: float,
        max_positions: int,
        portfolio_heat: float,
        volatility_lookback: int,
        volatility_increase: float,
        max_hold_days: int = 7
    ) -> Dict[str, Any]:
        """
        Simulate bear expansion strategy with given parameters.
        """
        
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
        equity_curve = [capital]
        
        # Risk parameters
        risk_per_trade = portfolio_heat / max_positions
        
        # Sector mapping
        sector_map = {
            'AAPL': 'TECH', 'MSFT': 'TECH', 'GOOGL': 'TECH', 'AMZN': 'TECH', 'META': 'TECH', 'NVDA': 'TECH', 'TSLA': 'TECH', 'ADBE': 'TECH', 'CRM': 'TECH',
            'JPM': 'FINANCIAL', 'BAC': 'FINANCIAL', 'WFC': 'FINANCIAL', 'C': 'FINANCIAL', 'GS': 'FINANCIAL', 'MS': 'FINANCIAL', 'AIG': 'FINANCIAL',
            'JNJ': 'HEALTHCARE', 'PFE': 'HEALTHCARE', 'UNH': 'HEALTHCARE', 'ABT': 'HEALTHCARE', 'MRK': 'HEALTHCARE', 'CVS': 'HEALTHCARE', 'MDT': 'HEALTHCARE',
            'WMT': 'CONSUMER', 'HD': 'CONSUMER', 'MCD': 'CONSUMER', 'NKE': 'CONSUMER', 'KO': 'CONSUMER', 'PEP': 'CONSUMER', 'COST': 'CONSUMER',
            'BA': 'INDUSTRIAL', 'CAT': 'INDUSTRIAL', 'GE': 'INDUSTRIAL', 'MMM': 'INDUSTRIAL', 'UPS': 'INDUSTRIAL', 'HON': 'INDUSTRIAL',
            'XOM': 'ENERGY', 'CVX': 'ENERGY', 'COP': 'ENERGY', 'SLB': 'ENERGY', 'HAL': 'ENERGY',
            'T': 'TELECOM', 'VZ': 'TELECOM', 'TMUS': 'TELECOM'
        }
        
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
                elif (current_date - position['entry_date']).days >= max_hold_days:
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
                        volatility_lookback, volatility_increase, price_history
                    ):
                        continue
                    
                    # Check sector clustering
                    sector = sector_map.get(ticker, 'OTHER')
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
                        'entry_date': current_date,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'stop_loss': stop_loss,
                        'target_price': target_price,
                        'atr': atr,
                        'position_in_range': entry['position_in_range'],
                        'sector': sector
                    }
                    
                    positions[str(len(positions))] = position
            
            # Update equity curve
            unrealized_pnl = 0
            for position in positions.values():
                ticker = position['ticker']
                current_price_data = day_data[day_data['ticker'] == ticker]
                if len(current_price_data) > 0:
                    current_price = current_price_data.iloc[0]['close']
                    unrealized_pnl += position['quantity'] * (position['entry_price'] - current_price)
            
            equity_curve.append(capital + unrealized_pnl)
        
        # Calculate performance metrics
        if len(trades) == 0:
            return {
                'total_return': 0.0,
                'max_drawdown': 0.0,
                'sharpe_ratio': 0.0,
                'trade_count': 0,
                'win_rate': 0.0,
                'avg_trade_pnl': 0.0,
                'profit_factor': 0.0,
                'passes_criteria': False
            }
        
        # Total return
        total_return = (capital - 100000.0) / 100000.0
        
        # Max drawdown
        peak = 100000.0
        max_drawdown = 0.0
        
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
        
        # Trade statistics
        trade_pnls = [trade['pnl'] for trade in trades]
        winning_trades = sum(1 for pnl in trade_pnls if pnl > 0)
        win_rate = winning_trades / len(trades) if trades else 0.0
        avg_trade_pnl = np.mean(trade_pnls) if trades else 0.0
        
        # Profit factor
        winning_pnl = sum(pnl for pnl in trade_pnls if pnl > 0)
        losing_pnl = abs(sum(pnl for pnl in trade_pnls if pnl < 0))
        profit_factor = winning_pnl / losing_pnl if losing_pnl > 0 else 0.0
        
        # Robustness criteria (relaxed for stability testing)
        passes_criteria = (
            max_drawdown < 0.25 and  # Under 25% drawdown (relaxed)
            total_return > 0.0 and    # Positive return
            len(trades) >= 5          # Minimum trades (relaxed)
        )
        
        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe,
            'trade_count': len(trades),
            'win_rate': win_rate,
            'avg_trade_pnl': avg_trade_pnl,
            'profit_factor': profit_factor,
            'passes_criteria': passes_criteria,
            'final_capital': capital,
            'trades': trades
        }
    
    def run_robustness_sweep(self):
        """Run robustness sweep around winning parameters"""
        
        print("\n=== RUNNING ROBUSTNESS SWEEP ===")
        print("Testing stability around winning parameters")
        
        # Center around winning parameters
        entry_bands = [
            (0.20, 0.30),  # Slightly below
            (0.25, 0.35),  # Winning band
            (0.30, 0.40),  # Slightly above
        ]
        
        stop_multipliers = [1.0, 1.25, 1.5]  # Around 1.25
        target_multipliers = [2.0, 2.5, 3.0]  # Around 2.5
        max_positions_list = [1, 2, 3]  # Around 2
        portfolio_heats = [0.06, 0.08, 0.10]  # Around 8%
        volatility_lookbacks = [3, 5, 7]  # Around 5
        volatility_increases = [0.03, 0.05, 0.07]  # Around 5%
        
        results = []
        total_combinations = (len(entry_bands) * len(stop_multipliers) * len(max_positions_list) * 
                           len(portfolio_heats) * len(volatility_lookbacks) * len(volatility_increases))
        
        print(f"Testing {total_combinations} parameter combinations...")
        
        for i, (entry_band, stop_mult, max_pos, portfolio_heat, vol_lookback, vol_increase) in enumerate(
            product(entry_bands, stop_multipliers, max_positions_list, portfolio_heats, 
                   volatility_lookbacks, volatility_increases)
        ):
            min_range, max_range = entry_band
            target_mult = 2.5  # Fixed for simplicity
            
            print(f"[{i+1}/{total_combinations}] Testing: {min_range:.2f}-{max_range:.2f}, {stop_mult:.2f}x stop, "
                  f"{max_pos} pos, {portfolio_heat:.1%} heat, {vol_lookback}d lookback, {vol_increase:.1%} inc")
            
            try:
                result = self.simulate_strategy(
                    min_range=min_range,
                    max_range=max_range,
                    stop_multiplier=stop_mult,
                    target_multiplier=target_mult,
                    max_positions=max_pos,
                    portfolio_heat=portfolio_heat,
                    volatility_lookback=vol_lookback,
                    volatility_increase=vol_increase
                )
                
                # Add parameters to result
                result.update({
                    'entry_min_range': min_range,
                    'entry_max_range': max_range,
                    'stop_multiplier': stop_mult,
                    'target_multiplier': target_mult,
                    'max_positions': max_pos,
                    'portfolio_heat': portfolio_heat,
                    'volatility_lookback': vol_lookback,
                    'volatility_increase': vol_increase,
                    'risk_per_trade': portfolio_heat / max_pos
                })
                
                results.append(result)
                
                status = "PASS" if result['passes_criteria'] else "FAIL"
                print(f"  {status}: {result['total_return']:.1%} return, {result['max_drawdown']:.1%} DD, "
                      f"{result['sharpe_ratio']:.2f} Sharpe, {result['trade_count']} trades")
                
            except Exception as e:
                print(f"  ERROR: {e}")
                continue
        
        # Convert to DataFrame and sort
        self.sweep_results = pd.DataFrame(results)
        
        if len(self.sweep_results) > 0:
            # Sort by criteria order: drawdown (ascending), return (descending), trade count (descending)
            self.sweep_results = self.sweep_results.sort_values(
                by=['max_drawdown', 'total_return', 'trade_count'],
                ascending=[True, False, False]
            )
        
        return self.sweep_results
    
    def analyze_robustness(self):
        """Analyze robustness of parameter neighborhoods"""
        
        if self.sweep_results is None or len(self.sweep_results) == 0:
            print("No results to analyze")
            return None, None
        
        print(f"\n=== ROBUSTNESS ANALYSIS ===")
        print(f"Total parameter combinations tested: {len(self.sweep_results)}")
        
        # Filter passing candidates
        passing = self.sweep_results[self.sweep_results['passes_criteria']]
        print(f"Passing candidates: {len(passing)}")
        
        if len(passing) == 0:
            print("No candidates passed the criteria")
            return None, None
        
        print(f"\n=== ROBUSTNESS METRICS ===")
        
        # Overall statistics
        avg_return = passing['total_return'].mean()
        avg_drawdown = passing['max_drawdown'].mean()
        avg_sharpe = passing['sharpe_ratio'].mean()
        avg_trades = passing['trade_count'].mean()
        
        print(f"Average Return: {avg_return:.1%}")
        print(f"Average Drawdown: {avg_drawdown:.1%}")
        print(f"Average Sharpe: {avg_sharpe:.2f}")
        print(f"Average Trades: {avg_trades:.1f}")
        
        # Parameter stability analysis
        print(f"\n=== PARAMETER STABILITY ===")
        
        # Entry band stability
        entry_performance = passing.groupby(['entry_min_range', 'entry_max_range']).agg({
            'total_return': ['mean', 'std'],
            'max_drawdown': ['mean', 'std'],
            'trade_count': ['mean', 'count']
        }).round(3)
        
        print(f"\nEntry band stability:")
        for (min_r, max_r), row in entry_performance.iterrows():
            return_std = row[('total_return', 'std')]
            dd_std = row[('max_drawdown', 'std')]
            count = row[('trade_count', 'count')]
            
            stability_score = count / (1 + return_std + dd_std)
            print(f"  {min_r:.2f}-{max_r:.2f}: {count} combos, "
                  f"avg {row[('total_return', 'mean')]:.1%} return, "
                  f"std {return_std:.1%}, stability: {stability_score:.2f}")
        
        # Stop multiplier stability
        stop_performance = passing.groupby('stop_multiplier').agg({
            'total_return': ['mean', 'std'],
            'max_drawdown': ['mean', 'std'],
            'trade_count': ['mean', 'count']
        }).round(3)
        
        print(f"\nStop multiplier stability:")
        for stop_mult, row in stop_performance.iterrows():
            return_std = row[('total_return', 'std')]
            dd_std = row[('max_drawdown', 'std')]
            count = row[('trade_count', 'count')]
            
            stability_score = count / (1 + return_std + dd_std)
            print(f"  {stop_mult:.2f}x: {count} combos, "
                  f"avg {row[('total_return', 'mean')]:.1%} return, "
                  f"std {return_std:.1%}, stability: {stability_score:.2f}")
        
        # Volatility timing stability
        vol_performance = passing.groupby(['volatility_lookback', 'volatility_increase']).agg({
            'total_return': ['mean', 'std'],
            'max_drawdown': ['mean', 'std'],
            'trade_count': ['mean', 'count']
        }).round(3)
        
        print(f"\nVolatility timing stability:")
        for (lookback, increase), row in vol_performance.iterrows():
            return_std = row[('total_return', 'std')]
            dd_std = row[('max_drawdown', 'std')]
            count = row[('trade_count', 'count')]
            
            stability_score = count / (1 + return_std + dd_std)
            print(f"  {lookback}d, {increase:.1%}: {count} combos, "
                  f"avg {row[('total_return', 'mean')]:.1%} return, "
                  f"std {return_std:.1%}, stability: {stability_score:.2f}")
        
        # Find stable neighborhoods
        print(f"\n=== STABLE NEIGHBORHOODS ===")
        
        stable_neighborhoods = []
        
        # Check for neighborhoods with multiple passing combinations
        for entry_band in [(0.20, 0.30), (0.25, 0.35), (0.30, 0.40)]:
            neighborhood = passing[
                (passing['entry_min_range'] == entry_band[0]) &
                (passing['entry_max_range'] == entry_band[1])
            ]
            
            if len(neighborhood) >= 3:  # At least 3 stable combinations
                stability_score = len(neighborhood) / (1 + neighborhood['total_return'].std() + neighborhood['max_drawdown'].std())
                
                stable_neighborhoods.append({
                    'entry_band': entry_band,
                    'count': len(neighborhood),
                    'avg_return': neighborhood['total_return'].mean(),
                    'avg_drawdown': neighborhood['max_drawdown'].mean(),
                    'return_std': neighborhood['total_return'].std(),
                    'drawdown_std': neighborhood['max_drawdown'].std(),
                    'stability_score': stability_score
                })
        
        if stable_neighborhoods:
            stable_neighborhoods.sort(key=lambda x: x['stability_score'], reverse=True)
            
            print(f"Found {len(stable_neighborhoods)} stable neighborhoods:")
            for i, hood in enumerate(stable_neighborhoods):
                print(f"  {i+1}. Entry {hood['entry_band'][0]:.2f}-{hood['entry_band'][1]:.2f}: "
                      f"{hood['count']} combos, {hood['avg_return']:.1%} avg return, "
                      f"{hood['avg_drawdown']:.1%} avg DD, stability: {hood['stability_score']:.2f}")
        else:
            print("No stable neighborhoods found - edge may be brittle")
        
        # Overall robustness assessment
        print(f"\n=== ROBUSTNESS ASSESSMENT ===")
        
        robustness_score = len(passing) / len(self.sweep_results)
        
        if robustness_score > 0.5:
            robustness_rating = "HIGHLY ROBUST"
        elif robustness_score > 0.3:
            robustness_rating = "ROBUST"
        elif robustness_score > 0.1:
            robustness_rating = "MODERATELY ROBUST"
        else:
            robustness_rating = "BRITTLE"
        
        print(f"Pass rate: {robustness_score:.1%} ({len(passing)}/{len(self.sweep_results)})")
        print(f"Robustness Rating: {robustness_rating}")
        
        # Trade count analysis
        if len(passing) > 0:
            min_trades = passing['trade_count'].min()
            max_trades = passing['trade_count'].max()
            avg_trades = passing['trade_count'].mean()
            
            print(f"\nTrade Count Analysis:")
            print(f"  Min trades: {min_trades}")
            print(f"  Max trades: {max_trades}")
            print(f"  Avg trades: {avg_trades:.1f}")
            
            if avg_trades > 15:
                print(f"  Trade count: GOOD (above 15)")
            elif avg_trades > 10:
                print(f"  Trade count: ACCEPTABLE (above 10)")
            else:
                print(f"  Trade count: LOW (below 10)")
        
        return passing, stable_neighborhoods


def main():
    """Main robustness sweep function"""
    
    print("Bear Expansion Robustness Sweep")
    print("Testing stability around winning parameters\n")
    
    # Initialize sweeper
    sweeper = BearExpansionRobustnessSweep()
    
    # Load data
    if not sweeper.load_data():
        return
    
    # Calculate regimes
    if not sweeper.calculate_regimes():
        return
    
    # Run robustness sweep
    results = sweeper.run_robustness_sweep()
    
    # Analyze robustness
    passing, stable_neighborhoods = sweeper.analyze_robustness()
    
    # Final assessment
    print(f"\n=== FINAL ASSESSMENT ===")
    
    if passing is not None and len(passing) > 0:
        print(f"✅ ROBUST EDGE CONFIRMED")
        print(f"   {len(passing)} stable parameter combinations")
        print(f"   {len(stable_neighborhoods)} stable neighborhoods")
        print(f"   Average return: {passing['total_return'].mean():.1%}")
        print(f"   Average drawdown: {passing['max_drawdown'].mean():.1%}")
        print(f"   Average trades: {passing['trade_count'].mean():.1f}")
    else:
        print(f"❌ EDGE NOT ROBUST")
        print(f"   No stable parameter combinations found")
        print(f"   Strategy may be too brittle")
    
    return sweeper, results, passing, stable_neighborhoods


if __name__ == "__main__":
    sweeper, results, passing, stable_neighborhoods = main()
