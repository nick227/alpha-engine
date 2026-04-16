"""
Bear Expansion Parameter Sweep

Simplified optimization without VectorBT dependencies.
Focus on finding stable parameter neighborhoods for the bear expansion edge.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from itertools import product

import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime


class BearExpansionParameterSweep:
    """
    Parameter sweep for bear expansion strategy.
    
    Strategy is frozen:
    - Regime: (BEAR, EXPANSION) only
    - Direction: short only
    - Entry: position_in_range band
    - No ranking, no extra regimes, no blending
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        self.sweep_results = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for parameter sweep...")
        
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
                        trend_regime == TrendRegime.BEAR and 
                        volatility_regime == VolatilityRegime.EXPANSION
                    )
                })
        
        self.regime_data = pd.DataFrame(regime_data)
        print(f"Calculated regimes for {len(self.regime_data)} data points")
        
        return True
    
    def simulate_strategy(
        self, 
        min_range: float, 
        max_range: float, 
        stop_multiplier: float, 
        target_multiplier: float,
        max_positions: int,
        portfolio_heat: float,
        max_hold_days: int = 10
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
        
        # Simulation variables
        capital = 100000.0
        positions = {}
        trades = []
        equity_curve = [capital]
        
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
                        'position_in_range': entry['position_in_range']
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
        
        # Check criteria
        passes_criteria = (
            max_drawdown < 0.18 and  # Under 18% drawdown
            sharpe > 1.0 and        # Above 1.0 Sharpe
            len(trades) >= 10       # Minimum trades
        )
        
        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe,
            'trade_count': len(trades),
            'win_rate': win_rate,
            'avg_trade_pnl': avg_trade_pnl,
            'passes_criteria': passes_criteria,
            'final_capital': capital,
            'trades': trades
        }
    
    def run_parameter_sweep(self):
        """Run parameter sweep"""
        
        print("\n=== RUNNING PARAMETER SWEEP ===")
        
        # Parameter grid
        entry_bands = [(0.15, 0.25), (0.20, 0.30), (0.25, 0.35)]
        stop_multipliers = [1.25, 1.50, 1.75]
        target_multipliers = [3.0]  # Fixed for now
        max_positions_list = [2, 3, 4]
        portfolio_heats = [0.075, 0.10, 0.125]
        
        results = []
        total_combinations = len(entry_bands) * len(stop_multipliers) * len(max_positions_list) * len(portfolio_heats)
        
        print(f"Testing {total_combinations} parameter combinations...")
        
        for i, (entry_band, stop_mult, target_mult, max_pos, portfolio_heat) in enumerate(
            product(entry_bands, stop_multipliers, target_multipliers, max_positions_list, portfolio_heats)
        ):
            min_range, max_range = entry_band
            print(f"\n[{i+1}/{total_combinations}] Testing: {min_range:.2f}-{max_range:.2f}, {stop_mult:.2f}x stop, {max_pos} pos, {portfolio_heat:.1%} heat")
            
            try:
                result = self.simulate_strategy(
                    min_range=min_range,
                    max_range=max_range,
                    stop_multiplier=stop_mult,
                    target_multiplier=target_mult,
                    max_positions=max_pos,
                    portfolio_heat=portfolio_heat
                )
                
                # Add parameters to result
                result.update({
                    'entry_min_range': min_range,
                    'entry_max_range': max_range,
                    'stop_multiplier': stop_mult,
                    'target_multiplier': target_mult,
                    'max_positions': max_pos,
                    'portfolio_heat': portfolio_heat,
                    'risk_per_trade': portfolio_heat / max_pos
                })
                
                results.append(result)
                
                status = "PASS" if result['passes_criteria'] else "FAIL"
                print(f"  {status}: {result['total_return']:.1%} return, {result['max_drawdown']:.1%} DD, {result['sharpe_ratio']:.2f} Sharpe, {result['trade_count']} trades")
                
            except Exception as e:
                print(f"  ERROR: {e}")
                continue
        
        # Convert to DataFrame and sort
        self.sweep_results = pd.DataFrame(results)
        
        if len(self.sweep_results) > 0:
            # Sort by criteria order: drawdown (ascending), sharpe (descending), return (descending)
            self.sweep_results = self.sweep_results.sort_values(
                by=['max_drawdown', 'sharpe_ratio', 'total_return'],
                ascending=[True, False, False]
            )
        
        return self.sweep_results
    
    def analyze_results(self):
        """Analyze sweep results"""
        
        if self.sweep_results is None or len(self.sweep_results) == 0:
            print("No results to analyze")
            return None, None
        
        print(f"\n=== SWEEP RESULTS ===")
        print(f"Total parameter combinations tested: {len(self.sweep_results)}")
        
        # Filter passing candidates
        passing = self.sweep_results[self.sweep_results['passes_criteria']]
        print(f"Passing candidates: {len(passing)}")
        
        if len(passing) == 0:
            print("No candidates passed the criteria")
            return None, None
        
        print(f"\n=== TOP PASSING CANDIDATES ===")
        
        # Show top 10 passing candidates
        top_candidates = passing.head(10)
        
        for idx, (_, row) in enumerate(top_candidates.iterrows()):
            print(f"\nCandidate {idx+1}:")
            print(f"  Entry band: {row['entry_min_range']:.2f}-{row['entry_max_range']:.2f}")
            print(f"  Stop multiplier: {row['stop_multiplier']:.2f}x ATR")
            print(f"  Max positions: {row['max_positions']}")
            print(f"  Portfolio heat: {row['portfolio_heat']:.1%}")
            print(f"  Risk per trade: {row['risk_per_trade']:.1%}")
            print(f"  Return: {row['total_return']:.1%}")
            print(f"  Drawdown: {row['max_drawdown']:.1%}")
            print(f"  Sharpe: {row['sharpe_ratio']:.2f}")
            print(f"  Trades: {row['trade_count']}")
            print(f"  Win rate: {row['win_rate']:.1%}")
        
        # Analyze parameter stability
        print(f"\n=== PARAMETER STABILITY ANALYSIS ===")
        
        # Entry band stability
        entry_band_performance = passing.groupby(['entry_min_range', 'entry_max_range']).agg({
            'total_return': 'mean',
            'max_drawdown': 'mean',
            'sharpe_ratio': 'mean',
            'trade_count': 'mean'
        }).round(3)
        
        print(f"\nEntry band performance:")
        for (min_r, max_r), row in entry_band_performance.iterrows():
            print(f"  {min_r:.2f}-{max_r:.2f}: {row['total_return']:.1%} return, {row['max_drawdown']:.1%} DD, {row['sharpe_ratio']:.2f} Sharpe")
        
        # Stop multiplier stability
        stop_performance = passing.groupby('stop_multiplier').agg({
            'total_return': 'mean',
            'max_drawdown': 'mean',
            'sharpe_ratio': 'mean',
            'trade_count': 'mean'
        }).round(3)
        
        print(f"\nStop multiplier performance:")
        for stop_mult, row in stop_performance.iterrows():
            print(f"  {stop_mult:.2f}x: {row['total_return']:.1%} return, {row['max_drawdown']:.1%} DD, {row['sharpe_ratio']:.2f} Sharpe")
        
        # Position limits stability
        pos_performance = passing.groupby('max_positions').agg({
            'total_return': 'mean',
            'max_drawdown': 'mean',
            'sharpe_ratio': 'mean',
            'trade_count': 'mean'
        }).round(3)
        
        print(f"\nPosition limits performance:")
        for max_pos, row in pos_performance.iterrows():
            print(f"  {max_pos}: {row['total_return']:.1%} return, {row['max_drawdown']:.1%} DD, {row['sharpe_ratio']:.2f} Sharpe")
        
        # Find stable parameter neighborhoods
        print(f"\n=== STABLE PARAMETER NEIGHBORHOODS ===")
        
        stable_candidates = []
        
        for _, row in top_candidates.iterrows():
            # Find similar candidates
            similar = passing[
                (abs(passing['entry_min_range'] - row['entry_min_range']) < 0.05) &
                (abs(passing['entry_max_range'] - row['entry_max_range']) < 0.05) &
                (abs(passing['stop_multiplier'] - row['stop_multiplier']) < 0.25)
            ]
            
            if len(similar) >= 3:  # At least 3 similar candidates
                stable_candidates.append({
                    'entry_min_range': row['entry_min_range'],
                    'entry_max_range': row['entry_max_range'],
                    'stop_multiplier': row['stop_multiplier'],
                    'similar_count': len(similar),
                    'avg_return': similar['total_return'].mean(),
                    'avg_drawdown': similar['max_drawdown'].mean(),
                    'avg_sharpe': similar['sharpe_ratio'].mean()
                })
        
        if stable_candidates:
            stable_df = pd.DataFrame(stable_candidates)
            stable_df = stable_df.drop_duplicates()
            
            print(f"Found {len(stable_df)} stable parameter neighborhoods:")
            for idx, row in stable_df.iterrows():
                print(f"  {row['entry_min_range']:.2f}-{row['entry_max_range']:.2f}, {row['stop_multiplier']:.2f}x stop: {row['similar_count']} similar combos, {row['avg_return']:.1%} avg return")
        else:
            print("No stable parameter neighborhoods found")
        
        return top_candidates, stable_candidates
    
    def get_top_parameters(self, count: int = 5) -> List[Dict]:
        """Get top parameter sets for Alpha Engine testing"""
        
        if self.sweep_results is None or len(self.sweep_results) == 0:
            return []
        
        passing = self.sweep_results[self.sweep_results['passes_criteria']]
        
        if len(passing) == 0:
            return []
        
        top_params = []
        
        for _, row in passing.head(count).iterrows():
            top_params.append({
                'entry_min_range': row['entry_min_range'],
                'entry_max_range': row['entry_max_range'],
                'stop_multiplier': row['stop_multiplier'],
                'target_multiplier': row['target_multiplier'],
                'max_positions': int(row['max_positions']),
                'portfolio_heat': row['portfolio_heat'],
                'risk_per_trade': row['risk_per_trade'],
                'expected_return': row['total_return'],
                'expected_drawdown': row['max_drawdown'],
                'expected_sharpe': row['sharpe_ratio'],
                'expected_trades': int(row['trade_count']),
                'expected_win_rate': row['win_rate']
            })
        
        return top_params


def main():
    """Main sweep function"""
    
    print("Bear Expansion Parameter Sweep")
    print("Focused optimization for validated edge\n")
    
    # Initialize sweeper
    sweeper = BearExpansionParameterSweep()
    
    # Load data
    if not sweeper.load_data():
        return
    
    # Calculate regimes
    if not sweeper.calculate_regimes():
        return
    
    # Run sweep
    results = sweeper.run_parameter_sweep()
    
    # Analyze results
    top_candidates, stable_neighborhoods = sweeper.analyze_results()
    
    # Get top parameters for Alpha Engine
    top_params = sweeper.get_top_parameters()
    
    print(f"\n=== TOP PARAMETERS FOR ALPHA ENGINE ===")
    for i, params in enumerate(top_params):
        print(f"\nParameter Set {i+1}:")
        print(f"  Entry band: {params['entry_min_range']:.2f}-{params['entry_max_range']:.2f}")
        print(f"  Stop multiplier: {params['stop_multiplier']:.2f}x ATR")
        print(f"  Target multiplier: {params['target_multiplier']:.2f}x ATR")
        print(f"  Max positions: {params['max_positions']}")
        print(f"  Portfolio heat: {params['portfolio_heat']:.1%}")
        print(f"  Risk per trade: {params['risk_per_trade']:.1%}")
        print(f"  Expected performance: {params['expected_return']:.1%} return, {params['expected_drawdown']:.1%} DD, {params['expected_sharpe']:.2f} Sharpe")
        print(f"  Expected trades: {params['expected_trades']}, win rate: {params['expected_win_rate']:.1%}")
    
    return sweeper, results, top_params


if __name__ == "__main__":
    sweeper, results, top_params = main()
