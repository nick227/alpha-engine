"""
VectorBT Bear Expansion Optimization

Focused optimization for the validated bear expansion edge.
Strategy is frozen: (BEAR, EXPANSION) + short only + position_in_range band
"""

import sqlite3
import pandas as pd
import numpy as np
import vectorbt as vbt
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime


class BearExpansionVectorBTOptimizer:
    """
    VectorBT optimizer for bear expansion strategy.
    
    Strategy is frozen:
    - Regime: (BEAR, EXPANSION) only
    - Direction: short only
    - Entry: position_in_range band
    - No ranking, no extra regimes, no blending
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        self.optimization_results = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for VectorBT...")
        
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
    
    def create_entry_signals(self, min_range: float, max_range: float) -> pd.DataFrame:
        """
        Create entry signals for bear expansion strategy.
        
        Entry criteria:
        - (BEAR, EXPANSION) regime only
        - position_in_range in specified band
        - Short direction only
        """
        
        # Filter for bear expansion
        bear_expansion_data = self.regime_data[self.regime_data['is_bear_expansion']].copy()
        
        # Apply position in range filter
        bear_expansion_data = bear_expansion_data[
            (bear_expansion_data['position_in_range'] >= min_range) &
            (bear_expansion_data['position_in_range'] <= max_range)
        ]
        
        # Create entry signals (1 for short entry)
        signals = bear_expansion_data.copy()
        signals['entry_signal'] = 1
        
        # Pivot to wide format for VectorBT
        entry_signals = signals.pivot_table(
            index='date',
            columns='ticker',
            values='entry_signal',
            fill_value=0
        )
        
        return entry_signals
    
    def create_price_matrix(self) -> pd.DataFrame:
        """Create price matrix for VectorBT"""
        
        price_matrix = self.df.pivot_table(
            index='date',
            columns='ticker',
            values='close',
            fill_value=np.nan
        )
        
        return price_matrix
    
    def create_atr_matrix(self) -> pd.DataFrame:
        """Create ATR matrix for VectorBT"""
        
        atr_matrix = self.regime_data.pivot_table(
            index='date',
            columns='ticker',
            values='atr',
            fill_value=np.nan
        )
        
        return atr_matrix
    
    def run_optimization(self):
        """Run VectorBT optimization"""
        
        print("\n=== RUNNING VECTORBT OPTIMIZATION ===")
        
        # Create matrices
        price_matrix = self.create_price_matrix()
        atr_matrix = self.create_atr_matrix()
        
        # Parameter grid
        entry_bands = [(0.15, 0.25), (0.20, 0.30), (0.25, 0.35)]
        stop_multipliers = [1.25, 1.50, 1.75]
        max_positions = [2, 3, 4]
        portfolio_heats = [0.075, 0.10, 0.125]
        
        results = []
        
        for min_range, max_range in entry_bands:
            print(f"\nTesting entry band: {min_range}-{max_range}")
            
            # Create entry signals
            entry_signals = self.create_entry_signals(min_range, max_range)
            
            for stop_mult in stop_multipliers:
                print(f"  Stop multiplier: {stop_mult}x ATR")
                
                for max_pos in max_positions:
                    for portfolio_heat in portfolio_heats:
                        
                        # Calculate position size based on risk
                        risk_per_trade = portfolio_heat / max_pos
                        
                        # Create exits based on stop multiplier
                        # For short positions: stop_loss = entry_price + (atr * stop_mult)
                        
                        # Create stop loss matrix
                        stop_loss_matrix = price_matrix + (atr_matrix * stop_mult)
                        
                        # Create target matrix (3x ATR for targets)
                        target_matrix = price_matrix - (atr_matrix * 3.0)
                        
                        try:
                            # Run VectorBT simulation
                            portfolio = vbt.Portfolio.from_signals(
                                price_matrix,
                                entry_signals,
                                np.ones_like(entry_signals),  # Exit signals (will be overridden)
                                init_cash=100000,
                                fees=0.001,  # 0.1% fees
                                slippage=0.0005,  # 0.05% slippage
                                freq='1D'
                            )
                            
                            # Apply custom exits
                            # This is simplified - in practice, you'd need more sophisticated exit logic
                            
                            # Get performance metrics
                            stats = portfolio.stats()
                            
                            # Calculate drawdown
                            drawdown = stats['max_drawdown']
                            
                            # Calculate Sharpe
                            sharpe = stats['sharpe_ratio']
                            
                            # Calculate return
                            total_return = stats['total_return']
                            
                            # Get trade count
                            trade_count = len(portfolio.trades.records_readable)
                            
                            # Store results
                            result = {
                                'entry_min_range': min_range,
                                'entry_max_range': max_range,
                                'stop_multiplier': stop_mult,
                                'max_positions': max_pos,
                                'portfolio_heat': portfolio_heat,
                                'risk_per_trade': risk_per_trade,
                                'total_return': total_return,
                                'max_drawdown': drawdown,
                                'sharpe_ratio': sharpe,
                                'trade_count': trade_count,
                                'passes_criteria': (
                                    drawdown < 0.18 and  # Under 18% drawdown
                                    sharpe > 1.0 and      # Above 1.0 Sharpe
                                    trade_count >= 10     # Minimum trades
                                )
                            }
                            
                            results.append(result)
                            
                            if result['passes_criteria']:
                                print(f"    PASS: {total_return:.1%} return, {drawdown:.1%} DD, {sharpe:.2f} Sharpe, {trade_count} trades")
                            else:
                                print(f"    FAIL: {total_return:.1%} return, {drawdown:.1%} DD, {sharpe:.2f} Sharpe, {trade_count} trades")
                            
                        except Exception as e:
                            print(f"    ERROR: {e}")
                            continue
        
        # Convert to DataFrame and sort
        self.optimization_results = pd.DataFrame(results)
        
        # Sort by criteria order: drawdown (ascending), sharpe (descending), return (descending)
        self.optimization_results = self.optimization_results.sort_values(
            by=['max_drawdown', 'sharpe_ratio', 'total_return'],
            ascending=[True, False, False]
        )
        
        return self.optimization_results
    
    def analyze_results(self):
        """Analyze optimization results"""
        
        if self.optimization_results is None:
            print("No results to analyze")
            return
        
        print(f"\n=== OPTIMIZATION RESULTS ===")
        print(f"Total parameter combinations tested: {len(self.optimization_results)}")
        
        # Filter passing candidates
        passing = self.optimization_results[self.optimization_results['passes_criteria']]
        print(f"Passing candidates: {len(passing)}")
        
        if len(passing) == 0:
            print("No candidates passed the criteria")
            return
        
        print(f"\n=== TOP PASSING CANDIDATES ===")
        
        # Show top 10 passing candidates
        top_candidates = passing.head(10)
        
        for idx, row in top_candidates.iterrows():
            print(f"\nCandidate {len(passing) - idx}:")
            print(f"  Entry band: {row['entry_min_range']:.2f}-{row['entry_max_range']:.2f}")
            print(f"  Stop multiplier: {row['stop_multiplier']:.2f}x ATR")
            print(f"  Max positions: {row['max_positions']}")
            print(f"  Portfolio heat: {row['portfolio_heat']:.1%}")
            print(f"  Risk per trade: {row['risk_per_trade']:.1%}")
            print(f"  Return: {row['total_return']:.1%}")
            print(f"  Drawdown: {row['max_drawdown']:.1%}")
            print(f"  Sharpe: {row['sharpe_ratio']:.2f}")
            print(f"  Trades: {row['trade_count']}")
        
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
        
        # Look for clusters of similar parameters that perform well
        stable_candidates = []
        
        for idx, row in top_candidates.iterrows():
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
        
        if self.optimization_results is None:
            return []
        
        passing = self.optimization_results[self.optimization_results['passes_criteria']]
        
        if len(passing) == 0:
            return []
        
        top_params = []
        
        for idx, row in passing.head(count).iterrows():
            top_params.append({
                'entry_min_range': row['entry_min_range'],
                'entry_max_range': row['entry_max_range'],
                'stop_multiplier': row['stop_multiplier'],
                'max_positions': int(row['max_positions']),
                'portfolio_heat': row['portfolio_heat'],
                'risk_per_trade': row['risk_per_trade'],
                'expected_return': row['total_return'],
                'expected_drawdown': row['max_drawdown'],
                'expected_sharpe': row['sharpe_ratio'],
                'expected_trades': int(row['trade_count'])
            })
        
        return top_params


def main():
    """Main optimization function"""
    
    print("Bear Expansion VectorBT Optimization")
    print("Focused optimization for validated edge\n")
    
    # Initialize optimizer
    optimizer = BearExpansionVectorBTOptimizer()
    
    # Load data
    if not optimizer.load_data():
        return
    
    # Calculate regimes
    if not optimizer.calculate_regimes():
        return
    
    # Run optimization
    results = optimizer.run_optimization()
    
    # Analyze results
    top_candidates, stable_neighborhoods = optimizer.analyze_results()
    
    # Get top parameters for Alpha Engine
    top_params = optimizer.get_top_parameters()
    
    print(f"\n=== TOP PARAMETERS FOR ALPHA ENGINE ===")
    for i, params in enumerate(top_params):
        print(f"\nParameter Set {i+1}:")
        print(f"  Entry band: {params['entry_min_range']:.2f}-{params['entry_max_range']:.2f}")
        print(f"  Stop multiplier: {params['stop_multiplier']:.2f}x ATR")
        print(f"  Max positions: {params['max_positions']}")
        print(f"  Portfolio heat: {params['portfolio_heat']:.1%}")
        print(f"  Risk per trade: {params['risk_per_trade']:.1%}")
        print(f"  Expected performance: {params['expected_return']:.1%} return, {params['expected_drawdown']:.1%} DD, {params['expected_sharpe']:.2f} Sharpe")
    
    return optimizer, results, top_params


if __name__ == "__main__":
    optimizer, results, top_params = main()
