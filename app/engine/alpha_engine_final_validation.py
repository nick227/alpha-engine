"""
Alpha Engine Final Validation

Test robust candidate edge in full Alpha Engine with concentration risk analysis.

Parameters based on robustness sweep:
- Entry band: 0.30-0.40
- Stop: 1.25x ATR
- Vol timing: 5-day lookback, 5% increase
- Max positions: 2-3
- Portfolio heat: 8-10%
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


class AlphaEngineFinalValidation:
    """
    Final validation of robust candidate edge in Alpha Engine.
    Focus on concentration risk and stability analysis.
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        self.validation_results = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for Alpha Engine validation...")
        
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
    
    def simulate_alpha_engine(self, min_range: float, max_range: float, stop_multiplier: float,
                           target_multiplier: float, max_positions: int, portfolio_heat: float,
                           volatility_lookback: int, volatility_increase: float) -> Dict[str, Any]:
        """
        Simulate Alpha Engine behavior with full risk controls.
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
                'trades': []
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
        
        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe,
            'trade_count': len(trades),
            'win_rate': win_rate,
            'avg_trade_pnl': avg_trade_pnl,
            'profit_factor': profit_factor,
            'final_capital': capital,
            'trades': trades,
            'equity_curve': equity_curve
        }
    
    def analyze_concentration_risk(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze concentration risk in trades.
        """
        
        if not trades:
            return {}
        
        trades_df = pd.DataFrame(trades)
        
        # 1. Trade contribution analysis
        total_pnl = trades_df['pnl'].sum()
        trades_df['pnl_abs'] = abs(trades_df['pnl'])
        trades_df['pnl_pct'] = trades_df['pnl'] / total_pnl
        
        # Top 5 trades contribution
        top_5_contribution = trades_df.nlargest(5, 'pnl_abs')['pnl_abs'].sum() / abs(total_pnl) * 100
        
        # Top 10 trades contribution
        top_10_contribution = trades_df.nlargest(10, 'pnl_abs')['pnl_abs'].sum() / abs(total_pnl) * 100
        
        # 2. Sector contribution analysis
        sector_pnl = trades_df.groupby('sector')['pnl'].sum().sort_values(ascending=False)
        sector_contribution = (sector_pnl.abs() / abs(total_pnl) * 100).round(1)
        
        # 3. Monthly P&L analysis
        trades_df['month'] = pd.to_datetime(trades_df['entry_date']).dt.to_period('M')
        monthly_pnl = trades_df.groupby('month')['pnl'].sum()
        monthly_contribution = (monthly_pnl.abs() / abs(total_pnl) * 100).round(1)
        
        # 4. Quarterly P&L analysis
        trades_df['quarter'] = pd.to_datetime(trades_df['entry_date']).dt.to_period('Q')
        quarterly_pnl = trades_df.groupby('quarter')['pnl'].sum()
        quarterly_contribution = (quarterly_pnl.abs() / abs(total_pnl) * 100).round(1)
        
        # 5. Rolling 20-trade performance
        trades_df['cumulative_pnl'] = trades_df['pnl'].cumsum()
        rolling_20_trade = []
        
        for i in range(20, len(trades_df) + 1):
            window_trades = trades_df.iloc[i-20:i]
            window_return = window_trades['pnl'].sum() / 100000.0
            rolling_20_trade.append({
                'end_trade': i,
                'window_return': window_return * 100,
                'window_sharpe': self._calculate_window_sharpe(window_trades['pnl'].tolist())
            })
        
        return {
            'trade_contribution': {
                'top_5_pct': top_5_contribution,
                'top_10_pct': top_10_contribution,
                'total_trades': len(trades),
                'total_pnl': total_pnl
            },
            'sector_contribution': sector_contribution.to_dict(),
            'monthly_contribution': monthly_contribution.to_dict(),
            'quarterly_contribution': quarterly_contribution.to_dict(),
            'rolling_20_trade': rolling_20_trade,
            'concentration_risk': {
                'high_concentration': top_5_contribution > 50,
                'sector_dominance': sector_contribution.iloc[0] > 40 if len(sector_contribution) > 0 else False,
                'monthly_volatility': monthly_contribution.std() > 15 if len(monthly_contribution) > 0 else False
            }
        }
    
    def _calculate_window_sharpe(self, pnls: List[float]) -> float:
        """Calculate Sharpe ratio for a window of P&Ls"""
        if len(pnls) < 2:
            return 0.0
        
        returns = np.array(pnls) / 100000.0
        return np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0.0
    
    def run_final_validation(self):
        """
        Run final validation with winning parameters from robustness sweep.
        """
        
        print("\n=== ALPHA ENGINE FINAL VALIDATION ===")
        print("Testing robust candidate edge with concentration risk analysis\n")
        
        # Test both entry bands for stability comparison
        test_configs = [
            {
                'name': 'Winning Band (0.30-0.40)',
                'min_range': 0.30,
                'max_range': 0.40,
                'stop_multiplier': 1.25,
                'target_multiplier': 2.5,
                'max_positions': 2,
                'portfolio_heat': 0.08,
                'volatility_lookback': 5,
                'volatility_increase': 0.05
            },
            {
                'name': 'Previous Band (0.25-0.35)',
                'min_range': 0.25,
                'max_range': 0.35,
                'stop_multiplier': 1.25,
                'target_multiplier': 2.5,
                'max_positions': 2,
                'portfolio_heat': 0.08,
                'volatility_lookback': 5,
                'volatility_increase': 0.05
            }
        ]
        
        results = []
        
        for config in test_configs:
            print(f"\n--- Testing {config['name']} ---")
            
            result = self.simulate_alpha_engine(
                min_range=config['min_range'],
                max_range=config['max_range'],
                stop_multiplier=config['stop_multiplier'],
                target_multiplier=config['target_multiplier'],
                max_positions=config['max_positions'],
                portfolio_heat=config['portfolio_heat'],
                volatility_lookback=config['volatility_lookback'],
                volatility_increase=config['volatility_increase']
            )
            
            # Add concentration risk analysis
            concentration_analysis = self.analyze_concentration_risk(result['trades'])
            
            result.update({
                'config_name': config['name'],
                'concentration_analysis': concentration_analysis
            })
            
            results.append(result)
            
            # Print key metrics
            print(f"Return: {result['total_return']:+.1%}")
            print(f"Drawdown: {result['max_drawdown']:.1%}")
            print(f"Sharpe: {result['sharpe_ratio']:.2f}")
            print(f"Trades: {result['trade_count']}")
            print(f"Win Rate: {result['win_rate']:.1%}")
            print(f"Profit Factor: {result['profit_factor']:.2f}")
            
            if concentration_analysis:
                print(f"Top 5 Trades: {concentration_analysis['trade_contribution']['top_5_pct']:.1f}%")
                print(f"Concentration Risk: {'HIGH' if concentration_analysis['concentration_risk']['high_concentration'] else 'LOW'}")
        
        # Compare results
        print(f"\n=== COMPARISON ===")
        
        for result in results:
            print(f"\n{result['config_name']}:")
            print(f"  Return: {result['total_return']:+.1%}")
            print(f"  Drawdown: {result['max_drawdown']:.1%}")
            print(f"  Sharpe: {result['sharpe_ratio']:.2f}")
            print(f"  Trades: {result['trade_count']}")
            print(f"  Top 5 Contribution: {result['concentration_analysis']['trade_contribution']['top_5_pct']:.1f}%")
            print(f"  Sector Dominance: {result['concentration_analysis']['concentration_risk']['sector_dominance']}")
        
        # Determine if robust
        print(f"\n=== ROBUSTNESS ASSESSMENT ===")
        
        winning_result = results[0]  # 0.30-0.40 band
        previous_result = results[1]  # 0.25-0.35 band
        
        # Check if winning band is clearly better
        is_robust = (
            winning_result['total_return'] > previous_result['total_return'] * 1.2 and  # 20% better
            winning_result['max_drawdown'] <= previous_result['max_drawdown'] * 1.1 and  # Similar drawdown
            winning_result['trade_count'] >= 20 and  # Minimum trades
            winning_result['concentration_analysis']['trade_contribution']['top_5_pct'] < 50  # Not too concentrated
        )
        
        if is_robust:
            print("✅ ROBUST EDGE CONFIRMED")
            print("   - Winning band clearly outperforms previous")
            print("   - Trade count healthy")
            print("   - Concentration risk controlled")
        else:
            print("❌ EDGE NOT ROBUST")
            print("   - Performance not clearly superior")
            print("   - Or concentration risk too high")
        
        return results, is_robust
    
    def print_detailed_analysis(self, results: List[Dict[str, Any]]):
        """
        Print detailed analysis as requested.
        """
        
        winning_result = results[0]
        concentration = winning_result['concentration_analysis']
        
        print(f"\n=== DETAILED ANALYSIS ===")
        
        # Core metrics
        print(f"\nCORE METRICS:")
        print(f"  Return: {winning_result['total_return']:+.1%}")
        print(f"  Drawdown: {winning_result['max_drawdown']:.1%}")
        print(f"  Sharpe: {winning_result['sharpe_ratio']:.2f}")
        print(f"  Trade Count: {winning_result['trade_count']}")
        
        # Concentration risk
        print(f"\nCONCENTRATION RISK:")
        print(f"  Top 5 Trades: {concentration['trade_contribution']['top_5_pct']:.1f}%")
        print(f"  Top 10 Trades: {concentration['trade_contribution']['top_10_pct']:.1f}%")
        print(f"  Risk Level: {'HIGH' if concentration['concentration_risk']['high_concentration'] else 'LOW'}")
        
        # Sector breakdown
        print(f"\nSECTOR BREAKDOWN:")
        for sector, contribution in concentration['sector_contribution'].items():
            print(f"  {sector}: {contribution:.1f}%")
        
        # Monthly P&L
        print(f"\nMONTHLY P&L:")
        for month, contribution in concentration['monthly_contribution'].items():
            pnl = contribution * winning_result['concentration_analysis']['trade_contribution']['total_pnl'] / 100
            print(f"  {month}: {pnl:+,.0f} ({contribution:.1f}%)")
        
        # Quarterly P&L
        print(f"\nQUARTERLY P&L:")
        for quarter, contribution in concentration['quarterly_contribution'].items():
            pnl = contribution * winning_result['concentration_analysis']['trade_contribution']['total_pnl'] / 100
            print(f"  {quarter}: {pnl:+,.0f} ({contribution:.1f}%)")
        
        # Rolling 20-trade performance
        if concentration['rolling_20_trade']:
            print(f"\nROLLING 20-TRADE PERFORMANCE:")
            for window in concentration['rolling_20_trade'][-5:]:  # Last 5 windows
                print(f"  Trades {window['end_trade']-19}-{window['end_trade']}: "
                      f"{window['window_return']:+.1f}% return, {window['window_sharpe']:.2f} Sharpe")


def main():
    """Main validation function"""
    
    print("Alpha Engine Final Validation")
    print("Testing robust candidate edge with concentration risk analysis\n")
    
    # Initialize validator
    validator = AlphaEngineFinalValidation()
    
    # Load data
    if not validator.load_data():
        return
    
    # Calculate regimes
    if not validator.calculate_regimes():
        return
    
    # Run final validation
    results, is_robust = validator.run_final_validation()
    
    # Print detailed analysis
    validator.print_detailed_analysis(results)
    
    # Final assessment
    print(f"\n=== FINAL ASSESSMENT ===")
    
    if is_robust:
        print("✅ PRODUCTION CANDIDATE CONFIRMED")
        print("   - Robust edge with controlled concentration risk")
        print("   - Ready for paper trading validation")
    else:
        print("❌ NOT READY FOR PRODUCTION")
        print("   - Edge not robust or concentration risk too high")
        print("   - Further refinement needed")
    
    return validator, results, is_robust


if __name__ == "__main__":
    validator, results, is_robust = main()
