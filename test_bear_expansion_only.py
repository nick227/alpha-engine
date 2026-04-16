"""
Test Bear Expansion Single-Regime Strategy

Isolated test of the validated edge:
- ONLY (BEAR, EXPANSION) regime
- Position in range < 0.4
- Short positions only
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from app.trading.bear_expansion_trader import BearExpansionTrader, BearExpansionConfig
from app.core.regime_v3 import RegimeClassifierV3, TrendRegime, VolatilityRegime


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
        volume
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
        
        if len(ticker_data) < 200:  # Need at least 200 days for MA200
            continue
        
        # Calculate moving averages
        ticker_data['ma50'] = ticker_data['close'].rolling(window=50, min_periods=50).mean()
        ticker_data['ma200'] = ticker_data['close'].rolling(window=200, min_periods=200).mean()
        
        # Calculate ATR
        ticker_data['prev_close'] = ticker_data['close'].shift(1)
        ticker_data['tr'] = abs(ticker_data['close'] - ticker_data['prev_close'])
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


def test_bear_expansion_strategy(df):
    """Test the isolated bear expansion strategy"""
    
    print("\n=== BEAR EXPANSION SINGLE-REGIME TEST ===")
    print("Isolated strategy: ONLY (BEAR, EXPANSION) + position_in_range < 0.4")
    
    # Initialize trader with conservative config
    config = BearExpansionConfig(
        max_position_in_range=0.4,       # Enter below 40% of range
        base_risk_per_trade=0.02,        # 2% risk per trade
        atr_stop_multiplier=2.0,         # Stop at 2x ATR
        atr_target_multiplier=3.0,       # Target at 3x ATR
        max_concurrent_positions=5,       # Max 5 positions
        max_portfolio_heat=0.15,         # 15% max exposure
        use_trailing_stop=False,         # Simple stop for now
        max_hold_days=10                 # Max 10 days hold
    )
    
    trader = BearExpansionTrader(initial_capital=100000.0, config=config)
    
    # Simulate trading day by day
    trading_days = sorted(df['date'].unique())
    
    print(f"Simulating {len(trading_days)} trading days...")
    
    for day_idx, current_date in enumerate(trading_days):
        if day_idx % 100 == 0:
            print(f"  Processing day {day_idx + 1}/{len(trading_days)}: {current_date.strftime('%Y-%m-%d')}")
        
        # Get current day's data
        day_data = df[df['date'] == current_date]
        
        # Update existing positions
        market_data = {}
        for _, row in day_data.iterrows():
            market_data[row['ticker']] = {
                'price': row['close'],
                'atr': row['atr']
            }
        
        trader.update_positions(market_data)
        
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
                
                from app.core.regime_v3 import RegimeClassification
                regime = RegimeClassification(
                    trend_regime=trend_regime,
                    volatility_regime=volatility_regime,
                    combined_regime=f"({trend_regime}, {volatility_regime})",
                    price_vs_ma50=price_vs_ma50,
                    ma50_vs_ma200=ma50_vs_ma200,
                    atr_percentile=atr_percentile,
                    volatility_value=row['atr'] / row['close']
                )
                
                # Calculate position in range
                position_in_range = (row['close'] - row['low_20d']) / (row['high_20d'] - row['low_20d'])
                
                # Enter trade (only if bear expansion criteria met)
                trader.enter_trade(
                    ticker=ticker,
                    entry_price=row['close'],
                    regime=regime,
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
    print(f"\n=== BEAR EXPANSION STRATEGY RESULTS ===")
    
    print(f"\nPortfolio Performance:")
    print(f"  Initial Capital: ${summary['capital']['initial']:,.0f}")
    print(f"  Final Capital: ${summary['capital']['current']:,.0f}")
    print(f"  Total P&L: ${summary['capital']['total_pnl']:,.0f} ({summary['capital']['pnl_percentage']:+.1f}%)")
    print(f"  Total Trades: {summary['performance']['total_trades']}")
    print(f"  Win Rate: {summary['performance']['win_rate']:.1%}")
    print(f"  Avg P&L per Trade: ${summary['performance']['avg_trade_pnl']:+.2f}")
    print(f"  Max Drawdown: {summary['performance']['max_drawdown']:.1f}%")
    print(f"  Sharpe Ratio: {summary['performance']['sharpe_ratio']:.2f}")
    
    print(f"\nEntry Statistics:")
    print(f"  Entry Attempts: {summary['entry_stats']['entry_attempts']}")
    print(f"  Entry Rejections: {summary['entry_stats']['entry_rejections']}")
    print(f"  Acceptance Rate: {summary['entry_stats']['acceptance_rate']:.1%}")
    
    print(f"\nRisk Management:")
    print(f"  Base Risk per Trade: {summary['risk_metrics']['base_risk_per_trade']:.1%}")
    print(f"  Max Concurrent Positions: {summary['risk_metrics']['max_concurrent_positions']}")
    print(f"  Max Portfolio Heat: {summary['risk_metrics']['max_portfolio_heat']:.1%}")
    
    # Trade analysis
    if trade_details:
        trades_df = pd.DataFrame(trade_details)
        
        print(f"\nTrade Analysis:")
        print(f"  Average Position in Range: {trades_df['position_in_range'].mean():.1%}")
        print(f"  Average Hold Days: {trades_df['hold_days'].mean():.1f}")
        print(f"  Exit Reasons:")
        for reason, count in trades_df['exit_reason'].value_counts().items():
            print(f"    {reason}: {count}")
        
        # Performance by position in range
        print(f"\nPerformance by Entry Location:")
        ranges = [(0.0, 0.2), (0.2, 0.3), (0.3, 0.4)]
        for low, high in ranges:
            subset = trades_df[(trades_df['position_in_range'] >= low) & (trades_df['position_in_range'] < high)]
            if len(subset) > 0:
                win_rate = subset['win'].mean()
                avg_pnl = subset['realized_pnl'].mean()
                print(f"  {low:.1f}-{high:.1f} range: {len(subset)} trades, {win_rate:.1%} win rate, {avg_pnl:+.2f} avg P&L")
    
    # Assessment
    print(f"\n=== ASSESSMENT ===")
    
    total_return = summary['capital']['pnl_percentage']
    win_rate = summary['performance']['win_rate']
    sharpe = summary['performance']['sharpe_ratio']
    max_dd = summary['performance']['max_drawdown']
    
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
    elif max_dd < 20:
        risk_rating = "MODERATE"
    else:
        risk_rating = "HIGH"
    
    # Overall rating
    if total_return > 5 and max_dd < 15 and sharpe > 1.0:
        overall_rating = "TRADEABLE EDGE"
    elif total_return > 0 and max_dd < 25:
        overall_rating = "PROMISING"
    else:
        overall_rating = "NEEDS WORK"
    
    print(f"  Performance: {perf_rating} ({total_return:+.1f}% return)")
    print(f"  Risk: {risk_rating} ({max_dd:.1f}% max drawdown)")
    print(f"  Win Rate: {win_rate:.1%}")
    print(f"  Sharpe: {sharpe:.2f}")
    print(f"  Overall: {overall_rating}")
    
    # Compare to baseline
    print(f"\n=== BASELINE COMPARISON ===")
    
    if len(df) > 0:
        first_price = df.groupby('ticker')['close'].first().mean()
        last_price = df.groupby('ticker')['close'].last().mean()
        baseline_return = ((last_price - first_price) / first_price) * 100
        
        print(f"  Buy-and-Hold Return: {baseline_return:+.1f}%")
        print(f"  Bear Expansion Return: {total_return:+.1f}%")
        print(f"  Alpha: {total_return - baseline_return:+.1f}%")
    
    return {
        'summary': summary,
        'trade_details': trade_details,
        'total_return': total_return,
        'win_rate': win_rate,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'overall_rating': overall_rating
    }


def main():
    """Main test function"""
    
    print("Bear Expansion Single-Regime Strategy Test")
    print("Testing isolated validated edge\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Test the isolated strategy
    results = test_bear_expansion_strategy(df)
    
    return results


if __name__ == "__main__":
    results = main()
