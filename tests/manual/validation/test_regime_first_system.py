"""
Test Regime-First Trading System

Tests the clean, minimal implementation against historical data.
Focuses on regime-only edge without ranking complexity.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from app.trading.regime_first_trader import RegimeFirstTrader, TradeDirection, RiskParameters
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
        
        # Calculate returns
        ticker_data['return_5d'] = ticker_data['close'].pct_change(5)
        
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


def test_regime_first_system(df):
    """Test the regime-first trading system on historical data"""
    
    print("\n=== REGIME-FIRST SYSTEM TEST ===")
    
    # Initialize trader
    trader = RegimeFirstTrader(
        initial_capital=100000.0,
        risk_params=RiskParameters(
            base_risk_per_trade=0.02,  # 2% risk per trade
            max_portfolio_heat=0.20,   # 20% max exposure
            max_positions_per_regime=5,  # Max 5 positions per regime
            atr_stop_multiplier=2.0,    # 2x ATR stop
            atr_target_multiplier=3.0,  # 3x ATR target
            max_concurrent_positions=10
        )
    )
    
    # Initialize regime classifier
    classifier = RegimeClassifierV3()
    
    # Simulate trading day by day
    trading_days = sorted(df['date'].unique())
    
    print(f"Simulating {len(trading_days)} trading days...")
    
    for day_idx, current_date in enumerate(trading_days):
        if day_idx % 50 == 0:
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
                
                # Check if regime is tradeable
                if not trader.is_regime_tradeable(regime):
                    continue
                
                # Simple entry logic based on regime
                direction = None
                if regime.trend_regime == TrendRegime.BULL:
                    direction = TradeDirection.LONG
                elif regime.trend_regime == TrendRegime.BEAR:
                    direction = TradeDirection.SHORT
                else:
                    continue
                
                # Enter trade
                trader.enter_trade(
                    ticker=ticker,
                    direction=direction,
                    entry_price=row['close'],
                    regime=regime,
                    atr=row['atr'],
                    entry_time=current_date
                )
                
            except Exception as e:
                continue
    
    # Get final results
    summary = trader.get_portfolio_summary()
    regime_analysis = trader.get_regime_analysis()
    
    # Print results
    print(f"\n=== REGIME-FIRST SYSTEM RESULTS ===")
    
    print(f"\nPortfolio Performance:")
    print(f"  Initial Capital: ${summary['capital']['initial']:,.0f}")
    print(f"  Final Capital: ${summary['capital']['current']:,.0f}")
    print(f"  Total P&L: ${summary['capital']['total_pnl']:,.0f} ({summary['capital']['pnl_percentage']:+.1f}%)")
    print(f"  Total Trades: {summary['performance']['total_trades']}")
    print(f"  Win Rate: {summary['performance']['win_rate']:.1%}")
    print(f"  Avg P&L per Trade: ${summary['performance']['avg_trade_pnl']:+.2f}")
    
    print(f"\nRegime Performance:")
    for regime, perf in regime_analysis['performance_by_regime'].items():
        print(f"  {regime}:")
        print(f"    Trades: {perf['trades']}")
        print(f"    Win Rate: {perf['win_rate']:.1%}")
        print(f"    Total P&L: ${perf['total_pnl']:+.0f}")
        print(f"    Expectancy: {perf['expectancy']:+.3f}")
    
    print(f"\nRisk Management:")
    print(f"  Base Risk per Trade: {summary['risk_parameters']['base_risk_per_trade']:.1%}")
    print(f"  Max Portfolio Heat: {summary['risk_parameters']['max_portfolio_heat']:.1%}")
    print(f"  Max Concurrent Positions: {summary['risk_parameters']['max_concurrent_positions']}")
    print(f"  Final Portfolio Heat: {summary['positions']['portfolio_heat']:.1%}")
    
    # Assessment
    print(f"\n=== ASSESSMENT ===")
    
    total_return = summary['capital']['pnl_percentage']
    win_rate = summary['performance']['win_rate']
    
    if total_return > 5.0:
        print(f"  POSITIVE: System generated {total_return:.1f}% return")
    elif total_return > 0:
        print(f"  NEUTRAL: System generated {total_return:.1f}% return")
    else:
        print(f"  NEGATIVE: System lost {abs(total_return):.1f}%")
    
    if win_rate > 0.55:
        print(f"  GOOD: {win_rate:.1%} win rate")
    elif win_rate > 0.50:
        print(f"  ACCEPTABLE: {win_rate:.1%} win rate")
    else:
        print(f"  POOR: {win_rate:.1%} win rate")
    
    # Compare to baseline (no regime filtering)
    print(f"\n=== BASELINE COMPARISON ===")
    
    # Simple buy-and-hold baseline
    if len(df) > 0:
        first_price = df.groupby('ticker')['close'].first().mean()
        last_price = df.groupby('ticker')['close'].last().mean()
        baseline_return = ((last_price - first_price) / first_price) * 100
        
        print(f"  Buy-and-Hold Return: {baseline_return:+.1f}%")
        print(f"  Regime-First Return: {total_return:+.1f}%")
        print(f"  Alpha: {total_return - baseline_return:+.1f}%")
    
    return {
        'summary': summary,
        'regime_analysis': regime_analysis,
        'total_return': total_return,
        'win_rate': win_rate
    }


def main():
    """Main test function"""
    
    print("Regime-First Trading System Test")
    print("Testing clean, minimal regime-based trading\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Test the system
    results = test_regime_first_system(df)
    
    return results


if __name__ == "__main__":
    results = main()
