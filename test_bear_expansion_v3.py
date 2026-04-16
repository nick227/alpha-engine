"""
Test Bear Expansion Strategy V3 - Quality Over Quantity

Key improvements:
1. Trade quality filters (ONE additional constraint)
2. Anti-clustering controls  
3. Reduced aggressiveness
4. Smoother equity curve focus
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from app.trading.bear_expansion_trader_v3 import BearExpansionTraderV3, BearExpansionConfigV3
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
        return None
    
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


def test_bear_expansion_v3(df, confirmation_type="volatility_timing"):
    """Test the quality-focused bear expansion strategy"""
    
    print(f"\n=== BEAR EXPANSION V3 TEST - {confirmation_type.upper()} ===")
    print("Quality over quantity: Anti-clustering + reduced aggressiveness")
    
    # Initialize trader with quality-focused config
    config = BearExpansionConfigV3(
        min_position_in_range=0.25,         # Shifted to 0.25-0.35
        max_position_in_range=0.35,
        base_risk_per_trade=0.01,          # 1% risk per trade (reduced)
        atr_stop_multiplier=1.25,           # Tighter stop
        atr_target_multiplier=2.5,          # Faster target
        max_concurrent_positions=2,         # Reduced from 3
        max_portfolio_heat=0.08,            # Reduced to 8%
        max_positions_per_sector=1,         # Prevent sector clustering
        max_hold_days=7,                    # Reduced hold period
        
        # ENTRY CONFIRMATION (choose ONE)
        use_volatility_timing=(confirmation_type == "volatility_timing"),
        volatility_lookback_days=5,
        min_atr_increase_pct=0.05,
        
        use_breakout_confirmation=(confirmation_type == "breakout_confirmation"),
        breakout_lookback_days=10,
        
        use_delayed_entry=(confirmation_type == "delayed_entry"),
        entry_delay_days=1
    )
    
    trader = BearExpansionTraderV3(initial_capital=100000.0, config=config)
    
    # Simulate trading day by day
    trading_days = sorted(df['date'].unique())
    
    print(f"Simulating {len(trading_days)} trading days...")
    
    for day_idx, current_date in enumerate(trading_days):
        if day_idx % 100 == 0:
            print(f"  Processing day {day_idx + 1}/{len(trading_days)}: {current_date.strftime('%Y-%m-%d')}")
        
        # Get current day's data
        day_data = df[df['date'] == current_date]
        
        # Update price history for confirmations
        for _, row in day_data.iterrows():
            trader.update_price_history(row['ticker'], current_date, row['close'], row['atr'])
        
        # Update existing positions
        market_data = {}
        for _, row in day_data.iterrows():
            market_data[row['ticker']] = {
                'price': row['close'],
                'atr': row['atr']
            }
        
        trader.update_positions(market_data, current_date)
        
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
    print(f"\n=== BEAR EXPANSION V3 RESULTS ({confirmation_type.upper()}) ===")
    
    print(f"\nPortfolio Performance:")
    print(f"  Initial Capital: ${summary['capital']['initial']:,.0f}")
    print(f"  Final Capital: ${summary['capital']['current']:,.0f}")
    print(f"  Total P&L: ${summary['capital']['total_pnl']:,.0f} ({summary['capital']['pnl_percentage']:+.1f}%)")
    print(f"  Total Trades: {summary['performance']['total_trades']}")
    print(f"  Win Rate: {summary['performance']['win_rate']:.1%}")
    print(f"  Avg P&L per Trade: ${summary['performance']['avg_trade_pnl']:+.2f}")
    print(f"  Max Drawdown: {summary['performance']['max_drawdown']:.1f}%")
    print(f"  Sharpe Ratio: {summary['performance']['sharpe_ratio']:.2f}")
    print(f"  Profit Factor: {summary['performance']['profit_factor']:.2f}")
    print(f"  Largest Win: ${summary['performance']['largest_win']:+,.0f}")
    print(f"  Largest Loss: ${summary['performance']['largest_loss']:+,.0f}")
    
    print(f"\nEntry Statistics:")
    print(f"  Entry Attempts: {summary['entry_stats']['entry_attempts']}")
    print(f"  Entry Rejections: {summary['entry_stats']['entry_rejections']}")
    print(f"  Acceptance Rate: {summary['entry_stats']['acceptance_rate']:.1%}")
    print(f"  Entry Band: {summary['risk_metrics']['entry_band']}")
    
    print(f"\nRisk Management:")
    print(f"  Base Risk per Trade: {summary['risk_metrics']['base_risk_per_trade']:.1%}")
    print(f"  Max Concurrent Positions: {summary['risk_metrics']['max_concurrent_positions']}")
    print(f"  Max Portfolio Heat: {summary['risk_metrics']['max_portfolio_heat']:.1%}")
    print(f"  Stop Multiplier: {summary['risk_metrics']['stop_multiplier']:.2f}x ATR")
    print(f"  Target Multiplier: {summary['risk_metrics']['target_multiplier']:.2f}x ATR")
    
    print(f"\nSector Distribution:")
    for sector, count in summary['sector_stats'].items():
        print(f"  {sector}: {count} positions")
    
    # Trade analysis
    if trade_details:
        trades_df = pd.DataFrame(trade_details)
        
        print(f"\nTrade Analysis:")
        print(f"  Average Position in Range: {trades_df['position_in_range'].mean():.1%}")
        print(f"  Average Hold Days: {trades_df['hold_days'].mean():.1f}")
        print(f"  Exit Reasons:")
        for reason, count in trades_df['exit_reason'].value_counts().items():
            print(f"    {reason}: {count}")
        
        # Performance by sector
        print(f"\nPerformance by Sector:")
        for sector in trades_df['sector'].unique():
            sector_trades = trades_df[trades_df['sector'] == sector]
            if len(sector_trades) > 0:
                win_rate = sector_trades['win'].mean()
                avg_pnl = sector_trades['realized_pnl'].mean()
                print(f"  {sector}: {len(sector_trades)} trades, {win_rate:.1%} win rate, {avg_pnl:+.2f} avg P&L")
        
        # Trade quality analysis
        print(f"\nTrade Quality Analysis:")
        winning_trades = trades_df[trades_df['win']]
        losing_trades = trades_df[~trades_df['win']]
        
        if len(winning_trades) > 0:
            print(f"  Winning Trades: {len(winning_trades)}")
            print(f"    Avg Win: ${winning_trades['realized_pnl'].mean():+,.0f}")
            print(f"    Avg Hold: {winning_trades['hold_days'].mean():.1f} days")
        
        if len(losing_trades) > 0:
            print(f"  Losing Trades: {len(losing_trades)}")
            print(f"    Avg Loss: ${losing_trades['realized_pnl'].mean():+,.0f}")
            print(f"    Avg Hold: {losing_trades['hold_days'].mean():.1f} days")
    
    # Assessment
    print(f"\n=== ASSESSMENT ===")
    
    total_return = summary['capital']['pnl_percentage']
    win_rate = summary['performance']['win_rate']
    sharpe = summary['performance']['sharpe_ratio']
    max_dd = summary['performance']['max_drawdown']
    profit_factor = summary['performance']['profit_factor']
    
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
    elif max_dd < 15:
        risk_rating = "MODERATE"
    elif max_dd < 20:
        risk_rating = "HIGH"
    else:
        risk_rating = "VERY HIGH"
    
    # Quality rating
    if profit_factor > 2.0:
        quality_rating = "EXCELLENT"
    elif profit_factor > 1.5:
        quality_rating = "GOOD"
    elif profit_factor > 1.0:
        quality_rating = "ACCEPTABLE"
    else:
        quality_rating = "POOR"
    
    # Overall rating
    if total_return > 0 and max_dd < 15 and sharpe > 0.5 and profit_factor > 1.2:
        overall_rating = "TRADEABLE EDGE"
    elif total_return > 0 and max_dd < 20:
        overall_rating = "PROMISING"
    else:
        overall_rating = "NEEDS WORK"
    
    print(f"  Performance: {perf_rating} ({total_return:+.1f}% return)")
    print(f"  Risk: {risk_rating} ({max_dd:.1f}% max drawdown)")
    print(f"  Quality: {quality_rating} (profit factor: {profit_factor:.2f})")
    print(f"  Win Rate: {win_rate:.1%}")
    print(f"  Sharpe: {sharpe:.2f}")
    print(f"  Overall: {overall_rating}")
    
    # Compare to V2
    print(f"\n=== V2 vs V3 COMPARISON ===")
    v2_return = 2.6  # From previous test
    v2_dd = 20.7
    v2_sharpe = 0.84
    v2_trades = 38
    
    print(f"  V2: {v2_return:+.1f}% return, {v2_dd:.1f}% DD, {v2_sharpe:.2f} Sharpe, {v2_trades} trades")
    print(f"  V3: {total_return:+.1f}% return, {max_dd:.1f}% DD, {sharpe:.2f} Sharpe, {summary['performance']['total_trades']} trades")
    print(f"  Improvement: {total_return - v2_return:+.1f}% return, {v2_dd - max_dd:+.1f}% DD reduction")
    
    # Compare to baseline
    print(f"\n=== BASELINE COMPARISON ===")
    
    if len(df) > 0:
        first_price = df.groupby('ticker')['close'].first().mean()
        last_price = df.groupby('ticker')['close'].last().mean()
        baseline_return = ((last_price - first_price) / first_price) * 100
        
        print(f"  Buy-and-Hold Return: {baseline_return:+.1f}%")
        print(f"  Bear Expansion V3 Return: {total_return:+.1f}%")
        print(f"  Alpha: {total_return - baseline_return:+.1f}%")
    
    return {
        'summary': summary,
        'trade_details': trade_details,
        'total_return': total_return,
        'win_rate': win_rate,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'overall_rating': overall_rating,
        'confirmation_type': confirmation_type
    }


def main():
    """Main test function"""
    
    print("Bear Expansion Strategy V3 Test")
    print("Quality over quantity: Anti-clustering + reduced aggressiveness\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Test all three confirmation types
    confirmation_types = ["volatility_timing", "breakout_confirmation", "delayed_entry"]
    results = []
    
    for confirmation_type in confirmation_types:
        print(f"\n{'='*60}")
        print(f"Testing {confirmation_type.upper()} confirmation")
        print(f"{'='*60}")
        
        result = test_bear_expansion_v3(df, confirmation_type)
        results.append(result)
    
    # Compare results
    print(f"\n{'='*60}")
    print("CONFIRMATION TYPE COMPARISON")
    print(f"{'='*60}")
    
    print(f"\n{'Confirmation Type':<20} {'Return':<10} {'DD':<8} {'Sharpe':<8} {'Trades':<8} {'Win Rate':<10} {'Rating':<15}")
    print("-" * 80)
    
    for result in results:
        print(f"{result['confirmation_type']:<20} "
              f"{result['total_return']:+.1f}%{'':<6} "
              f"{result['max_drawdown']:.1f}%{'':<5} "
              f"{result['sharpe']:.2f}{'':<5} "
              f"{result['summary']['performance']['total_trades']:<8} "
              f"{result['win_rate']:.1%}{'':<4} "
              f"{result['overall_rating']:<15}")
    
    # Find best performer
    best_result = max(results, key=lambda x: (
        x['total_return'] > 0,  # Must be positive
        x['max_drawdown'] < 20,  # Reasonable drawdown
        x['sharpe_ratio']  # Higher Sharpe
    ))
    
    print(f"\n=== BEST PERFORMER ===")
    print(f"Confirmation Type: {best_result['confirmation_type'].upper()}")
    print(f"Return: {best_result['total_return']:+.1f}%")
    print(f"Drawdown: {best_result['max_drawdown']:.1f}%")
    print(f"Sharpe: {best_result['sharpe']:.2f}")
    print(f"Trades: {best_result['summary']['performance']['total_trades']}")
    print(f"Win Rate: {best_result['win_rate']:.1%}")
    print(f"Overall Rating: {best_result['overall_rating']}")
    
    return results


if __name__ == "__main__":
    results = main()
