"""
Analyze Completed Trades from Regime-First System

Direct analysis of completed trades to understand the regime bias.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from app.trading.regime_first_trader import RegimeFirstTrader, TradeDirection
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
        
        # Calculate 20-day high/low
        ticker_data['high_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).max()
        ticker_data['low_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).min()
        
        # Calculate 10-day ATR for volatility timing
        ticker_data['atr_10d'] = ticker_data['tr'].rolling(window=10, min_periods=10).mean()
        
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


def run_trade_analysis(df):
    """Run the regime-first system and analyze completed trades"""
    
    print("\n=== TRADE ANALYSIS ===")
    
    # Initialize trader
    trader = RegimeFirstTrader(initial_capital=100000.0)
    
    # Simulate trading
    trading_days = sorted(df['date'].unique())
    
    print(f"Simulating {len(trading_days)} trading days...")
    
    completed_trades = []
    
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
        
        # Check for exits and capture completed trades
        positions_to_close = []
        
        for position_id, position in trader.positions.items():
            ticker = position.ticker
            
            if ticker not in market_data:
                continue
            
            current_price = market_data[ticker]['price']
            position.update_price(current_price)
            
            # Check for exit
            should_exit, reason = position.should_exit(current_price)
            
            if should_exit:
                positions_to_close.append((position_id, current_price, reason))
        
        # Close positions and capture data
        for position_id, exit_price, reason in positions_to_close:
            position = trader.positions[position_id]
            
            # Get entry data for analysis
            ticker = position.ticker
            entry_data = df[(df['ticker'] == ticker) & (df['date'] == position.entry_time.date())]
            
            if len(entry_data) > 0:
                entry_row = entry_data.iloc[0]
                
                # Calculate entry metrics
                distance_to_high = (entry_row['high_20d'] - entry_row['close']) / entry_row['high_20d']
                distance_to_low = (entry_row['close'] - entry_row['low_20d']) / entry_row['low_20d']
                position_in_range = (entry_row['close'] - entry_row['low_20d']) / (entry_row['high_20d'] - entry_row['low_20d'])
                atr_ratio = entry_row['atr'] / entry_row['atr_10d']
                
                # Store completed trade data
                completed_trades.append({
                    'ticker': ticker,
                    'regime': position.regime_at_entry,
                    'direction': position.direction.value,
                    'entry_date': position.entry_time.date(),
                    'entry_price': position.entry_price,
                    'exit_date': position.exit_time.date(),
                    'exit_price': exit_price,
                    'exit_reason': reason,
                    'realized_pnl': position.realized_pnl,
                    'win': position.realized_pnl > 0,
                    'distance_to_high': distance_to_high,
                    'distance_to_low': distance_to_low,
                    'position_in_range': position_in_range,
                    'atr_ratio': atr_ratio,
                    'atr': entry_row['atr'],
                    'ma50': entry_row['ma50'],
                    'ma200': entry_row['ma200']
                })
            
            # Close the position
            trader.close_position(position_id, exit_price, reason)
        
        # Update positions
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
    
    # Convert to DataFrame
    trades_df = pd.DataFrame(completed_trades)
    
    if len(trades_df) == 0:
        print("No completed trades for analysis")
        return None
    
    print(f"Analyzed {len(trades_df)} completed trades")
    
    return trades_df


def analyze_directional_bias(trades_df):
    """Analyze directional bias by regime"""
    
    print("\n=== DIRECTIONAL BIAS ANALYSIS ===")
    
    # Group by regime and direction
    bias_stats = {}
    
    for regime in trades_df['regime'].unique():
        regime_trades = trades_df[trades_df['regime'] == regime]
        
        long_trades = regime_trades[regime_trades['direction'] == 'long']
        short_trades = regime_trades[regime_trades['direction'] == 'short']
        
        bias_stats[regime] = {
            'total_trades': len(regime_trades),
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'long_win_rate': long_trades['win'].mean() if len(long_trades) > 0 else 0,
            'short_win_rate': short_trades['win'].mean() if len(short_trades) > 0 else 0,
            'overall_win_rate': regime_trades['win'].mean(),
            'long_pnl': long_trades['realized_pnl'].sum() if len(long_trades) > 0 else 0,
            'short_pnl': short_trades['realized_pnl'].sum() if len(short_trades) > 0 else 0,
            'total_pnl': regime_trades['realized_pnl'].sum()
        }
    
    # Print results
    print("Regime           | Total | Long | Short | Long Win% | Short Win% | Overall Win% | Long PnL | Short PnL | Total PnL")
    print("-----------------|-------|------|-------|-----------|------------|-------------|----------|-----------|----------")
    
    for regime, stats in bias_stats.items():
        print(f"{regime:<16} | {stats['total_trades']:5} | {stats['long_trades']:4} | {stats['short_trades']:5} | "
              f"{stats['long_win_rate']:9.1%} | {stats['short_win_rate']:10.1%} | "
              f"{stats['overall_win_rate']:11.1%} | {stats['long_pnl']:8.0f} | {stats['short_pnl']:10.0f} | {stats['total_pnl']:9.0f}")
    
    return bias_stats


def analyze_entry_location(trades_df):
    """Analyze entry location by regime"""
    
    print("\n=== ENTRY LOCATION ANALYSIS ===")
    
    # Analyze by regime
    location_stats = {}
    
    for regime in trades_df['regime'].unique():
        regime_trades = trades_df[trades_df['regime'] == regime]
        
        location_stats[regime] = {
            'avg_position_in_range': regime_trades['position_in_range'].mean(),
            'avg_distance_to_high': regime_trades['distance_to_high'].mean(),
            'avg_distance_to_low': regime_trades['distance_to_low'].mean(),
            'win_rate': regime_trades['win'].mean(),
            'avg_pnl': regime_trades['realized_pnl'].mean(),
            'trade_count': len(regime_trades)
        }
    
    # Print results
    print("Regime           | Position in Range | Distance to High | Distance to Low | Win Rate | Avg PnL | Trades")
    print("-----------------|-------------------|------------------|----------------|----------|---------|--------")
    
    for regime, stats in location_stats.items():
        print(f"{regime:<16} | {stats['avg_position_in_range']:15.1%} | "
              f"{stats['avg_distance_to_high']:14.1%} | {stats['avg_distance_to_low']:12.1%} | "
              f"{stats['win_rate']:8.1%} | {stats['avg_pnl']:7.1f} | {stats['trade_count']:6}")
    
    return location_stats


def analyze_volatility_timing(trades_df):
    """Analyze volatility timing by regime"""
    
    print("\n=== VOLATILITY TIMING ANALYSIS ===")
    
    # Analyze by regime and outcome
    vol_stats = {}
    
    for regime in trades_df['regime'].unique():
        regime_trades = trades_df[trades_df['regime'] == regime]
        
        winning_trades = regime_trades[regime_trades['win']]
        losing_trades = regime_trades[~regime_trades['win']]
        
        vol_stats[regime] = {
            'avg_atr_ratio_all': regime_trades['atr_ratio'].mean(),
            'avg_atr_ratio_win': winning_trades['atr_ratio'].mean() if len(winning_trades) > 0 else 0,
            'avg_atr_ratio_loss': losing_trades['atr_ratio'].mean() if len(losing_trades) > 0 else 0,
            'win_rate': regime_trades['win'].mean(),
            'avg_pnl': regime_trades['realized_pnl'].mean(),
            'trade_count': len(regime_trades)
        }
    
    # Print results
    print("Regime           | ATR Ratio (All) | ATR Ratio (Win) | ATR Ratio (Loss) | Win Rate | Avg PnL | Trades")
    print("-----------------|------------------|------------------|-------------------|----------|---------|--------")
    
    for regime, stats in vol_stats.items():
        print(f"{regime:<16} | {stats['avg_atr_ratio_all']:14.3f} | "
              f"{stats['avg_atr_ratio_win']:14.3f} | {stats['avg_atr_ratio_loss']:15.3f} | "
              f"{stats['win_rate']:8.1%} | {stats['avg_pnl']:7.1f} | {stats['trade_count']:6}")
    
    return vol_stats


def main():
    """Main analysis function"""
    
    print("Completed Trades Analysis")
    print("Analyzing regime bias in actual trading results\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Run trade analysis
    trades_df = run_trade_analysis(df)
    
    if trades_df is None or len(trades_df) == 0:
        print("No trade data available for analysis")
        return
    
    # Run analyses
    bias_stats = analyze_directional_bias(trades_df)
    location_stats = analyze_entry_location(trades_df)
    vol_stats = analyze_volatility_timing(trades_df)
    
    # Summary assessment
    print("\n=== CRITICAL FINDINGS ===")
    
    # Focus on the critical split
    bull_expansion = bias_stats.get('(BULL, EXPANSION)', {})
    bear_expansion = bias_stats.get('(BEAR, EXPANSION)', {})
    
    if bull_expansion and bear_expansion:
        print(f"\nCRITICAL REGIME SPLIT:")
        print(f"  (BULL, EXPANSION): {bull_expansion['total_pnl']:+.0f} PnL, {bull_expansion['overall_win_rate']:.1%} win rate")
        print(f"  (BEAR, EXPANSION): {bear_expansion['total_pnl']:+.0f} PnL, {bear_expansion['overall_win_rate']:.1%} win rate")
        
        # Directional analysis
        print(f"\nDIRECTIONAL ANALYSIS:")
        if bull_expansion['long_trades'] > bull_expansion['short_trades']:
            print(f"  BULL: Mostly long ({bull_expansion['long_trades']}/{bull_expansion['total_trades']})")
            print(f"  BULL Long Win Rate: {bull_expansion['long_win_rate']:.1%}")
        else:
            print(f"  BULL: Mixed directions")
        
        if bear_expansion['short_trades'] > bear_expansion['long_trades']:
            print(f"  BEAR: Mostly short ({bear_expansion['short_trades']}/{bear_expansion['total_trades']})")
            print(f"  BEAR Short Win Rate: {bear_expansion['short_win_rate']:.1%}")
        else:
            print(f"  BEAR: Mixed directions")
    
    # Entry location analysis
    bull_location = location_stats.get('(BULL, EXPANSION)', {})
    bear_location = location_stats.get('(BEAR, EXPANSION)', {})
    
    if bull_location and bear_location:
        print(f"\nENTRY LOCATION ANALYSIS:")
        print(f"  BULL expansion: {bull_location['avg_position_in_range']:.1%} position in range")
        print(f"  BEAR expansion: {bear_location['avg_position_in_range']:.1%} position in range")
        
        if bull_location['avg_position_in_range'] > 0.7:
            print(f"  WARNING: BULL entries near range highs (EXHAUSTION)")
        if bear_location['avg_position_in_range'] < 0.3:
            print(f"  GOOD: BEAR entries near range lows (BREAKDOWN)")
    
    # Volatility timing
    bull_vol = vol_stats.get('(BULL, EXPANSION)', {})
    bear_vol = vol_stats.get('(BEAR, EXPANSION)', {})
    
    if bull_vol and bear_vol:
        print(f"\nVOLATILITY TIMING ANALYSIS:")
        print(f"  BULL ATR Ratio: {bull_vol['avg_atr_ratio_all']:.3f}")
        print(f"  BEAR ATR Ratio: {bear_vol['avg_atr_ratio_all']:.3f}")
        
        if bull_vol['avg_atr_ratio_all'] > bear_vol['avg_atr_ratio_all']:
            print(f"  BULL: Later in expansion (worse timing)")
        else:
            print(f"  BEAR: Later in expansion (worse timing)")
    
    return {
        'trades_df': trades_df,
        'bias_stats': bias_stats,
        'location_stats': location_stats,
        'vol_stats': vol_stats
    }


if __name__ == "__main__":
    results = main()
