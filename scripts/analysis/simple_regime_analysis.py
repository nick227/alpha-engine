"""
Simple Regime Analysis

Direct analysis of why (BEAR, EXPANSION) profits while (BULL, EXPANSION) loses.
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


def analyze_regime_performance(df):
    """Analyze performance by regime using simple forward returns"""
    
    print("\n=== REGIME PERFORMANCE ANALYSIS ===")
    
    # Initialize classifier
    classifier = RegimeClassifierV3()
    
    # Store regime performance data
    regime_data = []
    
    # Process each ticker
    for ticker in df['ticker'].unique():
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        print(f"  Analyzing {ticker}...")
        
        for i in range(200, len(ticker_data) - 5):  # Need history and future
            current_row = ticker_data.iloc[i]
            
            # Get historical ATR for regime calculation
            historical_atr = ticker_data.iloc[:i]['atr'].dropna().tolist()
            
            if len(historical_atr) < 20:
                continue
            
            # Calculate regime
            try:
                price_vs_ma50 = (current_row['close'] - current_row['ma50']) / current_row['ma50']
                ma50_vs_ma200 = (current_row['ma50'] - current_row['ma200']) / current_row['ma200']
                
                # Trend regime
                if price_vs_ma50 > 0.02 and ma50_vs_ma200 > 0.02:
                    trend_regime = TrendRegime.BULL
                elif price_vs_ma50 < -0.02 and ma50_vs_ma200 < -0.02:
                    trend_regime = TrendRegime.BEAR
                else:
                    trend_regime = TrendRegime.CHOP
                
                # Volatility regime
                atr_percentile = sum(1 for x in historical_atr if x <= current_row['atr']) / len(historical_atr)
                
                if atr_percentile >= 0.8:
                    volatility_regime = VolatilityRegime.EXPANSION
                elif atr_percentile <= 0.2:
                    volatility_regime = VolatilityRegime.COMPRESSION
                else:
                    volatility_regime = VolatilityRegime.NORMAL
                
                # Create regime string without .value
                regime_str = f"({trend_regime}, {volatility_regime})"
                
                # Calculate forward returns
                future_5d_return = ticker_data.iloc[i + 5]['close'] / current_row['close'] - 1
                future_10d_return = ticker_data.iloc[i + 10]['close'] / current_row['close'] - 1
                
                # Calculate entry location metrics
                distance_to_high = (current_row['high_20d'] - current_row['close']) / current_row['high_20d']
                distance_to_low = (current_row['close'] - current_row['low_20d']) / current_row['low_20d']
                position_in_range = (current_row['close'] - current_row['low_20d']) / (current_row['high_20d'] - current_row['low_20d'])
                
                # Store regime data
                regime_data.append({
                    'ticker': ticker,
                    'date': current_row['date'],
                    'regime': regime_str,
                    'trend_regime': str(trend_regime),
                    'volatility_regime': str(volatility_regime),
                    'close': current_row['close'],
                    'ma50': current_row['ma50'],
                    'ma200': current_row['ma200'],
                    'atr': current_row['atr'],
                    'atr_percentile': atr_percentile,
                    'price_vs_ma50': price_vs_ma50,
                    'ma50_vs_ma200': ma50_vs_ma200,
                    'future_5d_return': future_5d_return,
                    'future_10d_return': future_10d_return,
                    'win_5d': future_5d_return > 0,
                    'win_10d': future_10d_return > 0,
                    'distance_to_high': distance_to_high,
                    'distance_to_low': distance_to_low,
                    'position_in_range': position_in_range
                })
                
            except Exception as e:
                continue
    
    # Convert to DataFrame
    regime_df = pd.DataFrame(regime_data)
    
    if len(regime_df) == 0:
        print("No regime data available for analysis")
        return None
    
    print(f"Analyzed {len(regime_df)} regime observations")
    
    return regime_df


def analyze_regime_bias(regime_df):
    """Analyze regime bias and performance"""
    
    print("\n=== REGIME BIAS ANALYSIS ===")
    
    # Group by regime
    regime_stats = {}
    
    for regime in regime_df['regime'].unique():
        regime_trades = regime_df[regime_df['regime'] == regime]
        
        # Calculate performance metrics
        win_rate_5d = regime_trades['win_5d'].mean()
        win_rate_10d = regime_trades['win_10d'].mean()
        avg_return_5d = regime_trades['future_5d_return'].mean()
        avg_return_10d = regime_trades['future_10d_return'].mean()
        
        # Calculate expectancy
        expectancy_5d = win_rate_5d * avg_return_5d - (1 - win_rate_5d) * abs(avg_return_5d)
        expectancy_10d = win_rate_10d * avg_return_10d - (1 - win_rate_10d) * abs(avg_return_10d)
        
        # Entry location metrics
        avg_position_in_range = regime_trades['position_in_range'].mean()
        avg_atr_percentile = regime_trades['atr_percentile'].mean()
        
        regime_stats[regime] = {
            'observations': len(regime_trades),
            'win_rate_5d': win_rate_5d,
            'win_rate_10d': win_rate_10d,
            'avg_return_5d': avg_return_5d,
            'avg_return_10d': avg_return_10d,
            'expectancy_5d': expectancy_5d,
            'expectancy_10d': expectancy_10d,
            'avg_position_in_range': avg_position_in_range,
            'avg_atr_percentile': avg_atr_percentile
        }
    
    # Print results
    print("Regime           | Obs | 5d Win% | 10d Win% | 5d Return | 10d Return | 5d Exp | 10d Exp | Position | ATR Pct")
    print("-----------------|-----|---------|----------|-----------|------------|---------|----------|----------|---------")
    
    for regime, stats in regime_stats.items():
        print(f"{regime:<16} | {stats['observations']:4} | {stats['win_rate_5d']:7.1%} | "
              f"{stats['win_rate_10d']:8.1%} | {stats['avg_return_5d']:9.2%} | "
              f"{stats['avg_return_10d']:10.2%} | {stats['expectancy_5d']:6.3f} | "
              f"{stats['expectancy_10d']:8.3f} | {stats['avg_position_in_range']:7.1%} | {stats['avg_atr_percentile']:6.1%}")
    
    return regime_stats


def analyze_directional_bias(regime_df):
    """Analyze directional bias within regimes"""
    
    print("\n=== DIRECTIONAL BIAS ANALYSIS ===")
    
    # Analyze BULL vs BEAR expansion specifically
    bull_expansion = regime_df[regime_df['regime'] == '(BULL, EXPANSION)']
    bear_expansion = regime_df[regime_df['regime'] == '(BEAR, EXPANSION)']
    
    print(f"\nBULL EXPANSION ANALYSIS:")
    if len(bull_expansion) > 0:
        print(f"  Observations: {len(bull_expansion)}")
        print(f"  Win Rate (5d): {bull_expansion['win_5d'].mean():.1%}")
        print(f"  Win Rate (10d): {bull_expansion['win_10d'].mean():.1%}")
        print(f"  Avg Return (5d): {bull_expansion['future_5d_return'].mean():.2%}")
        print(f"  Avg Return (10d): {bear_expansion['future_10d_return'].mean():.2%}")
        print(f"  Position in Range: {bull_expansion['position_in_range'].mean():.1%}")
        print(f"  ATR Percentile: {bull_expansion['atr_percentile'].mean():.1%}")
        
        # Analyze by position in range
        bull_high = bull_expansion[bull_expansion['position_in_range'] > 0.7]
        bull_mid = bull_expansion[(bull_expansion['position_in_range'] >= 0.3) & (bull_expansion['position_in_range'] <= 0.7)]
        bull_low = bull_expansion[bull_expansion['position_in_range'] < 0.3]
        
        print(f"\n  BULL by Position:")
        print(f"    High (>70%): {len(bull_high)} obs, {bull_high['win_5d'].mean():.1%} win rate")
        print(f"    Mid (30-70%): {len(bull_mid)} obs, {bull_mid['win_5d'].mean():.1%} win rate")
        print(f"    Low (<30%): {len(bull_low)} obs, {bull_low['win_5d'].mean():.1%} win rate")
    
    print(f"\nBEAR EXPANSION ANALYSIS:")
    if len(bear_expansion) > 0:
        print(f"  Observations: {len(bear_expansion)}")
        print(f"  Win Rate (5d): {bear_expansion['win_5d'].mean():.1%}")
        print(f"  Win Rate (10d): {bear_expansion['win_10d'].mean():.1%}")
        print(f"  Avg Return (5d): {bear_expansion['future_5d_return'].mean():.2%}")
        print(f"  Avg Return (10d): {bear_expansion['future_10d_return'].mean():.2%}")
        print(f"  Position in Range: {bear_expansion['position_in_range'].mean():.1%}")
        print(f"  ATR Percentile: {bear_expansion['atr_percentile'].mean():.1%}")
        
        # Analyze by position in range
        bear_high = bear_expansion[bear_expansion['position_in_range'] > 0.7]
        bear_mid = bear_expansion[(bear_expansion['position_in_range'] >= 0.3) & (bear_expansion['position_in_range'] <= 0.7)]
        bear_low = bear_expansion[bear_expansion['position_in_range'] < 0.3]
        
        print(f"\n  BEAR by Position:")
        print(f"    High (>70%): {len(bear_high)} obs, {bear_high['win_5d'].mean():.1%} win rate")
        print(f"    Mid (30-70%): {len(bear_mid)} obs, {bear_mid['win_5d'].mean():.1%} win rate")
        print(f"    Low (<30%): {len(bear_low)} obs, {bear_low['win_5d'].mean():.1%} win rate")
    
    return {
        'bull_expansion': bull_expansion,
        'bear_expansion': bear_expansion
    }


def analyze_volatility_timing(regime_df):
    """Analyze volatility timing effects"""
    
    print("\n=== VOLATILITY TIMING ANALYSIS ===")
    
    # Analyze by ATR percentile
    for regime in ['(BULL, EXPANSION)', '(BEAR, EXPANSION)']:
        regime_data = regime_df[regime_df['regime'] == regime]
        
        if len(regime_data) == 0:
            continue
        
        print(f"\n{regime}:")
        
        # Split by ATR percentile
        early_expansion = regime_data[regime_data['atr_percentile'] < 0.9]
        late_expansion = regime_data[regime_data['atr_percentile'] >= 0.9]
        
        print(f"  Early Expansion (<90% ATR): {len(early_expansion)} obs, {early_expansion['win_5d'].mean():.1%} win rate")
        print(f"  Late Expansion (>=90% ATR): {len(late_expansion)} obs, {late_expansion['win_5d'].mean():.1%} win rate")
        
        # Split by position in range
        high_entries = regime_data[regime_data['position_in_range'] > 0.7]
        low_entries = regime_data[regime_data['position_in_range'] < 0.3]
        
        print(f"  High Entries (>70% range): {len(high_entries)} obs, {high_entries['win_5d'].mean():.1%} win rate")
        print(f"  Low Entries (<30% range): {len(low_entries)} obs, {low_entries['win_5d'].mean():.1%} win rate")


def main():
    """Main analysis function"""
    
    print("Simple Regime Analysis")
    print("Analyzing why (BEAR, EXPANSION) profits while (BULL, EXPANSION) loses\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Analyze regime performance
    regime_df = analyze_regime_performance(df)
    
    if regime_df is None:
        return
    
    # Run analyses
    regime_stats = analyze_regime_bias(regime_df)
    directional_stats = analyze_directional_bias(regime_df)
    analyze_volatility_timing(regime_df)
    
    # Summary assessment
    print("\n=== CRITICAL FINDINGS ===")
    
    bull_expansion = regime_stats.get('(BULL, EXPANSION)', {})
    bear_expansion = regime_stats.get('(BEAR, EXPANSION)', {})
    
    if bull_expansion and bear_expansion:
        print(f"\nREGIME PERFORMANCE COMPARISON:")
        print(f"  (BULL, EXPANSION):")
        print(f"    Win Rate: {bull_expansion['win_rate_5d']:.1%}")
        print(f"    Avg Return: {bull_expansion['avg_return_5d']:.2%}")
        print(f"    Position in Range: {bull_expansion['avg_position_in_range']:.1%}")
        print(f"    ATR Percentile: {bull_expansion['avg_atr_percentile']:.1%}")
        
        print(f"  (BEAR, EXPANSION):")
        print(f"    Win Rate: {bear_expansion['win_rate_5d']:.1%}")
        print(f"    Avg Return: {bear_expansion['avg_return_5d']:.2%}")
        print(f"    Position in Range: {bear_expansion['avg_position_in_range']:.1%}")
        print(f"    ATR Percentile: {bear_expansion['avg_atr_percentile']:.1%}")
        
        # Key insights
        print(f"\nKEY INSIGHTS:")
        
        if bull_expansion['avg_position_in_range'] > 0.6:
            print(f"  WARNING: BULL expansion entries near range highs (EXHAUSTION)")
        
        if bear_expansion['avg_position_in_range'] < 0.4:
            print(f"  GOOD: BEAR expansion entries near range lows (BREAKDOWN)")
        
        if bull_expansion['avg_atr_percentile'] > bear_expansion['avg_atr_percentile']:
            print(f"  BULL: Later in expansion (worse timing)")
        else:
            print(f"  BEAR: Later in expansion (worse timing)")
        
        win_rate_diff = bull_expansion['win_rate_5d'] - bear_expansion['win_rate_5d']
        return_diff = bull_expansion['avg_return_5d'] - bear_expansion['avg_return_5d']
        
        print(f"\nPERFORMANCE GAP:")
        print(f"  Win Rate Difference: {win_rate_diff:+.1%}")
        print(f"  Return Difference: {return_diff:+.2%}")
        
        if win_rate_diff < -0.05:
            print(f"  CONCLUSION: BEAR expansion significantly outperforms BULL expansion")
        elif win_rate_diff > 0.05:
            print(f"  CONCLUSION: BULL expansion significantly outperforms BEAR expansion")
        else:
            print(f"  CONCLUSION: Performance difference is minimal")
    
    return {
        'regime_df': regime_df,
        'regime_stats': regime_stats,
        'directional_stats': directional_stats
    }


if __name__ == "__main__":
    results = main()
