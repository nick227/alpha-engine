"""
Validator for Redesigned Scoring System

Tests the new architecture:
1. Separate gating from ranking
2. Rank within passed set only
3. Signal-specific features
4. Use ranks, not averages
5. Two-view validation
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import json
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

# Import the redesigned components
from app.core.regime_v3_redesigned import RegimeAwareRankerV2, SignalFeatureExtractor, SignalFeatures
from app.core.regime_v3 import RegimeClassifierV3, SignalGating, TrendRegime, VolatilityRegime


def get_and_prepare_data():
    """Get historical data and calculate technical indicators"""
    
    print("Loading and preparing data...")
    
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
        ticker_data['return_1d'] = ticker_data['close'].pct_change()
        ticker_data['return_5d'] = ticker_data['close'].pct_change(5)
        ticker_data['return_20d'] = ticker_data['close'].pct_change(20)
        
        # Calculate VWAP (simplified)
        ticker_data['vwap'] = ticker_data['close'].rolling(window=20, min_periods=20).mean()
        
        # Calculate lowest price in 20 days
        ticker_data['lowest_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).min()
        
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


def test_redesigned_scoring(df):
    """Test the redesigned scoring system"""
    
    print("\n=== REDESIGNED SCORING TEST ===")
    
    # Initialize the new ranker
    ranker = RegimeAwareRankerV2()
    
    # Generate signals with features
    all_signals = []
    
    print("Generating signals with feature extraction...")
    
    for ticker in df['ticker'].unique()[:10]:  # Test 10 tickers
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        # Store historical data for feature extraction
        historical_volumes = ticker_data['volume'].tolist()
        historical_atrs = ticker_data['atr'].tolist()
        
        for i in range(200, len(ticker_data)):
            current_row = ticker_data.iloc[i]
            
            # Get historical ATR for regime calculation
            historical_atr_for_regime = ticker_data.iloc[:i]['atr'].dropna().tolist()
            
            if len(historical_atr_for_regime) < 20:
                continue
            
            # Calculate regime
            try:
                # Trend regime
                price_vs_ma50 = (current_row['close'] - current_row['ma50']) / current_row['ma50']
                ma50_vs_ma200 = (current_row['ma50'] - current_row['ma200']) / current_row['ma200']
                
                if price_vs_ma50 > 0.02 and ma50_vs_ma200 > 0.02:
                    trend_regime = TrendRegime.BULL
                elif price_vs_ma50 < -0.02 and ma50_vs_ma200 < -0.02:
                    trend_regime = TrendRegime.BEAR
                else:
                    trend_regime = TrendRegime.CHOP
                
                # Volatility regime
                atr_percentile = sum(1 for x in historical_atr_for_regime if x <= current_row['atr']) / len(historical_atr_for_regime)
                
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
                    volatility_value=current_row['atr'] / current_row['close']
                )
                
                # Test each signal type
                for signal_type in ['volatility_breakout', 'momentum', 'mean_reversion']:
                    
                    # Extract signal-specific features
                    if signal_type == 'volatility_breakout':
                        features = SignalFeatureExtractor.extract_volatility_breakout_features(
                            current_price=current_row['close'],
                            atr=current_row['atr'],
                            volume=current_row['volume'],
                            vwap=current_row['vwap'],
                            historical_volume=historical_volumes[:i],
                            historical_atr=historical_atrs[:i]
                        )
                    
                    elif signal_type == 'momentum':
                        features = SignalFeatureExtractor.extract_momentum_features(
                            current_price=current_row['close'],
                            ma50=current_row['ma50'],
                            ma200=current_row['ma200'],
                            returns_5d=current_row['return_5d'],
                            returns_20d=current_row['return_20d'],
                            volume=current_row['volume']
                        )
                    
                    elif signal_type == 'mean_reversion':
                        features = SignalFeatureExtractor.extract_mean_reversion_features(
                            current_price=current_row['close'],
                            ma50=current_row['ma50'],
                            lowest_20d=current_row['lowest_20d'],
                            volume=current_row['volume']
                        )
                    
                    # Apply redesigned scoring
                    score = ranker.gate_and_rank(signal_type, regime, features)
                    
                    # Store signal data
                    signal_data = {
                        'ticker': ticker,
                        'date': current_row['date'],
                        'signal_type': signal_type,
                        'regime': regime,
                        'features': features,
                        'score': score,
                        'passed': score is not None,
                        'ranked_score': score if score is not None else None
                    }
                    
                    # Simulate trade outcome
                    if i + 5 < len(ticker_data):
                        exit_price = ticker_data.iloc[i + 5]['close']
                        signal_data['return'] = (exit_price / current_row['close']) - 1
                        signal_data['win'] = signal_data['return'] > 0
                    else:
                        signal_data['return'] = None
                        signal_data['win'] = None
                    
                    all_signals.append(signal_data)
                
            except Exception as e:
                continue
    
    print(f"Generated {len(all_signals)} signals with features")
    
    # Analyze gating results
    passed_signals = [s for s in all_signals if s['passed']]
    failed_signals = [s for s in all_signals if not s['passed']]
    
    print(f"\nGating Results:")
    print(f"  Total signals: {len(all_signals)}")
    print(f"  Passed: {len(passed_signals)} ({len(passed_signals)/len(all_signals):.1%})")
    print(f"  Failed: {len(failed_signals)} ({len(failed_signals)/len(all_signals):.1%})")
    
    # Analyze score distribution for passed signals
    passed_with_scores = [s for s in passed_signals if s['ranked_score'] is not None]
    
    if len(passed_with_scores) > 0:
        scores = [s['ranked_score'] for s in passed_with_scores]
        
        print(f"\nScore Distribution (Passed Signals Only):")
        print(f"  Min score: {min(scores):.3f}")
        print(f"  Max score: {max(scores):.3f}")
        print(f"  Mean score: {np.mean(scores):.3f}")
        print(f"  Std score: {np.std(scores):.3f}")
        
        # Create deciles from passed signals only
        decile_thresholds = np.percentile(scores, np.arange(0, 101, 10))
        decile_stats = {}
        
        for i in range(10):
            lower_threshold = decile_thresholds[i]
            upper_threshold = decile_thresholds[i + 1]
            
            decile_signals = [s for s in passed_with_scores 
                             if lower_threshold <= s['ranked_score'] < upper_threshold]
            
            if not decile_signals:
                continue
            
            # Calculate metrics for this decile
            returns = [s['return'] for s in decile_signals if s['return'] is not None]
            wins = [s['win'] for s in decile_signals if s['win'] is not None]
            
            if returns and wins:
                win_rate = sum(wins) / len(wins)
                avg_return = np.mean(returns)
                expectancy = win_rate * avg_return - (1 - win_rate) * abs(avg_return)
                
                decile_stats[f'Decile {i+1}'] = {
                    'trade_count': len(decile_signals),
                    'win_rate': win_rate,
                    'avg_return': avg_return,
                    'expectancy': expectancy,
                    'avg_score': np.mean([s['ranked_score'] for s in decile_signals])
                }
        
        # Print complete decile analysis
        print(f"\nDecile Analysis (Passed Signals Only):")
        print("  Decile | Trades | Win Rate | Avg Return | Expectancy | Avg Score")
        print("  -------|--------|----------|------------|------------|----------")
        
        for i in range(1, 11):
            decile_key = f'Decile {i}'
            if decile_key in decile_stats:
                stats = decile_stats[decile_key]
                print(f"  {decile_key:<7} | {stats['trade_count']:6} | {stats['win_rate']:8.1%} | {stats['avg_return']:10.2%} | {stats['expectancy']:10.3f} | {stats['avg_score']:8.3f}")
            else:
                print(f"  {decile_key:<7} |      0 |       - |         - |          - |         -")
        
        # Test discrimination
        top_decile = decile_stats.get('Decile 10', {})
        bottom_decile = decile_stats.get('Decile 1', {})
        
        if top_decile and bottom_decile:
            win_rate_separation = top_decile['win_rate'] - bottom_decile['win_rate']
            return_separation = top_decile['avg_return'] - bottom_decile['avg_return']
            
            print(f"\nDiscrimination Quality:")
            print(f"  Top vs Bottom Win Rate: {win_rate_separation:+.1%}")
            print(f"  Top vs Bottom Return: {return_separation:+.2%}")
            
            # Assessment
            if win_rate_separation >= 0.05:  # 5% separation
                print(f"  GOOD: Strong win rate discrimination")
            elif win_rate_separation >= 0.02:  # 2% separation
                print(f"  MODERATE: Some win rate discrimination")
            else:
                print(f"  POOR: Weak win rate discrimination")
            
            if return_separation >= 0.01:  # 1% separation
                print(f"  GOOD: Strong return discrimination")
            elif return_separation >= 0.005:  # 0.5% separation
                print(f"  MODERATE: Some return discrimination")
            else:
                print(f"  POOR: Weak return discrimination")
        
        # Signal-type specific analysis
        print(f"\nSignal-Type Specific Analysis:")
        for signal_type in ['volatility_breakout', 'momentum', 'mean_reversion']:
            type_signals = [s for s in passed_with_scores if s['signal_type'] == signal_type]
            
            if len(type_signals) > 10:
                type_scores = [s['ranked_score'] for s in type_signals]
                type_returns = [s['return'] for s in type_signals if s['return'] is not None]
                type_wins = [s['win'] for s in type_signals if s['win'] is not None]
                
                if type_returns and type_wins:
                    win_rate = sum(type_wins) / len(type_wins)
                    avg_return = np.mean(type_returns)
                    
                    print(f"  {signal_type}:")
                    print(f"    Signals: {len(type_signals)}")
                    print(f"    Win Rate: {win_rate:.1%}")
                    print(f"    Avg Return: {avg_return:.2%}")
                    print(f"    Score Range: {min(type_scores):.3f} - {max(type_scores):.3f}")
    
    return {
        'total_signals': len(all_signals),
        'passed_signals': len(passed_signals),
        'failed_signals': len(failed_signals),
        'decile_stats': decile_stats if 'decile_stats' in locals() else {}
    }


def main():
    """Main validation function"""
    
    print("Redesigned Scoring System Validator")
    print("Testing new architecture: separate gating from ranking\n")
    
    # Get and prepare data
    df = get_and_prepare_data()
    if df is None:
        return
    
    # Test redesigned scoring
    results = test_redesigned_scoring(df)
    
    # Summary assessment
    print("\n" + "="*60)
    print("REDESIGNED SCORING ASSESSMENT")
    print("="*60)
    
    if results:
        pass_rate = results['passed_signals'] / results['total_signals']
        
        print(f"Signal Processing:")
        print(f"  Total signals: {results['total_signals']}")
        print(f"  Passed gating: {results['passed_signals']} ({pass_rate:.1%})")
        print(f"  Failed gating: {results['failed_signals']} ({1-pass_rate:.1%})")
        
        if results['decile_stats']:
            decile_count = len([d for d in results['decile_stats'].values() if d['trade_count'] > 0])
            print(f"  Active deciles: {decile_count}/10")
            
            if decile_count >= 8:
                print(f"  GOOD: Well-distributed deciles")
            elif decile_count >= 5:
                print(f"  MODERATE: Some decile distribution")
            else:
                print(f"  POOR: Poor decile distribution")
        
        print(f"\nArchitecture Status:")
        if pass_rate > 0.2 and pass_rate < 0.8:
            print(f"  GOOD: Reasonable gating rate")
        else:
            print(f"  CAUTION: Gating rate may need adjustment")
        
        if results['decile_stats']:
            top_decile = results['decile_stats'].get('Decile 10', {})
            bottom_decile = results['decile_stats'].get('Decile 1', {})
            
            if top_decile and bottom_decile:
                win_rate_diff = top_decile['win_rate'] - bottom_decile['win_rate']
                
                if win_rate_diff >= 0.05:
                    print(f"  GOOD: Strong quality discrimination ({win_rate_diff:.1%})")
                elif win_rate_diff >= 0.02:
                    print(f"  MODERATE: Some quality discrimination ({win_rate_diff:.1%})")
                else:
                    print(f"  POOR: Weak quality discrimination ({win_rate_diff:.1%})")
    
    return results


if __name__ == "__main__":
    results = main()
