import sqlite3
import numpy as np
from datetime import datetime, timedelta
import pandas as pd

def get_price_data():
    """Get all price data for analysis"""
    conn = sqlite3.connect("data/alpha.db")
    query = "SELECT ticker, date, close FROM price_data ORDER BY ticker, date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def calculate_volatility(df, window=20):
    """Calculate rolling volatility for regime analysis"""
    
    volatility_data = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate daily returns
        ticker_df['daily_return'] = ticker_df['close'].pct_change()
        
        # Calculate rolling volatility
        ticker_df['volatility'] = ticker_df['daily_return'].rolling(window=window).std() * np.sqrt(252)
        
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['volatility']):
                volatility_data.append({
                    'ticker': ticker,
                    'date': pd.to_datetime(row['date']).date(),
                    'volatility': row['volatility']
                })
    
    return pd.DataFrame(volatility_data)

def get_market_regime(df):
    """Classify market regime based on overall market trend"""
    
    # Use equal-weighted market index
    daily_returns = []
    
    for date in df['date'].unique():
        date_df = df[df['date'] == date]
        if len(date_df) > 0:
            # Calculate equal-weighted return
            date_df = date_df.sort_values('ticker')
            date_df['prev_close'] = date_df['close'].shift(1)
            date_df['daily_return'] = date_df['close'] / date_df['prev_close'] - 1
            market_return = date_df['daily_return'].mean()
            daily_returns.append({'date': date, 'market_return': market_return})
    
    market_df = pd.DataFrame(daily_returns)
    market_df = market_df.sort_values('date')
    
    # Calculate market trend
    market_df['market_trend_20d'] = market_df['market_return'].rolling(20).mean()
    
    # Classify regimes
    regimes = []
    for idx, row in market_df.iterrows():
        if pd.notna(row['market_trend_20d']):
            if row['market_trend_20d'] > 0.001:
                regime = 'bull'
            elif row['market_trend_20d'] < -0.001:
                regime = 'bear'
            else:
                regime = 'neutral'
        else:
            regime = 'unknown'
        
        regimes.append({
            'date': row['date'],
            'regime': regime,
            'market_return': row['market_return']
        })
    
    return pd.DataFrame(regimes)

def multi_day_momentum_5d(df, hold_days=3, threshold=0.03):
    """Generate 5-day momentum signals"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Find signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['return_5d']) and row['return_5d'] > threshold:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date
                exit_idx = idx + hold_days
                if exit_idx < len(ticker_df):
                    exit_price = ticker_df.iloc[exit_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    results.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_return': row['return_5d'],
                        'trade_return': trade_return
                    })
    
    return results

def test_regime_splits(signals, market_regimes):
    """Test signal performance across different market regimes"""
    
    print("=== REGIME ANALYSIS ===")
    
    # Merge signals with regimes
    signals_df = pd.DataFrame(signals)
    regime_df = market_regimes.copy()
    regime_df['date'] = pd.to_datetime(regime_df['date']).dt.date
    
    merged = signals_df.merge(regime_df, on='date', how='left')
    
    # Analyze by regime
    regime_results = {}
    
    for regime in ['bull', 'bear', 'neutral']:
        regime_signals = merged[merged['regime'] == regime]
        
        if len(regime_signals) == 0:
            continue
        
        trade_returns = regime_signals['trade_return'] - 0.0015
        win_rate = (trade_returns > 0).mean()
        total_return = np.prod(1 + trade_returns) - 1
        
        regime_results[regime] = {
            'signals': len(regime_signals),
            'win_rate': win_rate,
            'return': total_return
        }
        
        print(f"{regime.upper():8s}: {len(regime_signals):3d} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    return regime_results

def test_volatility_splits(signals, volatility_df):
    """Test signal performance across volatility regimes"""
    
    print("\n=== VOLATILITY ANALYSIS ===")
    
    # Merge signals with volatility
    signals_df = pd.DataFrame(signals)
    vol_df = volatility_df.copy()
    vol_df['date'] = pd.to_datetime(vol_df['date']).dt.date
    
    merged = signals_df.merge(vol_df, on='date', how='left')
    
    # Classify volatility regimes
    vol_median = merged['volatility'].median()
    
    merged['vol_regime'] = merged['volatility'].apply(
        lambda x: 'high' if x > vol_median else 'low'
    )
    
    # Analyze by volatility
    vol_results = {}
    
    for vol_regime in ['high', 'low']:
        vol_signals = merged[merged['vol_regime'] == vol_regime]
        
        if len(vol_signals) == 0:
            continue
        
        trade_returns = vol_signals['trade_return'] - 0.0015
        win_rate = (trade_returns > 0).mean()
        total_return = np.prod(1 + trade_returns) - 1
        
        vol_results[vol_regime] = {
            'signals': len(vol_signals),
            'win_rate': win_rate,
            'return': total_return
        }
        
        print(f"{vol_regime.upper():5s} VOL: {len(vol_signals):3d} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    return vol_results

def test_overlap_safe(signals):
    """Test with overlap control (1 position at a time)"""
    
    print("\n=== OVERLAP-SAFE TEST ===")
    
    # Sort all signals by date
    all_signals = sorted(signals, key=lambda x: x['date'])
    
    selected_returns = []
    current_end_date = None
    
    for signal in all_signals:
        signal_date = signal['date']
        
        # Skip if overlapping
        if current_end_date and signal_date <= current_end_date:
            continue
        
        # Take this signal
        trade_return = signal['trade_return'] - 0.0015
        selected_returns.append(trade_return)
        
        # Set end date (3-day hold)
        current_end_date = signal_date + timedelta(days=3)
    
    if selected_returns:
        win_rate = sum(1 for r in selected_returns if r > 0) / len(selected_returns)
        total_return = np.prod(1 + np.array(selected_returns)) - 1
        
        print(f"Overlap-safe: {len(selected_returns)} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
        
        return {
            'signals': len(selected_returns),
            'win_rate': win_rate,
            'return': total_return,
            'returns': selected_returns
        }
    else:
        print("No overlap-safe signals found")
        return None

def test_outlier_robustness(signals):
    """Test robustness by removing top outliers"""
    
    print("\n=== OUTLIER ROBUSTNESS TEST ===")
    
    # Original returns
    original_returns = [s['trade_return'] - 0.0015 for s in signals]
    original_win_rate = sum(1 for r in original_returns if r > 0) / len(original_returns)
    original_return = np.prod(1 + np.array(original_returns)) - 1
    
    print(f"Original: {len(signals)} signals, {original_win_rate:.1%} win rate, {original_return:+.2%} return")
    
    # Remove top 5 winners
    sorted_returns = sorted(original_returns, reverse=True)
    top5_threshold = sorted_returns[4] if len(sorted_returns) >= 5 else sorted_returns[-1]
    
    filtered_returns = [r for r in original_returns if r <= top5_threshold]
    
    if filtered_returns:
        filtered_win_rate = sum(1 for r in filtered_returns if r > 0) / len(filtered_returns)
        filtered_return = np.prod(1 + np.array(filtered_returns)) - 1
        
        print(f"Without top 5: {len(filtered_returns)} signals, {filtered_win_rate:.1%} win rate, {filtered_return:+.2%} return")
        
        # Check robustness
        robust = filtered_win_rate > 0.52 and filtered_return > 0
        print(f"Robustness: {'ROBUST' if robust else 'FRAGILE'}")
        
        return {
            'signals': len(filtered_returns),
            'win_rate': filtered_win_rate,
            'return': filtered_return,
            'robust': robust
        }
    else:
        print("No returns after removing outliers")
        return None

def stabilize_5d_momentum():
    """Comprehensive stabilization analysis of 5d momentum"""
    
    print("=== 5D MOMENTUM STABILIZATION ANALYSIS ===")
    print("Testing robustness across regimes and conditions\n")
    
    # Get data
    df = get_price_data()
    volatility_df = calculate_volatility(df)
    market_regimes = get_market_regime(df)
    
    # Generate signals
    signals = multi_day_momentum_5d(df, 3, 0.03)
    print(f"Total signals: {len(signals)}")
    
    # Test 1: Regime splits
    regime_results = test_regime_splits(signals, market_regimes)
    
    # Test 2: Volatility splits
    vol_results = test_volatility_splits(signals, volatility_df)
    
    # Test 3: Overlap-safe
    overlap_results = test_overlap_safe(signals)
    
    # Test 4: Outlier robustness
    outlier_results = test_outlier_robustness(signals)
    
    # Summary assessment
    print(f"\n=== STABILIZATION SUMMARY ===")
    
    # Check criteria
    criteria_met = []
    
    # Regime consistency
    positive_regimes = sum(1 for r in regime_results.values() if r['return'] > 0)
    if positive_regimes >= 2:  # At least 2 regimes positive
        criteria_met.append("Regime consistency: GOOD")
    else:
        criteria_met.append("Regime consistency: POOR")
    
    # Volatility filtering
    if vol_results.get('low', {}).get('return', 0) > 0:
        criteria_met.append("Low volatility filtering: HELPS")
    else:
        criteria_met.append("Low volatility filtering: NO HELP")
    
    # Overlap safety
    if overlap_results and overlap_results['return'] > 0:
        criteria_met.append("Overlap control: MAINTAINS EDGE")
    else:
        criteria_met.append("Overlap control: DESTROYS EDGE")
    
    # Outlier robustness
    if outlier_results and outlier_results['robust']:
        criteria_met.append("Outlier robustness: GOOD")
    else:
        criteria_met.append("Outlier robustness: POOR")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    # Overall stability score
    stability_score = sum([
        positive_regimes >= 2,
        vol_results.get('low', {}).get('return', 0) > 0,
        overlap_results and overlap_results['return'] > 0,
        outlier_results and outlier_results['robust']
    ])
    
    print(f"\nStability score: {stability_score}/4")
    
    if stability_score >= 3:
        print(">>> VERDICT: STABLE EDGE")
        print("Signal remains positive across most conditions")
    elif stability_score >= 2:
        print(">>> VERDICT: MARGINALLY STABLE")
        print("Signal works in specific conditions")
    else:
        print(">>> VERDICT: UNSTABLE")
        print("Signal too fragile for deployment")
    
    # Recommendations
    print(f"\n=== RECOMMENDATIONS ===")
    
    if vol_results.get('low', {}).get('return', 0) > vol_results.get('high', {}).get('return', 0):
        print("Consider volatility filtering (avoid high-vol periods)")
    
    if regime_results.get('bull', {}).get('return', 0) > regime_results.get('bear', {}).get('return', 0):
        print("Consider regime filtering (avoid bear markets)")
    
    if overlap_results and overlap_results['return'] > 0:
        print("Overlap control maintains edge - good for risk management")
    
    if not outlier_results or not outlier_results['robust']:
        print("Signal depends on outliers - needs position sizing limits")

if __name__ == "__main__":
    stabilize_5d_momentum()
