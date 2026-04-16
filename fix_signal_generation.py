import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta

def get_price_data():
    """Get all price data for analysis"""
    conn = sqlite3.connect("data/alpha.db")
    query = "SELECT ticker, date, close FROM price_data ORDER BY ticker, date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def multi_day_momentum_5d_fixed(df, hold_days=3, threshold=0.03):
    """Fixed 5-day momentum signal generation"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # CRITICAL FIX: Reset index after sorting
        ticker_df = ticker_df.reset_index(drop=True)
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Find signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['return_5d']) and row['return_5d'] > threshold:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date (now works correctly)
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

def test_fixed_signal_generation():
    """Test the fixed signal generation"""
    
    print("=== TESTING FIXED SIGNAL GENERATION ===")
    
    # Get price data
    df = get_price_data()
    
    # Generate signals with fixed function
    fixed_signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    print(f"Fixed signals: {len(fixed_signals)}")
    
    # Check distribution
    ticker_dist = {}
    for signal in fixed_signals:
        ticker = signal['ticker']
        ticker_dist[ticker] = ticker_dist.get(ticker, 0) + 1
    
    print(f"\nSignal distribution by ticker:")
    for ticker, count in sorted(ticker_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ticker}: {count} signals")
    
    return fixed_signals

def rerun_stabilization_with_fixed_signals():
    """Rerun stabilization tests with correct signals"""
    
    print("\n=== RERUNNING STABILIZATION (FIXED SIGNALS) ===")
    
    # Get price data
    df = get_price_data()
    
    # Generate fixed signals
    signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    print(f"Total signals: {len(signals)}")
    
    # Basic metrics
    trade_returns = [s['trade_return'] - 0.0015 for s in signals]
    win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
    total_return = np.prod(1 + np.array(trade_returns)) - 1
    
    print(f"Basic metrics: {len(signals)} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    # Test 1: Overlap-safe
    print(f"\n=== OVERLAP-SAFE TEST ===")
    
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
        overlap_win_rate = sum(1 for r in selected_returns if r > 0) / len(selected_returns)
        overlap_return = np.prod(1 + np.array(selected_returns)) - 1
        
        print(f"Overlap-safe: {len(selected_returns)} signals, {overlap_win_rate:.1%} win rate, {overlap_return:+.2%} return")
    
    # Test 2: Outlier robustness
    print(f"\n=== OUTLIER ROBUSTNESS TEST ===")
    
    # Remove top 5 winners
    sorted_returns = sorted(trade_returns, reverse=True)
    top5_threshold = sorted_returns[4] if len(sorted_returns) >= 5 else sorted_returns[-1]
    
    filtered_returns = [r for r in trade_returns if r <= top5_threshold]
    
    if filtered_returns:
        filtered_win_rate = sum(1 for r in filtered_returns if r > 0) / len(filtered_returns)
        filtered_return = np.prod(1 + np.array(filtered_returns)) - 1
        
        print(f"Without top 5: {len(filtered_returns)} signals, {filtered_win_rate:.1%} win rate, {filtered_return:+.2%} return")
        
        robust = filtered_win_rate > 0.52 and filtered_return > 0
        print(f"Robustness: {'ROBUST' if robust else 'FRAGILE'}")
    
    # Test 3: Volatility filtering (fixed)
    print(f"\n=== VOLATILITY FILTERING TEST ===")
    
    # Calculate volatility correctly
    volatility_data = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        ticker_df = ticker_df.reset_index(drop=True)
        
        # Calculate daily returns
        ticker_df['daily_return'] = ticker_df['close'].pct_change()
        
        # Calculate rolling volatility
        ticker_df['volatility'] = ticker_df['daily_return'].rolling(20).std() * np.sqrt(252)
        
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['volatility']):
                volatility_data.append({
                    'ticker': ticker,
                    'date': pd.to_datetime(row['date']).date(),
                    'volatility': row['volatility']
                })
    
    vol_df = pd.DataFrame(volatility_data)
    
    # Merge signals with volatility
    signals_df = pd.DataFrame(signals)
    vol_df['date'] = pd.to_datetime(vol_df['date']).dt.date
    
    merged = signals_df.merge(vol_df, on=['ticker', 'date'], how='left')
    
    # Remove signals without volatility data
    merged = merged.dropna(subset=['volatility'])
    
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
        
        vol_trade_returns = vol_signals['trade_return'] - 0.0015
        vol_win_rate = (vol_trade_returns > 0).mean()
        vol_total_return = np.prod(1 + vol_trade_returns) - 1
        
        vol_results[vol_regime] = {
            'signals': len(vol_signals),
            'win_rate': vol_win_rate,
            'return': vol_total_return
        }
        
        print(f"{vol_regime.upper():5s} VOL: {len(vol_signals):3d} signals, {vol_win_rate:.1%} win rate, {vol_total_return:+.2%} return")
    
    # Summary assessment
    print(f"\n=== FIXED STABILIZATION SUMMARY ===")
    
    criteria_met = []
    
    # Basic criteria
    if win_rate > 0.52:
        criteria_met.append("Win rate: GOOD")
    else:
        criteria_met.append("Win rate: POOR")
    
    if total_return > 0:
        criteria_met.append("Return: POSITIVE")
    else:
        criteria_met.append("Return: NEGATIVE")
    
    # Overlap safety
    if selected_returns and overlap_return > 0:
        criteria_met.append("Overlap safety: MAINTAINS EDGE")
    else:
        criteria_met.append("Overlap safety: DESTROYS EDGE")
    
    # Outlier robustness
    if filtered_returns and filtered_win_rate > 0.52 and filtered_return > 0:
        criteria_met.append("Outlier robustness: GOOD")
    else:
        criteria_met.append("Outlier robustness: POOR")
    
    # Volatility filtering
    if vol_results.get('low', {}).get('return', 0) > 0:
        criteria_met.append("Low volatility filtering: HELPS")
    else:
        criteria_met.append("Low volatility filtering: NO HELP")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    return {
        'signals': len(signals),
        'win_rate': win_rate,
        'return': total_return,
        'overlap_results': {'signals': len(selected_returns), 'win_rate': overlap_win_rate, 'return': overlap_return} if selected_returns else None,
        'outlier_results': {'signals': len(filtered_returns), 'win_rate': filtered_win_rate, 'return': filtered_return} if filtered_returns else None,
        'vol_results': vol_results
    }

if __name__ == "__main__":
    # Test fixed signal generation
    fixed_signals = test_fixed_signal_generation()
    
    # Rerun stabilization tests
    results = rerun_stabilization_with_fixed_signals()
