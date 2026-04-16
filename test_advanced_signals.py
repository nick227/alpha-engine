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

def multi_day_momentum(df, lookback_days=3, hold_days=1, threshold=0.03):
    """Multi-day momentum: 3-5 day returns -> hold 1-3 days"""
    
    print(f"=== MULTI-DAY MOMENTUM ({lookback_days}d lookback, {hold_days}d hold) ===")
    
    # Group by ticker
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate multi-day returns
        ticker_df[f'return_{lookback_days}d'] = ticker_df['close'].pct_change(lookback_days)
        
        # Find signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row[f'return_{lookback_days}d']) and row[f'return_{lookback_days}d'] > threshold:
                
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
                        'signal_return': row[f'return_{lookback_days}d'],
                        'trade_return': trade_return
                    })
    
    return results

def cross_sectional_momentum(df, top_pct=0.1, bottom_pct=0.1):
    """Cross-sectional momentum: long top 10%, short bottom 10%"""
    
    print(f"=== CROSS-SECTIONAL MOMENTUM (top {top_pct*100:.0f}%, bottom {bottom_pct*100:.0f}%) ===")
    
    results = []
    
    # Group by date
    for date in df['date'].unique():
        date_df = df[df['date'] == date].copy()
        
        if len(date_df) < 10:  # Need minimum stocks
            continue
        
        # Calculate 1-day returns
        date_df = date_df.sort_values('ticker')
        date_df['prev_close'] = date_df['close'].shift(1)
        date_df['daily_return'] = date_df['close'] / date_df['prev_close'] - 1
        
        # Drop first row (no previous close)
        date_df = date_df.dropna(subset=['daily_return'])
        
        if len(date_df) < 10:
            continue
        
        # Rank stocks
        date_df = date_df.sort_values('daily_return')
        
        # Select top and bottom
        n_stocks = len(date_df)
        top_n = max(1, int(n_stocks * top_pct))
        bottom_n = max(1, int(n_stocks * bottom_pct))
        
        # Long top performers
        top_stocks = date_df.tail(top_n)
        for _, row in top_stocks.iterrows():
            # Get next day return
            next_date = pd.to_datetime(row['date']) + timedelta(days=1)
            next_day_df = df[(df['ticker'] == row['ticker']) & (df['date'] == next_date.strftime('%Y-%m-%d'))]
            
            if not next_day_df.empty:
                next_close = next_day_df.iloc[0]['close']
                trade_return = (next_close / row['close']) - 1
                
                results.append({
                    'date': pd.to_datetime(row['date']).date(),
                    'ticker': row['ticker'],
                    'signal_return': row['daily_return'],
                    'trade_return': trade_return,
                    'position': 'long'
                })
        
        # Short bottom performers
        bottom_stocks = date_df.head(bottom_n)
        for _, row in bottom_stocks.iterrows():
            # Get next day return (short position = negative return)
            next_date = pd.to_datetime(row['date']) + timedelta(days=1)
            next_day_df = df[(df['ticker'] == row['ticker']) & (df['date'] == next_date.strftime('%Y-%m-%d'))]
            
            if not next_day_df.empty:
                next_close = next_day_df.iloc[0]['close']
                trade_return = -(next_close / row['close'] - 1)  # Short position
                
                results.append({
                    'date': pd.to_datetime(row['date']).date(),
                    'ticker': row['ticker'],
                    'signal_return': row['daily_return'],
                    'trade_return': trade_return,
                    'position': 'short'
                })
    
    return results

def mean_reversion_scaled(df, drop_threshold=0.03):
    """Mean reversion: drops >2-4% -> test next-day bounce"""
    
    print(f"=== MEAN REVERSION (drops >{drop_threshold*100:.0f}%) ===")
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate daily returns
        ticker_df['daily_return'] = ticker_df['close'].pct_change()
        
        # Find drop signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['daily_return']) and row['daily_return'] < -drop_threshold:
                
                # Calculate next day return
                signal_date = row['date']
                entry_price = row['close']
                
                next_idx = idx + 1
                if next_idx < len(ticker_df):
                    exit_price = ticker_df.iloc[next_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    results.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_return': row['daily_return'],
                        'trade_return': trade_return
                    })
    
    return results

def gap_reversal(df, gap_threshold=0.03):
    """Gap reversal: gap up >3% -> test intraday fade"""
    
    print(f"=== GAP REVERSAL (gap up >{gap_threshold*100:.0f}%) ===")
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate gaps
        ticker_df['prev_close'] = ticker_df['close'].shift(1)
        ticker_df['gap_pct'] = (ticker_df['close'] - ticker_df['prev_close']) / ticker_df['prev_close']
        
        # Find gap up signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['gap_pct']) and row['gap_pct'] > gap_threshold:
                
                # Test intraday fade (same day close)
                # For simplicity, we'll test next day reversal
                next_idx = idx + 1
                if next_idx < len(ticker_df):
                    entry_price = row['close']  # Enter at gap close
                    exit_price = ticker_df.iloc[next_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    results.append({
                        'date': pd.to_datetime(row['date']).date(),
                        'ticker': ticker,
                        'signal_return': row['gap_pct'],
                        'trade_return': trade_return
                    })
    
    return results

def analyze_signals(results, signal_name):
    """Analyze signal results"""
    
    if not results:
        print(f"No signals found for {signal_name}")
        return
    
    # Apply friction
    trade_returns = [r['trade_return'] - 0.0015 for r in results]
    
    # Calculate metrics
    win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
    total_return = np.prod(1 + np.array(trade_returns)) - 1
    median_return = np.median(trade_returns)
    
    # Distribution
    q25 = np.percentile(trade_returns, 25)
    q75 = np.percentile(trade_returns, 75)
    
    print(f"Signals: {len(results)}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Total return: {total_return:+.2%}")
    print(f"Median return: {median_return:+.2%}")
    print(f"Q25/Q75: {q25:+.2%}/{q75:+.2%}")
    
    # Check minimum criteria
    if len(results) >= 100 and win_rate > 0.55 and total_return > 0:
        print(">>> MEETS CRITERIA: Real edge detected!")
    elif len(results) >= 100 and win_rate > 0.52:
        print(">>> DIRECTIONAL BIAS: Needs refinement")
    else:
        print(">>> NO EDGE: Signal rejected")
    
    return {
        'signals': len(results),
        'win_rate': win_rate,
        'return': total_return,
        'median': median_return
    }

def test_all_advanced_signals():
    """Test all advanced signals"""
    
    print("=== ADVANCED SIGNALS TEST ===")
    print("Using 30 tickers, 4 years of data\n")
    
    # Get price data
    df = get_price_data()
    
    all_results = {}
    
    # Test 1: Multi-day momentum
    for lookback in [3, 5, 10]:
        for hold in [1, 3]:
            results = multi_day_momentum(df, lookback, hold, 0.03)
            signal_name = f"multi_day_momentum_{lookback}d_{hold}d"
            print(f"\nTesting {signal_name}...")
            all_results[signal_name] = analyze_signals(results, signal_name)
    
    # Test 2: Cross-sectional momentum
    results = cross_sectional_momentum(df, 0.1, 0.1)
    print(f"\nTesting cross_sectional_momentum...")
    all_results['cross_sectional_momentum'] = analyze_signals(results, 'cross_sectional_momentum')
    
    # Test 3: Mean reversion
    for threshold in [0.02, 0.03, 0.04]:
        results = mean_reversion_scaled(df, threshold)
        signal_name = f"mean_reversion_{threshold*100:.0f}%"
        print(f"\nTesting {signal_name}...")
        all_results[signal_name] = analyze_signals(results, signal_name)
    
    # Test 4: Gap reversal
    for threshold in [0.02, 0.03, 0.04]:
        results = gap_reversal(df, threshold)
        signal_name = f"gap_reversal_{threshold*100:.0f}%"
        print(f"\nTesting {signal_name}...")
        all_results[signal_name] = analyze_signals(results, signal_name)
    
    # Summary
    print(f"\n=== SUMMARY ===")
    print("Signal                              Signals  Win Rate  Return")
    print("-" * 55)
    
    for signal_name, metrics in all_results.items():
        if metrics:
            print(f"{signal_name:<30} {metrics['signals']:>8}   {metrics['win_rate']:>7.1%}   {metrics['return']:>+6.2%}")
    
    # Find best signals
    viable_signals = [(name, m) for name, m in all_results.items() 
                     if m and m['signals'] >= 100 and m['win_rate'] > 0.52]
    
    if viable_signals:
        print(f"\n=== VIABLE SIGNALS ===")
        for name, metrics in viable_signals:
            print(f"{name}: {metrics['win_rate']:.1%} win rate, {metrics['return']:+.2%} return")
    else:
        print(f"\n=== NO VIABLE SIGNALS FOUND ===")
        print("All signals failed to meet minimum criteria")

if __name__ == "__main__":
    test_all_advanced_signals()
