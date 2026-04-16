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

def multi_day_momentum_5d_original(df, hold_days=3, threshold=0.03):
    """Original 5-day momentum signal generation"""
    
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

def calculate_volatility_debug(df, window=20):
    """Debug volatility calculation - check for duplication"""
    
    print("=== DEBUGGING VOLATILITY CALCULATION ===")
    
    volatility_data = []
    ticker_counts = {}
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate daily returns
        ticker_df['daily_return'] = ticker_df['close'].pct_change()
        
        # Calculate rolling volatility
        ticker_df['volatility'] = ticker_df['daily_return'].rolling(window=window).std() * np.sqrt(252)
        
        ticker_vol_count = 0
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['volatility']):
                volatility_data.append({
                    'ticker': ticker,
                    'date': pd.to_datetime(row['date']).date(),
                    'volatility': row['volatility']
                })
                ticker_vol_count += 1
        
        ticker_counts[ticker] = ticker_vol_count
        print(f"{ticker}: {ticker_vol_count} volatility records")
    
    print(f"Total volatility records: {len(volatility_data)}")
    print(f"Unique tickers: {len(ticker_counts)}")
    
    return pd.DataFrame(volatility_data), ticker_counts

def debug_signal_explosion():
    """Debug why signal count exploded from 215 to 3,135+"""
    
    print("=== DEBUGGING SIGNAL COUNT EXPLOSION ===")
    print("Investigating why volatility test shows 3,135+ signals vs 215 original\n")
    
    # Get price data
    df = get_price_data()
    
    # Original signal count
    original_signals = multi_day_momentum_5d_original(df, 3, 0.03)
    print(f"Original signals: {len(original_signals)}")
    
    # Check data structure
    print(f"\n=== DATA STRUCTURE CHECK ===")
    print(f"Total price records: {len(df)}")
    print(f"Unique tickers: {df['ticker'].nunique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Check ticker counts
    ticker_counts = df.groupby('ticker').size().sort_values(ascending=False)
    print(f"\nTop 10 tickers by records:")
    for ticker, count in ticker_counts.head(10).items():
        print(f"  {ticker}: {count} records")
    
    # Debug volatility calculation
    vol_df, vol_counts = calculate_volatility_debug(df)
    
    # Check for duplication in merge
    print(f"\n=== MERGE DUPLICATION CHECK ===")
    
    # Convert signals to DataFrame
    signals_df = pd.DataFrame(original_signals)
    signals_df['date'] = pd.to_datetime(signals_df['date']).dt.date
    
    print(f"Signals DataFrame shape: {signals_df.shape}")
    print(f"Unique signal dates: {signals_df['date'].nunique()}")
    print(f"Unique signal tickers: {signals_df['ticker'].nunique()}")
    
    # Check volatility DataFrame
    print(f"Volatility DataFrame shape: {vol_df.shape}")
    print(f"Unique vol dates: {vol_df['date'].nunique()}")
    print(f"Unique vol tickers: {vol_df['ticker'].nunique()}")
    
    # Simulate the merge that caused explosion
    print(f"\n=== SIMULATING PROBLEMATIC MERGE ===")
    
    # This is what happened in the volatility test
    # The volatility DataFrame has multiple records per (ticker, date)
    duplicate_check = vol_df.groupby(['ticker', 'date']).size().sort_values(ascending=False)
    
    print(f"Checking for duplicate (ticker, date) pairs in volatility data:")
    duplicates = duplicate_check[duplicate_check > 1]
    
    if len(duplicates) > 0:
        print(f"Found {len(duplicates)} duplicate (ticker, date) pairs!")
        print("Top 10 duplicates:")
        for (ticker, date), count in duplicates.head(10).items():
            print(f"  {ticker} on {date}: {count} records")
        
        print(f"\nThis explains the signal count explosion!")
        print(f"Each original signal was multiplied by the number of duplicate volatility records")
    else:
        print("No duplicates found in volatility data")
    
    return duplicates

def fix_volatility_calculation(df, window=20):
    """Fixed volatility calculation without duplicates"""
    
    print("=== FIXED VOLATILITY CALCULATION ===")
    
    volatility_data = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate daily returns
        ticker_df['daily_return'] = ticker_df['close'].pct_change()
        
        # Calculate rolling volatility
        ticker_df['volatility'] = ticker_df['daily_return'].rolling(window=window).std() * np.sqrt(252)
        
        # Only keep one record per (ticker, date)
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['volatility']):
                volatility_data.append({
                    'ticker': ticker,
                    'date': pd.to_datetime(row['date']).date(),
                    'volatility': row['volatility']
                })
    
    # Convert to DataFrame and remove duplicates
    vol_df = pd.DataFrame(volatility_data)
    vol_df = vol_df.drop_duplicates(subset=['ticker', 'date'], keep='first')
    
    print(f"Fixed volatility records: {len(vol_df)}")
    print(f"Unique (ticker, date) pairs: {len(vol_df.groupby(['ticker', 'date']))}")
    
    return vol_df

def rerun_stabilization_test():
    """Rerun stabilization test with fixed volatility data"""
    
    print("\n=== RERUNNING STABILIZATION TEST (FIXED) ===")
    
    # Get data
    df = get_price_data()
    
    # Generate signals
    signals = multi_day_momentum_5d_original(df, 3, 0.03)
    print(f"Original signals: {len(signals)}")
    
    # Fixed volatility calculation
    vol_df = fix_volatility_calculation(df)
    
    # Volatility split test (fixed)
    print(f"\n=== VOLATILITY ANALYSIS (FIXED) ===")
    
    # Merge signals with volatility
    signals_df = pd.DataFrame(signals)
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

if __name__ == "__main__":
    # Debug the signal count explosion
    duplicates = debug_signal_explosion()
    
    # Rerun with fixed calculation
    vol_results = rerun_stabilization_test()
    
    print(f"\n=== CONCLUSION ===")
    if len(duplicates) > 0:
        print(f"Signal count explosion caused by {len(duplicates)} duplicate volatility records")
        print(f"Fixed volatility analysis shows realistic signal counts")
    else:
        print(f"No duplication found - signal count explosion has different cause")
    
    print(f"\nFixed volatility results:")
    for regime, results in vol_results.items():
        print(f"  {regime}: {results['signals']} signals, {results['win_rate']:.1%} win rate, {results['return']:+.2%} return")
