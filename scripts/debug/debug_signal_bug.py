import sqlite3
import pandas as pd
import numpy as np

def get_price_data():
    """Get all price data for analysis"""
    conn = sqlite3.connect("data/alpha.db")
    query = "SELECT ticker, date, close FROM price_data ORDER BY ticker, date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def debug_signal_generation():
    """Debug the signal generation bug"""
    
    print("=== DEBUGGING SIGNAL GENERATION BUG ===")
    
    # Get price data
    df = get_price_data()
    
    print(f"Total price records: {len(df)}")
    print(f"Unique tickers: {df['ticker'].nunique()}")
    
    # Check the original function step by step
    all_signals = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Count potential signals
        potential_signals = ticker_df[pd.notna(ticker_df['return_5d']) & (ticker_df['return_5d'] > 0.03)]
        
        print(f"{ticker}: {len(potential_signals)} potential signals")
        
        # Find signals (this is where the bug might be)
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['return_5d']) and row['return_5d'] > 0.03:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date - THIS MIGHT BE THE BUG
                exit_idx = idx + 3
                if exit_idx < len(ticker_df):
                    exit_price = ticker_df.iloc[exit_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    all_signals.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_return': row['return_5d'],
                        'trade_return': trade_return
                    })
        
        # Debug: check if we're getting signals for each ticker
        if ticker == 'AAPL':
            print(f"  AAPL signals so far: {len([s for s in all_signals if s['ticker'] == 'AAPL'])}")
        elif ticker == 'NVDA':
            print(f"  NVDA signals so far: {len([s for s in all_signals if s['ticker'] == 'NVDA'])}")
    
    print(f"\nTotal signals generated: {len(all_signals)}")
    
    # Check final distribution
    final_dist = {}
    for signal in all_signals:
        ticker = signal['ticker']
        final_dist[ticker] = final_dist.get(ticker, 0) + 1
    
    print(f"\nFinal signal distribution:")
    for ticker, count in sorted(final_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ticker}: {count} signals")
    
    return all_signals

def test_individual_ticker():
    """Test signal generation for individual tickers"""
    
    print("\n=== TESTING INDIVIDUAL TICKERS ===")
    
    # Get price data
    df = get_price_data()
    
    # Test just NVDA (should have 413 signals)
    nvda_df = df[df['ticker'] == 'NVDA'].copy()
    nvda_df = nvda_df.sort_values('date')
    
    print(f"NVDA price records: {len(nvda_df)}")
    
    # Calculate 5-day returns
    nvda_df['return_5d'] = nvda_df['close'].pct_change(5)
    
    # Count signals
    nvda_signals = nvda_df[pd.notna(nvda_df['return_5d']) & (nvda_df['return_5d'] > 0.03)]
    print(f"NVDA potential signals: {len(nvda_signals)}")
    
    # Generate actual signals
    nvda_actual = []
    for idx, row in nvda_df.iterrows():
        if pd.notna(row['return_5d']) and row['return_5d'] > 0.03:
            
            # Calculate hold period return
            signal_date = row['date']
            entry_price = row['close']
            
            # Find exit date
            exit_idx = idx + 3
            if exit_idx < len(nvda_df):
                exit_price = nvda_df.iloc[exit_idx]['close']
                trade_return = (exit_price / entry_price) - 1
                
                nvda_actual.append({
                    'date': pd.to_datetime(signal_date).date(),
                    'ticker': 'NVDA',
                    'signal_return': row['return_5d'],
                    'trade_return': trade_return
                })
    
    print(f"NVDA actual signals: {len(nvda_actual)}")
    
    # Check first few signals
    if nvda_actual:
        print(f"First few NVDA signals:")
        for i, signal in enumerate(nvda_actual[:5]):
            print(f"  {signal['date']}: {signal['signal_return']:.2%} -> {signal['trade_return']:.2%}")
    
    return nvda_actual

if __name__ == "__main__":
    # Debug the full signal generation
    all_signals = debug_signal_generation()
    
    # Test individual ticker
    nvda_signals = test_individual_ticker()
