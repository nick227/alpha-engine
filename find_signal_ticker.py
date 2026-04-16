import sqlite3
import pandas as pd

def get_price_data():
    """Get all price data for analysis"""
    conn = sqlite3.connect("data/alpha.db")
    query = "SELECT ticker, date, close FROM price_data ORDER BY ticker, date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def find_signal_ticker():
    """Find which ticker is generating all the signals"""
    
    print("=== FINDING SIGNAL TICKER ===")
    
    # Get price data
    df = get_price_data()
    
    # Check each ticker individually
    ticker_signals = {}
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Count signals
        signals = ticker_df[pd.notna(ticker_df['return_5d']) & (ticker_df['return_5d'] > 0.03)]
        
        ticker_signals[ticker] = len(signals)
        
        if len(signals) > 0:
            print(f"{ticker}: {len(signals)} signals")
    
    # Find the culprit
    total_signals = sum(ticker_signals.values())
    print(f"\nTotal signals across all tickers: {total_signals}")
    
    # Show top signal generators
    sorted_signals = sorted(ticker_signals.items(), key=lambda x: x[1], reverse=True)
    print(f"\nTop signal generators:")
    for ticker, count in sorted_signals[:5]:
        print(f"  {ticker}: {count} signals")
    
    return sorted_signals

def fix_signal_generation():
    """Generate signals for all tickers correctly"""
    
    print("\n=== FIXING SIGNAL GENERATION ===")
    
    # Get price data
    df = get_price_data()
    
    # Generate signals for all tickers
    all_signals = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Find signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['return_5d']) and row['return_5d'] > 0.03:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date
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
    
    print(f"Total signals from all tickers: {len(all_signals)}")
    
    # Check ticker distribution
    ticker_dist = {}
    for signal in all_signals:
        ticker = signal['ticker']
        ticker_dist[ticker] = ticker_dist.get(ticker, 0) + 1
    
    print(f"\nSignal distribution by ticker:")
    for ticker, count in sorted(ticker_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ticker}: {count} signals")
    
    return all_signals

if __name__ == "__main__":
    # Find which ticker is causing the issue
    signal_generators = find_signal_ticker()
    
    # Fix signal generation
    all_signals = fix_signal_generation()
