import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

def get_sp500_tickers():
    """Get S&P 500 tickers from Wikipedia"""
    
    print("Fetching S&P 500 tickers...")
    
    # Get S&P 500 tickers from Wikipedia
    try:
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(sp500_url)
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].tolist()
        
        # Clean ticker symbols
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        
        print(f"Found {len(tickers)} S&P 500 tickers")
        return tickers
        
    except Exception as e:
        print(f"Error fetching S&P 500 tickers: {e}")
        # Fallback to common liquid stocks
        return [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'JNJ', 'V',
            'PG', 'UNH', 'HD', 'MA', 'PYPL', 'DIS', 'ADBE', 'NFLX', 'CRM', 'BAC',
            'XOM', 'KO', 'PEP', 'CVX', 'T', 'INTC', 'WMT', 'CSCO', 'PFE', 'MRK'
        ]

def download_historical_data(tickers, start_date="2021-01-01", end_date="2024-12-31"):
    """Download historical data for tickers"""
    
    print(f"Downloading data from {start_date} to {end_date}...")
    
    all_data = []
    failed_tickers = []
    
    for i, ticker in enumerate(tickers):
        try:
            print(f"Downloading {ticker} ({i+1}/{len(tickers)})...")
            
            # Download data
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)
            
            if hist.empty:
                print(f"No data for {ticker}")
                failed_tickers.append(ticker)
                continue
            
            # Prepare data
            hist.reset_index(inplace=True)
            hist['ticker'] = ticker
            
            # Rename columns to match schema
            hist = hist.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            # Select required columns
            hist = hist[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
            
            # Remove rows with missing data
            hist = hist.dropna()
            
            # Check data quality
            if len(hist) < 200:  # Need at least ~200 days
                print(f"Insufficient data for {ticker}: {len(hist)} days")
                failed_tickers.append(ticker)
                continue
            
            all_data.append(hist)
            
            # Rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            failed_tickers.append(ticker)
            continue
    
    if failed_tickers:
        print(f"Failed to download {len(failed_tickers)} tickers: {failed_tickers[:10]}...")
    
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        print(f"Successfully downloaded data for {len(all_data)} tickers")
        print(f"Total rows: {len(combined_data)}")
        return combined_data
    else:
        print("No data downloaded successfully")
        return None

def validate_data_quality(df):
    """Validate data quality and remove bad rows"""
    
    print("Validating data quality...")
    
    initial_rows = len(df)
    
    # Remove rows with invalid prices
    df = df[
        (df['open'] > 0) & 
        (df['high'] > 0) & 
        (df['low'] > 0) & 
        (df['close'] > 0) &
        (df['volume'] > 0)
    ]
    
    # Remove rows with invalid price relationships
    df = df[
        (df['low'] <= df['open']) &
        (df['low'] <= df['close']) &
        (df['high'] >= df['open']) &
        (df['high'] >= df['close'])
    ]
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['ticker', 'date'], keep='first')
    
    final_rows = len(df)
    
    print(f"Data validation complete:")
    print(f"  Initial rows: {initial_rows}")
    print(f"  Final rows: {final_rows}")
    print(f"  Removed: {initial_rows - final_rows} ({(initial_rows - final_rows)/initial_rows:.1%})")
    
    return df

def load_data_to_database(df, db_path="data/alpha.db"):
    """Load validated data into database using UPSERT"""
    
    print(f"Loading data to {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create price_data table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price_data (
        ticker TEXT,
        date DATE,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (ticker, date)
    )
    """)
    
    # Prepare data for insertion
    data_to_insert = df.to_dict('records')
    
    # Use UPSERT (INSERT OR REPLACE)
    insert_query = """
    INSERT OR REPLACE INTO price_data 
    (ticker, date, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    # Insert data in batches
    batch_size = 1000
    inserted_count = 0
    
    for i in range(0, len(data_to_insert), batch_size):
        batch = data_to_insert[i:i+batch_size]
        
        cursor.executemany(insert_query, [
            (
                row['ticker'], 
                row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else row['date'],
                row['open'],
                row['high'], 
                row['low'],
                row['close'],
                int(row['volume'])
            )
            for row in batch
        ])
        
        inserted_count += len(batch)
        print(f"Inserted {inserted_count}/{len(data_to_insert)} rows...")
    
    conn.commit()
    conn.close()
    
    print(f"Data loading complete: {inserted_count} rows inserted")

def verify_dataset(db_path="data/alpha.db"):
    """Verify the loaded dataset"""
    
    print("Verifying dataset...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get dataset stats
    cursor.execute("SELECT COUNT(*) FROM price_data")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM price_data")
    unique_tickers = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(date), MAX(date) FROM price_data")
    date_range = cursor.fetchone()
    
    cursor.execute("SELECT ticker, COUNT(*) as days FROM price_data GROUP BY ticker ORDER BY days DESC LIMIT 10")
    top_tickers = cursor.fetchall()
    
    print(f"Dataset verification:")
    print(f"  Total rows: {total_rows}")
    print(f"  Unique tickers: {unique_tickers}")
    print(f"  Date range: {date_range[0]} to {date_range[1]}")
    print(f"  Top 10 tickers by days:")
    for ticker, days in top_tickers:
        print(f"    {ticker}: {days} days")
    
    conn.close()
    
    return total_rows, unique_tickers

def main():
    """Main function to expand dataset"""
    
    print("=== DATASET EXPANSION ===")
    print("Expanding to S&P 500 with multi-year data\n")
    
    # Step 1: Get tickers
    tickers = get_sp500_tickers()
    
    # Step 2: Download data
    data = download_historical_data(tickers, start_date="2021-01-01", end_date="2024-12-31")
    
    if data is None:
        print("Failed to download data")
        return
    
    # Step 3: Validate data
    validated_data = validate_data_quality(data)
    
    # Step 4: Load to database
    load_data_to_database(validated_data)
    
    # Step 5: Verify dataset
    total_rows, unique_tickers = verify_dataset()
    
    # Check if we meet targets
    if unique_tickers >= 100 and total_rows >= 50000:
        print(f"\n=== SUCCESS ===")
        print(f"Dataset expanded successfully:")
        print(f"  {unique_tickers} tickers (target: 100+)")
        print(f"  {total_rows} rows (target: 50,000+)")
        print(f"Ready to re-run signal tests")
    else:
        print(f"\n=== INSUFFICIENT DATA ===")
        print(f"Need at least 100 tickers and 50,000 rows")
        print(f"Got: {unique_tickers} tickers, {total_rows} rows")

if __name__ == "__main__":
    main()
