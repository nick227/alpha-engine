#!/usr/bin/env python
"""Download historical price data from Tiingo API."""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
import pandas as pd

OUTPUT_DIR = Path("data/raw_dumps/tiingo")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
if not TIINGO_API_KEY:
    print("ERROR: TIINGO_API_KEY not found in environment")
    sys.exit(1)

TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
    "TSLA", "BRK-B", "JPM", "V", "UNH", "HD", "PG", "MA", "DIS", "PYPL",
    "BAC", "XOM", "PFE", "CSCO", "INTC", "VZ", "T", "MRK", "ABT", "KO",
    "WMT", "PEP", "CVX", "ABBV", "ACN", "NKE", "ADBE", "CRM", "NFLX", "TXN",
    "AMD", "QCOM", "AVGO", "ORCL", "IBM", "NOW", "INTU", "AMAT", "BKNG", "ISRG"
]


def download_ticker(ticker: str, start_date: str, end_date: str) -> bool:
    """Download historical data for a single ticker from Tiingo."""
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "token": TIINGO_API_KEY,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"  {ticker}: HTTP {response.status_code}")
            return False
        
        data = response.json()
        if not data:
            print(f"  {ticker}: No data")
            return False
        
        df = pd.DataFrame(data)
        df = df.sort_values("date")
        
        output_file = OUTPUT_DIR / f"{ticker}.csv"
        df.to_csv(output_file, index=False)
        print(f"  {ticker}: {len(df)} rows -> {output_file.name}")
        return True
    except Exception as e:
        print(f"  {ticker}: ERROR - {e}")
        return False


def main():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # 2 years
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    print(f"Downloading {len(TICKERS)} tickers from {start_str} to {end_str}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    
    success = 0
    failed = 0
    
    for i, ticker in enumerate(TICKERS, 1):
        print(f"[{i}/{len(TICKERS)}]", end=" ")
        if download_ticker(ticker, start_str, end_str):
            success += 1
        else:
            failed += 1
    
    print()
    print(f"Done: {success} succeeded, {failed} failed")
    return 0 if success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
