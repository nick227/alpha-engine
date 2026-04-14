#!/usr/bin/env python
"""Download historical price data from Yahoo Finance using direct CSV download."""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

OUTPUT_DIR = Path("data/raw_dumps/yahoo")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
    "TSLA", "BRK-B", "JPM", "V", "UNH", "HD", "PG", "MA", "DIS", "PYPL",
    "BAC", "XOM", "PFE", "CSCO", "INTC", "VZ", "T", "MRK", "ABT", "KO",
    "WMT", "PEP", "CVX", "ABBV", "ACN", "NKE", "ADBE", "CRM", "NFLX", "TXN",
    "AMD", "QCOM", "AVGO", "ORCL", "IBM", "NOW", "INTU", "AMAT", "BKNG", "ISRG"
]


def download_ticker(ticker: str, period1: int, period2: int) -> bool:
    """Download historical data using Yahoo Finance CSV endpoint."""
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"  {ticker}: HTTP {response.status_code}")
            return False
        
        content = response.text
        if "error" in content.lower() or "null" in content[:100]:
            print(f"  {ticker}: No data")
            return False
        
        output_file = OUTPUT_DIR / f"{ticker}.csv"
        with open(output_file, "w") as f:
            f.write(content)
        
        lines = content.strip().split("\n")
        print(f"  {ticker}: {len(lines)-1} rows -> {output_file.name}")
        return True
    except Exception as e:
        print(f"  {ticker}: ERROR - {e}")
        return False


def main():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    
    period1 = int(start_date.timestamp())
    period2 = int(end_date.timestamp())
    
    print(f"Downloading {len(TICKERS)} tickers from {start_date.date()} to {end_date.date()}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    
    success = 0
    failed = 0
    
    for i, ticker in enumerate(TICKERS, 1):
        print(f"[{i}/{len(TICKERS)}]", end=" ")
        if download_ticker(ticker, period1, period2):
            success += 1
        else:
            failed += 1
    
    print()
    print(f"Done: {success} succeeded, {failed} failed")
    return 0 if success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
