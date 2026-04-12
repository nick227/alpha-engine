#!/usr/bin/env python
"""Download historical price data from Financial Modeling Prep API."""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

OUTPUT_DIR = Path("data/raw_dumps/fmp")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
if not FMP_API_KEY:
    print("ERROR: FMP_API_KEY not found in environment")
    sys.exit(1)

TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
    "TSLA", "BRK-B", "JPM", "V", "UNH", "HD", "PG", "MA", "DIS", "PYPL",
    "BAC", "XOM", "PFE", "CSCO", "INTC", "VZ", "T", "MRK", "ABT", "KO",
    "WMT", "PEP", "CVX", "ABBV", "ACN", "NKE", "ADBE", "CRM", "NFLX", "TXN",
    "AMD", "QCOM", "AVGO", "ORCL", "IBM", "NOW", "INTU", "AMAT", "BKNG", "ISRG"
]


def download_ticker(ticker: str) -> bool:
    """Download historical data for a single ticker from FMP."""
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
    params = {
        "apikey": FMP_API_KEY,
        "serietype": "line",
        "outputsize": "full"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"  {ticker}: HTTP {response.status_code}")
            return False
        
        data = response.json()
        if not isinstance(data, dict) or "historical" not in data:
            print(f"  {ticker}: No data in response")
            return False
        
        historical = data["historical"]
        if not historical:
            print(f"  {ticker}: Empty historical data")
            return False
        
        records = []
        for item in historical:
            records.append({
                "date": item["date"],
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "adjClose": item.get("adjClose", item.get("close")),
                "volume": item.get("volume")
            })
        
        df = pd.DataFrame(records)
        df = df.sort_values("date")
        
        output_file = OUTPUT_DIR / f"{ticker}.csv"
        df.to_csv(output_file, index=False)
        print(f"  {ticker}: {len(df)} rows -> {output_file.name}")
        return True
    except Exception as e:
        print(f"  {ticker}: ERROR - {e}")
        return False


def main():
    print(f"Downloading {len(TICKERS)} tickers from FMP API")
    print(f"API Key: {FMP_API_KEY[:10]}...")
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    
    success = 0
    failed = 0
    
    for i, ticker in enumerate(TICKERS, 1):
        print(f"[{i}/{len(TICKERS)}]", end=" ")
        if download_ticker(ticker):
            success += 1
        else:
            failed += 1
    
    print()
    print(f"Done: {success} succeeded, {failed} failed")
    return 0 if success > 0 else 1


if __name__ == "__main__":
    import pandas as pd
    sys.exit(main())
