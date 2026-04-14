#!/usr/bin/env python
"""Download historical price data from Yahoo Finance."""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf

OUTPUT_DIR = Path("data/raw_dumps/yahoo")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
    "TSLA", "BRK-B", "JPM", "V", "UNH", "HD", "PG", "MA", "DIS", "PYPL",
    "BAC", "XOM", "PFE", "CSCO", "INTC", "VZ", "T", "MRK", "ABT", "KO",
    "WMT", "PEP", "CVX", "ABBV", "ACN", "NKE", "ADBE", "CRM", "NFLX", "TXN",
    "AMD", "QCOM", "AVGO", "ORCL", "IBM", "NOW", "INTU", "AMAT", "BKNG", "ISRG",
    "GILD", "MDLZ", "ADP", "REGN", "VRTX", "ZTS", "MMM", "TMO", "UNP", "HON",
    "CAT", "GE", "BA", "LMT", "RTX", "NOC", "UPS", "FDX", "GS", "MS",
    "C", "WFC", "BLK", "SCHW", "AXP", "SPGI", "MCO", "CME", "ICE", "AON",
    "MMC", "MET", "PRU", "AFL", "TRV", "CBOE", "COF", "SYF", "USB", "PNC",
    "TFC", "BK", "STT", "SCHD", "VOO", "IVV", "VTI", "VEA", "VWO", "BND"
]


def download_ticker(ticker: str, start_date: str, end_date: str) -> bool:
    """Download historical data for a single ticker."""
    try:
        ticker_obj = yf.Ticker(ticker)
        df = ticker_obj.history(start=start_date, end=end_date, auto_adjust=True)
        
        if df.empty:
            print(f"  {ticker}: No data")
            return False
        
        output_file = OUTPUT_DIR / f"{ticker}.csv"
        df.to_csv(output_file)
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
