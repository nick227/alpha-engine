"""Download recent stock price data from Alpha Vantage."""
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

SYMBOLS = ["SPY", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "AMD", "QQQ", "IWM"]
OUTPUT_DIR = "data/raw_dumps/alpha_vantage"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "EY8J3ZVMQNHVWAI8")

os.makedirs(OUTPUT_DIR, exist_ok=True)

for symbol in SYMBOLS:
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={API_KEY}&datatype=csv"

    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200 and "timestamp" in r.text[:200]:
            path = os.path.join(OUTPUT_DIR, f"{symbol}.csv")
            with open(path, "w") as f:
                f.write(r.text)
            print(f"Downloaded: {symbol} -> {path}")
        else:
            print(f"Failed: {symbol} - {r.text[:100]}")
        time.sleep(12)  # Alpha Vantage rate limit: 5 calls/min
    except Exception as e:
        print(f"Error: {symbol} - {e}")

print("Done!")
