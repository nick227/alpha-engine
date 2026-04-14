"""Download recent stock price data from Yahoo Finance."""
import os
import time
import requests

SYMBOLS = ["SPY", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "AMD", "QQQ", "IWM"]
OUTPUT_DIR = "data/raw_dumps/yahoo"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Date range: last 2 years
START = int(time.time()) - (730 * 24 * 60 * 60)
END = int(time.time())

for symbol in SYMBOLS:
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={START}&period2={END}&interval=1d&events=history"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if "Date" in r.text[:100]:
            path = os.path.join(OUTPUT_DIR, f"{symbol}.csv")
            with open(path, "w") as f:
                f.write(r.text)
            print(f"Downloaded: {symbol} -> {path}")
        else:
            print(f"Failed: {symbol} - {r.text[:100]}")
        time.sleep(2)  # Rate limit
    except Exception as e:
        print(f"Error: {symbol} - {e}")

print("Done!")
