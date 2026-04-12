import asyncio
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from app.core.bars import build_bars_provider
from app.db.repository import AlphaRepository

async def backfill_aapl():
    load_dotenv()
    repo = AlphaRepository("data/alpha.db")
    
    # Fallback to yfinance as Alpaca returned 403
    provider_name = "yfinance"
    print(f"Using provider: {provider_name}")
    
    try:
        provider = build_bars_provider(provider_name)
    except Exception as e:
        print(f"Failed to build {provider_name}, falling back to yfinance. Error: {e}")
        provider = build_bars_provider("yfinance")

    ticker = "AAPL"
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=120)  # Fetch 120 days to be safe for indicators + 30 trades
    
    print(f"Fetching bars for {ticker} from {start.date()} to {end.date()}...")
    
    bars = provider.fetch_bars(
        timeframe="1d",
        ticker=ticker,
        start=start,
        end=end
    )
    
    if not bars:
        print("No bars found!")
        return

    print(f"Found {len(bars)} bars. Saving to database...")
    
    # Save to price_bars table
    # AlphaRepository.save_price_bars exists (inherited or direct)
    repo.save_price_bars(ticker, "1d", bars)
    
    print("Backfill complete.")

if __name__ == "__main__":
    asyncio.run(backfill_aapl())
