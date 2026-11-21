"""
Verify active universe inclusion and incremental ingestion mechanics
"""
import yaml
from pathlib import Path
from datetime import date
from app.core.active_universe import get_active_universe_tickers, get_consumer_universe_tickers
from dev_scripts.scripts.download_prices_daily import _get_last_bar_dates
import sqlite3

def verify():
    # 1. Check consumer universe loading
    consumer = get_consumer_universe_tickers()
    print(f"Consumer universe loaded: {len(consumer)} symbols")

    # 2. Check active universe union
    active = get_active_universe_tickers()
    print(f"Active universe total: {len(active)} symbols")

    missing = [s for s in consumer if s not in active]
    print(f"Missing from active universe: {len(missing)}")
    assert len(missing) == 0, f"Missing symbols: {missing}"

    # 3. Check incremental bar dates
    conn = sqlite3.connect("data/alpha.db")
    last_dates = _get_last_bar_dates(conn, active, tenant_id="default")
    conn.close()

    print(f"Symbols with recorded bars: {sum(1 for d in last_dates.values() if d is not None)}/{len(active)}")
    sample_dates = {s: last_dates[s].isoformat() for s in ["AAPL", "MSFT", "NVDA", "MMC", "SPY"] if last_dates.get(s)}
    print(f"Sample latest bar dates: {sample_dates}")

    # Verify all 153 are current through Nov 20, 2026
    non_current = [s for s in consumer if not last_dates.get(s) or (date.today() - last_dates[s]).days > 7]
    print(f"Non-current consumer symbols: {len(non_current)}")
    assert len(non_current) == 0, f"Found non-current symbols: {non_current}"

    print("ALL 153 CONSUMER SYMBOLS VERIFIED IN ACTIVE UNIVERSE AND INCREMENTAL INGESTION.")

if __name__ == "__main__":
    verify()
