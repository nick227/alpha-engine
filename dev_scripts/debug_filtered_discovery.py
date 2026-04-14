import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.discovery.runner import run_discovery
from app.discovery.strategies import VALID_PRICE_SYMBOLS

def debug_filtered_discovery():
    """Debug what symbols are being filtered and why."""
    print(f"Valid symbols in price database: {len(VALID_PRICE_SYMBOLS)}")
    print(f"Sample valid symbols: {list(VALID_PRICE_SYMBOLS)[:10]}")
    
    # Run discovery
    result = run_discovery(
        db_path="data/alpha.db",
        as_of=datetime(2026, 4, 1).date(),
        min_avg_dollar_volume_20d=1_000_000,
        use_feature_snapshot=True
    )
    
    print(f"\nDiscovery results:")
    total_candidates = 0
    for strategy_name, strategy_data in result.get("strategies", {}).items():
        top_candidates = strategy_data.get("top", [])
        total_candidates += len(top_candidates)
        
        print(f"  {strategy_name}: {len(top_candidates)} candidates")
        for i, candidate in enumerate(top_candidates[:3]):  # Show first 3
            symbol = candidate['symbol']
            is_valid = symbol in VALID_PRICE_SYMBOLS
            print(f"    {i+1}. {symbol}: {'VALID' if is_valid else 'INVALID'}")
            if not is_valid:
                print(f"        Not in price database")
    
    print(f"\nTotal candidates: {total_candidates}")
    
    # Check feature snapshot for some symbols
    conn = sqlite3.connect("data/alpha.db")
    conn.row_factory = sqlite3.Row
    
    # Get a few feature rows to see what's in them
    features = conn.execute("""
        SELECT symbol, close, dollar_volume
        FROM feature_snapshot
        WHERE as_of_date = '2026-04-01'
        ORDER BY dollar_volume DESC
        LIMIT 10
    """).fetchall()
    
    print(f"\nSample feature rows (ordered by dollar_volume):")
    for f in features:
        is_valid = f['symbol'] in VALID_PRICE_SYMBOLS
        print(f"  {f['symbol']}: close={f['close']}, vol=${f['dollar_volume']:,.0f}, valid={is_valid}")
    
    conn.close()

if __name__ == "__main__":
    debug_filtered_discovery()
