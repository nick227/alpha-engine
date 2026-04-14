import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository
from app.engine.replay_sqlite import SQLitePriceRepository

def debug_specific_valid_prediction():
    """Debug one specific prediction that should be scoreable."""
    repo = Repository(db_path="data/alpha.db")
    prices = SQLitePriceRepository(repo)
    
    # Get one discovery prediction
    prediction = repo.conn.execute("""
        SELECT id, strategy_id, ticker, timestamp, entry_price, prediction, confidence, horizon
        FROM predictions 
        WHERE mode = 'discovery' AND strategy_id LIKE '%_v1_default'
        LIMIT 1
    """).fetchone()
    
    if not prediction:
        print("No discovery predictions found")
        repo.close()
        return
    
    print(f"Debugging prediction: {prediction['strategy_id']} {prediction['ticker']}")
    print(f"  ID: {prediction['id']}")
    print(f"  Timestamp: {prediction['timestamp']}")
    print(f"  Entry: {prediction['entry_price']}")
    print(f"  Direction: {prediction['prediction']}")
    print(f"  Horizon: {prediction['horizon']}")
    
    # Parse dates
    created_at = datetime.fromisoformat(prediction['timestamp'].replace('Z', '+00:00'))
    expiry = created_at + timedelta(days=5)  # 5d horizon
    
    print(f"  Created: {created_at.isoformat()}")
    print(f"  Expiry: {expiry.isoformat()}")
    
    # Check price data around expiry
    print(f"\nPrice data for {prediction['ticker']}:")
    
    # Check price on expiry date
    price_on_expiry = repo.conn.execute("""
        SELECT close, timestamp FROM price_bars 
        WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) = ?
        ORDER BY timestamp
        LIMIT 3
    """, (prediction['ticker'], expiry.date())).fetchall()
    
    print(f"  On {expiry.date()}:")
    for p in price_on_expiry:
        print(f"    {p['timestamp']}: {p['close']}")
    
    # Check price after expiry
    price_after = repo.conn.execute("""
        SELECT close, timestamp FROM price_bars 
        WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) > ?
        ORDER BY timestamp
        LIMIT 3
    """, (prediction['ticker'], expiry.date())).fetchall()
    
    print(f"  After {expiry.date()}:")
    for p in price_after:
        print(f"    {p['timestamp']}: {p['close']}")
    
    # Test the price repository method
    print(f"\nTesting price repository:")
    try:
        exit_price = prices.get_exit_price_at_or_after(prediction['ticker'], expiry)
        print(f"  Exit price from repo: {exit_price}")
        
        if exit_price is not None:
            print(f"  Return: {(exit_price - prediction['entry_price']) / prediction['entry_price']:.3f}")
    except Exception as e:
        print(f"  Error getting exit price: {e}")
        import traceback
        traceback.print_exc()
    
    repo.close()

if __name__ == "__main__":
    debug_specific_valid_prediction()
