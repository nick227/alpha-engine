import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from create_discovery_predictions_v3 import create_discovery_predictions

def debug_prediction_creation():
    """Debug prediction creation process."""
    conn = sqlite3.connect("data/alpha.db")
    conn.row_factory = sqlite3.Row
    
    target_date = datetime(2026, 3, 25, tzinfo=timezone.utc)
    
    print(f"Debugging prediction creation for {target_date.date()}")
    
    # Check discovery candidates
    candidates = conn.execute("""
        SELECT symbol, strategy_type, score FROM discovery_candidates
        WHERE as_of_date = ?
        ORDER BY strategy_type, score DESC
        LIMIT 10
    """, (target_date.date(),)).fetchall()
    
    print(f"\nDiscovery candidates:")
    for c in candidates:
        print(f"  {c['symbol']}: {c['strategy_type']} (score: {c['score']:.3f})")
    
    # Clear existing predictions
    conn.execute("""
        DELETE FROM predictions 
        WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
    """)
    conn.commit()
    
    # Create predictions
    print(f"\nCreating predictions...")
    predictions_created = create_discovery_predictions(
        conn,
        target_date.date(),
        max_per_strategy=3
    )
    
    print(f"Created {predictions_created} predictions")
    
    # Check what was actually created
    predictions = conn.execute("""
        SELECT strategy_id, ticker, timestamp, confidence, horizon FROM predictions 
        WHERE mode = 'discovery' AND strategy_id LIKE '%_v1_default'
    """).fetchall()
    
    print(f"\nPredictions in database:")
    for p in predictions:
        print(f"  {p['strategy_id']} {p['ticker']}: {p['confidence']:.3f} ({p['horizon']}) at {p['timestamp']}")
    
    # Check if these symbols have price data
    expiry_date = target_date + timedelta(days=5)
    
    print(f"\nChecking price data for expiry date {expiry_date.date()}:")
    for p in predictions:
        price_check = conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) = ?
        """, (p['ticker'], expiry_date.date())).fetchone()[0]
        
        print(f"  {p['ticker']}: {price_check} price bars")
    
    conn.close()

if __name__ == "__main__":
    debug_prediction_creation()
