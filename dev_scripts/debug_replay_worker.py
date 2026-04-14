import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository
from app.engine.replay_sqlite import SQLitePredictionRepository

def debug_unscored_predictions():
    """Debug what predictions the replay worker sees."""
    repo = Repository(db_path="data/alpha.db")
    predictions = SQLitePredictionRepository(repo, tenant_id="default")
    
    now = datetime(2026, 4, 15, tzinfo=timezone.utc)
    
    print(f"Checking unscored predictions as of {now.isoformat()}")
    
    unscored = list(predictions.list_unscored_predictions(now))
    
    print(f"Found {len(unscored)} unscored predictions:")
    
    for i, pred in enumerate(unscored[:10]):  # Show first 10
        print(f"  {i+1}. {pred.strategy_id} {pred.ticker}")
        print(f"     Created: {pred.created_at}")
        print(f"     Horizon: {pred.horizon_minutes} minutes")
        print(f"     Mode: {pred.mode}")
        print(f"     Entry: {pred.entry_price}")
        print()
    
    # Check specifically for discovery predictions
    discovery_unscored = [p for p in unscored if p.mode == "discovery"]
    print(f"Discovery predictions: {len(discovery_unscored)}")
    
    for pred in discovery_unscored:
        expiry = pred.created_at + timedelta(minutes=pred.horizon_minutes)
        print(f"  {pred.strategy_id} {pred.ticker}: expires {expiry.isoformat()} (now: {now.isoformat()})")
        print(f"     Expired: {expiry <= now}")
    
    repo.close()

if __name__ == "__main__":
    debug_unscored_predictions()
