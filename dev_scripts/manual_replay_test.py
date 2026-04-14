import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository
from app.engine.replay_sqlite import (
    SQLiteMetricsUpdater,
    SQLiteOutcomeWriter,
    SQLitePredictionRepository,
    SQLitePriceRepository,
)
from app.engine.replay_worker import ReplayWorker

def manual_replay():
    """Manually run replay worker to debug scoring."""
    repo = Repository(db_path="data/alpha.db")
    
    predictions = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    
    worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)
    
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)
    
    print(f"Running replay at {now.isoformat()}")
    
    # Get unscored predictions first
    unscored = list(predictions.list_unscored_predictions(now))
    print(f"Found {len(unscored)} unscored predictions")
    
    discovery_unscored = [p for p in unscored if p.mode == "discovery"]
    print(f"Discovery predictions to score: {len(discovery_unscored)}")
    
    for pred in discovery_unscored:
        expiry = pred.created_at + timedelta(minutes=pred.horizon_minutes)
        print(f"  {pred.strategy_id} {pred.ticker}: expires {expiry.isoformat()}, expired: {expiry <= now}")
        print(f"    Entry price: {pred.entry_price}")
    
    # Run the replay
    print("\nRunning replay worker...")
    try:
        scored = worker.run_once(now)
        print(f"Replay scored {scored} predictions")
    except Exception as e:
        print(f"Error during replay: {e}")
        import traceback
        traceback.print_exc()
    
    # Check if outcomes were created
    conn = repo.conn
    discovery_outcomes = conn.execute("""
        SELECT COUNT(*) FROM prediction_outcomes po
        JOIN predictions p ON p.id = po.prediction_id
        WHERE p.mode = 'discovery'
    """).fetchone()[0]
    
    print(f"Discovery outcomes created: {discovery_outcomes}")
    
    repo.close()

if __name__ == "__main__":
    manual_replay()
