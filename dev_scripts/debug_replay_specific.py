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

def debug_replay_specific():
    """Debug replay on specific predictions."""
    repo = Repository(db_path="data/alpha.db")
    
    target_date = datetime(2026, 3, 25, tzinfo=timezone.utc)
    expiry_date = target_date + timedelta(days=5)
    replay_time = expiry_date.replace(hour=13)  # Run after 12:00 expiry
    
    print(f"Debugging replay for {replay_time.date()} at {replay_time.time()}")
    
    # Get discovery predictions
    predictions_repo = SQLitePredictionRepository(repo)
    unscored = list(predictions_repo.list_unscored_predictions(replay_time))
    
    print(f"Total unscored predictions: {len(unscored)}")
    
    discovery_unscored = [p for p in unscored if p.mode == "discovery"]
    print(f"Discovery predictions: {len(discovery_unscored)}")
    
    for pred in discovery_unscored:
        print(f"\n  {pred.strategy_id} {pred.ticker}:")
        print(f"    Created: {pred.created_at}")
        print(f"    Expiry: {pred.created_at + timedelta(minutes=pred.horizon_minutes)}")
        print(f"    Expired: {(pred.created_at + timedelta(minutes=pred.horizon_minutes)) <= replay_time}")
        print(f"    Entry price: {pred.entry_price}")
        print(f"    Horizon: {pred.horizon_minutes} minutes")
    
    # Test price repository directly
    prices = SQLitePriceRepository(repo)
    
    print(f"\nTesting price repository:")
    for pred in discovery_unscored[:3]:  # Test first 3
        try:
            exit_price = prices.get_exit_price_at_or_after(pred.ticker, replay_time)
            print(f"  {pred.ticker}: exit_price = {exit_price}")
        except Exception as e:
            print(f"  {pred.ticker}: Error = {e}")
    
    # Run replay
    print(f"\nRunning replay...")
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    
    worker = ReplayWorker(predictions=predictions_repo, prices=prices, outcomes=outcomes, metrics=metrics)
    
    scored = worker.run_once(replay_time)
    print(f"Replay scored {scored} predictions")
    
    repo.close()

if __name__ == "__main__":
    debug_replay_specific()
