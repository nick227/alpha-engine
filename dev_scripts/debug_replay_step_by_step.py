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

def debug_replay_step_by_step():
    """Debug replay worker step by step."""
    repo = Repository(db_path="data/alpha.db")
    
    predictions = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)
    
    print(f"Debugging replay at {now.isoformat()}")
    
    # Get discovery predictions
    unscored = list(predictions.list_unscored_predictions(now))
    discovery_unscored = [p for p in unscored if p.mode == "discovery"]
    
    print(f"Found {len(discovery_unscored)} discovery predictions")
    
    # Process each prediction manually
    for i, pred in enumerate(discovery_unscored):
        print(f"\n--- Processing prediction {i+1}: {pred.strategy_id} {pred.ticker} ---")
        
        expiry_raw = pred.created_at + timedelta(minutes=pred.horizon_minutes)
        expiry = expiry_raw.astimezone(timezone.utc).replace(second=0, microsecond=0)
        
        print(f"Created: {pred.created_at.isoformat()}")
        print(f"Expiry raw: {expiry_raw.isoformat()}")
        print(f"Expiry: {expiry.isoformat()}")
        print(f"Now: {now.isoformat()}")
        print(f"Expired: {expiry <= now}")
        
        if expiry > now:
            print("Skipping: not expired yet")
            continue
        
        print("Prediction is expired - should be scored")
        
        # Check entry price
        print(f"Entry price: {pred.entry_price}")
        if pred.entry_price <= 0:
            print("Skipping: invalid entry price")
            continue
        
        # Try to get exit price
        try:
            exit_price = prices.get_exit_price_at_or_after(pred.ticker, expiry)
            print(f"Exit price: {exit_price}")
            
            if exit_price is None:
                print("Skipping: no exit price found")
                continue
                
            # Calculate return
            return_pct = (exit_price - pred.entry_price) / pred.entry_price
            print(f"Return: {return_pct:.3f} ({return_pct:.1%})")
            
            # Create outcome
            outcome_payload = {
                "prediction_id": pred.id,
                "strategy_id": pred.strategy_id,
                "ticker": pred.ticker,
                "track": pred.track,
                "mode": pred.mode,
                "regime": pred.regime,
                "entry_price": pred.entry_price,
                "exit_price": exit_price,
                "return_pct": return_pct,
                "residual_alpha": return_pct,  # No market return for simplicity
                "direction_correct": return_pct > 0,
                "exit_reason": "completed",
                "evaluated_at": now.isoformat(),
            }
            
            print(f"Creating outcome: {outcome_payload}")
            outcome_id = outcomes.write_outcome(outcome_payload)
            print(f"Outcome created: {outcome_id}")
            
            # Mark as scored
            predictions.mark_scored(pred.id, outcome_id)
            print(f"Marked as scored")
            
        except Exception as e:
            print(f"Error processing prediction: {e}")
            import traceback
            traceback.print_exc()
    
    repo.close()

if __name__ == "__main__":
    debug_replay_step_by_step()
