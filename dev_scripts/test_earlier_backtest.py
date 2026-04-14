import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.discovery.runner import run_discovery
from create_discovery_predictions_v3 import create_discovery_predictions
from app.db.repository import AlphaRepository
from app.engine.replay_sqlite import (
    SQLiteMetricsUpdater,
    SQLiteOutcomeWriter,
    SQLitePredictionRepository,
    SQLitePriceRepository,
)
from app.engine.replay_worker import ReplayWorker

def test_earlier_backtest():
    """Test backtest with earlier dates that have price data."""
    repo = AlphaRepository("data/alpha.db")
    
    # Use a date that allows for 5-day horizon within available price data
    # Latest price is 2026-04-11, so 5-day prediction from 2026-04-06 expires on 2026-04-11
    target_date = datetime(2026, 4, 6, tzinfo=timezone.utc)
    
    print(f"Running discovery for {target_date.date()}")
    
    # Clear existing discovery predictions
    repo.conn.execute("""
        DELETE FROM predictions 
        WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
    """)
    repo.conn.execute("""
        DELETE FROM prediction_outcomes 
        WHERE prediction_id IN (
            SELECT id FROM predictions 
            WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
        )
    """)
    repo.conn.commit()
    
    # Run discovery
    result = run_discovery(
        db_path=repo.db_path,
        as_of=target_date.date(),
        min_avg_dollar_volume_20d=1_000_000,
        use_feature_snapshot=True
    )
    
    # Count candidates
    total_candidates = 0
    for strategy_name, strategy_data in result.get("strategies", {}).items():
        top_candidates = strategy_data.get("top", [])
        total_candidates += len(top_candidates)
    
    print(f"Discovery found {total_candidates} candidates")
    
    # Create predictions
    predictions_created = create_discovery_predictions(
        repo.conn,
        target_date.date(),
        max_per_strategy=3
    )
    
    print(f"Created {predictions_created} predictions")
    
    # Check what symbols we have predictions for
    predictions = repo.conn.execute("""
        SELECT strategy_id, ticker, timestamp FROM predictions 
        WHERE mode = 'discovery' AND strategy_id LIKE '%_v1_default'
    """).fetchall()
    
    print(f"Predictions created:")
    for p in predictions:
        print(f"  {p['strategy_id']} {p['ticker']} at {p['timestamp']}")
    
    # Run replay at the expiry date
    expiry_date = target_date + timedelta(days=5)
    print(f"\nRunning replay at expiry date: {expiry_date.date()}")
    
    # Set up replay
    predictions_repo = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    
    worker = ReplayWorker(predictions=predictions_repo, prices=prices, outcomes=outcomes, metrics=metrics)
    
    # Check if we have price data for the symbols
    for p in predictions:
        price_check = repo.conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) = ?
        """, (p['ticker'], expiry_date.date())).fetchone()[0]
        
        print(f"  {p['ticker']}: {price_check} price bars on {expiry_date.date()}")
    
    # Run replay
    scored = worker.run_once(expiry_date)
    print(f"Replay scored {scored} predictions")
    
    # Check outcomes
    outcomes_count = repo.conn.execute("""
        SELECT COUNT(*) FROM prediction_outcomes po
        JOIN predictions p ON p.id = po.prediction_id
        WHERE p.mode = 'discovery'
    """).fetchone()[0]
    
    print(f"Discovery outcomes created: {outcomes_count}")
    
    # Show outcomes
    if outcomes_count > 0:
        outcomes = repo.conn.execute("""
            SELECT p.strategy_id, p.ticker, po.return_pct, po.direction_correct
            FROM prediction_outcomes po
            JOIN predictions p ON p.id = po.prediction_id
            WHERE p.mode = 'discovery'
        """).fetchall()
        
        print("Outcomes:")
        for o in outcomes:
            print(f"  {o['strategy_id']} {o['ticker']}: {o['return_pct']:.3f} ({'Correct' if o['direction_correct'] else 'Wrong'})")
    
    repo.close()

if __name__ == "__main__":
    test_earlier_backtest()
