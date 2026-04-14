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

def test_liquid_backtest():
    """Test backtest with liquid symbols that have price data."""
    repo = AlphaRepository("data/alpha.db")
    
    # Use an earlier date to ensure we have price data for 5-day horizon
    target_date = datetime(2026, 4, 1, tzinfo=timezone.utc)
    expiry_date = target_date + timedelta(days=5)  # 2026-04-06
    
    print(f"Running discovery for {target_date.date()}, expiry on {expiry_date.date()}")
    
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
        max_per_strategy=5
    )
    
    print(f"Created {predictions_created} predictions")
    
    # Get predictions and check for liquid symbols
    predictions = repo.conn.execute("""
        SELECT strategy_id, ticker, timestamp FROM predictions 
        WHERE mode = 'discovery' AND strategy_id LIKE '%_v1_default'
    """).fetchall()
    
    print(f"\nChecking price data availability:")
    liquid_predictions = []
    
    for p in predictions:
        # Check if symbol has price data on expiry date
        price_check = repo.conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) = ?
        """, (p['ticker'], expiry_date.date())).fetchone()[0]
        
        # Also check a few days around expiry
        price_check_plus = repo.conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) BETWEEN ? AND ?
        """, (p['ticker'], expiry_date.date(), (expiry_date + timedelta(days=2)).date())).fetchone()[0]
        
        print(f"  {p['ticker']}: {price_check} bars on {expiry_date.date()}, {price_check_plus} bars within 2 days")
        
        if price_check > 0:
            liquid_predictions.append(p)
    
    print(f"\nLiquid predictions with price data: {len(liquid_predictions)}")
    
    if len(liquid_predictions) == 0:
        print("No liquid predictions found - discovery is finding illiquid symbols")
        repo.close()
        return
    
    # Run replay on liquid predictions only
    print(f"\nRunning replay at expiry date: {expiry_date.date()}")
    
    # Set up replay
    predictions_repo = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    
    worker = ReplayWorker(predictions=predictions_repo, prices=prices, outcomes=outcomes, metrics=metrics)
    
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
        
        # Compute trust scores
        from app.engine.trust_engine import TrustEngine
        trust_engine = TrustEngine()
        
        print(f"\nTrust Scores:")
        for strategy_id in ["realness_repricer_v1_default", "narrative_lag_v1_default"]:
            try:
                result = trust_engine.compute_strategy_trust(
                    conn=repo.conn,
                    tenant_id="default",
                    strategy_id=strategy_id,
                    horizon="5d",
                    as_of=expiry_date
                )
                
                print(f"  {strategy_id}: trust={result.trust_score:.3f} (n={result.sample_size})")
                
            except Exception as e:
                print(f"  {strategy_id}: Error computing trust - {e}")
    
    repo.close()

if __name__ == "__main__":
    test_liquid_backtest()
