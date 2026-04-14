#!/usr/bin/env python
"""
Historical Discovery Backtest

Runs discovery on historical dates to generate real predictions with actual outcomes.
This replaces synthetic testing with proper backtest evaluation.

Usage:
    python historical_discovery_backtest.py --start-date 2025-01-01 --end-date 2025-01-31
    python historical_discovery_backtest.py --days 30  # Last 30 days of available data
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.discovery.runner import run_discovery
from app.db.repository import AlphaRepository
from app.engine.replay_worker import ReplayWorker
from app.engine.replay_sqlite import (
    SQLiteOutcomeWriter,
    SQLitePredictionRepository,
    SQLitePriceRepository,
    SQLiteMetricsUpdater,
)
from create_discovery_predictions_v3 import create_discovery_predictions


def get_available_date_range(conn: sqlite3.Connection) -> tuple[datetime, datetime]:
    """Get the range of dates available for historical testing."""
    result = conn.execute("""
        SELECT 
            MIN(DATE(timestamp)) as min_date,
            MAX(DATE(timestamp)) as max_date
        FROM price_bars
        WHERE tenant_id = 'ml_train'
    """).fetchone()
    
    if not result or not result['min_date']:
        raise ValueError("No price data available")
    
    return (
        datetime.fromisoformat(result['min_date']).replace(tzinfo=timezone.utc),
        datetime.fromisoformat(result['max_date']).replace(tzinfo=timezone.utc)
    )


def run_historical_discovery(
    repo: AlphaRepository,
    target_date: datetime,
    min_avg_dollar_volume_20d: int = 1_000_000
) -> int:
    """Run discovery for a specific historical date."""
    print(f"\n{'='*60}")
    print(f"Running discovery for {target_date.date()}")
    print(f"{'='*60}")
    
    # Run discovery
    result = run_discovery(
        db_path=repo.db_path,
        as_of=target_date.date(),
        min_avg_dollar_volume_20d=min_avg_dollar_volume_20d,
        use_feature_snapshot=True
    )
    
    print(f"Discovery results:")
    
    # Count total candidates across all strategies
    total_candidates = 0
    strategy_counts = {}
    
    for strategy_name, strategy_data in result.get("strategies", {}).items():
        top_candidates = strategy_data.get("top", [])
        total_candidates += len(top_candidates)
        strategy_counts[strategy_name] = len(top_candidates)
    
    print(f"  Total candidates: {total_candidates}")
    print(f"  By strategy: {strategy_counts}")
    
    # Create predictions from discovery candidates
    predictions_created = create_discovery_predictions(
        repo.conn,
        target_date.date(),
        max_per_strategy=5  # Limit to top 5 per strategy for manageable dataset
    )
    
    print(f"  Predictions created: {predictions_created}")
    
    return total_candidates


def run_replay_for_date(repo: AlphaRepository, target_date: datetime) -> int:
    """Run replay to score predictions up to target date."""
    # Add 5 days for 5d horizon predictions to resolve
    replay_date = target_date + timedelta(days=7)
    
    predictions = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    
    worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)
    scored = worker.run_once(replay_date)
    
    print(f"  Replay scored: {scored} predictions")
    return scored


def compute_final_trust(repo: AlphaRepository) -> None:
    """Compute trust scores for all discovery strategies."""
    from app.engine.trust_engine import TrustEngine
    
    trust_engine = TrustEngine()
    
    discovery_strategies = [
        "realness_repricer_v1_default",
        "narrative_lag_v1_default"
    ]
    
    print(f"\n{'='*60}")
    print("FINAL TRUST SCORES")
    print(f"{'='*60}")
    
    for strategy_id in discovery_strategies:
        try:
            result = trust_engine.compute_strategy_trust(
                conn=repo.conn,
                tenant_id="default",
                strategy_id=strategy_id,
                horizon="5d",
                as_of=datetime.now(timezone.utc)
            )
            
            print(f"{strategy_id}:")
            print(f"  Trust: {result.trust_score:.3f}")
            print(f"  Calibration: {result.calibration_score:.3f}")
            print(f"  Stability: {result.stability_score:.3f}")
            print(f"  Sample Size: {result.sample_size}")
            print()
            
        except Exception as e:
            print(f"Error computing trust for {strategy_id}: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical discovery backtest")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Number of days to backtest")
    parser.add_argument("--db", default="data/alpha.db", help="Database path")
    
    args = parser.parse_args()
    
    repo = AlphaRepository(args.db)
    
    # Get available date range
    min_date, max_date = get_available_date_range(repo.conn)
    print(f"Available data range: {min_date.date()} to {max_date.date()}")
    
    # Determine backtest dates
    if args.days:
        end_date = max_date
        start_date = end_date - timedelta(days=args.days)
    elif args.start_date and args.end_date:
        start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
        end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
    else:
        # Default: last 30 days
        end_date = max_date
        start_date = end_date - timedelta(days=30)
    
    # Clamp to available range
    start_date = max(start_date, min_date)
    end_date = min(end_date, max_date)
    
    print(f"Backtest range: {start_date.date()} to {end_date.date()}")
    print(f"Total days: {(end_date - start_date).days}")
    
    # Clear existing discovery predictions for clean backtest
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
    print("Cleared existing discovery predictions")
    
    # Run historical backtest
    current_date = start_date
    total_candidates = 0
    total_predictions = 0
    total_scored = 0
    
    while current_date <= end_date:
        try:
            # Run discovery
            candidates = run_historical_discovery(repo, current_date)
            total_candidates += candidates
            
            # Run replay to score expired predictions
            scored = run_replay_for_date(repo, current_date)
            total_scored += scored
            
            current_date += timedelta(days=1)
            
        except Exception as e:
            print(f"Error processing {current_date.date()}: {e}")
            current_date += timedelta(days=1)
            continue
    
    print(f"\n{'='*60}")
    print("BACKTEST SUMMARY")
    print(f"{'='*60}")
    print(f"Total candidates generated: {total_candidates}")
    print(f"Total predictions created: {total_predictions}")
    print(f"Total predictions scored: {total_scored}")
    
    # Get final prediction count
    final_predictions = repo.conn.execute("""
        SELECT COUNT(*) FROM predictions 
        WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
    """).fetchone()[0]
    
    print(f"Final discovery predictions: {final_predictions}")
    
    # Compute trust scores
    compute_final_trust(repo)
    
    repo.close()


if __name__ == "__main__":
    main()
