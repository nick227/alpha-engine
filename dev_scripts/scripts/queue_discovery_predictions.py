#!/usr/bin/env python3
"""
Queue Discovery Predictions Script

Queues discovery candidates (especially silent_compounder) into prediction_queue
for processing by the existing prediction pipeline.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.repository import AlphaRepository
from app.engine.discovery_integration import DiscoveryIntegration


def queue_discovery_predictions(
    repo: AlphaRepository,
    as_of_date: datetime,
    max_per_strategy: int = 10,
    horizon_days: int = 5
) -> int:
    """
    Queue discovery predictions for a specific date.
    
    Args:
        repo: Database repository
        as_of_date: Date to run discovery
        max_per_strategy: Max candidates per strategy
        horizon_days: Prediction horizon in days
        
    Returns:
        Number of candidates queued
    """
    integration = DiscoveryIntegration(repo)
    
    # Queue discovery predictions
    candidates = integration.queue_discovery_predictions(
        as_of_date=as_of_date,
        horizon_days=horizon_days,
        max_per_strategy=max_per_strategy
    )
    
    # Insert into prediction_queue
    for candidate in candidates:
        repo.conn.execute("""
            INSERT OR REPLACE INTO prediction_queue (
                tenant_id, as_of_date, symbol, source, metadata_json
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            "default",
            candidate["as_of_date"],
            candidate["symbol"],
            candidate["source"],
            str(candidate["metadata_json"])
        ))
    
    # Create consensus seeds for PredictedSeriesBuilder compatibility
    for candidate in candidates:
        metadata = candidate["metadata_json"]
        integration.create_consensus_seed(
            symbol=candidate["symbol"],  # Use candidate symbol, not metadata
            confidence=metadata["confidence"],
            direction=metadata["direction"]
        )
    
    repo.conn.commit()
    print(f"Queued {len(candidates)} discovery candidates for {as_of_date.date()}")
    return len(candidates)


def batch_queue_discovery(
    repo: AlphaRepository,
    start_date: datetime,
    end_date: datetime,
    max_per_strategy: int = 10,
    horizon_days: int = 5
) -> int:
    """
    Batch queue discovery predictions for date range.
    
    Args:
        repo: Database repository
        start_date: Start date for queuing
        end_date: End date for queuing
        max_per_strategy: Max candidates per strategy
        horizon_days: Prediction horizon in days
        
    Returns:
        Total number of candidates queued
    """
    integration = DiscoveryIntegration(repo)
    
    total_queued = integration.batch_queue_discovery(
        start_date=start_date,
        end_date=end_date,
        horizon_days=horizon_days,
        max_per_strategy=max_per_strategy
    )
    
    return total_queued


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Queue discovery predictions into prediction_queue",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    parser.add_argument("--db", default="data/alpha.db", help="Database path")
    parser.add_argument("--tenant-id", default="default", help="Tenant ID")
    parser.add_argument("--as-of", help="Date to queue (YYYY-MM-DD). Default: today")
    parser.add_argument("--start-date", help="Start date for batch (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for batch (YYYY-MM-DD)")
    parser.add_argument("--max-per-strategy", type=int, default=10, help="Max candidates per strategy")
    parser.add_argument("--horizon-days", type=int, default=5, help="Prediction horizon in days")
    parser.add_argument("--force", action="store_true", help="Force re-queue existing dates")
    
    args = parser.parse_args()
    
    repo = AlphaRepository(args.db)
    
    # Fix database locking issues
    repo.conn.execute("PRAGMA journal_mode=WAL;")
    repo.conn.execute("PRAGMA busy_timeout=5000;")
    
    # Resolve dates
    if args.as_of:
        as_of_date = datetime.fromisoformat(args.as_of).replace(tzinfo=timezone.utc)
        # Clear existing queue for this date if not forcing
        if not args.force:
            repo.conn.execute("""
                DELETE FROM prediction_queue 
                WHERE tenant_id = ? AND as_of_date = ?
            """, (args.tenant_id, as_of_date.date()))
        
        total_queued = queue_discovery_predictions(
            repo=repo,
            as_of_date=as_of_date,
            max_per_strategy=args.max_per_strategy,
            horizon_days=args.horizon_days
        )
        
        print(f"✅ Queued {total_queued} discovery predictions for {as_of_date.date()}")
        
    elif args.start_date and args.end_date:
        start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
        end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
        
        total_queued = batch_queue_discovery(
            repo=repo,
            start_date=start_date,
            end_date=end_date,
            max_per_strategy=args.max_per_strategy,
            horizon_days=args.horizon_days
        )
        
        print(f"✅ Batch queued {total_queued} discovery predictions from {start_date.date()} to {end_date.date()}")
        
    else:
        # Default: queue today
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        total_queued = queue_discovery_predictions(
            repo=repo,
            as_of_date=today,
            max_per_strategy=args.max_per_strategy,
            horizon_days=args.horizon_days
        )
        
        print(f"✅ Queued {total_queued} discovery predictions for today")
    
    repo.close()


if __name__ == "__main__":
    main()
