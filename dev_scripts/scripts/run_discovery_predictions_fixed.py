#!/usr/bin/env python3
"""
Direct Discovery Prediction Runner

Bypasses PredictedSeriesBuilder to create discovery predictions directly.
This avoids the start_level requirement and consensus complexity.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.repository import AlphaRepository


def create_discovery_predictions(
    repo: AlphaRepository,
    as_of_date: datetime,
    horizon_days: int = 5,
    max_per_strategy: int = 10
) -> int:
    """
    Create discovery predictions directly in predictions table.
    Bypasses PredictedSeriesBuilder for simplicity.
    """
    # Get discovery candidates from queue
    rows = repo.conn.execute("""
        SELECT symbol, metadata_json 
        FROM prediction_queue 
        WHERE tenant_id = ? AND as_of_date = ? AND source = 'discovery' AND status = 'pending'
        LIMIT ?
    """, ("default", as_of_date.date(), max_per_strategy)).fetchall()
    
    if not rows:
        print(f"No pending discovery predictions for {as_of_date.date()}")
        return 0
    
    created = 0
    for row in rows:
        symbol = row['symbol']
        metadata = row['metadata_json']
        
        try:
            meta = json.loads(metadata)
        except:
            print(f"Invalid metadata for {symbol}")
            continue
        
        confidence = meta.get('confidence', 0.5)
        direction = meta.get('direction', 'UP')
        strategy = meta.get('strategy', 'silent_compounder')
        
        # Create prediction directly
        pred_id = str(uuid.uuid4())
        created_at = as_of_date.replace(hour=12, minute=0, second=0, microsecond=0)
        expiry = created_at + timedelta(days=horizon_days)
        
        # Map direction to prediction format
        prediction = "BUY" if direction == "UP" else "SELL"
        
        # Get entry price (use latest close)
        price_row = repo.conn.execute("""
            SELECT close FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) <= ?
            ORDER BY timestamp DESC LIMIT 1
        """, (symbol, as_of_date.date())).fetchone()
        
        if not price_row:
            print(f"No price data for {symbol}")
            continue
            
        entry_price = float(price_row['close'])
        
        # Insert prediction
        repo.conn.execute("""
            INSERT INTO predictions (
                id, tenant_id, strategy_id, ticker, mode, timestamp, 
                confidence, prediction, horizon, entry_price, scored_event_id,
                feature_snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pred_id,
            "default",
            f"{strategy}_v1_direct",
            symbol,
            "discovery",
            created_at.isoformat(),
            confidence,
            prediction,
            f"{horizon_days}d",
            entry_price,
            pred_id,  # Use same ID for scored_event_id
            json.dumps(meta)  # Store discovery metadata as feature snapshot
        ))
        
        # Update queue status
        processed_meta = meta.copy()
        processed_meta['processed_at'] = datetime.now(timezone.utc).isoformat()
        
        repo.conn.execute("""
            UPDATE prediction_queue 
            SET status = 'processed', metadata_json = ?
            WHERE tenant_id = ? AND as_of_date = ? AND symbol = ?
        """, (
            json.dumps(processed_meta),
            "default",
            as_of_date.date(),
            symbol
        ))
        
        created += 1
        print(f"Created prediction: {symbol} {prediction} (conf: {confidence:.3f})")
    
    repo.conn.commit()
    print(f"Created {created} discovery predictions for {as_of_date.date()}")
    return created


def main() -> None:
    parser = argparse.ArgumentParser(description="Run discovery predictions directly")
    parser.add_argument("--db", default="data/alpha.db", help="Database path")
    parser.add_argument("--as-of", help="Date to run (YYYY-MM-DD). Default: today")
    parser.add_argument("--horizon-days", type=int, default=5, help="Prediction horizon")
    parser.add_argument("--max-per-strategy", type=int, default=10, help="Max predictions")
    
    args = parser.parse_args()
    
    repo = AlphaRepository(args.db)
    
    # Fix database locking issues
    repo.conn.execute("PRAGMA journal_mode=WAL;")
    repo.conn.execute("PRAGMA busy_timeout=5000;")
    
    # Resolve date
    if args.as_of:
        as_of_date = datetime.fromisoformat(args.as_of).replace(tzinfo=timezone.utc)
    else:
        as_of_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    print(f"Running discovery predictions for {as_of_date.date()}")
    
    created = create_discovery_predictions(
        repo=repo,
        as_of_date=as_of_date,
        horizon_days=args.horizon_days,
        max_per_strategy=args.max_per_strategy
    )
    
    print(f"✅ Created {created} discovery predictions")
    repo.close()


if __name__ == "__main__":
    main()
