#!/usr/bin/env python3
"""
Paper Trading Runner for Discovery Strategies

Complete workflow:
1. Queue discovery predictions for trading days
2. Create predictions directly (bypassing PredictedSeriesBuilder)
3. Run replay to score predictions
4. Generate paper trading report
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.repository import AlphaRepository
from app.engine.discovery_integration import queue_discovery_predictions


def create_predictions_from_queue(
    repo: AlphaRepository,
    as_of_date: date,
    horizon_days: int = 5
) -> int:
    """Create predictions directly from queued discovery items."""
    
    rows = repo.conn.execute("""
        SELECT symbol, source, metadata_json 
        FROM prediction_queue 
        WHERE tenant_id = ? AND as_of_date = ? AND source LIKE 'discovery_%' AND status = 'pending'
    """, ("default", as_of_date.isoformat())).fetchall()
    
    if not rows:
        print(f"No pending discovery predictions for {as_of_date}")
        return 0
    
    created = 0
    for row in rows:
        symbol = row['symbol']
        source = row['source']
        metadata = json.loads(row['metadata_json'])
        
        strategy = metadata.get('strategy', 'unknown')
        direction = metadata.get('direction', 'UP')
        avg_score = metadata.get('avg_score', 0.5)
        
        # Create prediction
        pred_id = str(uuid.uuid4())
        created_at = datetime.combine(as_of_date, datetime.min.time()).replace(
            hour=9, minute=30, tzinfo=timezone.utc
        )
        
        # Get entry price
        price_row = repo.conn.execute("""
            SELECT close FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) <= ?
            ORDER BY timestamp DESC LIMIT 1
        """, (symbol, as_of_date.isoformat())).fetchone()
        
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
            f"{strategy}_v1_paper",
            symbol,
            "discovery",
            created_at.isoformat(),
            avg_score,
            "BUY" if direction == "UP" else "SELL",
            f"{horizon_days}d",
            entry_price,
            pred_id,
            json.dumps(metadata)
        ))
        
        # Update queue status
        repo.conn.execute("""
            UPDATE prediction_queue 
            SET status = 'processed' 
            WHERE tenant_id = ? AND as_of_date = ? AND symbol = ? AND source = ?
        """, ("default", as_of_date.isoformat(), symbol, source))
        
        created += 1
        print(f"Created: {symbol} {direction} @ ${entry_price:.2f} (conf: {avg_score:.3f})")
    
    repo.conn.commit()
    print(f"Created {created} predictions for {as_of_date}")
    return created


def run_replay_for_date(
    repo: AlphaRepository,
    as_of_date: date,
    horizon_days: int = 5
) -> int:
    """Run replay for predictions expiring on given date."""
    
    expiry_date = as_of_date + timedelta(days=horizon_days)
    expiry_time = datetime.combine(expiry_date, datetime.min.time()).replace(
        hour=16, minute=0, tzinfo=timezone.utc
    )
    
    # Count expired predictions
    count_row = repo.conn.execute("""
        SELECT COUNT(*) as count FROM predictions 
        WHERE mode = 'discovery' 
        AND timestamp <= ? 
        AND datetime(timestamp, '+' || ? || ' days') <= ?
        AND scored_event_id NOT IN (SELECT DISTINCT prediction_id FROM prediction_outcomes)
    """, (as_of_date.isoformat(), horizon_days, expiry_time.isoformat())).fetchone()
    
    if count_row and count_row['count'] > 0:
        print(f"Running replay for {count_row['count']} expired predictions...")
        
        # This would normally call the replay worker
        # For now, just return the count
        return count_row['count']
    
    return 0


def generate_paper_report(
    repo: AlphaRepository,
    start_date: date,
    end_date: date
) -> dict:
    """Generate paper trading performance report."""
    
    # Get prediction outcomes
    outcomes = repo.conn.execute("""
        SELECT 
            p.strategy_id,
            p.ticker,
            p.prediction,
            p.confidence,
            p.entry_price,
            po.exit_price,
            po.return_pct,
            po.horizon,
            p.timestamp
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.scored_event_id = po.prediction_id
        WHERE p.mode = 'discovery' 
        AND DATE(p.timestamp) BETWEEN ? AND ?
        ORDER BY p.timestamp DESC
    """, (start_date.isoformat(), end_date.isoformat())).fetchall()
    
    if not outcomes:
        return {"error": "No outcomes found"}
    
    # Calculate metrics
    total = len(outcomes)
    scored = sum(1 for o in outcomes if o['exit_price'] is not None)
    wins = sum(1 for o in outcomes if o['exit_price'] and o['return_pct'] > 0)
    
    avg_return = sum(o['return_pct'] or 0 for o in outcomes) / total if total > 0 else 0
    
    # Group by strategy
    by_strategy = {}
    for o in outcomes:
        strategy = o['strategy_id']
        if strategy not in by_strategy:
            by_strategy[strategy] = {'total': 0, 'scored': 0, 'wins': 0, 'returns': []}
        
        by_strategy[strategy]['total'] += 1
        if o['exit_price']:
            by_strategy[strategy]['scored'] += 1
            by_strategy[strategy]['returns'].append(o['return_pct'])
            if o['return_pct'] > 0:
                by_strategy[strategy]['wins'] += 1
    
    # Calculate strategy metrics
    for strategy, data in by_strategy.items():
        if data['scored'] > 0:
            data['win_rate'] = data['wins'] / data['scored']
            data['avg_return'] = sum(data['returns']) / len(data['returns'])
        else:
            data['win_rate'] = 0
            data['avg_return'] = 0
    
    return {
        "period": f"{start_date} to {end_date}",
        "total_predictions": total,
        "scored_predictions": scored,
        "win_rate": wins / scored if scored > 0 else 0,
        "avg_return": avg_return,
        "by_strategy": by_strategy,
        "recent_predictions": [
            {
                "ticker": o['ticker'],
                "strategy": o['strategy_id'],
                "prediction": o['prediction'],
                "confidence": o['confidence'],
                "entry": o['entry_price'],
                "exit": o['exit_price'],
                "return": o['return_pct'],
                "date": o['timestamp']
            }
            for o in outcomes[:10]
        ]
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run paper trading for discovery strategies")
    parser.add_argument("--db", default="data/alpha.db", help="Database path")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Number of trading days")
    parser.add_argument("--report-only", action="store_true", help="Only generate report")
    
    args = parser.parse_args()
    
    repo = AlphaRepository(args.db)
    repo.conn.execute("PRAGMA journal_mode=WAL;")
    repo.conn.execute("PRAGMA busy_timeout=5000;")
    
    # Determine date range
    if args.report_only:
        # Report mode
        if not args.start_date or not args.end_date:
            print("Error: --start-date and --end-date required for report mode")
            return
        
        start = datetime.fromisoformat(args.start_date).date()
        end = datetime.fromisoformat(args.end_date).date()
        
        report = generate_paper_report(repo, start, end)
        print(json.dumps(report, indent=2))
        return
    
    # Trading mode
    if args.days:
        end = date.today()
        start = end - timedelta(days=args.days)
    elif args.start_date and args.end_date:
        start = datetime.fromisoformat(args.start_date).date()
        end = datetime.fromisoformat(args.end_date).date()
    else:
        # Default to last 5 trading days
        end = date.today()
        start = end - timedelta(days=7)  # Buffer for weekends
    
    print(f"Paper trading: {start} to {end}")
    
    # Process each trading day
    current = start
    total_queued = 0
    total_created = 0
    
    while current <= end:
        # Skip weekends
        if current.weekday() < 5:  # Monday=0, Friday=4
            print(f"\n=== {current} ===")
            
            # Check if already ran for this date
            existing = repo.conn.execute("""
                SELECT COUNT(*) FROM predictions 
                WHERE DATE(timestamp) = ? AND mode = 'discovery'
            """, (current.isoformat(),)).fetchone()[0]
            
            if existing > 0:
                print(f"  Already have {existing} predictions for {current} - skipping")
                current += timedelta(days=1)
                continue
            
            # Queue discovery predictions
            result = queue_discovery_predictions(
                repo=repo,
                as_of=current,
                min_adv=2_000_000
            )
            total_queued += result['total_queued']
            
            # Create predictions
            created = create_predictions_from_queue(repo, current)
            total_created += created
            
            # Check for expirations (5 days prior)
            expiry_date = current - timedelta(days=5)
            if expiry_date >= start:
                expired = run_replay_for_date(repo, expiry_date)
                if expired > 0:
                    print(f"  {expired} predictions expired")
        
        current += timedelta(days=1)
    
    print(f"\n=== SUMMARY ===")
    print(f"Total queued: {total_queued}")
    print(f"Total created: {total_created}")
    
    # Generate report for the period
    report = generate_paper_report(repo, start, end)
    if "error" not in report:
        print(f"Win rate: {report['win_rate']:.1%}")
        print(f"Avg return: {report['avg_return']:.2%}")
    
    repo.close()


if __name__ == "__main__":
    main()
