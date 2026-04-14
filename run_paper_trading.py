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
            WHERE tenant_id = 'default' AND ticker = ? AND timeframe = '1d' AND DATE(timestamp) <= ?
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


def run_replay_for_expired(
    repo: AlphaRepository,
    as_of_date: date,
) -> int:
    """
    Score all discovery predictions whose horizon has expired by as_of_date.

    Handles mixed horizons (5d for balance_sheet_survivor, 20d for silent_compounder).
    Writes directly to prediction_outcomes — bypasses SQLitePredictionRepository's
    strategies JOIN which would miss synthetic strategy IDs like 'silent_compounder_v1_paper'.
    """
    cutoff_dt = datetime.combine(as_of_date, datetime.min.time()).replace(
        hour=23, minute=59, tzinfo=timezone.utc
    )

    # All unscored discovery predictions regardless of creation date
    rows = repo.conn.execute("""
        SELECT p.id, p.ticker, p.prediction, p.entry_price, p.horizon, p.timestamp
        FROM predictions p
        LEFT JOIN prediction_outcomes po
            ON po.prediction_id = p.id AND po.tenant_id = p.tenant_id
        WHERE p.tenant_id = 'default'
          AND p.mode = 'discovery'
          AND po.id IS NULL
    """).fetchall()

    if not rows:
        return 0

    scored = 0
    for row in rows:
        # Parse horizon string e.g. "5d", "20d"
        horizon_str = str(row["horizon"]).strip().lower()
        try:
            horizon_days = int(horizon_str.rstrip("d"))
        except (ValueError, AttributeError):
            horizon_days = 5

        # Parse creation timestamp
        try:
            created_at = datetime.fromisoformat(str(row["timestamp"]))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        expiry_dt = created_at + timedelta(days=horizon_days)
        if expiry_dt > cutoff_dt:
            continue  # Not yet expired

        expiry_date = expiry_dt.date()

        # Fetch exit price: first daily close on or after expiry date
        exit_row = repo.conn.execute("""
            SELECT close FROM price_bars
            WHERE tenant_id = 'default' AND ticker = ? AND timeframe = '1d'
              AND DATE(timestamp) >= ?
            ORDER BY timestamp ASC LIMIT 1
        """, (str(row["ticker"]), expiry_date.isoformat())).fetchone()

        if not exit_row:
            # Fallback: feature_snapshot (covers full 4,652-symbol universe)
            exit_row = repo.conn.execute("""
                SELECT close FROM feature_snapshot
                WHERE symbol = ? AND as_of_date >= ?
                ORDER BY as_of_date ASC LIMIT 1
            """, (str(row["ticker"]), expiry_date.isoformat())).fetchone()

        if not exit_row:
            continue

        exit_price = float(exit_row["close"])
        entry_price = float(row["entry_price"])
        if entry_price <= 0:
            continue

        return_pct = (exit_price / entry_price) - 1.0
        direction = str(row["prediction"])  # 'BUY' or 'SELL'
        direction_correct = (return_pct > 0 and direction == "BUY") or \
                            (return_pct < 0 and direction == "SELL")

        outcome_id = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        repo.conn.execute("""
            INSERT OR REPLACE INTO prediction_outcomes
              (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct,
               max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outcome_id, "default", str(row["id"]),
            exit_price, return_pct, 1 if direction_correct else 0,
            max(return_pct, 0.0), min(return_pct, 0.0),
            now_iso, "horizon", return_pct,
        ))
        scored += 1

    if scored > 0:
        repo.conn.commit()
        print(f"  Scored {scored} expired discovery predictions")
    return scored


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
            
            # Score any discovery predictions whose horizon expired by today
            run_replay_for_expired(repo, current)
        
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
