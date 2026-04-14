import sqlite3
import json
from datetime import datetime, timezone

# Generate predictions directly from discovery queue metadata
conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Get queued discovery items
queue_items = conn.execute("""
    SELECT symbol, metadata_json, created_at
    FROM prediction_queue
    WHERE source = 'discovery' AND status = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT 50
""").fetchall()

print(f"Processing {len(queue_items)} discovery predictions...")

predictions_created = 0
for item in queue_items:
    try:
        metadata = json.loads(item['metadata_json'])
        strategy_id = metadata.get('strategy_id', '')
        direction = metadata.get('direction', 'flat')
        confidence = float(metadata.get('confidence', 0.5))
        
        # Create prediction directly
        prediction_id = f"disc_{item['symbol']}_{strategy_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        conn.execute("""
            INSERT INTO predictions (
                id, tenant_id, strategy_id, ticker, timestamp, prediction, confidence, 
                horizon, entry_price, mode, feature_snapshot_json, regime, trend_strength, 
                idempotency_key, scored_outcome_id, scored_at, run_id, predicted_return, 
                direction, prediction_id, discovery_overlap, playbook_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            prediction_id,
            "default",
            strategy_id,
            item['symbol'],
            item['created_at'],
            direction,
            confidence,
            "5d",  # horizon
            None,  # entry_price - will be filled by replay
            "discovery",
            "{}",  # feature_snapshot_json
            None,  # regime
            None,  # trend_strength
            f"{prediction_id}_{item['created_at']}",  # idempotency_key
            None,  # scored_outcome_id
            None,  # scored_at
            None,  # run_id
            0.0,  # predicted_return
            direction,
            prediction_id,
            0,  # discovery_overlap
            None   # playbook_id
        ))
        
        # Mark queue item as processed
        conn.execute("""
            UPDATE prediction_queue 
            SET status = 'processed'
            WHERE symbol = ? AND source = 'discovery' AND as_of_date = '2026-04-10'
        """, (item['symbol'],))
        
        predictions_created += 1
        
        if predictions_created <= 10:
            print(f"Created: {item['symbol']} {strategy_id} -> {direction} (conf: {confidence:.2f})")
            
    except Exception as e:
        print(f"Error processing {item['symbol']}: {e}")
        # Mark as failed
        conn.execute("""
            UPDATE prediction_queue 
            SET status = 'failed'
            WHERE symbol = ? AND source = 'discovery' AND as_of_date = '2026-04-10'
        """, (item['symbol'],))

conn.commit()

print(f"\nGenerated {predictions_created} predictions from discovery candidates")

# Update queue status summary
queue_status = conn.execute("""
    SELECT source, status, COUNT(*) as count
    FROM prediction_queue
    WHERE as_of_date = '2026-04-10'
    GROUP BY source, status
""").fetchall()

print(f"\n=== Final Queue Status ===")
for row in queue_status:
    print(f"{row['source']} {row['status']}: {row['count']}")
