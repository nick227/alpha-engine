import sqlite3
import json
from datetime import datetime, timezone

# Queue predictions from discovery candidates
conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Get discovery candidates and queue them as predictions
candidates = conn.execute("""
    SELECT symbol, strategy_type, score, reason, metadata_json
    FROM discovery_candidates
    WHERE as_of_date = '2026-04-10'
    ORDER BY strategy_type, score DESC
""").fetchall()

print("=== Queuing Discovery Predictions ===")

queued_count = 0
for candidate in candidates:
    # Map discovery strategy to strategy_id
    strategy_id = f"{candidate['strategy_type']}_v1_default"
    
    # Determine direction from score (higher score = more likely UP)
    direction = "UP" if candidate['score'] > 0.7 else "DOWN"
    confidence = float(candidate['score'])
    
    # Queue prediction
    conn.execute("""
        INSERT OR REPLACE INTO prediction_queue (
            tenant_id, as_of_date, symbol, source, priority, status, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "default",
        "2026-04-10",
        candidate['symbol'],
        "discovery",
        1,  # priority
        "pending",
        json.dumps({
            "strategy_id": strategy_id,
            "direction": direction,
            "confidence": confidence,
            "reason": candidate['reason'],
            "discovery_metadata": candidate['metadata_json']
        }),
        datetime.now(timezone.utc).isoformat()
    ))
    
    queued_count += 1
    if queued_count <= 10:  # Show first 10
        print(f"Queued: {candidate['symbol']} {candidate['strategy_type']} -> {direction} (conf: {confidence:.2f})")

conn.commit()

# Get queue summary
queue_summary = conn.execute("""
    SELECT source, status, COUNT(*) as count
    FROM prediction_queue
    WHERE as_of_date = '2026-04-10'
    GROUP BY source, status
""").fetchall()

print(f"\n=== Queue Summary ===")
print(f"Total candidates queued: {queued_count}")
for row in queue_summary:
    print(f"{row['source']} {row['status']}: {row['count']}")
