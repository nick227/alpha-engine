import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check our discovery predictions
predictions = conn.execute("""
    SELECT id, strategy_id, ticker, timestamp, direction, confidence
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
    ORDER BY timestamp
""").fetchall()

print(f"Discovery predictions: {len(predictions)}")
for p in predictions:
    print(f"  {p['strategy_id']} {p['ticker']}: {p['direction']} at {p['timestamp']}")

# Check what replay considers unscored
replay_date = datetime(2026, 4, 15, tzinfo=timezone.utc)
unscored = conn.execute("""
    SELECT p.id, p.strategy_id, p.ticker, p.timestamp
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.timestamp <= ? AND o.prediction_id IS NULL
    ORDER BY p.timestamp DESC
    LIMIT 10
""", (replay_date.isoformat(),)).fetchall()

print(f"\nUnscored predictions (what replay sees): {len(unscored)}")
for p in unscored:
    print(f"  {p['strategy_id']} {p['ticker']} at {p['timestamp']}")

# Check specifically for our discovery predictions in unscored list
discovery_unscored = conn.execute("""
    SELECT COUNT(*) as count
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.timestamp <= ? AND o.prediction_id IS NULL 
      AND p.strategy_id LIKE '%_v1_default' AND p.mode = 'discovery'
""", (replay_date.isoformat(),)).fetchone()

print(f"\nDiscovery predictions that should be scored: {discovery_unscored['count']}")
