import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check discovery predictions with their dates
predictions = conn.execute("""
    SELECT strategy_id, ticker, timestamp, direction, confidence
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default'
    ORDER BY timestamp DESC
    LIMIT 10
""").fetchall()

print("=== Discovery Predictions ===")
for row in predictions:
    print(f"{row['strategy_id']} {row['ticker']}: {row['direction']} (conf: {row['confidence']:.2f}) at {row['timestamp']}")

# Check replay cutoff
replay_date = "2026-04-12T00:00:00+00:00"
print(f"\nReplay was run at: {replay_date}")

# Count predictions before replay date
before_replay = conn.execute("""
    SELECT COUNT(*) as count
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default' AND timestamp <= ?
""", (replay_date,)).fetchone()

print(f"Discovery predictions before replay: {before_replay['count']}")
