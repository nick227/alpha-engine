import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check what predictions were actually created
predictions = conn.execute("""
    SELECT strategy_id, ticker, confidence, direction, timestamp
    FROM predictions
    WHERE timestamp >= '2026-04-10T00:00:00Z'
    ORDER BY timestamp DESC
    LIMIT 10
""").fetchall()

print("=== Recent Predictions Created ===")
for row in predictions:
    print(f"{row['strategy_id']} {row['ticker']}: {row['direction']} (conf: {row['confidence']:.2f})")

# Check what's in queue now
queue = conn.execute("""
    SELECT source, status, COUNT(*) as count
    FROM prediction_queue
    WHERE as_of_date = '2026-04-10'
    GROUP BY source, status
""").fetchall()

print(f"\n=== Current Queue Status ===")
for row in queue:
    print(f"{row['source']} {row['status']}: {row['count']}")

# Check predicted_series points
series = conn.execute("""
    SELECT strategy_id, ticker, COUNT(*) as points
    FROM predicted_series_points
    WHERE timestamp >= '2026-04-10'
    GROUP BY strategy_id, ticker
    LIMIT 10
""").fetchall()

print(f"\n=== Predicted Series Points ===")
for row in series:
    print(f"{row['strategy_id']} {row['ticker']}: {row['points']} points")
