import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check prediction queue
queue = conn.execute("""
    SELECT source, symbol, status, created_at
    FROM prediction_queue
    ORDER BY created_at DESC
    LIMIT 20
""").fetchall()

print("=== Prediction Queue ===")
for row in queue:
    print(f"{row['source']} {row['symbol']}: {row['status']} (created: {row['created_at']})")

# Check if our discovery strategies exist in strategies table
strategies = conn.execute("""
    SELECT id, name
    FROM strategies
    ORDER BY name
""").fetchall()

print(f"\n=== Available Strategies ===")
for row in strategies:
    print(f"{row['id']}: {row['name']}")

# Check if discovery candidates have strategy_type mapping
candidates = conn.execute("""
    SELECT DISTINCT strategy_type
    FROM discovery_candidates
    WHERE as_of_date = '2026-04-10'
    LIMIT 10
""").fetchall()

print(f"\n=== Discovery Strategy Types ===")
for row in candidates:
    print(f"{row['strategy_type']}")
