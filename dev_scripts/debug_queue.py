import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check queue entries for discovery source
queue_items = conn.execute("""
    SELECT as_of_date, symbol, status, metadata_json, created_at
    FROM prediction_queue
    WHERE source = 'discovery' AND as_of_date = '2026-04-10'
    ORDER BY created_at DESC
    LIMIT 10
""").fetchall()

print("=== Discovery Queue Items ===")
for i, row in enumerate(queue_items):
    print(f"{i+1}. {row['symbol']}: {row['status']}")
    metadata = row['metadata_json']
    if metadata:
        import json
        try:
            meta = json.loads(metadata)
            strategy_id = meta.get('strategy_id', 'N/A')
            direction = meta.get('direction', 'N/A')
            confidence = meta.get('confidence', 0)
            print(f"   -> {strategy_id} {direction} conf={confidence}")
        except:
            print(f"   -> Invalid JSON: {metadata}")

# Check if strategies exist
strategies = conn.execute("""
    SELECT id, name FROM strategies WHERE name LIKE '%_v1_default'
""").fetchall()

print(f"\n=== Discovery Strategies ===")
for row in strategies:
    print(f"{row['id']}: {row['name']}")
