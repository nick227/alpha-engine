import sqlite3
import json

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check remaining unprocessed discovery items
remaining = conn.execute("""
    SELECT symbol, metadata_json, created_at
    FROM prediction_queue
    WHERE source = 'discovery' AND status = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT 10
""").fetchall()

print(f"Remaining pending discovery items: {len(remaining)}")

for i, item in enumerate(remaining):
    metadata = json.loads(item['metadata_json'])
    print(f"{i+1}. {item['symbol']}: {metadata.get('strategy_id')} -> {metadata.get('direction')} (conf: {metadata.get('confidence'):.2f})")
