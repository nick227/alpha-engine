import sqlite3
from datetime import datetime
import os

# Test database connection
db_path = 'data/alpha.db'
print(f"Current working directory: {os.getcwd()}")
print(f"Database path: {db_path}")
print(f"Database exists: {os.path.exists(db_path)}")

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

# Test query
query = """
SELECT 
    p.id, p.strategy_id, p.ticker, p.timestamp,
    p.prediction, p.confidence, p.horizon,
    p.entry_price, p.mode, p.regime,
    po.return_pct, po.direction_correct, po.max_runup, po.max_drawdown,
    po.evaluated_at
FROM predictions p
LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
WHERE DATE(p.timestamp) = DATE(?)
AND p.mode = ?
AND po.return_pct IS NOT NULL
ORDER BY p.timestamp
LIMIT 5
"""

date = datetime(2026, 4, 15)
cursor = conn.execute(query, (date.date(), 'backfill'))
results = cursor.fetchall()

print(f"Found {len(results)} results for {date.date()} with mode 'backfill'")
for row in results:
    print(f"  {row['ticker']} - {row['strategy_id']} - {row['return_pct']}")

conn.close()
