"""
Audit derived / materialized tables from prediction_outcomes
"""
import sqlite3

conn = sqlite3.connect("/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Find all tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print("Tables in alpha.db:", tables)

# Check strategy_performance
if "strategy_performance" in tables:
    cur.execute("SELECT * FROM strategy_performance")
    rows = [dict(r) for r in cur.fetchall()]
    print(f"\nstrategy_performance ({len(rows)} rows):")
    for r in rows:
        print(f"  {r}")

# Check strategy_weights
if "strategy_weights" in tables:
    cur.execute("SELECT * FROM strategy_weights")
    rows = [dict(r) for r in cur.fetchall()]
    print(f"\nstrategy_weights ({len(rows)} rows):")
    for r in rows:
        print(f"  {r}")

# Check recommendations or consensus_signals
if "consensus_signals" in tables:
    cur.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM consensus_signals")
    print("\nconsensus_signals:", dict(cur.fetchone()))

# Check how strategy_performance is computed in alpha-engine
conn.close()
