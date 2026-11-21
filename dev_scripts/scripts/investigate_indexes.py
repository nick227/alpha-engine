"""
Investigate index definitions and query plans
"""
import sqlite3

def check_indexes(path):
    print(f"\n==========================================")
    print(f"Index analysis: {path}")
    print(f"==========================================")
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # Get CREATE TABLE DDL
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='price_bars'")
    print("Table DDL:\n", cur.fetchone()[0])

    # Get index_list
    cur.execute("PRAGMA index_list('price_bars')")
    indexes = cur.fetchall()
    print("Index list:", indexes)

    for idx in indexes:
        idx_name = idx[1]
        cur.execute(f"PRAGMA index_xinfo('{idx_name}')")
        xinfo = cur.fetchall()
        print(f"\nIndex '{idx_name}' xinfo:")
        for col in xinfo:
            print(f"  seqno={col[0]}, cid={col[1]}, name={col[2]}, desc={col[3]}, coll={col[4]}, key={col[5]}")

    # Test query plan for 4-column filter: tenant, ticker, timeframe, timestamp
    q = "EXPLAIN QUERY PLAN SELECT * FROM price_bars WHERE tenant_id = 'default' AND ticker = 'AAPL' AND timeframe = '1d' AND timestamp = '2026-04-15'"
    cur.execute(q)
    print(f"\nPlan for 4-col filter:\n", cur.fetchall())

    conn.close()

if __name__ == "__main__":
    check_indexes("/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db")
    check_indexes("/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db.backup_20261120_pre_backfill")
