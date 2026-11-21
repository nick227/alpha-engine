"""
Reconciliation & Investigation Script for Phase 2.5 Closeout
"""
import os
import sqlite3
import json
from datetime import datetime, timezone

DB_CURRENT = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db"
DB_BACKUP = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db.backup_20261120_pre_backfill"

def inspect_db(path):
    print(f"\n==========================================")
    print(f"Inspecting: {path}")
    print(f"==========================================")
    size_bytes = os.path.getsize(path)
    print(f"File size: {size_bytes} bytes ({round(size_bytes / (1024*1024), 2)} MB)")
    
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("PRAGMA schema_version;")
    schema_ver = cur.fetchone()[0]
    print(f"Schema version: {schema_ver}")

    cur.execute("PRAGMA integrity_check;")
    ic = [r[0] for r in cur.fetchall()]
    print(f"Integrity check: {ic[:5]}")

    cur.execute("SELECT COUNT(*) FROM price_bars;")
    total_bars = cur.fetchone()[0]
    print(f"Total price_bars: {total_bars}")

    cur.execute("SELECT tenant_id, timeframe, COUNT(*), MIN(timestamp), MAX(timestamp) FROM price_bars GROUP BY tenant_id, timeframe;")
    tenant_tf = [dict(r) for r in cur.fetchall()]
    print(f"By tenant & timeframe:")
    for row in tenant_tf:
        print(f"  - Tenant: {row['tenant_id']}, TF: {row['timeframe']}, Count: {row['COUNT(*)']}, Min: {row['MIN(timestamp)']}, Max: {row['MAX(timestamp)']}")

    # Check other tables that might have 33M rows or check if there was another table
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'index');")
    tables_and_indexes = [dict(r) for r in cur.fetchall()]
    
    conn.close()
    return {
        "path": path,
        "size_bytes": size_bytes,
        "size_mb": round(size_bytes / (1024*1024), 2),
        "schema_version": schema_ver,
        "integrity_check": ic,
        "total_bars": total_bars,
        "by_tenant_tf": tenant_tf
    }

if __name__ == "__main__":
    res_curr = inspect_db(DB_CURRENT)
    res_back = inspect_db(DB_BACKUP)
