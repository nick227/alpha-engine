import sqlite3
import os
import sys
import json

db_path = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db"

if not os.path.exists(db_path):
    print(f"Error: {db_path} does not exist")
    sys.exit(1)

file_size_bytes = os.path.getsize(db_path)
file_size_mb = file_size_bytes / (1024 * 1024)
file_size_gb = file_size_bytes / (1024 * 1024 * 1024)

print(f"Database file: {db_path}")
print(f"File size: {file_size_bytes:,} bytes ({file_size_mb:.2f} MB, {file_size_gb:.2f} GB)")

# Open in read-only mode via URI
uri = f"file:{db_path}?mode=ro"
conn = sqlite3.connect(uri, uri=True)
cursor = conn.cursor()

# 1. Pragma inspections
cursor.execute("PRAGMA page_size;")
page_size = cursor.fetchone()[0]

cursor.execute("PRAGMA page_count;")
page_count = cursor.fetchone()[0]

cursor.execute("PRAGMA freelist_count;")
freelist_count = cursor.fetchone()[0]

total_calculated_bytes = page_size * page_count
freelist_bytes = freelist_count * page_size
freelist_mb = freelist_bytes / (1024 * 1024)
freelist_pct = (freelist_count / page_count * 100) if page_count else 0

print(f"\n--- SQLite Pragmas ---")
print(f"Page size: {page_size:,} bytes")
print(f"Page count: {page_count:,}")
print(f"Calculated size: {total_calculated_bytes:,} bytes ({total_calculated_bytes / (1024*1024*1024):.2f} GB)")
print(f"Freelist page count: {freelist_count:,} ({freelist_mb:.2f} MB, {freelist_pct:.2f}% of database)")

# 2. Check dbstat availability
has_dbstat = False
try:
    cursor.execute("SELECT name, SUM(pgsize) AS bytes, ROUND(SUM(pgsize) / 1048576.0, 1) AS mb FROM dbstat GROUP BY name ORDER BY bytes DESC LIMIT 30;")
    dbstat_rows = cursor.fetchall()
    has_dbstat = True
    print("\n--- dbstat breakdown (Top 30 objects) ---")
    print(f"{'Object Name':<40} | {'Type':<8} | {'Bytes':<15} | {'Size (MB)':<12} | {'% of DB':<8}")
    print("-" * 90)
    for row in dbstat_rows:
        name, b, mb = row
        # Find type from sqlite_master
        cursor.execute("SELECT type FROM sqlite_master WHERE name = ?", (name,))
        type_res = cursor.fetchone()
        obj_type = type_res[0] if type_res else "unknown"
        pct = (b / file_size_bytes * 100) if file_size_bytes else 0
        print(f"{name:<40} | {obj_type:<8} | {b:<15,} | {mb:<12.1f} | {pct:<8.2f}%")
except Exception as e:
    print(f"\ndbstat query failed: {e}")

# 3. List all tables and indexes from sqlite_master
cursor.execute("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table', 'index') ORDER BY type, name;")
objects = cursor.fetchall()

tables = [obj for obj in objects if obj[0] == 'table' and not obj[1].startswith('sqlite_')]
indexes = [obj for obj in objects if obj[0] == 'index' and not obj[1].startswith('sqlite_')]

print(f"\nTotal user tables: {len(tables)}, total user indexes: {len(indexes)}")

# 4. For each table, get row count
print("\n--- User Tables Row Count ---")
table_stats = []
for t in tables:
    t_name = t[1]
    try:
        cursor.execute(f"SELECT COUNT(*) FROM \"{t_name}\";")
        row_count = cursor.fetchone()[0]
        table_stats.append((t_name, row_count))
    except Exception as e:
        table_stats.append((t_name, -1))

table_stats.sort(key=lambda x: x[1], reverse=True)
for name, count in table_stats:
    print(f"{name:<40} : {count:>12,} rows")

# 5. For the top tables, check columns, dates, sample row length
print("\n--- Detailed Inspection of Top Tables ---")
for name, count in table_stats[:10]:
    if count <= 0:
        continue
    print(f"\nTable: {name} ({count:,} rows)")
    cursor.execute(f"PRAGMA table_info(\"{name}\");")
    columns = [col[1] for col in cursor.fetchall()]
    print(f"Columns: {', '.join(columns)}")
    
    # Check date column if exists
    date_cols = [c for c in columns if 'date' in c.lower() or 'time' in c.lower() or 'created' in c.lower() or 'timestamp' in c.lower()]
    for dc in date_cols:
        try:
            cursor.execute(f"SELECT MIN(\"{dc}\"), MAX(\"{dc}\") FROM \"{name}\";")
            min_d, max_d = cursor.fetchone()
            print(f"  Range of {dc}: {min_d} to {max_d}")
        except Exception:
            pass

    # Sample average payload size
    try:
        # Sum length of all columns for a sample of 1000 rows
        length_exprs = " + ".join([f"LENGTH(CAST(\"{c}\" AS BLOB))" for c in columns])
        cursor.execute(f"SELECT AVG({length_exprs}) FROM (SELECT * FROM \"{name}\" LIMIT 1000);")
        avg_row_len = cursor.fetchone()[0]
        if avg_row_len:
            est_data_mb = (avg_row_len * count) / (1024 * 1024)
            print(f"  Avg row data width: {avg_row_len:.1f} bytes -> Est. raw data: {est_data_mb:.1f} MB")
    except Exception as e:
        print(f"  Row width estimation error: {e}")

conn.close()
