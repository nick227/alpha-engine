import sqlite3
import os
import shutil
import time

src_db = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db"
backup_db = "/home/administrator/web/alpha-trading-platform/alpha-engine/backups_pre_compact/alpha.db.backup_20261124_full"
dst_db = "/home/administrator/web/alpha-trading-platform/alpha-engine/scratch/alpha_compact.db"

print(f"=== Starting Alpha Engine Compact DB Build ===")
start_time = time.time()

# 1. Complete backup of original alpha.db
if not os.path.exists(backup_db):
    print(f"1. Creating full backup of {src_db} -> {backup_db}...")
    shutil.copy2(src_db, backup_db)
    print(f"   Backup created successfully: {os.path.getsize(backup_db):,} bytes")
else:
    print(f"1. Full backup already exists: {backup_db} ({os.path.getsize(backup_db):,} bytes)")

if os.path.exists(dst_db):
    os.remove(dst_db)

# 2. Connect to source read-only
print("2. Connecting to source database...")
src_conn = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
src_cursor = src_conn.cursor()

# Get pragmas from source
src_cursor.execute("PRAGMA user_version;")
user_version = src_cursor.fetchone()[0]

src_cursor.execute("PRAGMA page_size;")
page_size = src_cursor.fetchone()[0]

# 3. Connect to target database
print("3. Initializing target compact database...")
dst_conn = sqlite3.connect(dst_db)
dst_cursor = dst_conn.cursor()

dst_cursor.execute(f"PRAGMA page_size = {page_size};")
dst_cursor.execute("PRAGMA synchronous = OFF;")
dst_cursor.execute("PRAGMA journal_mode = MEMORY;")

# 4. Get all tables and indexes from sqlite_master
src_cursor.execute("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' ORDER BY type DESC, name ASC;")
schema_objects = src_cursor.fetchall()

tables = [obj for obj in schema_objects if obj[0] == 'table']
indexes = [obj for obj in schema_objects if obj[0] == 'index']

print(f"Found {len(tables)} tables and {len(indexes)} indexes.")

# Attach destination to source for fast transfer
src_conn.close()

# Reopen with target attached to source
print("4. Attaching target database to source for fast internal transfer...")
conn = sqlite3.connect(src_db)
conn.execute("PRAGMA synchronous = OFF;")
conn.execute("PRAGMA cache_size = -64000;") # 64MB cache
conn.execute(f"ATTACH DATABASE '{dst_db}' AS target;")
conn.execute(f"PRAGMA target.page_size = {page_size};")
conn.execute("PRAGMA target.synchronous = OFF;")
conn.execute("PRAGMA target.journal_mode = MEMORY;")
conn.execute(f"PRAGMA target.user_version = {user_version};")

# Create tables first (without secondary indexes yet, for fast load)
print("5. Creating tables in target database...")
for t in tables:
    sql = t[3]
    if sql:
        # Replace CREATE TABLE with CREATE TABLE target.
        create_sql = sql.replace("CREATE TABLE ", "CREATE TABLE target.", 1)
        create_sql = create_sql.replace("CREATE TABLE IF NOT EXISTS ", "CREATE TABLE IF NOT EXISTS target.", 1)
        conn.execute(create_sql)

conn.commit()

# Populate tables
print("6. Transferring operational data...")
table_counts = {}

for t in tables:
    t_name = t[1]
    t0 = time.time()
    
    if t_name == "price_bars":
        print(f"   Filtering price_bars (excluding tenant 'ml_train')...")
        conn.execute("INSERT INTO target.price_bars SELECT * FROM main.price_bars WHERE tenant_id != 'ml_train';")
    elif t_name == "ml_learning_rows":
        print(f"   Excluding one-off training rows from ml_learning_rows...")
        # Schema is created, but no rows inserted
        pass
    else:
        conn.execute(f"INSERT INTO target.\"{t_name}\" SELECT * FROM main.\"{t_name}\";")
    
    conn.commit()
    dst_count = conn.execute(f"SELECT COUNT(*) FROM target.\"{t_name}\";").fetchone()[0]
    src_count = conn.execute(f"SELECT COUNT(*) FROM main.\"{t_name}\";").fetchone()[0]
    table_counts[t_name] = (src_count, dst_count)
    dt = time.time() - t0
    if dt > 0.5 or t_name in ['price_bars', 'feature_snapshot', 'discovery_candidate_outcomes']:
        print(f"   Table {t_name:<35} : {dst_count:>10,} rows (src: {src_count:>10,}) [{dt:.1f}s]")

# Detach target before index creation
conn.commit()
conn.execute("DETACH DATABASE target;")
conn.close()

# Create secondary indexes directly in target
print("7. Building indexes directly in target database...")
target_conn = sqlite3.connect(dst_db)
target_conn.execute("PRAGMA synchronous = OFF;")
target_conn.execute("PRAGMA cache_size = -64000;")
for idx in indexes:
    sql = idx[3]
    if sql:
        t0 = time.time()
        target_conn.execute(sql)
        dt = time.time() - t0
        if dt > 0.5:
            print(f"   Index {idx[1]:<35} [{dt:.1f}s]")

target_conn.commit()

# 8. Finalizing target database (WAL mode, VACUUM, PRAGMA integrity_check)
print("8. Finalizing target database (WAL mode, VACUUM, PRAGMA integrity_check)...")
target_conn.execute("PRAGMA journal_mode = WAL;")
target_conn.execute("PRAGMA synchronous = NORMAL;")
target_conn.execute("VACUUM;")

integrity = target_conn.execute("PRAGMA integrity_check;").fetchall()
print(f"   Integrity check: {integrity}")

target_conn.close()

# Remove any temporary WAL/SHM left over
compact_size = os.path.getsize(dst_db)
compact_mb = compact_size / (1024 * 1024)
compact_gb = compact_size / (1024 * 1024 * 1024)

orig_size = os.path.getsize(src_db)
orig_mb = orig_size / (1024 * 1024)
orig_gb = orig_size / (1024 * 1024 * 1024)

saved_mb = orig_mb - compact_mb
saved_pct = (saved_mb / orig_mb) * 100

print(f"\n=== Compact DB Build Complete in {time.time() - start_time:.1f}s ===")
print(f"Original DB : {orig_size:,} bytes ({orig_mb:.2f} MB / {orig_gb:.2f} GB)")
print(f"Compact DB  : {compact_size:,} bytes ({compact_mb:.2f} MB / {compact_gb:.2f} GB)")
print(f"Reclaimed   : {saved_mb:.2f} MB ({saved_pct:.1f}% reduction)")
