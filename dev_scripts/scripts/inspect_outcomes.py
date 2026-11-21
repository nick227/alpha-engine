import sqlite3
import yaml
import json
from datetime import datetime, timezone

db_path = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Check distribution of exit_price, exit_reason, evaluated_at
cur.execute("SELECT tenant_id, COUNT(*) as c FROM prediction_outcomes GROUP BY tenant_id")
print("All outcomes by tenant:", [dict(r) for r in cur.fetchall()])

# What were the 377 outcomes previously reported?
cur.execute("""
    SELECT COUNT(*) as c 
    FROM prediction_outcomes po
    JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
    WHERE po.exit_price BETWEEN 100.35 AND 100.75
""")
print("Outcomes with exit_price ~ 100.5:", cur.fetchone()["c"])

cur.execute("""
    SELECT po.tenant_id, COUNT(*) as c 
    FROM prediction_outcomes po
    WHERE po.exit_price BETWEEN 100.35 AND 100.75
    GROUP BY po.tenant_id
""")
print("exit_price ~ 100.5 by tenant:", [dict(r) for r in cur.fetchall()])

cur.execute("""
    SELECT p.ticker, p.entry_price, po.exit_price, po.return_pct, po.evaluated_at, po.tenant_id
    FROM prediction_outcomes po
    JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
    WHERE po.exit_price BETWEEN 100.35 AND 100.75
    LIMIT 10
""")
print("Sample exit_price ~ 100.5:", [dict(r) for r in cur.fetchall()])

# Check YAML config
with open("/home/administrator/web/alpha-trading-platform/alpha-engine/config/consumer_universe_150.yaml") as f:
    cfg = yaml.safe_load(f)
symbols = cfg.get("symbols", [])
print(f"Loaded {len(symbols)} symbols from YAML config.")
