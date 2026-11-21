"""
Fill the single missing date (2026-09-22) for the 60 scheduled symbols.
Uses INSERT OR IGNORE so existing bars are untouched.
"""

import sqlite3
import time
from datetime import datetime, timezone
import yfinance as yf
import yaml

DB_PATH = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db"
CONFIG_PATH = "/home/administrator/web/alpha-trading-platform/alpha-engine/config/consumer_universe_150.yaml"

def run_fill():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    symbols = []
    for s_list in data.values():
        if isinstance(s_list, list):
            for s in s_list:
                clean = str(s).strip().upper().replace(".", "-")
                if clean not in symbols:
                    symbols.append(clean)

    missing_symbols = []
    for sym in symbols:
        cur.execute("SELECT 1 FROM price_bars WHERE tenant_id = 'default' AND ticker = ? AND timestamp LIKE '2026-09-22%' LIMIT 1", (sym,))
        if not cur.fetchone():
            missing_symbols.append(sym)

    print(f"Symbols missing 2026-09-22: {len(missing_symbols)}")
    if not missing_symbols:
        print("None missing!")
        return

    chunk_size = 15
    inserted_bars = 0

    for i in range(0, len(missing_symbols), chunk_size):
        chunk = missing_symbols[i:i+chunk_size]
        print(f"Fetching chunk {i//chunk_size + 1}: {chunk}...")
        df = yf.download(
            chunk,
            start="2026-09-21",
            end="2026-09-24",
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker" if len(chunk) > 1 else None
        )

        batch_to_insert = []
        for sym in chunk:
            sym_df = df[sym] if len(chunk) > 1 and sym in df else df
            if sym_df is None or sym_df.empty:
                continue
            for dt, row in sym_df.iterrows():
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                if not dt_str.startswith("2026-09-22"):
                    continue
                o = float(row.get("Open") or 0.0)
                h = float(row.get("High") or 0.0)
                l = float(row.get("Low") or 0.0)
                c = float(row.get("Close") or 0.0)
                v = float(row.get("Volume") or 0.0)
                if o > 0 and h > 0 and l > 0 and c > 0:
                    batch_to_insert.append(('default', sym, '1d', dt_str, o, h, l, c, v))

        if batch_to_insert:
            cur.executemany("""
                INSERT OR IGNORE INTO price_bars 
                (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_to_insert)
            conn.commit()
            inserted_bars += len(batch_to_insert)
            print(f"  Inserted {len(batch_to_insert)} bars.")

        time.sleep(1.0)

    print(f"Total inserted {inserted_bars} missing bars.")
    conn.close()

if __name__ == "__main__":
    run_fill()
