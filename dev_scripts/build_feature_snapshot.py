import sqlite3
import pandas as pd
import time

start = time.time()
conn = sqlite3.connect("data/alpha.db")

# Create feature_snapshot table if not exists
conn.execute("""
    CREATE TABLE IF NOT EXISTS feature_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        as_of_date TEXT,
        close REAL,
        return_63d REAL,
        volatility_20d REAL,
        price_percentile_252d REAL,
        dollar_volume REAL,
        volume_zscore_20d REAL
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_feature_symbol_date ON feature_snapshot(symbol, as_of_date)")


# Limit to last 2 years of data for speed
df = pd.read_sql("""
    SELECT ticker as symbol, timestamp, close, volume
    FROM price_bars
    WHERE timestamp >= '2024-01-01'
    ORDER BY ticker, timestamp
""", conn)

df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", utc=True)

rows = []

for symbol, g in df.groupby("symbol"):
    g = g.sort_values("timestamp")

    g["return_63d"] = g["close"].pct_change(63)
    g["volatility_20d"] = g["close"].pct_change().rolling(20).std()

    g["price_percentile_252d"] = g["close"].rolling(252).apply(
        lambda x: (x.iloc[-1] - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0,
        raw=False
    )

    g["dollar_volume"] = g["close"] * g["volume"]

    # Z-score with inf guard
    vol_std = g["volume"].rolling(20).std()
    vol_std = vol_std.replace(0, float('nan'))  # Avoid division by zero
    g["volume_zscore_20d"] = (
        (g["volume"] - g["volume"].rolling(20).mean()) / vol_std
    ).replace([float("inf"), -float("inf")], float('nan'))

    # Clamp extreme values
    g["volume_zscore_20d"] = g["volume_zscore_20d"].clip(-5, 5)
    g["return_63d"] = g["return_63d"].clip(-1, 1)

    # Save ALL valid rows, not just latest
    g_clean = g.dropna(subset=[
        "return_63d",
        "volatility_20d",
        "price_percentile_252d",
        "volume_zscore_20d"
    ])

    if g_clean.empty:
        continue

    df_out = g_clean[[
        "timestamp",
        "close",
        "return_63d",
        "volatility_20d",
        "price_percentile_252d",
        "dollar_volume",
        "volume_zscore_20d"
    ]].copy()

    df_out["symbol"] = symbol
    df_out["as_of_date"] = df_out["timestamp"].dt.date.astype(str)

    df_out = df_out.drop(columns=["timestamp"])

    df_out.to_sql("feature_snapshot", conn, if_exists="append", index=False, chunksize=1000)

total_rows = conn.execute("SELECT COUNT(*) FROM feature_snapshot").fetchone()[0]
print(f"Populated feature_snapshot with {total_rows} rows across {df['symbol'].nunique()} symbols")

# Verify
r = conn.execute("SELECT * FROM feature_snapshot LIMIT 5").fetchall()
print("\nSample data:")
for x in r:
    print(f"  {x}")

conn.close()
print(f"Elapsed: {time.time() - start:.2f}s")
