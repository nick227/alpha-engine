#!/usr/bin/env python
"""
Targeted feature-snapshot backfill for fear-regime dates.

Only computes and stores feature_snapshot rows for dates where VIX > VIX3M
(inverted term structure = fear regime). This is the minimal dataset needed
to expose sniper_coil to the historical stress events it was designed for:
2008 crisis, 2011 euro stress, 2018 vol spike, 2020 COVID, 2022 tightening.

Skips dates already present in feature_snapshot (fully idempotent).

Usage:
    python scripts/backfill_fear_regime_features.py
    python scripts/backfill_fear_regime_features.py --start-year 2007
    python scripts/backfill_fear_regime_features.py --start-year 2007 --min-spread 0.5
    python scripts/backfill_fear_regime_features.py --dry-run

Output:
    Rows inserted into feature_snapshot (symbol, as_of_date, close,
    return_63d, volatility_20d, price_percentile_252d, dollar_volume, volume_zscore_20d)
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

DB_PATH = _ROOT / "data" / "alpha.db"
TENANT = "ml_train"  # has 3,476+ tickers with history back to 2008
# Days of price history needed before earliest fear date (for 252d rolling window)
LOOKBACK_DAYS = 380


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_fear_regime_dates(conn: sqlite3.Connection, start_year: int, min_spread: float) -> list[str]:
    """Return sorted list of YYYY-MM-DD strings where VIX > VIX3M."""
    rows = conn.execute(
        """
        SELECT DISTINCT DATE(v.timestamp) as dt
        FROM price_bars v
        JOIN price_bars v3
          ON v3.ticker = '^VIX3M'
         AND v3.timeframe = '1d'
         AND DATE(v3.timestamp) = DATE(v.timestamp)
        WHERE v.ticker = '^VIX'
          AND v.timeframe = '1d'
          AND v.close - v3.close >= ?
          AND DATE(v.timestamp) >= ?
        ORDER BY dt
        """,
        (min_spread, f"{start_year}-01-01"),
    ).fetchall()
    return [str(r[0]) for r in rows]


def _already_backfilled(conn: sqlite3.Connection, dates: list[str], min_symbols: int = 100) -> set[str]:
    """Return dates that already have sufficient feature_snapshot rows."""
    if not dates:
        return set()
    ph = ",".join("?" * len(dates))
    rows = conn.execute(
        f"""
        SELECT as_of_date, COUNT(DISTINCT symbol) as n
        FROM feature_snapshot
        WHERE as_of_date IN ({ph})
        GROUP BY as_of_date
        HAVING n >= ?
        """,
        (*dates, min_symbols),
    ).fetchall()
    return {str(r[0]) for r in rows}


def _load_price_bars(conn: sqlite3.Connection, start_date: str, end_date: str) -> pd.DataFrame:
    """Load all daily price bars from default tenant between start and end dates."""
    print(f"  Loading price_bars [{start_date} -> {end_date}] tenant={TENANT!r} ...", flush=True)
    t0 = time.time()
    df = pd.read_sql(
        """
        SELECT ticker AS symbol, DATE(timestamp) AS ts, close, volume
        FROM price_bars
        WHERE tenant_id = ?
          AND timeframe = '1d'
          AND DATE(timestamp) >= ?
          AND DATE(timestamp) <= ?
        ORDER BY ticker, DATE(timestamp)
        """,
        conn,
        params=(TENANT, start_date, end_date),
    )
    elapsed = time.time() - t0
    print(f"  Loaded {len(df):,} rows, {df['symbol'].nunique():,} tickers ({elapsed:.1f}s)")
    return df


def _compute_features(df: pd.DataFrame, fear_dates: set[str]) -> pd.DataFrame:
    """
    Compute rolling features per ticker. Only returns rows whose date is in fear_dates.

    Returns a DataFrame with columns:
      symbol, as_of_date, close, return_63d, volatility_20d,
      price_percentile_252d, dollar_volume, volume_zscore_20d
    """
    df["ts"] = pd.to_datetime(df["ts"])
    all_chunks: list[pd.DataFrame] = []

    tickers = df["symbol"].unique()
    n = len(tickers)
    print(f"  Computing rolling features for {n:,} tickers ...", flush=True)
    t0 = time.time()

    for i, (sym, g) in enumerate(df.groupby("symbol", sort=False)):
        g = g.sort_values("ts").copy()

        g["return_63d"] = g["close"].pct_change(63).clip(-1, 1)
        g["volatility_20d"] = g["close"].pct_change().rolling(20).std()

        g["price_percentile_252d"] = g["close"].rolling(252).apply(
            lambda x: (x.iloc[-1] - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0.0,
            raw=False,
        )

        g["dollar_volume"] = g["close"] * g["volume"]

        vol_std = g["volume"].rolling(20).std().replace(0.0, float("nan"))
        g["volume_zscore_20d"] = (
            (g["volume"] - g["volume"].rolling(20).mean()) / vol_std
        ).replace([float("inf"), -float("inf")], float("nan")).clip(-5, 5)

        g_clean = g.dropna(subset=[
            "return_63d", "volatility_20d", "price_percentile_252d", "volume_zscore_20d",
        ])

        # Keep only fear-regime dates
        g_clean = g_clean[g_clean["ts"].dt.strftime("%Y-%m-%d").isin(fear_dates)]

        if g_clean.empty:
            continue

        out = g_clean[["ts", "close", "return_63d", "volatility_20d",
                        "price_percentile_252d", "dollar_volume", "volume_zscore_20d"]].copy()
        out["symbol"] = sym
        out["as_of_date"] = out["ts"].dt.strftime("%Y-%m-%d")
        out = out.drop(columns=["ts"])
        all_chunks.append(out)

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"    [{i+1:,}/{n:,}] {elapsed:.0f}s elapsed", flush=True)

    if not all_chunks:
        return pd.DataFrame()

    result = pd.concat(all_chunks, ignore_index=True)
    print(f"  Features computed: {len(result):,} rows across {result['symbol'].nunique():,} tickers ({time.time()-t0:.1f}s)")
    return result


def _insert_features(conn: sqlite3.Connection, df: pd.DataFrame, dry: bool) -> int:
    """INSERT OR IGNORE rows into feature_snapshot. Returns rows inserted."""
    if df.empty:
        return 0

    cols = ["symbol", "as_of_date", "close", "return_63d",
            "volatility_20d", "price_percentile_252d", "dollar_volume", "volume_zscore_20d"]
    df = df[cols].dropna(subset=["close"])

    if dry:
        print(f"  [dry-run] Would insert up to {len(df):,} rows")
        return 0

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    conn.executemany(
        """
        INSERT OR IGNORE INTO feature_snapshot
          (symbol, as_of_date, close, return_63d, volatility_20d,
           price_percentile_252d, dollar_volume, volume_zscore_20d)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill feature_snapshot for fear-regime dates")
    parser.add_argument("--db",          default=str(DB_PATH),  help="Path to alpha.db")
    parser.add_argument("--start-year",  type=int, default=2007, help="Earliest year to backfill (default: 2007)")
    parser.add_argument("--min-spread",  type=float, default=0.0, help="Min VIX-VIX3M spread to qualify (default: 0.0)")
    parser.add_argument("--dry-run",     action="store_true",    help="Compute but do not write")
    args = parser.parse_args()

    t_start = time.time()
    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Ensure feature_snapshot table exists
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
    conn.commit()

    # Get fear regime dates
    print(f"\n=== Backfill: fear-regime features (start_year={args.start_year}, min_spread={args.min_spread}) ===\n")
    all_fear_dates = _get_fear_regime_dates(conn, args.start_year, args.min_spread)
    print(f"Fear regime dates found: {len(all_fear_dates)}")
    if not all_fear_dates:
        print("Nothing to backfill.")
        conn.close()
        return 0

    # Skip dates already in feature_snapshot
    already_done = _already_backfilled(conn, all_fear_dates)
    fear_dates = [d for d in all_fear_dates if d not in already_done]
    print(f"Already backfilled:      {len(already_done)}")
    print(f"Need to backfill:        {len(fear_dates)}")

    if not fear_dates:
        print("\nAll fear-regime dates already have feature data. Nothing to do.")
        conn.close()
        return 0

    fear_dates_set = set(fear_dates)
    min_date = min(fear_dates)
    max_date = max(fear_dates)
    # Load bars from LOOKBACK_DAYS before earliest target date (for 252d rolling window)
    lookback_start = (date.fromisoformat(min_date) - timedelta(days=LOOKBACK_DAYS)).isoformat()

    print(f"\nDate range:    {min_date} -> {max_date}")
    print(f"Bar load from: {lookback_start} (includes {LOOKBACK_DAYS}d lookback buffer)")

    # Load price bars
    df_bars = _load_price_bars(conn, lookback_start, max_date)
    if df_bars.empty:
        print("No price bars loaded — check tenant and date range.")
        conn.close()
        return 1

    # Compute features, filtered to fear dates only
    df_features = _compute_features(df_bars, fear_dates_set)
    if df_features.empty:
        print("No features computed — no tickers had sufficient history for these dates.")
        conn.close()
        return 1

    # Write to DB
    print(f"\nInserting into feature_snapshot ...", flush=True)
    inserted = _insert_features(conn, df_features, dry=args.dry_run)

    elapsed = time.time() - t_start
    print(f"\n=== Done ===")
    print(f"Rows inserted:   {inserted:,}")
    print(f"Target dates:    {len(fear_dates)}")
    print(f"Elapsed:         {elapsed/60:.1f} min")

    # Quick verification
    if not args.dry_run:
        sample_date = fear_dates[len(fear_dates) // 2]
        n = conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM feature_snapshot WHERE as_of_date = ?",
            (sample_date,),
        ).fetchone()[0]
        print(f"Spot check {sample_date}: {n} symbols in feature_snapshot")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
