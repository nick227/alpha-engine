#!/usr/bin/env python
"""
Fetch ^VIX3M daily bars from yfinance and upsert into price_bars.

This is intentionally a small targeted utility so you don't have to rerun
scripts/expand_training_data.py (which rebuilds datasets and retrains).

Usage:
  python scripts/fetch_vix3m.py
  python scripts/fetch_vix3m.py --start 2018-01-01 --tenants default,ml_train
  python scripts/fetch_vix3m.py --symbol ^VIX3M --force
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _fetch_yfinance(symbol: str, *, start: str) -> pd.DataFrame:
    import yfinance as yf

    raw = yf.download(
        symbol,
        start=str(start),
        end=date.today().isoformat(),
        interval="1d",
        progress=False,
        auto_adjust=True,
    )
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    raw = raw.reset_index()
    raw.columns = [c[0].lower() if isinstance(c, tuple) else str(c).lower() for c in raw.columns]

    # yfinance sometimes returns "datetime" instead of "date" column name.
    if "date" not in raw.columns and "datetime" in raw.columns:
        raw = raw.rename(columns={"datetime": "date"})

    raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y-%m-%d")

    for col in ("open", "high", "low", "close", "volume"):
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce")
        else:
            raw[col] = pd.NA

    # Some index series have no volume; keep it but fill missing with 0.
    raw["volume"] = raw["volume"].fillna(0.0)

    out = raw[["date", "open", "high", "low", "close", "volume"]].dropna(subset=["close"]).copy()
    out = out.sort_values("date")
    return out


def _upsert_price_bars(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    df: pd.DataFrame,
    force: bool,
) -> int:
    if df.empty:
        return 0

    stmt = (
        "INSERT OR REPLACE INTO price_bars "
        "(tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume) "
        "VALUES (?,?,?,?,?,?,?,?,?)"
        if force
        else
        "INSERT OR IGNORE INTO price_bars "
        "(tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume) "
        "VALUES (?,?,?,?,?,?,?,?,?)"
    )

    rows = []
    for _, r in df.iterrows():
        ts = str(r["date"])[:10]
        rows.append(
            (
                str(tenant_id),
                str(ticker),
                "1d",
                ts,
                float(r["open"]) if pd.notna(r["open"]) else float(r["close"]),
                float(r["high"]) if pd.notna(r["high"]) else float(r["close"]),
                float(r["low"]) if pd.notna(r["low"]) else float(r["close"]),
                float(r["close"]),
                float(r["volume"]) if pd.notna(r["volume"]) else 0.0,
            )
        )

    # SQLite can be locked by another writer (streamlit app, training job, etc.).
    # Use a short retry/backoff to avoid failing the whole run.
    for attempt in range(0, 8):
        try:
            conn.executemany(stmt, rows)
            return len(rows)
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            time.sleep(0.25 * (2**attempt))
    # Final attempt (raise original-ish error)
    conn.executemany(stmt, rows)
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(_ROOT / "data" / "alpha.db"), help="SQLite DB path")
    p.add_argument("--symbol", default="^VIX3M", help="Symbol to fetch (default: ^VIX3M)")
    p.add_argument("--start", default="2018-01-01", help="Start date YYYY-MM-DD (default: 2018-01-01)")
    p.add_argument("--tenants", default="default,ml_train", help="Comma-separated tenants to write to")
    p.add_argument("--force", action="store_true", help="Overwrite existing bars (INSERT OR REPLACE)")
    args = p.parse_args(argv)

    db_path = Path(str(args.db))
    symbol = str(args.symbol).strip()
    start = str(args.start).strip()
    tenants = [t.strip() for t in str(args.tenants).split(",") if t.strip()]

    print(f"Fetching {symbol} from yfinance...")
    df = _fetch_yfinance(symbol, start=start)
    if df.empty:
        print("No bars returned.")
        return 1

    print(f"Got {len(df)} bars ({df['date'].min()} to {df['date'].max()})")

    conn = sqlite3.connect(str(db_path), timeout=60.0)
    try:
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=60000;")
        except Exception:
            pass
        # Rely on existing schema, but in case of a fresh DB ensure the table exists.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_bars (
                tenant_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (tenant_id, ticker, timeframe, timestamp)
            );
            """
        )

        total = 0
        for tenant in tenants:
            n = _upsert_price_bars(conn, tenant_id=tenant, ticker=symbol, df=df, force=bool(args.force))
            total += int(n)
            print(f"Inserted {n} rows into tenant={tenant}")
        conn.commit()
        print(f"Done. Total rows written: {total}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
