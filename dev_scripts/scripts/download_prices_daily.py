"""
Daily Price Downloader

Fetches OHLCV bars from Yahoo Finance for all symbols in feature_snapshot
and writes them directly into price_bars (tenant_id='default').

Incremental: only downloads from the day after each symbol's last bar.
Batch mode: yfinance multi-ticker downloads to minimize API calls.

Usage:
    python dev_scripts/scripts/download_prices_daily.py
    python dev_scripts/scripts/download_prices_daily.py --days 5        # force last N days
    python dev_scripts/scripts/download_prices_daily.py --dry-run       # show what would run
    python dev_scripts/scripts/download_prices_daily.py --symbols AAPL,MSFT
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yfinance as yf
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "alpha.db"
TENANT_ID = "default"
TIMEFRAME = "1d"
CHUNK_SIZE = 100        # symbols per yfinance batch request
DEFAULT_LOOKBACK = 30   # days to fetch for symbols with no existing bars
MIN_LOOKBACK = 2        # always fetch at least this many days back (catches today)


def _get_symbols(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM feature_snapshot ORDER BY symbol"
    ).fetchall()
    return [str(r[0]).upper() for r in rows]


def _get_last_bar_dates(
    conn: sqlite3.Connection, symbols: list[str], tenant_id: str
) -> dict[str, date | None]:
    """Return {symbol: last_bar_date} for symbols that already have bars."""
    result: dict[str, date | None] = {s: None for s in symbols}
    chunk = 900
    for i in range(0, len(symbols), chunk):
        batch = symbols[i : i + chunk]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(
            f"""
            SELECT ticker, MAX(DATE(timestamp)) as last_d
            FROM price_bars
            WHERE tenant_id = ? AND timeframe = ? AND ticker IN ({placeholders})
            GROUP BY ticker
            """,
            (tenant_id, TIMEFRAME, *batch),
        ).fetchall()
        for r in rows:
            sym = str(r[0]).upper()
            if r[1]:
                result[sym] = date.fromisoformat(str(r[1]))
    return result


def _fetch_batch(
    symbols: list[str], start: date, end: date
) -> dict[str, pd.DataFrame]:
    """
    Download OHLCV for a batch of symbols via yfinance.
    Returns {symbol: DataFrame} — only symbols with data are included.
    """
    ticker_str = " ".join(symbols)
    try:
        raw = yf.download(
            ticker_str,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"  [ERROR] yfinance batch download failed: {e}")
        return {}

    if raw is None or raw.empty:
        return {}

    out: dict[str, pd.DataFrame] = {}

    # yfinance without group_by returns MultiIndex columns: (Field, Ticker)
    # e.g. ('Close', 'AAPL'), ('Close', 'MSFT'), ...
    if isinstance(raw.columns, pd.MultiIndex):
        # Multi-ticker response
        available = set(raw.columns.get_level_values(1))
        for sym in symbols:
            sym_upper = sym.upper()
            if sym_upper not in available:
                continue
            try:
                # Stack fields for this ticker into a regular DataFrame
                df = raw.xs(sym_upper, axis=1, level=1).copy()
            except Exception:
                continue
            if not df.empty and "Close" in df.columns:
                out[sym_upper] = df
    else:
        # Single-ticker — columns are just field names
        sym = symbols[0].upper()
        df = raw.copy()
        if not df.empty and "Close" in df.columns:
            out[sym] = df

    return out


def _normalize_ts(ts) -> str:
    """
    Normalize any timestamp to midnight UTC ISO string.
    YYYY-MM-DDT00:00:00+00:00 is the canonical form for daily bars.
    This ensures INSERT OR REPLACE deduplicates the same trading day
    regardless of what time component yfinance returns.
    """
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    # Strip time → midnight UTC
    return datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc).isoformat()


def _df_to_rows(symbol: str, df: pd.DataFrame, tenant_id: str) -> list[tuple]:
    """Convert a yfinance DataFrame to price_bars INSERT tuples."""
    rows = []
    seen_dates: set[str] = set()
    for ts, row in df.iterrows():
        try:
            timestamp_iso = _normalize_ts(ts)
            if timestamp_iso in seen_dates:
                continue  # skip intraday duplicates within same batch
            seen_dates.add(timestamp_iso)

            def _f(val) -> float | None:
                if val is None:
                    return None
                try:
                    f = float(val)
                    return None if (f != f) else f  # NaN check
                except Exception:
                    return None

            o = _f(row.get("Open"))
            h = _f(row.get("High"))
            lo = _f(row.get("Low"))
            c = _f(row.get("Close"))
            v = _f(row.get("Volume")) or 0.0

            # Skip rows with any missing OHLC or zero/negative close
            if c is None or c <= 0 or o is None or h is None or lo is None:
                continue

            rows.append((tenant_id, symbol, TIMEFRAME, timestamp_iso, o, h, lo, c, v))
        except Exception:
            continue
    return rows


def _write_bars(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT OR REPLACE INTO price_bars
          (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def run_download(
    *,
    db_path: Path = DB_PATH,
    tenant_id: str = TENANT_ID,
    symbols: list[str] | None = None,
    force_days: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")

    all_symbols = symbols or _get_symbols(conn)
    print(f"Symbols to update: {len(all_symbols)}")

    last_dates = _get_last_bar_dates(conn, all_symbols, tenant_id)
    today = date.today()

    # Group symbols by their fetch start date
    # (symbols with the same last-bar date can share a batch fetch)
    if force_days is not None:
        fetch_start_global = today - timedelta(days=int(force_days))
        fetch_groups: dict[str, list[str]] = {fetch_start_global.isoformat(): all_symbols}
    else:
        by_start: dict[str, list[str]] = {}
        for sym in all_symbols:
            last = last_dates.get(sym)
            if last is None:
                # No existing bars — fetch DEFAULT_LOOKBACK days
                start = today - timedelta(days=DEFAULT_LOOKBACK)
            else:
                # Fetch from day after last bar, minimum MIN_LOOKBACK back
                incremental_start = last + timedelta(days=1)
                min_start = today - timedelta(days=MIN_LOOKBACK)
                start = min(incremental_start, min_start)

            if start > today:
                continue  # already current

            key = start.isoformat()
            by_start.setdefault(key, []).append(sym)

        fetch_groups = by_start

    if not fetch_groups:
        print("All symbols are current. Nothing to download.")
        conn.close()
        return {"downloaded": 0, "written": 0}

    total_written = 0
    total_errors = 0

    for start_str, group_symbols in sorted(fetch_groups.items()):
        start = date.fromisoformat(start_str)
        print(f"\nFetch window {start} -> {today}  ({len(group_symbols)} symbols)")

        if dry_run:
            print(f"  [DRY RUN] would download {len(group_symbols)} symbols")
            continue

        # Process in chunks to respect yfinance limits
        for chunk_start in range(0, len(group_symbols), CHUNK_SIZE):
            chunk = group_symbols[chunk_start : chunk_start + CHUNK_SIZE]
            chunk_num = chunk_start // CHUNK_SIZE + 1
            total_chunks = (len(group_symbols) + CHUNK_SIZE - 1) // CHUNK_SIZE

            print(f"  chunk {chunk_num}/{total_chunks} ({len(chunk)} symbols) ... ", end="", flush=True)

            batch_data = _fetch_batch(chunk, start, today)
            hits = len(batch_data)
            misses = len(chunk) - hits

            chunk_rows: list[tuple] = []
            for sym, df in batch_data.items():
                chunk_rows.extend(_df_to_rows(sym, df, tenant_id))

            written = _write_bars(conn, chunk_rows)
            total_written += written
            total_errors += misses

            print(f"got data for {hits}/{len(chunk)}  wrote {written} bars")

    conn.close()

    print(f"\nDone.  {total_written:,} bars written  {total_errors} symbols with no data")
    return {"downloaded": total_written, "errors": total_errors}


def main() -> int:
    p = argparse.ArgumentParser(description="Daily price bar downloader")
    p.add_argument("--db", default=str(DB_PATH))
    p.add_argument("--tenant-id", default=TENANT_ID)
    p.add_argument("--days", type=int, default=None, help="Force fetch last N days for all symbols")
    p.add_argument("--symbols", default=None, help="Comma-separated list of specific symbols")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None

    run_download(
        db_path=Path(args.db),
        tenant_id=args.tenant_id,
        symbols=syms,
        force_days=args.days,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
