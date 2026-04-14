#!/usr/bin/env python
"""
Bulk historical data loader.

Phase 1 — CSV ingest: loads all full_history/*.csv into price_bars (ml_train tenant).
           Skips files already fully loaded. Uses adj-close as close price.
           ~7,600 symbols, ~15M rows, pure disk I/O (~10-20 min).

Phase 2 — yfinance top-up: extends every symbol from its last bar to today.
           Batches 50 symbols per download call to minimize API round-trips.
           Targets symbols with last_bar < cutoff (default 2024-01-01).
           Writes to ml_train only.

Phase 3 — Summary report.

Usage:
    python scripts/bulk_load_history.py               # full run
    python scripts/bulk_load_history.py --csv-only    # skip yfinance
    python scripts/bulk_load_history.py --topup-only  # skip CSV phase
    python scripts/bulk_load_history.py --min-kb 200  # only files > 200 KB
    python scripts/bulk_load_history.py --dry-run     # audit only, no writes
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

DB_PATH   = _ROOT / "data" / "alpha.db"
FULL_HIST = _ROOT / "data" / "raw_dumps" / "full_history"
TENANT    = "ml_train"

# yfinance top-up: extend symbols whose last bar is before this date
TOPUP_CUTOFF = date(2024, 1, 1)
# Batch size for yfinance multi-ticker download
YF_BATCH = 50
# Polite sleep between yfinance batches (seconds)
YF_SLEEP  = 1.5


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _last_bars(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str]:
    """Return {ticker: last_date_str} for tickers that have bars in ml_train."""
    if not tickers:
        return {}
    ph = ",".join("?" * len(tickers))
    rows = conn.execute(
        f"SELECT ticker, MAX(DATE(timestamp)) FROM price_bars "
        f"WHERE tenant_id=? AND timeframe='1d' AND ticker IN ({ph}) "
        f"GROUP BY ticker",
        [TENANT] + tickers,
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _insert_df(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> int:
    """Insert rows from a normalised DataFrame (columns: date, open, high, low, close, volume)."""
    if df.empty:
        return 0
    rows = []
    for _, r in df.iterrows():
        try:
            rows.append((
                TENANT, ticker, "1d",
                str(r["date"])[:10],
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
            ))
        except (ValueError, TypeError):
            continue
    if not rows:
        return 0
    conn.executemany(
        "INSERT OR IGNORE INTO price_bars "
        "(tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    return len(rows)


def _tag(conn: sqlite3.Connection, ticker: str, source: str, asset_type: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO ticker_metadata (ticker, source, asset_type) VALUES (?,?,?)",
        (ticker, source, asset_type),
    )


def _ensure_metadata_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_metadata (
            ticker       TEXT NOT NULL,
            source       TEXT NOT NULL,
            asset_type   TEXT NOT NULL,
            description  TEXT,
            loaded_at    TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (ticker, source)
        )
    """)


# ---------------------------------------------------------------------------
# Phase 1 — CSV ingest
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception:
        return None
    df.columns = [c.strip().lower() for c in df.columns]
    # Normalise date column
    date_col = next((c for c in df.columns if c in ("date", "datetime", "time")), None)
    if date_col is None:
        return None
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    # Prefer adj close if present
    if "adj close" in df.columns:
        df["close"] = df["adj close"]
    elif "adjusted_close" in df.columns:
        df["close"] = df["adjusted_close"]
    needed = {"date", "open", "high", "low", "close", "volume"}
    if not needed.issubset(df.columns):
        return None
    df = df[list(needed)].copy()
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["close", "date"]).sort_values("date")


def phase1_csv(conn: sqlite3.Connection, min_kb: int, dry: bool) -> tuple[int, int]:
    """Load all CSVs >= min_kb. Returns (files_loaded, rows_inserted)."""
    csvs = sorted(f for f in FULL_HIST.glob("*.csv") if f.stat().st_size >= min_kb * 1024)
    print(f"Phase 1 — CSV ingest: {len(csvs)} files >= {min_kb} KB")

    # Which tickers are already fully loaded? (last bar >= 2023-12-28)
    all_tickers = [f.stem for f in csvs]
    last = _last_bars(conn, all_tickers)
    already_full = {t for t, d in last.items() if d >= "2023-12-28"}
    to_load = [f for f in csvs if f.stem not in already_full]
    print(f"  Already fully loaded: {len(already_full)}  To load: {len(to_load)}")

    files_done = 0
    rows_total = 0
    commit_every = 200

    for i, path in enumerate(to_load, 1):
        ticker = path.stem
        df = _read_csv(path)
        if df is None or df.empty:
            continue
        if not dry:
            n = _insert_df(conn, ticker, df)
            _tag(conn, ticker, "full_history", "equity")
            rows_total += n
            if i % commit_every == 0:
                conn.commit()
        files_done += 1
        if i % 500 == 0 or i == len(to_load):
            print(f"  [{i}/{len(to_load)}] {ticker}  rows_so_far={rows_total:,}")

    if not dry:
        conn.commit()

    print(f"  Done: {files_done} files, {rows_total:,} rows inserted")
    return files_done, rows_total


# ---------------------------------------------------------------------------
# Phase 2 — yfinance top-up
# ---------------------------------------------------------------------------

def _flatten_yf_df(raw: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """
    Normalise a yfinance DataFrame (possibly multi-level columns) into
    a clean {date, open, high, low, close, volume} frame for one ticker.
    Handles both old-style flat columns and new-style (field, ticker) tuples.
    """
    if raw is None or raw.empty:
        return None

    df = raw.reset_index()

    # Flatten multi-level column tuples: ("Close", "AAPL") → "close"
    def _flatten(c: object) -> str:
        if isinstance(c, tuple):
            return c[0].lower()
        return str(c).lower()

    df.columns = [_flatten(c) for c in df.columns]

    # Rename date variants
    for alias in ("datetime", "index", "date"):
        if alias in df.columns:
            df = df.rename(columns={alias: "date"})
            break

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    needed = {"date", "open", "high", "low", "close", "volume"}
    if not needed.issubset(df.columns):
        return None

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[list(needed)].dropna(subset=["close", "date"])
    return df if not df.empty else None


def _fetch_yf_batch(tickers: list[str], start: str) -> dict[str, pd.DataFrame]:
    """Download a batch of tickers from yfinance. Returns {ticker: df}."""
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance not installed — skipping")
        return {}

    try:
        raw = yf.download(
            tickers,
            start=start,
            end=date.today().isoformat(),
            interval="1d",
            progress=False,
            auto_adjust=True,
            group_by="ticker",
        )
    except Exception as e:
        print(f"  yfinance batch error: {e}")
        return {}

    if raw.empty:
        return {}

    results: dict[str, pd.DataFrame] = {}

    # Always treat as multi-ticker layout — yfinance uses (field, ticker) columns
    # regardless of single vs multi input in recent versions.
    if isinstance(raw.columns, pd.MultiIndex):
        top_level = raw.columns.get_level_values(0).unique().tolist()
        second_level = raw.columns.get_level_values(1).unique().tolist()

        # New yfinance: columns are (Price, Ticker) — e.g. ("Close", "AAPL")
        # Old yfinance: columns are (Ticker, Price) — e.g. ("AAPL", "Close")
        # Detect by checking if tickers appear in level 0 or level 1
        ticker_in_level1 = any(t in second_level for t in tickers)

        for t in tickers:
            try:
                if ticker_in_level1:
                    # New layout: (Price, Ticker)
                    sub = raw.xs(t, axis=1, level=1)
                else:
                    # Old layout: (Ticker, Price)
                    sub = raw[t]
                df = _flatten_yf_df(sub, t)
                if df is not None:
                    results[t] = df
            except Exception:
                continue
    else:
        # Flat columns — single ticker response
        t = tickers[0]
        df = _flatten_yf_df(raw, t)
        if df is not None:
            results[t] = df

    return results


def phase2_topup(conn: sqlite3.Connection, dry: bool) -> tuple[int, int]:
    """Top up all symbols whose last bar is before TOPUP_CUTOFF."""
    # All tickers currently in ml_train
    all_tickers = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM price_bars WHERE tenant_id=? AND timeframe='1d'",
        (TENANT,),
    ).fetchall()]

    last = _last_bars(conn, all_tickers)
    cutoff_str = TOPUP_CUTOFF.isoformat()
    needs_topup = sorted(t for t, d in last.items() if d < cutoff_str)

    # Also top up tickers from full_history not yet in DB at all
    csv_tickers = {f.stem for f in FULL_HIST.glob("*.csv")}
    in_db = set(last.keys())
    # Don't re-add these here — phase 1 handles them; phase 2 only tops up existing rows
    # But add any yfinance-only symbols that might be missing
    yf_only = {"^VIX","^VIX3M","BTC-USD","CL=F","DX-Y.NYB","GC=F","^GSPC","^TNX","^IXIC"}
    for sym in yf_only:
        if sym not in in_db or (sym in last and last[sym] < cutoff_str):
            if sym not in needs_topup:
                needs_topup.append(sym)

    print(f"\nPhase 2 — yfinance top-up: {len(needs_topup)} symbols need extension to today")
    if not needs_topup:
        return 0, 0

    # Work out earliest start needed
    start_str = (TOPUP_CUTOFF - timedelta(days=5)).isoformat()  # small overlap for safety

    batches = [needs_topup[i:i+YF_BATCH] for i in range(0, len(needs_topup), YF_BATCH)]
    print(f"  {len(batches)} batches of {YF_BATCH}")

    tickers_done = 0
    rows_total   = 0

    for bi, batch in enumerate(batches, 1):
        fetched = {} if dry else _fetch_yf_batch(batch, start_str)
        if not dry:
            for ticker, df in fetched.items():
                n = _insert_df(conn, ticker, df)
                rows_total += n
                if n > 0:
                    _tag(conn, ticker, "yfinance", "equity")
            conn.commit()
        tickers_done += len(fetched)

        pct = bi / len(batches) * 100
        print(f"  batch {bi:>4}/{len(batches)}  ({pct:.0f}%)  "
              f"tickers_updated={tickers_done}  rows_added={rows_total:,}")

        if bi < len(batches):
            time.sleep(YF_SLEEP)

    print(f"  Done: {tickers_done} symbols topped up, {rows_total:,} rows inserted")
    return tickers_done, rows_total


# ---------------------------------------------------------------------------
# Phase 3 — summary
# ---------------------------------------------------------------------------

def phase3_summary(conn: sqlite3.Connection) -> None:
    print("\nPhase 3 — Summary")
    total = conn.execute(
        "SELECT COUNT(*) FROM price_bars WHERE tenant_id=?", (TENANT,)
    ).fetchone()[0]
    tickers = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM price_bars WHERE tenant_id=?", (TENANT,)
    ).fetchone()[0]
    print(f"  ml_train: {tickers:,} tickers, {total:,} total bars")

    # By last-bar date bucket
    buckets = conn.execute("""
        SELECT
            CASE
                WHEN MAX(DATE(timestamp)) >= '2025-01-01' THEN 'current (2025+)'
                WHEN MAX(DATE(timestamp)) >= '2024-01-01' THEN '2024'
                WHEN MAX(DATE(timestamp)) >= '2023-01-01' THEN '2023'
                ELSE 'older'
            END as bucket,
            COUNT(DISTINCT ticker) as n
        FROM price_bars
        WHERE tenant_id=? AND timeframe='1d'
        GROUP BY 1 ORDER BY 1
    """, (TENANT,)).fetchall()
    print("  Last-bar freshness:")
    for b, n in buckets:
        print(f"    {b:<22}: {n:>5} tickers")

    # By history length bucket
    hist_buckets = conn.execute("""
        SELECT
            CASE
                WHEN COUNT(*) >= 5000 THEN '20+ years'
                WHEN COUNT(*) >= 2500 THEN '10-20 years'
                WHEN COUNT(*) >= 1250 THEN '5-10 years'
                WHEN COUNT(*) >= 500  THEN '2-5 years'
                ELSE '<2 years'
            END as depth,
            COUNT(DISTINCT ticker) as tickers
        FROM price_bars
        WHERE tenant_id=? AND timeframe='1d'
        GROUP BY ticker
        HAVING 1=1
    """, (TENANT,)).fetchall()
    # Re-aggregate in Python since SQLite can't group-of-groups easily
    from collections import Counter
    depth_conn = sqlite3.connect(str(DB_PATH))
    rows = depth_conn.execute("""
        SELECT ticker, COUNT(*) as n FROM price_bars
        WHERE tenant_id=? AND timeframe='1d'
        GROUP BY ticker
    """, (TENANT,)).fetchall()
    depth_conn.close()
    buckets2: Counter = Counter()
    for _, n in rows:
        if n >= 5000:    buckets2["20+ years"] += 1
        elif n >= 2500:  buckets2["10-20 years"] += 1
        elif n >= 1250:  buckets2["5-10 years"] += 1
        elif n >= 500:   buckets2["2-5 years"] += 1
        else:            buckets2["<2 years"] += 1
    print("  History depth:")
    for label in ("20+ years","10-20 years","5-10 years","2-5 years","<2 years"):
        print(f"    {label:<12}: {buckets2[label]:>5} tickers")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-only",   action="store_true", help="Skip yfinance top-up")
    parser.add_argument("--topup-only", action="store_true", help="Skip CSV phase")
    parser.add_argument("--dry-run",    action="store_true", help="Audit only, no writes")
    parser.add_argument("--min-kb",     type=int, default=100,
                        help="Minimum CSV size in KB to load (default: 100)")
    args = parser.parse_args()

    conn = _get_conn()
    _ensure_metadata_table(conn)

    t0 = time.time()

    if not args.topup_only:
        phase1_csv(conn, min_kb=args.min_kb, dry=args.dry_run)

    if not args.csv_only:
        phase2_topup(conn, dry=args.dry_run)

    phase3_summary(conn)
    conn.close()

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
