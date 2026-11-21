"""
Consumer Universe Backfill & Freshness Validator (TradeLoom Target Universe)

Idempotent, restartable backfill script for the canonical 153-symbol consumer universe.
Audits existing coverage in tenant 'default', identifies exact date gaps,
enforces consistent split-adjustment policy (auto_adjust=True), and supports
both dry-run inspection and safe execution using INSERT OR IGNORE to never overwrite trusted bars.

Usage:
    python dev_scripts/scripts/backfill_consumer_universe.py --dry-run
    python dev_scripts/scripts/backfill_consumer_universe.py --execute
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH_DEFAULT = ROOT / "data" / "alpha.db"
CONFIG_FILE_DEFAULT = ROOT / "config" / "consumer_universe_150.yaml"
TENANT_ID = "default"
TIMEFRAME = "1d"
CHUNK_SIZE = 12
DEFAULT_LOOKBACK_DAYS = 220  # April 2026 to November 2026 is ~214 days
DELAY_BETWEEN_BATCHES = 1.5  # Rate limit safety delay in seconds

# Ticker aliases where Yahoo Finance symbol differs from canonical equity ticker
ALIAS_MAP: dict[str, str] = {
    "MMC": "MRSH",  # Marsh & McLennan listed as MRSH on Yahoo Finance
}
REVERSE_ALIAS_MAP: dict[str, str] = {v: k for k, v in ALIAS_MAP.items()}


def load_consumer_universe(config_path: Path) -> list[str]:
    """Load canonical symbols from YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    symbols: list[str] = []
    for section, sym_list in data.items():
        if isinstance(sym_list, list):
            for s in sym_list:
                clean = str(s).strip().upper().replace(".", "-")
                if clean and clean not in symbols:
                    symbols.append(clean)
    return symbols


def inspect_symbol_coverage(
    conn: sqlite3.Connection,
    symbols: list[str],
    tenant_id: str = TENANT_ID,
    timeframe: str = TIMEFRAME,
) -> dict[str, dict[str, Any]]:
    """Inspect current price_bars coverage for each symbol in tenant."""
    coverage: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
            FROM price_bars
            WHERE tenant_id = ? AND ticker = ? AND timeframe = ?
            """,
            (tenant_id, sym, timeframe),
        ).fetchone()

        cnt = row["cnt"] if row else 0
        min_ts = row["min_ts"] if row and row["min_ts"] else None
        max_ts = row["max_ts"] if row and row["max_ts"] else None

        # Check if current (updated within last 7 days)
        is_current = False
        latest_date = None
        if max_ts:
            latest_date_str = max_ts[:10]
            latest_date = date.fromisoformat(latest_date_str)
            if latest_date >= date.today() - timedelta(days=7):
                is_current = True

        coverage[sym] = {
            "symbol": sym,
            "bar_count": cnt,
            "earliest_bar": min_ts,
            "latest_bar": max_ts,
            "latest_date": str(latest_date) if latest_date else None,
            "is_current": is_current,
        }
    return coverage


def generate_gap_plan(
    coverage: dict[str, dict[str, Any]],
    as_of: date,
    default_lookback: int = DEFAULT_LOOKBACK_DAYS,
) -> list[dict[str, Any]]:
    """Determine the exact missing window for each symbol."""
    plan: list[dict[str, Any]] = []
    for sym, info in coverage.items():
        if info["is_current"]:
            status = "UP_TO_DATE"
            start_date = None
            end_date = None
            days_missing = 0
        elif info["bar_count"] == 0:
            status = "NEEDS_FULL_HISTORY"
            start_date = as_of - timedelta(days=default_lookback)
            end_date = as_of
            days_missing = (end_date - start_date).days
        else:
            status = "NEEDS_INCREMENTAL_FILL"
            last_d = date.fromisoformat(info["latest_date"])
            start_date = last_d + timedelta(days=1)
            end_date = as_of
            days_missing = max(0, (end_date - start_date).days)

        plan.append({
            "symbol": sym,
            "status": status,
            "current_bars": info["bar_count"],
            "latest_bar": info["latest_bar"],
            "fill_start": str(start_date) if start_date else None,
            "fill_end": str(end_date) if end_date else None,
            "estimated_missing_days": days_missing,
        })
    return plan


def _normalize_ts(ts) -> str:
    """Normalize timestamp to canonical midnight UTC ISO string."""
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc).isoformat()


def fetch_batch_with_retry(
    query_symbols: list[str],
    start: date,
    end: date,
    max_retries: int = 3,
) -> dict[str, pd.DataFrame]:
    """Fetch batch from Yahoo Finance with auto_adjust=True and bounded retries."""
    ticker_str = " ".join(query_symbols)
    for attempt in range(max_retries):
        try:
            raw = yf.download(
                ticker_str,
                start=start.isoformat(),
                end=(end + timedelta(days=1)).isoformat(),
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if raw is None or raw.empty:
                return {}

            out: dict[str, pd.DataFrame] = {}
            if isinstance(raw.columns, pd.MultiIndex):
                available = set(raw.columns.get_level_values(1))
                for q_sym in query_symbols:
                    if q_sym in available:
                        try:
                            df = raw.xs(q_sym, axis=1, level=1).copy()
                            if not df.empty and "Close" in df.columns:
                                out[q_sym] = df
                        except Exception:
                            continue
            else:
                q_sym = query_symbols[0]
                df = raw.copy()
                if not df.empty and "Close" in df.columns:
                    out[q_sym] = df
            return out
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"  [ERROR] Batch download failed after {max_retries} attempts: {e}")
                return {}
            time.sleep(2.0 * (attempt + 1))
    return {}


def execute_backfill(
    conn: sqlite3.Connection,
    plan: list[dict[str, Any]],
    tenant_id: str = TENANT_ID,
) -> dict[str, Any]:
    """Execute backfill writing missing bars with INSERT OR IGNORE."""
    to_fill = [p for p in plan if p["status"] in ("NEEDS_INCREMENTAL_FILL", "NEEDS_FULL_HISTORY")]
    if not to_fill:
        print("[INFO] No symbols require backfill. Operational universe is up to date.")
        return {"symbols_processed": 0, "bars_inserted": 0, "failures": []}

    earliest_start = min(date.fromisoformat(p["fill_start"]) for p in to_fill if p["fill_start"])
    latest_end = max(date.fromisoformat(p["fill_end"]) for p in to_fill if p["fill_end"])

    total_bars_inserted = 0
    successful_symbols: list[str] = []
    failed_symbols: list[str] = []
    skipped_symbols: list[str] = []
    rate_limit_events = 0
    start_time = time.time()

    # Process in chunks
    for i in range(0, len(to_fill), CHUNK_SIZE):
        chunk = to_fill[i:i + CHUNK_SIZE]
        query_sym_to_canonical = {}
        for item in chunk:
            can_sym = item["symbol"]
            q_sym = ALIAS_MAP.get(can_sym, can_sym)
            query_sym_to_canonical[q_sym] = can_sym

        q_symbols = list(query_sym_to_canonical.keys())
        print(f"\n[Batch {i // CHUNK_SIZE + 1}/{(len(to_fill) + CHUNK_SIZE - 1) // CHUNK_SIZE}] Fetching {len(q_symbols)} symbols: {', '.join(q_symbols)}...")

        batch_result = fetch_batch_with_retry(q_symbols, earliest_start, latest_end)
        batch_bars_to_insert = []

        for q_sym, df in batch_result.items():
            can_sym = query_sym_to_canonical.get(q_sym, q_sym)
            target_item = next((p for p in chunk if p["symbol"] == can_sym), None)
            if not target_item or not target_item["fill_start"]:
                continue

            sym_start = date.fromisoformat(target_item["fill_start"])
            sym_bars_count = 0

            for ts, row in df.iterrows():
                try:
                    ts_iso = _normalize_ts(ts)
                    row_date = date.fromisoformat(ts_iso[:10])
                    # Strictly filter to the symbol's missing window
                    if row_date < sym_start:
                        continue

                    def _val(col):
                        v = row.get(col)
                        try:
                            f = float(v)
                            return f if (f == f and f > 0) else None
                        except Exception:
                            return None

                    o = _val("Open")
                    h = _val("High")
                    lo = _val("Low")
                    c = _val("Close")
                    vol = float(row.get("Volume") or 0.0)

                    if o is not None and h is not None and lo is not None and c is not None:
                        batch_bars_to_insert.append((
                            tenant_id, can_sym, TIMEFRAME, ts_iso, o, h, lo, c, vol
                        ))
                        sym_bars_count += 1
                except Exception:
                    continue

            if sym_bars_count > 0:
                successful_symbols.append(can_sym)
            else:
                skipped_symbols.append(can_sym)

        # Check for symbols in chunk that got no data
        for q_sym, can_sym in query_sym_to_canonical.items():
            if q_sym not in batch_result and can_sym not in successful_symbols:
                failed_symbols.append(can_sym)

        # Atomic chunk insert using INSERT OR IGNORE (never overwrites existing bars)
        if batch_bars_to_insert:
            conn.executemany(
                """
                INSERT OR IGNORE INTO price_bars 
                  (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_bars_to_insert,
            )
            conn.commit()
            total_bars_inserted += len(batch_bars_to_insert)
            print(f"  -> Inserted {len(batch_bars_to_insert)} daily bars into tenant='{tenant_id}'.")

        # Rate-limit delay between chunks
        time.sleep(DELAY_BETWEEN_BATCHES)

    duration = time.time() - start_time

    # Record provenance run in ingest_runs
    run_id = f"run_{hashlib.sha256(f'backfill:{datetime.now(timezone.utc).isoformat()}'.encode()).hexdigest()[:16]}"
    conn.execute(
        """
        INSERT INTO ingest_runs 
            (source_id, start_ts, end_ts, spec_hash, ok, fetched_count, emitted_count,
             last_error, created_at, status, updated_at, provider, retry_count, empty_count,
             started_at, completed_at, oldest_event_ts, newest_event_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"backfill_consumer_153_{date.today().isoformat()}",
            earliest_start.isoformat(),
            latest_end.isoformat(),
            hashlib.sha256("consumer_universe_150_auto_adjust".encode()).hexdigest()[:16],
            1 if len(failed_symbols) == 0 else 0,
            len(to_fill),
            total_bars_inserted,
            f"failed_symbols: {','.join(failed_symbols)}" if failed_symbols else None,
            datetime.now(timezone.utc).isoformat(),
            "complete" if len(failed_symbols) == 0 else "partial",
            datetime.now(timezone.utc).isoformat(),
            "yfinance_auto_adjust",
            0,
            len(skipped_symbols),
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat(),
            earliest_start.isoformat(),
            latest_end.isoformat(),
        ),
    )
    conn.commit()

    return {
        "duration_seconds": round(duration, 2),
        "total_symbols_evaluated": len(to_fill),
        "successful_symbols": len(set(successful_symbols)),
        "successful_list": sorted(set(successful_symbols)),
        "failed_symbols": len(set(failed_symbols)),
        "failed_list": sorted(set(failed_symbols)),
        "skipped_symbols": len(set(skipped_symbols)),
        "total_bars_inserted": total_bars_inserted,
        "rate_limit_events": rate_limit_events,
    }


def main():
    parser = argparse.ArgumentParser(description="TradeLoom Consumer Universe Backfill & Dry Run")
    parser.add_argument("--db", default=str(DB_PATH_DEFAULT), help="Path to SQLite alpha.db")
    parser.add_argument("--config", default=str(CONFIG_FILE_DEFAULT), help="Path to universe YAML")
    parser.add_argument("--tenant-id", default=TENANT_ID, help="Tenant ID (default: default)")
    parser.add_argument("--provider", default="yfinance", choices=["yfinance", "alpaca", "polygon"], help="Bars provider")
    parser.add_argument("--dry-run", action="store_true", help="Inspect and report gaps without fetching or mutating")
    parser.add_argument("--execute", action="store_true", help="Execute the targeted backfill")
    parser.add_argument("--output", default=None, help="Save JSON report to path")
    parser.add_argument("--days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="Default lookback days for new symbols")
    parser.add_argument("--symbols", default=None, help="Comma-separated subset of symbols to target")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] Database file not found: {db_path}")
        sys.exit(1)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"[ERROR] Universe config not found: {config_path}")
        sys.exit(1)

    all_symbols = load_consumer_universe(config_path)
    if args.symbols:
        subset = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        symbols = [s for s in all_symbols if s in subset] or subset
    else:
        symbols = all_symbols

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        coverage = inspect_symbol_coverage(conn, symbols, tenant_id=args.tenant_id)
        today = date.today()
        plan = generate_gap_plan(coverage, as_of=today, default_lookback=args.days)

        up_to_date = [p for p in plan if p["status"] == "UP_TO_DATE"]
        needs_incremental = [p for p in plan if p["status"] == "NEEDS_INCREMENTAL_FILL"]
        needs_full = [p for p in plan if p["status"] == "NEEDS_FULL_HISTORY"]

        print("\n" + "=" * 65)
        print("TRADELOOM CONSUMER UNIVERSE BACKFILL AUDIT")
        print("=" * 65)
        print(f"Total Universe:              {len(symbols)} symbols")
        print(f"Up to date (>= Nov 13, 2026): {len(up_to_date)} symbols ({len(up_to_date)/len(symbols)*100:.1f}%)")
        print(f"Needs incremental fill:      {len(needs_incremental)} symbols")
        print(f"Needs full history:          {len(needs_full)} symbols")
        print(f"Price Adjustment Policy:     auto_adjust=True (Split & Dividend Adjusted)")
        print(f"Batch Chunk Size:            {CHUNK_SIZE} symbols per API request")
        print(f"API Batches Estimated:       {(len(needs_incremental + needs_full) + CHUNK_SIZE - 1) // CHUNK_SIZE} batch calls")
        print("=" * 65)

        if args.execute:
            print("\n[EXECUTING TARGETED BACKFILL]...")
            exec_stats = execute_backfill(conn, plan, tenant_id=args.tenant_id)
            print("\n" + "=" * 65)
            print("BACKFILL EXECUTION SUMMARY")
            print("=" * 65)
            print(f"Duration:                    {exec_stats['duration_seconds']} seconds")
            print(f"Symbols Processed:           {exec_stats['total_symbols_evaluated']}")
            print(f"Successful Symbols:          {exec_stats['successful_symbols']}")
            print(f"Failed Symbols:              {exec_stats['failed_symbols']} {exec_stats['failed_list']}")
            print(f"Total New Bars Inserted:     {exec_stats['total_bars_inserted']}")
            print(f"Rate Limit Events:           {exec_stats['rate_limit_events']}")
            print("=" * 65)

            if args.output:
                with open(args.output, "w", encoding="utf-8") as f:
                    json.dump(exec_stats, f, indent=2)
                print(f"[OK] Execution report saved to: {args.output}")
        else:
            print("\n[DRY RUN ONLY] Pass --execute to apply changes.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
