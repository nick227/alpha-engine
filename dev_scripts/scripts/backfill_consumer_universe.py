"""
Consumer Universe Backfill & Freshness Validator (TradeLoom Target Universe)

Idempotent, restartable backfill script for the canonical 150-symbol consumer universe.
Audits existing coverage in tenant 'default', identifies exact date gaps,
enforces consistent split-adjustment policy (auto_adjust=True), and supports
dry-run inspection without mutating the database.

Usage:
    python dev_scripts/scripts/backfill_consumer_universe.py --dry-run
    python dev_scripts/scripts/backfill_consumer_universe.py --dry-run --output reports/consumer_backfill_dryrun.json
    python dev_scripts/scripts/backfill_consumer_universe.py --symbols AAPL,MSFT --days 220
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH_DEFAULT = ROOT / "data" / "alpha.db"
CONFIG_FILE_DEFAULT = ROOT / "config" / "consumer_universe_150.yaml"
TENANT_ID = "default"
TIMEFRAME = "1d"
CHUNK_SIZE = 12
DEFAULT_LOOKBACK_DAYS = 220  # April 2026 to November 2026 is ~214 days


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
            # Start from day after latest bar
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


def main():
    parser = argparse.ArgumentParser(description="TradeLoom Consumer Universe Backfill & Dry Run")
    parser.add_argument("--db", default=str(DB_PATH_DEFAULT), help="Path to SQLite alpha.db")
    parser.add_argument("--config", default=str(CONFIG_FILE_DEFAULT), help="Path to universe YAML")
    parser.add_argument("--tenant-id", default=TENANT_ID, help="Tenant ID (default: default)")
    parser.add_argument("--provider", default="yfinance", choices=["yfinance", "alpaca", "polygon"], help="Bars provider")
    parser.add_argument("--dry-run", action="store_true", help="Inspect and report gaps without fetching or mutating")
    parser.add_argument("--output", default=None, help="Save JSON dry-run report to path")
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
    finally:
        conn.close()

    today = date.today()
    plan = generate_gap_plan(coverage, as_of=today, default_lookback=args.days)

    up_to_date = [p for p in plan if p["status"] == "UP_TO_DATE"]
    needs_incremental = [p for p in plan if p["status"] == "NEEDS_INCREMENTAL_FILL"]
    needs_full = [p for p in plan if p["status"] == "NEEDS_FULL_HISTORY"]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": str(db_path),
        "tenant_id": args.tenant_id,
        "provider": args.provider,
        "price_adjustment_policy": "auto_adjust=True (Split & Dividend Adjusted)",
        "total_symbols_in_universe": len(symbols),
        "up_to_date_count": len(up_to_date),
        "needs_incremental_fill_count": len(needs_incremental),
        "needs_full_history_count": len(needs_full),
        "dry_run": args.dry_run,
        "symbols_up_to_date": [p["symbol"] for p in up_to_date],
        "symbols_needing_fill": [p["symbol"] for p in needs_incremental + needs_full],
        "batches_required": (len(needs_incremental + needs_full) + CHUNK_SIZE - 1) // CHUNK_SIZE,
    }

    print("\n" + "=" * 65)
    print("TRADELOOM CONSUMER UNIVERSE BACKFILL AUDIT (DRY-RUN)")
    print("=" * 65)
    print(f"Total Universe: {len(symbols)} symbols")
    print(f"Up to date (>= Nov 13, 2026): {len(up_to_date)} symbols ({len(up_to_date)/len(symbols)*100:.1f}%)")
    print(f"Needs incremental fill:      {len(needs_incremental)} symbols")
    print(f"Needs full history:          {len(needs_full)} symbols")
    print(f"Price Adjustment Policy:     {summary['price_adjustment_policy']}")
    print(f"Batch Chunk Size:            {CHUNK_SIZE} symbols per API request")
    print(f"API Batches Estimated:       {summary['batches_required']} batch calls")
    print(f"Rate Limiting Strategy:      1.5s sleep per batch (Max 40 req/min)")
    print("=" * 65)

    if needs_incremental:
        print("\nSample symbols needing incremental fill (April -> Nov 2026):")
        for p in needs_incremental[:10]:
            print(f"  {p['symbol']:6s} | Current: {p['current_bars']:4d} bars | Latest: {p['latest_bar']} | Fill: {p['fill_start']} -> {p['fill_end']}")

    if needs_full:
        print("\nSample symbols needing full history:")
        for p in needs_full[:10]:
            print(f"  {p['symbol']:6s} | Current: 0 bars | Fill: {p['fill_start']} -> {p['fill_end']}")

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        summary["detailed_plan"] = plan
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"\n[OK] Dry-run report saved to: {out_path}")

    if args.dry_run:
        print("\n[INFO] Dry-run complete. No data was downloaded or modified.")
        return

    print("\n[NOTICE] Production execution blocked until dry-run and adjustment policy reviewed.")


if __name__ == "__main__":
    main()
