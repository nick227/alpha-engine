#!/usr/bin/env python
"""
Fetch historical earnings surprises via yfinance.

Writes one JSON file per ticker to data/raw_dumps/earnings_surprises/.
Resumable: skips tickers that already have a file (use --force to overwrite).

Qualifying universe: Q3-Q5 by avg dollar-volume (same as scanner.py).

Output format per file (list of dicts, matches FMP stable/earnings schema):
  [{date, symbol, epsActual, epsEstimated}, ...]

yfinance returns roughly 100 quarters per ticker going back to 2001.
Speed: ~2s/ticker; 500 tickers ~17 min; full Q3+ universe (~1500) ~50 min.

Usage:
    python scripts/fetch_earnings_surprises.py
    python scripts/fetch_earnings_surprises.py --max-tickers 500
    python scripts/fetch_earnings_surprises.py --force
    python scripts/fetch_earnings_surprises.py --limit 60  # quarters per ticker
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import yfinance as yf

_ROOT = Path(__file__).resolve().parent.parent
DB = str(_ROOT / "data" / "alpha.db")
OUT_DIR = _ROOT / "data" / "raw_dumps" / "earnings_surprises"

# Minimum quarters of data to consider a ticker worthwhile
MIN_QUARTERS = 4

# yfinance request limit (quarters to fetch per ticker)
DEFAULT_LIMIT = 100


def _build_size_quintiles(conn: sqlite3.Connection) -> dict[str, str]:
    """
    Compute avg dollar-volume (last 252 1d bars) per ticker in the ml_train universe.
    Returns {ticker: 'Q1 micro' | ... | 'Q5 mega'}.
    """
    rows = conn.execute("""
        SELECT ticker, AVG(close * volume) as avg_dv
        FROM (
            SELECT ticker, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
            FROM price_bars
            WHERE tenant_id='ml_train' AND timeframe='1d'
        ) WHERE rn <= 252
        GROUP BY ticker
        HAVING avg_dv > 0
    """).fetchall()

    size_map = {r[0]: float(r[1]) for r in rows}
    vals = np.array(list(size_map.values()))
    if len(vals) == 0:
        return {}
    pcts = np.percentile(vals, [20, 40, 60, 80])

    labels = {}
    for ticker, dv in size_map.items():
        if dv < pcts[0]:
            labels[ticker] = "Q1 micro"
        elif dv < pcts[1]:
            labels[ticker] = "Q2 small"
        elif dv < pcts[2]:
            labels[ticker] = "Q3 mid"
        elif dv < pcts[3]:
            labels[ticker] = "Q4 large"
        else:
            labels[ticker] = "Q5 mega"
    return labels


def _fetch_surprises(ticker: str, limit: int = DEFAULT_LIMIT) -> list[dict] | None:
    """
    Fetch earnings_dates for ticker via yfinance.

    Returns list of dicts with keys: date, symbol, epsActual, epsEstimated.
    Returns None on exception, empty list if no data.
    Records with missing Reported EPS (future estimates) are excluded.
    """
    try:
        t = yf.Ticker(ticker)
        df = t.get_earnings_dates(limit=limit)
        if df is None or df.empty:
            return []

        records = []
        for ts, row in df.iterrows():
            actual = row.get("Reported EPS")
            estimate = row.get("EPS Estimate")
            # Skip future events and rows with no data
            if actual is None or (hasattr(actual, "__class__") and str(actual) in ("nan", "NaT")):
                continue
            try:
                actual_f = float(actual)
                estimate_f = float(estimate) if estimate is not None else None
            except (TypeError, ValueError):
                continue
            if estimate_f is None or str(estimate_f) == "nan":
                continue
            import math
            if math.isnan(actual_f) or math.isnan(estimate_f):
                continue

            # Convert timezone-aware timestamp to plain date string
            date_str = str(ts)[:10]

            records.append({
                "date": date_str,
                "symbol": ticker,
                "epsActual": round(actual_f, 4),
                "epsEstimated": round(estimate_f, 4),
            })

        return records
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch earnings surprises via yfinance")
    parser.add_argument("--max-tickers", type=int, default=0,
                        help="Stop after N tickers (0 = all)")
    parser.add_argument("--min-quintile", default="Q3",
                        choices=["Q1", "Q2", "Q3", "Q4", "Q5"],
                        help="Minimum size quintile to include (default: Q3)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Quarters to fetch per ticker (default: {DEFAULT_LIMIT})")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Extra seconds between requests (default 1.5; use 3.0 if hitting rate limits)")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if output file exists")
    parser.add_argument("--db", default=DB, help="Path to alpha.db")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    print("Computing size quintiles from price_bars...")
    quintiles = _build_size_quintiles(conn)
    conn.close()

    q_rank = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5": 5}
    min_rank = q_rank[args.min_quintile]
    qualifying = sorted(
        t for t, q in quintiles.items()
        if q_rank.get(q.split()[0], 0) >= min_rank
    )

    print(f"Universe: {len(qualifying)} tickers >= {args.min_quintile} by dollar-volume")
    if args.max_tickers:
        qualifying = qualifying[: args.max_tickers]
        print(f"Capped at {args.max_tickers} tickers")

    n = len(qualifying)
    success = skipped = empty = failed = 0
    consecutive_failures = 0
    BACKOFF_THRESHOLD = 5    # failures before sleeping
    BACKOFF_SLEEP = 120      # seconds to sleep on rate-limit detection

    for i, ticker in enumerate(qualifying, 1):
        out_file = OUT_DIR / f"{ticker}.json"

        if out_file.exists() and not args.force:
            skipped += 1
            consecutive_failures = 0
            if i % 200 == 0:
                print(f"  [{i}/{n}] {ticker}: cached (skipped={skipped})")
            continue

        data = _fetch_surprises(ticker, limit=args.limit)

        if args.delay > 0:
            time.sleep(args.delay)

        if data is None:
            failed += 1
            consecutive_failures += 1
            print(f"  [{i}/{n}] {ticker}: FETCH ERROR (total failed={failed})")
            # Rate-limit backoff: sleep 2 minutes after 5 consecutive failures
            if consecutive_failures == BACKOFF_THRESHOLD:
                print(f"  [{i}/{n}] {BACKOFF_THRESHOLD} consecutive failures — rate limit detected, "
                      f"sleeping {BACKOFF_SLEEP}s...")
                time.sleep(BACKOFF_SLEEP)
                consecutive_failures = 0
            continue

        consecutive_failures = 0

        if len(data) < MIN_QUARTERS:
            empty += 1
            out_file.write_text("[]")
            continue

        out_file.write_text(json.dumps(data, separators=(",", ":")))
        success += 1

        if i % 50 == 0 or i == n:
            pct = i / n * 100
            rate_s = (2.0 + args.delay)  # rough estimate
            eta_s = (n - i) * rate_s
            print(f"  [{i}/{n} {pct:.0f}%] fetched={success} skipped={skipped} "
                  f"empty={empty} failed={failed}  ETA~{eta_s/60:.0f}m")

    print()
    print(f"Done. fetched={success}, skipped={skipped}, empty={empty}, failed={failed}")
    print(f"Output: {OUT_DIR}")
    return 0 if success + skipped > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
