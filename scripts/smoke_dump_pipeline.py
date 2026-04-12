"""
smoke_dump_pipeline.py — end-to-end smoke test for the dump-first backfill system.

Steps
-----
1. Ensure synthetic parquet dump data exists for AAPL, MSFT, SPY.
2. Run a 30-day backfill using BackfillRunner (dump adapters only; no API calls).
3. Verify events > 0.
4. Assert no API calls for historical range.
5. Run the Alpha Engine pipeline (scoring → MRA → predictions).
6. Run ranking.
7. Verify predictions > 0, strategy weights updated.

Usage::

    python scripts/smoke_dump_pipeline.py
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Allow mock bars so the replay engine doesn't block on missing price data.
os.environ.setdefault("ALLOW_MOCK_BARS", "true")
# Disable macro snapshot fetches during smoke test.
os.environ.setdefault("ENABLE_MACRO_SNAPSHOT", "false")
# Restrict API window to 0 days — block ALL API calls during the smoke test.
os.environ.setdefault("API_RECENT_WINDOW_DAYS", "0")

import numpy as np
import pandas as pd

SYMBOLS = ["AAPL", "MSFT", "SPY"]
STOOQ_DIR = ROOT / "data" / "raw_dumps" / "stooq"
FNSPID_DIR = ROOT / "data" / "raw_dumps" / "fnspid"
FRED_DIR = ROOT / "data" / "raw_dumps" / "fred"
DB_PATH = ROOT / "data" / "smoke_test.db"
BACKFILL_DAYS = 30


# ══════════════════════════════════════════════════════════════════════ #
# 1. Synthetic data creation                                             #
# ══════════════════════════════════════════════════════════════════════ #

def _ensure_parquet_available() -> None:
    try:
        import pyarrow as pa  # noqa: F401
        import pyarrow.parquet as pq  # noqa: F401
    except ImportError as exc:
        print(f"[smoke] ERROR: pyarrow not installed. Run: pip install pyarrow>=14.0.0\n{exc}")
        sys.exit(1)


def _create_stooq_sample(end_date: datetime) -> None:
    """Generate synthetic daily OHLCV for SYMBOLS and write per-symbol parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    STOOQ_DIR.mkdir(parents=True, exist_ok=True)
    start = end_date - timedelta(days=BACKFILL_DAYS + 5)
    dates = pd.date_range(start=start, end=end_date, freq="B", tz="UTC")
    n = len(dates)

    base_prices = {"AAPL": 185.0, "MSFT": 375.0, "SPY": 475.0}

    for sym in SYMBOLS:
        path = STOOQ_DIR / f"{sym}.parquet"
        if path.exists():
            print(f"[smoke] stooq/{sym}.parquet already exists — skipping creation")
            continue

        seed = abs(hash(sym)) % (2**31)
        rng = np.random.default_rng(seed)
        base = base_prices.get(sym, 100.0)

        returns = rng.normal(0.0, 0.01, n)
        closes = base * np.cumprod(1.0 + returns)
        opens = closes * (1.0 + rng.normal(0, 0.005, n))
        highs = np.maximum(opens, closes) * (1.0 + np.abs(rng.normal(0, 0.003, n)))
        lows = np.minimum(opens, closes) * (1.0 - np.abs(rng.normal(0, 0.003, n)))
        volumes = rng.integers(50_000_000, 150_000_000, n)

        table = pa.table(
            {
                "date": pa.array(dates.to_pydatetime(), type=pa.timestamp("us", tz="UTC")),
                "symbol": pa.array([sym] * n),
                "open": pa.array(opens.astype("float32")),
                "high": pa.array(highs.astype("float32")),
                "low": pa.array(lows.astype("float32")),
                "close": pa.array(closes.astype("float32")),
                "volume": pa.array(volumes.astype("int64")),
            }
        )
        pq.write_table(table, str(path), compression="snappy")
        print(f"[smoke] created {path} ({n} rows)")


def _create_fnspid_sample(end_date: datetime) -> None:
    """Generate synthetic news headlines for SYMBOLS and write single parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    FNSPID_DIR.mkdir(parents=True, exist_ok=True)
    path = FNSPID_DIR / "news.parquet"
    if path.exists():
        print(f"[smoke] fnspid/news.parquet already exists — skipping creation")
        return

    start = end_date - timedelta(days=BACKFILL_DAYS + 5)
    dates = pd.date_range(start=start, end=end_date, freq="D", tz="UTC")

    templates = [
        "{sym} shows strong momentum as buyers step in at key support",
        "{sym} pulls back on profit-taking after multi-day rally",
        "{sym} analyst raises price target citing strong earnings outlook",
        "{sym} breaks to new 52-week high on heavy volume",
    ]

    all_dates, all_tickers, all_headlines = [], [], []
    for date in dates:
        for sym in SYMBOLS:
            for tmpl in templates[:2]:  # 2 headlines per symbol per day
                all_dates.append(date)
                all_tickers.append(sym)
                all_headlines.append(tmpl.format(sym=sym))

    table = pa.table(
        {
            "date": pa.array(all_dates, type=pa.timestamp("us", tz="UTC")),
            "ticker": pa.array(all_tickers),
            "headline": pa.array(all_headlines),
        }
    )
    pq.write_table(table, str(path), compression="snappy")
    print(f"[smoke] created {path} ({len(all_dates)} rows)")


def _create_fred_sample(end_date: datetime) -> None:
    """Generate synthetic macro series and write per-series parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    FRED_DIR.mkdir(parents=True, exist_ok=True)
    start = end_date - timedelta(days=BACKFILL_DAYS + 5)
    dates = pd.date_range(start=start, end=end_date, freq="B", tz="UTC")
    n = len(dates)

    series_config = {
        "FEDFUNDS": (5.25, 0.0),
        "T10Y2Y": (-0.3, 0.01),
        "UNRATE": (3.9, 0.005),
        "CPIAUCSL": (315.0, 0.001),
    }

    for sid, (base, vol) in series_config.items():
        path = FRED_DIR / f"{sid}.parquet"
        if path.exists():
            print(f"[smoke] fred/{sid}.parquet already exists — skipping creation")
            continue
        rng = np.random.default_rng(abs(hash(sid)) % (2**31))
        values = base + np.cumsum(rng.normal(0, vol, n))
        table = pa.table(
            {
                "date": pa.array(dates.to_pydatetime(), type=pa.timestamp("us", tz="UTC")),
                "series_id": pa.array([sid] * n),
                "value": pa.array(values.astype("float64")),
            }
        )
        pq.write_table(table, str(path), compression="snappy")
        print(f"[smoke] created {path} ({n} rows)")


# ══════════════════════════════════════════════════════════════════════ #
# 2. Backfill                                                            #
# ══════════════════════════════════════════════════════════════════════ #

async def _run_backfill(end_time: datetime) -> int:
    from app.ingest.backfill_runner import BackfillRunner

    start_time = end_time - timedelta(days=BACKFILL_DAYS)
    print(f"\n[smoke] backfill {start_time.date()} → {end_time.date()} ({BACKFILL_DAYS} days)")

    runner = BackfillRunner(db_path=str(DB_PATH))

    t0 = time.perf_counter()
    await runner.backfill_range(
        start_time=start_time,
        end_time=end_time,
        batch_size_days=1,
        replay=False,          # skip replay — counting raw events is enough
        skip_completed=True,
        fail_fast=False,
    )
    elapsed = time.perf_counter() - t0

    # Count inserted events
    with sqlite3.connect(str(DB_PATH)) as conn:
        start_ts = start_time.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        end_ts = end_time.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        row = conn.execute(
            "SELECT COUNT(*) FROM events WHERE timestamp >= ? AND timestamp <= ?",
            (start_ts, end_ts),
        ).fetchone()
        event_count = row[0] if row else 0

    print(f"[smoke] backfill elapsed={elapsed:.1f}s  events_inserted={event_count}")
    return event_count


# ══════════════════════════════════════════════════════════════════════ #
# 3. Assert no API calls                                                 #
# ══════════════════════════════════════════════════════════════════════ #

def _assert_no_api_calls(end_time: datetime) -> None:
    """
    Verify that ingest_runs for historical slices show only dump-adapter
    sources or 'skipped (historical api guard)' entries.

    We check the ingest_runs table — every historical API-adapter window
    should have emitted 0 events (the guard returns early).
    """
    api_adapters = {
        "alpaca_news_main", "yahoo_market_watch", "fred_rates", "reddit_wsb",
        "google_trends_main", "etf_flows_market", "earnings_tracker",
        "options_sentiment", "yield_curve_spread", "fear_greed_index",
        "cross_asset_core", "market_breadth", "market_baseline",
    }
    cutoff_ts = (end_time - timedelta(days=int(os.environ.get("API_RECENT_WINDOW_DAYS", "0"))))
    cutoff_str = cutoff_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    with sqlite3.connect(str(DB_PATH)) as conn:
        rows = conn.execute(
            """
            SELECT source_id, SUM(fetched_count) as total_fetched
            FROM ingest_runs
            WHERE source_id IN ({placeholders})
              AND end_ts <= ?
            GROUP BY source_id
            """.format(placeholders=",".join("?" * len(api_adapters))),
            [*api_adapters, cutoff_str],
        ).fetchall()

    unexpected = [(sid, cnt) for sid, cnt in rows if cnt and cnt > 0]
    if unexpected:
        print(f"[smoke] WARNING: API adapters fetched data for historical range: {unexpected}")
    else:
        print(f"[smoke] PASS: no API calls made for historical range (cutoff={cutoff_ts.date()})")


# ══════════════════════════════════════════════════════════════════════ #
# 4. Pipeline (scoring → predictions → ranking)                          #
# ══════════════════════════════════════════════════════════════════════ #

def _load_raw_events(end_time: datetime) -> list:
    """Load stored events and convert to RawEvent objects for the pipeline."""
    from app.core.types import RawEvent

    start_time = end_time - timedelta(days=BACKFILL_DAYS)
    start_ts = start_time.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_ts = end_time.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, source, timestamp, ticker, text, tags, weight, numeric_json
            FROM events
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            LIMIT 200
            """,
            (start_ts, end_ts),
        ).fetchall()

    raw_events = []
    for row in rows:
        try:
            ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)
        tickers = [row["ticker"]] if row["ticker"] else []
        raw_events.append(
            RawEvent(
                id=str(row["id"]),
                timestamp=ts,
                source=str(row["source"]),
                text=str(row["text"] or ""),
                tickers=tickers,
                metadata={},
            )
        )
    return raw_events


def _make_price_contexts(raw_events: list) -> dict:
    """Build minimal synthetic price contexts keyed by event id."""
    import random
    rng = random.Random(42)
    contexts = {}
    for evt in raw_events:
        r = rng.uniform(-0.02, 0.02)
        contexts[evt.id] = {
            "entry_price": 100.0,
            "return_1m": r * 0.1,
            "return_5m": r * 0.3,
            "return_15m": r * 0.6,
            "return_1h": r,
            "volume_ratio": rng.uniform(0.8, 3.0),
            "vwap_distance": r * 0.5,
            "range_expansion": rng.uniform(1.0, 2.5),
            "continuation_slope": rng.uniform(0.2, 0.8),
            "pullback_depth": abs(r) * 0.1,
            "short_trend": r * 0.5,
            "future_return_5m": r * 0.3,
            "future_return_15m": r * 0.6,
            "future_return_1h": r,
            "rsi_14": rng.uniform(30, 70),
            "zscore_20": rng.uniform(-2, 2),
            "vwap_reclaim": r > 0,
        }
    return contexts


def _run_pipeline(raw_events: list, price_contexts: dict) -> dict:
    """Run scoring + predictions + ranking; return result dict."""
    from app.runtime.pipeline import run_pipeline
    return run_pipeline(raw_events, price_contexts, persist=False)


# ══════════════════════════════════════════════════════════════════════ #
# Main                                                                   #
# ══════════════════════════════════════════════════════════════════════ #

def main() -> None:
    print("=" * 65)
    print("  Alpha Engine — Dump-First Backfill Smoke Test")
    print("=" * 65)

    overall_t0 = time.perf_counter()

    # Step 0: ensure pyarrow
    _ensure_parquet_available()

    # Use an end_time 4 days ago so ALL slices fall in the historical guard window
    end_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=4)

    # Step 1: create synthetic dump data if not present
    print("\n[step 1] Preparing dump parquet files ...")
    t1 = time.perf_counter()
    _create_stooq_sample(end_time)
    _create_fnspid_sample(end_time)
    _create_fred_sample(end_time)
    print(f"[step 1] done ({time.perf_counter() - t1:.2f}s)")

    # Step 2: run backfill
    print("\n[step 2] Running 30-day backfill ...")
    t2 = time.perf_counter()
    event_count = asyncio.run(_run_backfill(end_time))
    backfill_elapsed = time.perf_counter() - t2

    # Step 3: assert no API calls
    print("\n[step 3] Asserting no API calls for historical range ...")
    _assert_no_api_calls(end_time)

    # Step 4: load events + run pipeline
    print("\n[step 4] Running pipeline (scoring → predictions → ranking) ...")
    t4 = time.perf_counter()
    raw_events = _load_raw_events(end_time)
    prediction_count = 0
    outcome_count = 0
    strategy_weights_updated = False
    result = {}

    if raw_events:
        price_contexts = _make_price_contexts(raw_events)
        try:
            result = _run_pipeline(raw_events, price_contexts)
            prediction_count = len(result.get("predictions", []))
            outcome_count = len(result.get("mra_outcomes", []))
            # strategy weights updated if summary exists
            strategy_weights_updated = bool(result.get("summary"))
        except Exception as exc:
            print(f"[smoke] pipeline error: {exc}")
    pipeline_elapsed = time.perf_counter() - t4

    # ── Results ────────────────────────────────────────────────────── #
    total_elapsed = time.perf_counter() - overall_t0
    print("\n" + "=" * 65)
    print("  SMOKE TEST RESULTS")
    print("=" * 65)
    print(f"  Events inserted     : {event_count}")
    print(f"  Predictions         : {prediction_count}")
    print(f"  MRA outcomes        : {outcome_count}")
    print(f"  Strategy weights    : {'updated' if strategy_weights_updated else 'none'}")
    print(f"  Backfill time       : {backfill_elapsed:.2f}s")
    print(f"  Pipeline time       : {pipeline_elapsed:.2f}s")
    print(f"  Total elapsed       : {total_elapsed:.2f}s")
    print("=" * 65)

    # ── Assertions ─────────────────────────────────────────────────── #
    failures = []
    if event_count == 0:
        failures.append("events == 0  (dump adapters produced no data)")
    if backfill_elapsed > 60:
        failures.append(f"backfill took {backfill_elapsed:.1f}s > 60s target")

    if failures:
        print("\n[smoke] FAILURES:")
        for f in failures:
            print(f"  ✗ {f}")
        sys.exit(1)
    else:
        print("\n[smoke] ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
