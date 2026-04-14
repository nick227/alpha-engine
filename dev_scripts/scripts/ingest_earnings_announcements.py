#!/usr/bin/env python
"""
Earnings Announcements Nightly Ingester.

For a given date, finds tickers that reported earnings and had pre-fetched
surprise data in data/raw_dumps/earnings_surprises/, computes cross-sectional
z-scores, and runs them through the prediction pipeline to generate
EarningsDriftStrategy predictions.

Designed to run the morning after an earnings date (e.g., T+1 pre-market),
so price_context can include return_1d from the announcement day (T).

Usage:
    # Process yesterday's announcements (default):
    python scripts/ingest_earnings_announcements.py

    # Process a specific date:
    python scripts/ingest_earnings_announcements.py --date 2025-11-14

    # Dry run (print events without writing to DB):
    python scripts/ingest_earnings_announcements.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DB = str(_ROOT / "data" / "alpha.db")
DUMP_DIR = _ROOT / "data" / "raw_dumps" / "earnings_surprises"

# Minimum absolute z-score to include in the event batch
_MIN_ABS_Z = 0.3

# Entry event timestamp: 9:30 AM ET = 14:30 UTC
_ENTRY_HOUR_UTC = 14
_ENTRY_MINUTE_UTC = 30


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_size_quintiles(conn: sqlite3.Connection) -> dict[str, str]:
    """Return {ticker: 'Q3 mid'} for all tickers in price_bars (ml_train, 1d)."""
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

    labels: dict[str, str] = {}
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


def _get_announcement_day_return(conn: sqlite3.Connection, ticker: str, date_str: str) -> float | None:
    """Return the 1-day return for ticker on announcement date (open→close)."""
    row = conn.execute("""
        SELECT open, close
        FROM price_bars
        WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d'
          AND DATE(timestamp) = ?
        LIMIT 1
    """, (ticker, date_str)).fetchone()
    if not row or row[0] is None or row[1] is None or float(row[0]) == 0.0:
        return None
    return (float(row[1]) - float(row[0])) / float(row[0])


def _get_entry_price(conn: sqlite3.Connection, ticker: str, date_str: str) -> float | None:
    """Return the open of the next trading day after the announcement date."""
    row = conn.execute("""
        SELECT open
        FROM price_bars
        WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d'
          AND DATE(timestamp) > ?
        ORDER BY timestamp ASC
        LIMIT 1
    """, (ticker, date_str)).fetchone()
    if row and row[0]:
        return float(row[0])
    # Fallback: use close of announcement day
    row2 = conn.execute("""
        SELECT close
        FROM price_bars
        WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d'
          AND DATE(timestamp) = ?
        LIMIT 1
    """, (ticker, date_str)).fetchone()
    return float(row2[0]) if row2 and row2[0] else None


def _get_vix_term(conn: sqlite3.Connection, date_str: str) -> float | None:
    """Return VIX - VIX3M on or before date_str."""
    vix_row = conn.execute("""
        SELECT close FROM price_bars
        WHERE tenant_id='ml_train' AND ticker='^VIX' AND timeframe='1d'
          AND DATE(timestamp) <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (date_str,)).fetchone()
    v3m_row = conn.execute("""
        SELECT close FROM price_bars
        WHERE tenant_id='ml_train' AND ticker='^VIX3M' AND timeframe='1d'
          AND DATE(timestamp) <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (date_str,)).fetchone()
    if vix_row and v3m_row and vix_row[0] and v3m_row[0]:
        return float(vix_row[0]) - float(v3m_row[0])
    return None


# ---------------------------------------------------------------------------
# Event construction
# ---------------------------------------------------------------------------

def _next_trading_day_ts(date_str: str) -> datetime:
    """Return datetime for 9:30 AM ET the day after date_str (approximate)."""
    d = datetime.fromisoformat(date_str)
    next_d = d + timedelta(days=1)
    # Skip weekends
    while next_d.weekday() >= 5:
        next_d += timedelta(days=1)
    return next_d.replace(
        hour=_ENTRY_HOUR_UTC,
        minute=_ENTRY_MINUTE_UTC,
        second=0,
        microsecond=0,
        tzinfo=timezone.utc,
    )


# ---------------------------------------------------------------------------
# Cross-sectional z-score
# ---------------------------------------------------------------------------

def _zscores(surprises: list[float]) -> list[float]:
    """Compute cross-sectional z-scores."""
    arr = np.array(surprises, dtype=float)
    mu = np.mean(arr)
    sd = np.std(arr, ddof=1)
    if sd < 1e-9:
        return [0.0] * len(surprises)
    return list((arr - mu) / sd)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest earnings announcements for strategy prediction")
    parser.add_argument("--date", default=None, help="Announcement date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--db", default=DB, help="Path to alpha.db")
    parser.add_argument("--min-quintile", default="Q3", choices=["Q1", "Q2", "Q3", "Q4", "Q5"],
                        help="Minimum size quintile to include (default: Q3)")
    parser.add_argument("--dry-run", action="store_true", help="Print events without writing to DB")
    args = parser.parse_args()

    # Default: yesterday in UTC
    if args.date:
        target_date = str(args.date)
    else:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Earnings ingester — processing announcements for: {target_date}")

    if not DUMP_DIR.exists():
        print(f"Dump directory not found: {DUMP_DIR}")
        print("Run scripts/fetch_earnings_surprises.py first.")
        return 1

    # ------------------------------------------------------------------
    # Load size quintiles
    # ------------------------------------------------------------------
    conn = sqlite3.connect(args.db)
    quintiles = _get_size_quintiles(conn)
    q_rank = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5": 5}
    min_rank = q_rank[args.min_quintile]

    # ------------------------------------------------------------------
    # Find announcements on target date
    # ------------------------------------------------------------------
    candidates: list[dict] = []  # {ticker, eps_actual, eps_estimated, surprise_raw}
    for jf in DUMP_DIR.glob("*.json"):
        ticker = jf.stem
        try:
            records = json.loads(jf.read_text())
        except Exception:
            continue
        for rec in records:
            if rec.get("date") != target_date:
                continue
            actual = rec.get("epsActual")
            estimate = rec.get("epsEstimated")
            if actual is None or estimate is None:
                continue
            try:
                a = float(actual)
                e = float(estimate)
            except (TypeError, ValueError):
                continue
            if math.isnan(a) or math.isnan(e):
                continue
            # Surprise raw: % beat relative to |estimate| baseline
            baseline = abs(e) if abs(e) > 0.01 else 0.01
            surprise_raw = (a - e) / baseline
            if math.isnan(surprise_raw) or math.isinf(surprise_raw):
                continue
            candidates.append({
                "ticker": ticker,
                "eps_actual": a,
                "eps_estimated": e,
                "surprise_raw": surprise_raw,
            })

    print(f"Found {len(candidates)} ticker(s) with announcements on {target_date}")

    if not candidates:
        print("No announcements found — nothing to ingest.")
        conn.close()
        return 0

    # ------------------------------------------------------------------
    # Cross-sectional z-score
    # ------------------------------------------------------------------
    surprises = [c["surprise_raw"] for c in candidates]
    zs = _zscores(surprises)
    for c, z in zip(candidates, zs):
        c["surprise_z"] = round(z, 4)

    # ------------------------------------------------------------------
    # Filter by size quintile and build events
    # ------------------------------------------------------------------
    from app.core.types import RawEvent, ScoredEvent
    from app.core.scoring import score_event
    from app.core.mra import compute_mra
    from app.engine.runner import run_pipeline
    from app.db.repository import AlphaRepository

    vix_term = _get_vix_term(conn, target_date)

    events_built = 0
    raw_events: list[RawEvent] = []
    price_contexts: dict[str, dict] = {}

    for c in candidates:
        ticker = c["ticker"]
        sq = quintiles.get(ticker, "")
        sq_num = 0
        if sq and sq[0] == "Q" and sq[1:2].isdigit():
            sq_num = int(sq[1])

        if sq_num < min_rank:
            continue

        # Price context: build minimal context from DB
        return_1d = _get_announcement_day_return(conn, ticker, target_date)
        entry_price = _get_entry_price(conn, ticker, target_date)
        if entry_price is None:
            continue  # No price data — skip

        event_ts = _next_trading_day_ts(target_date)
        event_id = str(uuid4())

        # Build earnings metadata (injected into price_context by runner)
        metadata = {
            "earnings_announcement": True,
            "surprise_z": c["surprise_z"],
            "size_quintile": sq,
            "eps_actual": c["eps_actual"],
            "eps_estimated": c["eps_estimated"],
            "surprise_raw": round(c["surprise_raw"], 4),
        }

        raw = RawEvent(
            id=event_id,
            timestamp=event_ts,
            source="earnings_announcement",
            text=(
                f"{ticker} EPS actual={c['eps_actual']:.4f} "
                f"estimate={c['eps_estimated']:.4f} "
                f"surprise_z={c['surprise_z']:+.2f}"
            ),
            tickers=[ticker],
            tenant_id="default",
            metadata=metadata,
        )

        price_ctx: dict = {
            "entry_price": entry_price,
        }
        if return_1d is not None:
            price_ctx["return_1d"] = round(return_1d, 6)
        if vix_term is not None:
            price_ctx["vix_term"] = round(vix_term, 4)

        raw_events.append(raw)
        price_contexts[event_id] = price_ctx
        events_built += 1

    conn.close()
    print(f"Built {events_built} earnings events (size >= {args.min_quintile})")

    if not raw_events:
        print("No qualifying events after filters.")
        return 0

    if args.dry_run:
        print("\nDry run — events that would be processed:")
        for raw in raw_events:
            meta = raw.metadata
            print(
                f"  {raw.tickers[0]:6s}  surprise_z={meta.get('surprise_z'):+.2f}  "
                f"size={meta.get('size_quintile')}  "
                f"entry={price_contexts[raw.id].get('entry_price', 'n/a'):.2f}  "
                f"return_1d={price_contexts[raw.id].get('return_1d', 'n/a')}"
            )
        return 0

    # ------------------------------------------------------------------
    # Run pipeline — produces predictions in DB
    # ------------------------------------------------------------------
    result = run_pipeline(
        raw_events,
        price_contexts,
        persist=True,
        db_path=args.db,
        tenant_id="default",
    )

    preds = result.get("prediction_rows", [])
    # Count only earnings_drift predictions
    drift_preds = [p for p in preds if "earnings" in str(p.get("strategy_id", "")).lower()]
    print(
        f"Pipeline complete — total predictions={len(preds)}, "
        f"earnings_drift predictions={len(drift_preds)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
