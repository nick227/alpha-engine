#!/usr/bin/env python
"""
IC validation for Post-Earnings Announcement Drift (PEAD).

Signal: EPS surprise magnitude z-score (cross-sectional, per earnings date).
Forward target: 5-day return after announcement close (strictly out-of-sample).

Filters applied before IC calculation:
  1. Size >= Q3 (same qualifying universe as live strategy)
  2. Minimum |surprise| in top/bottom quartile (middle 50% excluded)
  3. Price confirmation on announcement day (optional, see --no-confirmation)
  4. Adequate price data: 5 trading days of returns must be available

Stratification output:
  - Overall IC + t-stat + n
  - By surprise quartile (Q1 bottom, Q4 top)
  - By size quintile (Q3/Q4/Q5)
  - By regime (fear = VIX > VIX3M, calm = VIX <= VIX3M)
  - By time period (pre-2020, 2020+)

Pass threshold (as per spec): IC >= 0.02, t-stat >= 3.0, n >= 15,000.
If below threshold: FMP estimate quality likely the cause — investigate before building strategy.

Usage:
    python scripts/validate_earnings_drift.py
    python scripts/validate_earnings_drift.py --from-date 2015-01-01
    python scripts/validate_earnings_drift.py --no-confirmation
    python scripts/validate_earnings_drift.py --hold-days 10
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
DB = str(_ROOT / "data" / "alpha.db")
SURPRISES_DIR = _ROOT / "data" / "raw_dumps" / "earnings_surprises"

IC_PASS_THRESHOLD = 0.02
TSTAT_PASS_THRESHOLD = 3.0
N_PASS_THRESHOLD = 15_000

# Clip raw surprise to avoid division artifacts on near-zero estimates
MAX_RAW_SURPRISE = 5.0   # |surprise| capped at 5x
MIN_ABS_ESTIMATE = 0.01  # skip events where |estimate| < 1 cent (meaningless %)


def _load_size_quintiles(conn: sqlite3.Connection) -> dict[str, str]:
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


def _get_price(conn: sqlite3.Connection, ticker: str, date: str) -> float | None:
    row = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') "
        "ORDER BY timestamp DESC LIMIT 1",
        (ticker, date, date),
    ).fetchone()
    return float(row[0]) if row else None


def _get_nth_trading_day_close(
    conn: sqlite3.Connection,
    ticker: str,
    after_date: str,
    n: int,
) -> float | None:
    """Return the closing price n trading days after after_date (exclusive)."""
    rows = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d' "
        "AND timestamp > ? "
        "ORDER BY timestamp ASC LIMIT ?",
        (ticker, after_date, n),
    ).fetchall()
    if len(rows) < n:
        return None
    return float(rows[-1][0])


def _get_vix_term(conn: sqlite3.Connection, date: str) -> float | None:
    row = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') "
        "ORDER BY timestamp DESC LIMIT 1",
        (date, date),
    ).fetchone()
    row3m = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX3M' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') "
        "ORDER BY timestamp DESC LIMIT 1",
        (date, date),
    ).fetchone()
    if row and row3m:
        return float(row[0]) - float(row3m[0])
    return None


def _spearman_ic(signals: list[float], returns: list[float]) -> tuple[float, float, int]:
    """Return (IC, t_stat, n)."""
    n = len(signals)
    if n < 10:
        return 0.0, 0.0, n
    ic, pval = stats.spearmanr(signals, returns)
    if not np.isfinite(ic):
        return 0.0, 0.0, n
    # t-stat from IC: t = IC * sqrt(n-2) / sqrt(1 - IC^2)
    denom = np.sqrt(1 - ic ** 2) if abs(ic) < 1.0 else 1e-9
    t = float(ic) * np.sqrt(n - 2) / denom
    return float(ic), float(t), n


def _print_ic_row(label: str, signals: list[float], returns: list[float]) -> None:
    if len(signals) < 10:
        print(f"  {label:<30s}  n={len(signals):>6}  (too few)")
        return
    ic, t, n = _spearman_ic(signals, returns)
    hit = "PASS" if abs(ic) >= IC_PASS_THRESHOLD and abs(t) >= TSTAT_PASS_THRESHOLD else "----"
    print(f"  {label:<30s}  IC={ic:+.4f}  t={t:+.2f}  n={n:>7,}  {hit}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate PEAD IC on FMP surprise data")
    parser.add_argument("--from-date", default="2015-01-01",
                        help="Earliest announcement date to include (default 2015-01-01)")
    parser.add_argument("--to-date", default="",
                        help="Latest announcement date (default: today)")
    parser.add_argument("--hold-days", type=int, default=5,
                        help="Forward return horizon in trading days (default 5)")
    parser.add_argument("--no-confirmation", action="store_true",
                        help="Skip price-confirmation filter")
    parser.add_argument("--min-quintile", default="Q3",
                        choices=["Q1", "Q2", "Q3", "Q4", "Q5"])
    parser.add_argument("--db", default=DB)
    args = parser.parse_args()

    to_date = args.to_date or _date.today().isoformat()
    from_date = args.from_date

    surprise_files = sorted(SURPRISES_DIR.glob("*.json"))
    if not surprise_files:
        print(f"ERROR: No surprise files in {SURPRISES_DIR}")
        print("Run scripts/fetch_earnings_surprises.py first.")
        return 1

    print(f"Loading surprise files from {SURPRISES_DIR} ({len(surprise_files)} tickers)...")

    conn = sqlite3.connect(args.db)

    print("Computing size quintiles...")
    quintiles = _load_size_quintiles(conn)
    q_rank = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5": 5}
    min_rank = q_rank[args.min_quintile]

    # Collect all events
    raw_events: list[dict[str, Any]] = []
    n_files = 0
    n_skipped_size = 0

    for f in surprise_files:
        ticker = f.stem
        quintile = quintiles.get(ticker, "")
        tier = q_rank.get(quintile.split()[0] if quintile else "", 0)
        if tier < min_rank:
            n_skipped_size += 1
            continue

        try:
            records = json.loads(f.read_text())
        except Exception:
            continue
        n_files += 1

        for rec in records:
            date_str = str(rec.get("date") or "").strip()
            if not date_str or date_str < from_date or date_str > to_date:
                continue
            try:
                reported = float(rec["epsActual"])
                estimated = float(rec["epsEstimated"])
            except (KeyError, TypeError, ValueError):
                continue

            if abs(estimated) < MIN_ABS_ESTIMATE:
                continue

            raw_surprise = (reported - estimated) / abs(estimated)
            raw_surprise = float(np.clip(raw_surprise, -MAX_RAW_SURPRISE, MAX_RAW_SURPRISE))

            raw_events.append({
                "ticker": ticker,
                "date": date_str,
                "raw_surprise": raw_surprise,
                "quintile": quintile,
            })

    print(f"  Loaded {len(raw_events):,} raw events from {n_files} tickers "
          f"(skipped {n_skipped_size} below {args.min_quintile})")

    if len(raw_events) < 100:
        print("ERROR: Too few events — fetch more data first.")
        conn.close()
        return 1

    # Cross-sectional z-score surprise per announcement date
    from collections import defaultdict
    by_date: dict[str, list[int]] = defaultdict(list)
    for i, ev in enumerate(raw_events):
        by_date[ev["date"]].append(i)

    for date_str, idxs in by_date.items():
        surprises = np.array([raw_events[i]["raw_surprise"] for i in idxs])
        if len(surprises) < 2:
            for i in idxs:
                raw_events[i]["surprise_z"] = raw_events[i]["raw_surprise"]
            continue
        mu, sd = surprises.mean(), surprises.std(ddof=0)
        for i in idxs:
            raw_events[i]["surprise_z"] = (raw_events[i]["raw_surprise"] - mu) / sd if sd > 0 else 0.0

    # Compute per-event metrics (price confirmation, forward return, regime)
    print(f"Computing {args.hold_days}d forward returns and filters...")
    n_total = len(raw_events)
    records_out: list[dict[str, Any]] = []
    n_no_price = n_no_forward = n_no_confirm = 0

    for i, ev in enumerate(raw_events):
        if i % 5000 == 0 and i > 0:
            print(f"  {i:>7,}/{n_total:,}  built={len(records_out):,}")

        ticker = ev["ticker"]
        date_str = ev["date"]

        # Announcement-day close (day 0)
        close_0 = _get_price(conn, ticker, date_str)
        if close_0 is None:
            n_no_price += 1
            continue

        # Previous close (for price confirmation filter)
        prev_rows = conn.execute(
            "SELECT close FROM price_bars "
            "WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d' "
            "AND timestamp < ? ORDER BY timestamp DESC LIMIT 1",
            (ticker, date_str),
        ).fetchone()
        prev_close = float(prev_rows[0]) if prev_rows else None

        # Price confirmation: day-0 move direction must match surprise direction
        if not args.no_confirmation and prev_close is not None:
            day0_return = (close_0 - prev_close) / prev_close if prev_close else 0.0
            surprise_sign = 1 if ev["raw_surprise"] >= 0 else -1
            move_sign = 1 if day0_return >= 0 else -1
            if surprise_sign != move_sign:
                n_no_confirm += 1
                continue

        # N-day forward return
        close_n = _get_nth_trading_day_close(conn, ticker, date_str, args.hold_days)
        if close_n is None:
            n_no_forward += 1
            continue
        fwd_return = (close_n - close_0) / close_0 if close_0 else 0.0

        # VIX term for regime stratification
        vix_term = _get_vix_term(conn, date_str)
        fear = vix_term is not None and vix_term > 0.0

        year = int(date_str[:4])

        records_out.append({
            "ticker": ticker,
            "date": date_str,
            "raw_surprise": ev["raw_surprise"],
            "surprise_z": ev["surprise_z"],
            "fwd_return": fwd_return,
            "quintile": ev["quintile"],
            "fear": fear,
            "year": year,
        })

    conn.close()

    n_built = len(records_out)
    print(f"\nEvent build complete:")
    print(f"  Raw events:        {n_total:>8,}")
    print(f"  No price (day 0):  {n_no_price:>8,}")
    print(f"  No forward return: {n_no_forward:>8,}")
    print(f"  Confirmation fail: {n_no_confirm:>8,}  {'(filter disabled)' if args.no_confirmation else ''}")
    print(f"  Final sample:      {n_built:>8,}")

    if n_built < 100:
        print("\nERROR: Sample too small for meaningful IC.")
        return 1

    # Compute surprise quartiles for stratification
    all_raw = np.array([r["raw_surprise"] for r in records_out])
    q25, q75 = float(np.percentile(all_raw, 25)), float(np.percentile(all_raw, 75))

    def _surprise_quartile(s: float) -> str:
        if s <= q25:
            return "Q1 (most negative)"
        elif s <= np.percentile(all_raw, 50):
            return "Q2"
        elif s <= q75:
            return "Q3"
        else:
            return "Q4 (most positive)"

    for r in records_out:
        r["surprise_quartile"] = _surprise_quartile(r["raw_surprise"])

    # Outer tails only (Q1 + Q4) — the tradeable set
    tail_records = [r for r in records_out if r["surprise_quartile"] in ("Q1 (most negative)", "Q4 (most positive)")]

    signals_all = [r["surprise_z"] for r in records_out]
    returns_all = [r["fwd_return"] for r in records_out]
    signals_tails = [r["surprise_z"] for r in tail_records]
    returns_tails = [r["fwd_return"] for r in tail_records]

    print()
    print("=" * 72)
    print(f"PEAD IC VALIDATION — {args.hold_days}d forward return")
    print(f"Period: {from_date} to {to_date}")
    print(f"Confirmation filter: {'OFF' if args.no_confirmation else 'ON'}")
    print(f"Surprise quartile thresholds: Q1<={q25:.3f}, Q4>={q75:.3f}")
    print("=" * 72)

    print("\n[1] OVERALL (all events)")
    _print_ic_row("All events", signals_all, returns_all)
    _print_ic_row("Tails only (Q1+Q4)", signals_tails, returns_tails)

    print("\n[2] BY SURPRISE QUARTILE")
    for sq in ("Q1 (most negative)", "Q2", "Q3", "Q4 (most positive)"):
        sub = [r for r in records_out if r["surprise_quartile"] == sq]
        _print_ic_row(sq, [r["surprise_z"] for r in sub], [r["fwd_return"] for r in sub])

    print("\n[3] BY SIZE QUINTILE (tails only)")
    for q in ("Q3 mid", "Q4 large", "Q5 mega"):
        sub = [r for r in tail_records if r["quintile"] == q]
        _print_ic_row(q, [r["surprise_z"] for r in sub], [r["fwd_return"] for r in sub])

    print("\n[4] BY REGIME (tails only)")
    fear_sub = [r for r in tail_records if r["fear"]]
    calm_sub = [r for r in tail_records if not r["fear"]]
    _print_ic_row("Fear (VIX > VIX3M)", [r["surprise_z"] for r in fear_sub], [r["fwd_return"] for r in fear_sub])
    _print_ic_row("Calm (VIX <= VIX3M)", [r["surprise_z"] for r in calm_sub], [r["fwd_return"] for r in calm_sub])

    print("\n[5] BY PERIOD (tails only)")
    for label, yr_min, yr_max in [("2015-2019", 2015, 2019), ("2020-2022", 2020, 2022), ("2023+", 2023, 9999)]:
        sub = [r for r in tail_records if yr_min <= r["year"] <= yr_max]
        _print_ic_row(label, [r["surprise_z"] for r in sub], [r["fwd_return"] for r in sub])

    print()
    print("=" * 72)
    ic_tails, t_tails, n_tails = _spearman_ic(signals_tails, returns_tails)
    pass_ic = abs(ic_tails) >= IC_PASS_THRESHOLD
    pass_t = abs(t_tails) >= TSTAT_PASS_THRESHOLD
    pass_n = n_tails >= N_PASS_THRESHOLD
    verdict = "PASS — build the strategy" if (pass_ic and pass_t and pass_n) else "FAIL — investigate before building"
    print(f"VERDICT: {verdict}")
    print(f"  Tails IC={ic_tails:+.4f} (need |IC|>={IC_PASS_THRESHOLD}): {'OK' if pass_ic else 'FAIL'}")
    print(f"  t-stat ={t_tails:+.2f}  (need |t|>={TSTAT_PASS_THRESHOLD}): {'OK' if pass_t else 'FAIL'}")
    print(f"  n      ={n_tails:>7,}  (need n>={N_PASS_THRESHOLD:,}): {'OK' if pass_n else 'FAIL'}")
    if not pass_n:
        print()
        print("  Sample too small — fetch more tickers (lower --min-quintile or increase universe).")
    if pass_ic and pass_t and not pass_n:
        print()
        print("  Signal looks real but sample is small — fetch the full Q3+ universe to confirm.")
    if not pass_ic:
        print()
        print("  IC below threshold — likely FMP estimate quality issue.")
        print("  Check: are estimates near zero? Is the universe too broad?")
        print("  Try --no-confirmation to isolate whether the confirmation filter helps.")
    print("=" * 72)

    return 0 if (pass_ic and pass_t) else 1


if __name__ == "__main__":
    sys.exit(main())
