#!/usr/bin/env python
"""
Paper trade daily runner.

Usage
-----
# Run for today (uses latest available price bars):
    python scripts/paper_trade_daily.py

# Run for a specific date (backfill / simulation):
    python scripts/paper_trade_daily.py --date 2025-03-14

# Initialize portfolio tables and print cost model, then exit:
    python scripts/paper_trade_daily.py --init

# Print portfolio status without processing any trades:
    python scripts/paper_trade_daily.py --status

# Override the candle_body p33 threshold:
    python scripts/paper_trade_daily.py --p33 -0.62

# Simulate a range of past dates (paper backfill):
    python scripts/paper_trade_daily.py --backfill 2024-01-01 2024-12-31

Notes
-----
- Entry price = signal candle's closing price (next-open approximation).
  In live use, replace with actual next-morning open via your broker API.
- VIX/VIX3M values use the prior close (the signal candle's date),
  ensuring no same-day lookahead.
- Transaction costs are subtracted at position close only.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from app.paper.scanner import scan, DEFAULT_P33, DB
from app.paper.portfolio import PaperPortfolio

_DB = DB


def _latest_trading_date() -> str:
    """Return the most recent date present in price_bars.
    Uses VIX table (small, ~10k rows) instead of full 25M-row price_bars scan.
    """
    conn = sqlite3.connect(_DB)
    row = conn.execute(
        "SELECT MAX(DATE(timestamp)) FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d'"
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        raise RuntimeError("No VIX data found — run expand_training_data.py first.")
    return row[0]


def _get_prices_for_tickers(date: str, tickers: list[str]) -> dict[str, float]:
    """Return {ticker: close_price} for a specific set of tickers on `date`.
    Queries each ticker individually using the (tenant_id, ticker, timeframe, timestamp)
    index — fast even on 25M rows because each lookup is an indexed point read.
    Use this for exit pricing (only open positions, typically <= 20 tickers).
    """
    if not tickers:
        return {}
    conn = sqlite3.connect(_DB)
    result: dict[str, float] = {}
    for ticker in tickers:
        row = conn.execute(
            "SELECT close FROM price_bars "
            "WHERE tenant_id='ml_train' AND ticker=? AND timeframe='1d' "
            "AND timestamp >= ? AND timestamp < date(?, '+1 day') "
            "ORDER BY timestamp DESC LIMIT 1",
            (ticker, date, date),
        ).fetchone()
        if row and row[0]:
            v = float(row[0])
            if v > 0:
                result[ticker] = v
    conn.close()
    return result


def _get_vix_term(date: str) -> float | None:
    """Uses timestamp range to hit the index."""
    conn = sqlite3.connect(_DB)
    vix_row = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') LIMIT 1",
        (date, date),
    ).fetchone()
    v3m_row = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX3M' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') LIMIT 1",
        (date, date),
    ).fetchone()
    conn.close()
    if vix_row and v3m_row:
        return float(vix_row[0]) - float(v3m_row[0])
    return None


def _trading_dates_between(start: str, end: str) -> list[str]:
    """Return sorted list of distinct trading dates in [start, end].
    Queries from the VIX table (~10k rows) rather than the 25M-row price_bars table.
    VIX has a row for every US market trading day — same set as ml_train equities.
    """
    conn = sqlite3.connect(_DB)
    rows = conn.execute(
        "SELECT DISTINCT DATE(timestamp) FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') ORDER BY 1",
        (start, end),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def run_day(date: str, portfolio: PaperPortfolio, p33: float, verbose: bool = True) -> None:
    """Execute one paper-trading day: check exits, then check entries.

    Performance design:
    - Exit pricing: targeted lookup for open positions only (uses index, ~20 queries)
    - Entry scanning: only triggered on fear days (scan() checks VIX first)
    - Entry pricing: reuses the close prices already loaded inside scan()
    """
    vix_term = _get_vix_term(date)

    # 1. Process exits — only fetch prices for tickers we actually hold
    open_tickers = list(portfolio.open_tickers())
    exit_prices = _get_prices_for_tickers(date, open_tickers) if open_tickers else {}
    exits = portfolio.process_exits(date, exit_prices, vix_term)

    # 2. Scan for new setups (scan() returns [] immediately on non-fear days)
    setups = scan(date, p33_threshold=p33, db=_DB)

    # 3. Enter new positions — prices come from the signal candle close (already in Setup)
    entry_prices = {s.ticker: s.close for s in setups}
    entries = portfolio.enter_positions(setups, date, entry_prices)

    if verbose:
        portfolio.print_daily_summary(date, exits, entries, vix_term)
    elif exits or entries:
        n_open = portfolio.open_position_count()
        vt_str = f"{vix_term:+.2f}" if vix_term is not None else "N/A"
        print(f"[{date}]  vix_term={vt_str}  "
              f"exits={len(exits):>2}  entries={len(entries):>2}  "
              f"open={n_open:>2}/{portfolio.max_positions}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper trade daily runner")
    parser.add_argument("--date", default=None,
                        help="Signal date YYYY-MM-DD (default: latest in DB)")
    parser.add_argument("--p33", type=float, default=DEFAULT_P33,
                        help=f"candle_body p33 threshold (default {DEFAULT_P33})")
    parser.add_argument("--init", action="store_true",
                        help="Initialize portfolio tables and print cost model, then exit")
    parser.add_argument("--status", action="store_true",
                        help="Print portfolio status without trading")
    parser.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                        help="Simulate all trading days in [START, END]")
    args = parser.parse_args()

    portfolio = PaperPortfolio()
    portfolio.initialize()

    if args.init:
        portfolio.print_cost_model()
        print("Tables initialized. Run without --init to start paper trading.")
        return

    if args.status:
        open_positions = portfolio.get_open_positions()
        trades = portfolio.get_trade_history()
        print(f"\nOpen positions: {len(open_positions)}/{portfolio.max_positions}")
        for p in open_positions:
            print(f"  {p.ticker:<8} entered {p.entry_date} @ {p.entry_price:.2f}  "
                  f"exit≤{p.target_exit_date}  [{p.size_quintile}]")
        if trades:
            net_bps = [t.net_pnl_bp for t in trades]
            win_rate = sum(1 for b in net_bps if b > 0) / len(net_bps)
            print(f"\nClosed trades: {len(trades)}")
            print(f"  Win rate : {win_rate:.1%}")
            print(f"  Mean net : {sum(net_bps)/len(net_bps):>+.1f} bp/trade")
            print(f"  Total net: {sum(net_bps):>+.0f} bp")
        return

    if args.backfill:
        start, end = args.backfill
        dates = _trading_dates_between(start, end)
        print(f"Backfill: {len(dates)} trading days from {start} to {end}")
        print(f"p33 threshold: {args.p33}")
        for d in dates:
            run_day(d, portfolio, args.p33, verbose=False)

        # Final summary after backfill
        trades = portfolio.get_trade_history()
        open_pos = portfolio.get_open_positions()
        print(f"\nBackfill complete.")
        print(f"  Closed trades: {len(trades)}")
        if trades:
            net_bps = [t.net_pnl_bp for t in trades]
            win_rate = sum(1 for b in net_bps if b > 0) / len(net_bps)
            print(f"  Win rate     : {win_rate:.1%}")
            print(f"  Mean net     : {sum(net_bps)/len(net_bps):>+.1f} bp/trade")
            print(f"  Total net    : {sum(net_bps):>+.0f} bp")

            # Breakdown by exit reason
            by_reason: dict[str, list[float]] = {}
            for t in trades:
                by_reason.setdefault(t.exit_reason, []).append(t.net_pnl_bp)
            print("\n  By exit reason:")
            for reason, bps in sorted(by_reason.items()):
                wr = sum(1 for b in bps if b > 0) / len(bps)
                print(f"    {reason:<16} n={len(bps):>4}  mean={sum(bps)/len(bps):>+.1f}bp  wr={wr:.1%}")

            # Breakdown by size quintile
            by_q: dict[str, list[float]] = {}
            for t in trades:
                by_q.setdefault(t.size_quintile or "unknown", []).append(t.net_pnl_bp)
            print("\n  By size quintile:")
            for q in ["Q4 large", "Q5 mega", "unknown"]:
                if q in by_q:
                    bps = by_q[q]
                    wr = sum(1 for b in bps if b > 0) / len(bps)
                    print(f"    {q:<16} n={len(bps):>4}  mean={sum(bps)/len(bps):>+.1f}bp  wr={wr:.1%}")

            # Monthly P&L
            monthly: dict[str, list[float]] = {}
            for t in trades:
                mo = t.exit_date[:7]
                monthly.setdefault(mo, []).append(t.net_pnl_bp)
            print("\n  Monthly net P&L (bp):")
            for mo in sorted(monthly):
                bps = monthly[mo]
                print(f"    {mo}  n={len(bps):>3}  mean={sum(bps)/len(bps):>+.1f}  total={sum(bps):>+.0f}")

        print(f"\n  Open positions at end: {len(open_pos)}")
        return

    # Single-day run
    date = args.date or _latest_trading_date()
    print(f"Paper trading date: {date}  (p33={args.p33})")
    run_day(date, portfolio, args.p33, verbose=True)


if __name__ == "__main__":
    main()
