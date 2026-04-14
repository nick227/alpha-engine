"""
IC validation for discovery strategies using feature_snapshot data.

For each strategy (realness_repricer, silent_compounder, narrative_lag,
balance_sheet_survivor, ownership_vacuum), computes:
  - Raw score per symbol per day (using the same logic as strategies.py)
  - Forward returns at 1d, 5d, 20d from subsequent feature_snapshot closes
  - Spearman IC (rank-correlation of score vs forward return)
  - t-stat, n, win rate among top-quintile picks

Horizons available given feature_snapshot coverage through 2026-04-10:
  1d: up to 2026-04-09
  5d: up to 2026-04-03
  20d: up to 2026-03-13

Usage:
  python scripts/validate_discovery_strategies.py
  python scripts/validate_discovery_strategies.py --db data/alpha.db --min-symbols 2000 --horizons 1,5,20
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Strategy score functions (mirror of app/discovery/strategies.py)
# ---------------------------------------------------------------------------

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))

def _clamp01(x: float) -> float:
    return _clamp(x, 0.0, 1.0)


def score_realness_repricer(
    price_percentile_252d: float | None,
    return_63d: float | None,
    **_,
) -> float | None:
    if price_percentile_252d is None or return_63d is None:
        return None
    depressed = 1.0 - _clamp01(price_percentile_252d)
    drawdown_proxy = _clamp(-return_63d / 0.5, 0.0, 1.0)
    raw = _clamp(0.6 * depressed + 0.4 * drawdown_proxy, 0.0, 1.0) ** 2.3
    return raw


def score_silent_compounder(
    volatility_20d: float | None,
    return_63d: float | None,
    **_,
) -> float | None:
    if volatility_20d is None or return_63d is None:
        return None
    low_vol = 1.0 - _clamp(volatility_20d / 0.08, 0.0, 1.0)
    steady = 1.0 - _clamp(abs(return_63d) / 0.3, 0.0, 1.0)
    if low_vol < 0.6 or steady < 0.6:
        return None
    return (0.6 * low_vol + 0.4 * steady) ** 1.8


def score_narrative_lag(
    return_63d: float | None,
    price_percentile_252d: float | None,
    **_,
) -> float | None:
    if return_63d is None:
        return None
    lag = _clamp(-return_63d / 0.3, 0.0, 1.0)
    undervalued = (1.0 - price_percentile_252d) if price_percentile_252d is not None else 0.5
    raw = _clamp(0.6 * lag + 0.4 * undervalued, 0.0, 1.0) ** 2.5
    return raw


def score_balance_sheet_survivor(
    return_63d: float | None,
    volatility_20d: float | None,
    **_,
) -> float | None:
    if return_63d is None or volatility_20d is None:
        return None
    distress = _clamp(-return_63d / 0.5, 0.0, 1.0)
    stability = 1.0 - _clamp(volatility_20d / 0.1, 0.0, 1.0)
    return _clamp(0.6 * distress + 0.4 * stability, 0.0, 1.0) ** 1.7


def score_ownership_vacuum(
    volume_zscore_20d: float | None,
    dollar_volume: float | None,
    **_,
) -> float | None:
    if volume_zscore_20d is None or dollar_volume is None:
        return None
    spike = _clamp(volume_zscore_20d / 5.0, 0.0, 1.0)
    low_liquidity = 1.0 - _clamp(dollar_volume / 10_000_000.0, 0.0, 1.0)
    return _clamp(0.7 * spike + 0.3 * low_liquidity, 0.0, 1.0) ** 1.8


STRATEGY_FNS = {
    "realness_repricer": score_realness_repricer,
    "silent_compounder": score_silent_compounder,
    "narrative_lag": score_narrative_lag,
    "balance_sheet_survivor": score_balance_sheet_survivor,
    "ownership_vacuum": score_ownership_vacuum,
}

# ownership_vacuum is ambiguous direction; others are implicitly bullish (expect recovery/continuation)
STRATEGY_DIRECTION = {
    "realness_repricer": 1,       # bullish: depressed price → expect bounce
    "silent_compounder": 1,       # bullish: stable compounder
    "narrative_lag": 1,           # bullish: lagging → expect catchup
    "balance_sheet_survivor": 1,  # bullish: distress stabilizing
    "ownership_vacuum": 1,        # bullish assumption: volume spike → accumulation
}

# ---------------------------------------------------------------------------
# Spearman rank correlation (no scipy dependency)
# ---------------------------------------------------------------------------

def _spearman_ic(scores: list[float], returns: list[float]) -> tuple[float, float, int]:
    """Return (IC, t_stat, n)."""
    pairs = [(s, r) for s, r in zip(scores, returns) if s is not None and r is not None]
    n = len(pairs)
    if n < 30:
        return 0.0, 0.0, n

    def _rank(vals):
        indexed = sorted(enumerate(vals), key=lambda x: x[1])
        ranks = [0.0] * len(vals)
        for rank_idx, (orig_idx, _) in enumerate(indexed):
            ranks[orig_idx] = float(rank_idx + 1)
        return ranks

    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    rx = _rank(xs)
    ry = _rank(ys)

    mx = sum(rx) / n
    my = sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    if dx == 0 or dy == 0:
        return 0.0, 0.0, n

    ic = num / (dx * dy)
    t = ic * math.sqrt((n - 2) / max(1e-12, 1 - ic ** 2))
    return ic, t, n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="IC validation for discovery strategies")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--min-symbols", type=int, default=2000, help="Min symbols per date to include (default: 2000)")
    p.add_argument("--horizons", default="1,5,20", help="Comma-separated horizons in days (default: 1,5,20)")
    p.add_argument("--strategies", default=None, help="Comma-separated subset of strategies to test (default: all)")
    args = p.parse_args(argv)

    horizons = [int(x) for x in str(args.horizons).split(",") if x.strip()]
    max_h = max(horizons)

    strategies = list(STRATEGY_FNS.keys())
    if args.strategies:
        strategies = [s.strip() for s in str(args.strategies).split(",") if s.strip()]

    print(f"Loading feature_snapshot from {args.db}...")
    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT symbol, as_of_date, close,
               return_63d, volatility_20d, price_percentile_252d,
               dollar_volume, volume_zscore_20d
        FROM feature_snapshot
        ORDER BY as_of_date ASC, symbol ASC
        """
    ).fetchall()
    conn.close()

    print(f"  Loaded {len(rows):,} rows.")

    # Build dict: (symbol, date) -> row fields
    # Also build list of all dates and close lookup: symbol -> sorted list of (date, close)
    from collections import defaultdict
    date_syms: dict[str, list[dict]] = defaultdict(list)
    close_series: dict[str, list[tuple[str, float]]] = defaultdict(list)  # symbol -> [(date, close), ...]

    for r in rows:
        d = str(r["as_of_date"])
        sym = str(r["symbol"])
        close = float(r["close"]) if r["close"] is not None else None
        row_dict = {
            "symbol": sym,
            "as_of_date": d,
            "close": close,
            "return_63d": float(r["return_63d"]) if r["return_63d"] is not None else None,
            "volatility_20d": float(r["volatility_20d"]) if r["volatility_20d"] is not None else None,
            "price_percentile_252d": float(r["price_percentile_252d"]) if r["price_percentile_252d"] is not None else None,
            "dollar_volume": float(r["dollar_volume"]) if r["dollar_volume"] is not None else None,
            "volume_zscore_20d": float(r["volume_zscore_20d"]) if r["volume_zscore_20d"] is not None else None,
        }
        date_syms[d].append(row_dict)
        if close is not None:
            close_series[sym].append((d, close))

    # Sort close_series by date
    for sym in close_series:
        close_series[sym].sort(key=lambda x: x[0])

    # Build fast forward-return lookup: (symbol, date) -> N-day future close
    # For each symbol, index dates and allow Nth-bar lookup
    sym_date_idx: dict[str, dict[str, int]] = {}  # symbol -> {date -> idx}
    for sym, series in close_series.items():
        sym_date_idx[sym] = {d: i for i, (d, _) in enumerate(series)}

    def get_forward_close(sym: str, from_date: str, n_bars: int) -> float | None:
        series = close_series.get(sym)
        idx_map = sym_date_idx.get(sym)
        if not series or not idx_map:
            return None
        idx = idx_map.get(from_date)
        if idx is None:
            return None
        target_idx = idx + n_bars
        if target_idx >= len(series):
            return None
        return series[target_idx][1]

    # Filter dates to those with >= min_symbols AND where we have max_h bars of forward data
    all_dates = sorted(date_syms.keys())
    print(f"  Found {len(all_dates)} distinct dates.")

    # Results collection: per strategy, per horizon -> list of (score, return)
    ic_data: dict[str, dict[int, list[tuple[float, float]]]] = {
        s: {h: [] for h in horizons} for s in strategies
    }

    dates_used = 0
    for date_idx, d in enumerate(all_dates):
        syms_on_day = date_syms[d]
        if len(syms_on_day) < args.min_symbols:
            continue

        for h in horizons:
            for strat in strategies:
                fn = STRATEGY_FNS[strat]
                direction = STRATEGY_DIRECTION.get(strat, 1)
                for row_dict in syms_on_day:
                    sym = row_dict["symbol"]
                    entry = row_dict["close"]
                    if entry is None or entry <= 0:
                        continue
                    score = fn(**row_dict)
                    if score is None:
                        continue
                    fwd = get_forward_close(sym, d, h)
                    if fwd is None:
                        continue
                    fwd_return = (fwd / entry - 1.0) * direction
                    ic_data[strat][h].append((score, fwd_return))

        dates_used += 1
        if (date_idx + 1) % 50 == 0:
            print(f"  ... processed {date_idx + 1}/{len(all_dates)} dates")

    print(f"\nProcessed {dates_used} trading days.\n")

    # Per-strategy, per-horizon: compute IC, win rate of top quintile
    pass_threshold = 0.02  # min |IC|
    t_threshold = 3.0      # min |t|
    n_threshold = 1000     # min samples

    results: list[dict] = []

    for strat in strategies:
        for h in horizons:
            pairs = ic_data[strat][h]
            ic, t, n = _spearman_ic([p[0] for p in pairs], [p[1] for p in pairs])

            # Win rate in top-quintile (score >= 80th percentile)
            if pairs:
                sorted_by_score = sorted(pairs, key=lambda x: x[0], reverse=True)
                top_q = sorted_by_score[: max(1, len(sorted_by_score) // 5)]
                wins = sum(1 for _, r in top_q if r > 0)
                wr = wins / len(top_q)
                avg_ret = sum(r for _, r in top_q) / len(top_q)
            else:
                wr = 0.0
                avg_ret = 0.0

            passed = abs(ic) >= pass_threshold and abs(t) >= t_threshold and n >= n_threshold
            results.append(
                {
                    "strategy": strat,
                    "horizon_days": h,
                    "IC": ic,
                    "t_stat": t,
                    "n": n,
                    "top_quintile_winrate": wr,
                    "top_quintile_avg_return": avg_ret,
                    "passed": passed,
                }
            )

    # Print results
    print("=" * 70)
    print("DISCOVERY STRATEGY IC VALIDATION")
    print("=" * 70)
    print(f"{'Strategy':<28} {'H':>3} {'IC':>8} {'t':>8} {'n':>8} {'WR(top-Q)':>10} {'AvgR(top-Q)':>12} {'Status'}")
    print("-" * 70)

    for r in results:
        status = "[PASS]" if r["passed"] else "      "
        print(
            f"{r['strategy']:<28} {r['horizon_days']:>3}d"
            f" {r['IC']:>+8.4f} {r['t_stat']:>+8.2f} {r['n']:>8,}"
            f" {r['top_quintile_winrate']:>9.1%} {r['top_quintile_avg_return']:>+11.4%}"
            f"  {status}"
        )
        if results.index(r) % len(horizons) == len(horizons) - 1 and results.index(r) < len(results) - 1:
            print()

    print("\n" + "=" * 70)
    print("SUMMARY (passing IC >= 0.02, |t| >= 3.0, n >= 1000):")
    passing = [r for r in results if r["passed"]]
    print(f"  {len(passing)}/{len(results)} strategy-horizon combinations passed.")
    for r in passing:
        print(f"  [PASS] {r['strategy']:28} {r['horizon_days']}d  IC={r['IC']:+.4f} t={r['t_stat']:+.2f} n={r['n']:,}")

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
