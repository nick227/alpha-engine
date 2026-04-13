#!/usr/bin/env python
"""
IC validation for the three live continuous technical strategies.

Strategies under test (all configured for 15m horizon but run on 1d bars):
    technical_rsi_v1       — RSI(14) mean-reversion,   gate |RSI - 50| >= 20
    technical_bollinger_v1 — zscore(20) mean-reversion, gate |z| >= 2.0
    baseline_momentum_v1   — 5-bar momentum,            gate |trend| >= 0.004

Signal → forward return IC at multiple horizons (1d, 5d, 10d, 21d).
Regime conditioning on VIX term structure. Time-period stability (decade slices).

Pass threshold: |IC| >= 0.02, |t-stat| >= 3.0, n >= 3,000.

Usage:
    python scripts/validate_technical_strategies.py
    python scripts/validate_technical_strategies.py --from-date 2010-01-01
    python scripts/validate_technical_strategies.py --strategy rsi
    python scripts/validate_technical_strategies.py --min-tickers 300
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
DB = str(_ROOT / "data" / "alpha.db")

# ─── Pass thresholds ──────────────────────────────────────────────────── #
IC_PASS = 0.02
T_PASS  = 3.0
N_PASS  = 3_000

# ─── Strategy gate parameters (must match experiments/strategies/*.json) ─ #
RSI_OVERSOLD   = 30.0
RSI_OVERBOUGHT = 70.0
ZSCORE_THRESH  = 2.0
MOMENTUM_MIN   = 0.004   # baseline_momentum_v1.json: min_short_trend=0.004

# ─── Horizons to test (in trading days) ──────────────────────────────── #
HORIZONS = [1, 5, 10, 21]

# ─── Decade/period labels for stability check ─────────────────────────── #
PERIODS = [
    ("2010-2014", "2010-01-01", "2014-12-31"),
    ("2015-2019", "2015-01-01", "2019-12-31"),
    ("2020-2022", "2020-01-01", "2022-12-31"),
    ("2023+",     "2023-01-01", "2099-12-31"),
]


# ─── Helpers ──────────────────────────────────────────────────────────── #

def _spearman_ic(signal: np.ndarray, fwd: np.ndarray) -> tuple[float, float, int]:
    """Spearman IC, t-stat, and n after dropping NaN pairs."""
    mask = np.isfinite(signal) & np.isfinite(fwd)
    s = signal[mask]
    f = fwd[mask]
    n = int(len(s))
    if n < 10:
        return 0.0, 0.0, n
    ic, _ = stats.spearmanr(s, f)
    if not math.isfinite(ic):
        return 0.0, 0.0, n
    t = ic * math.sqrt(n - 2) / math.sqrt(max(1.0 - ic ** 2, 1e-9))
    return float(ic), float(t), n


def _pass_str(ic: float, t: float, n: int) -> str:
    if n < N_PASS:
        return "INSUFFICIENT_N"
    if abs(ic) >= IC_PASS and abs(t) >= T_PASS:
        return "PASS"
    return "FAIL"


def _rsi_rolling(closes: pd.Series, period: int = 14) -> pd.Series:
    """Simple-rolling-mean RSI — matches price_context._rsi()."""
    delta = closes.diff()
    up   = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    roll_up   = up.rolling(period, min_periods=period).mean()
    roll_down = down.rolling(period, min_periods=period).mean()
    rs = roll_up / roll_down.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.clip(0.0, 100.0)


def _zscore_rolling(closes: pd.Series, period: int = 20) -> pd.Series:
    """20-bar price z-score — matches price_context zscore_20."""
    mean = closes.rolling(period, min_periods=period).mean()
    std  = closes.rolling(period, min_periods=period).std(ddof=0)
    return (closes - mean) / std.replace(0.0, np.nan)


def _momentum_rolling(closes: pd.Series, bars: int = 5) -> pd.Series:
    """5-bar momentum — matches price_context short_trend."""
    return closes.pct_change(periods=bars)


# ─── Data loading ─────────────────────────────────────────────────────── #

def _load_bars(db: str, from_date: str, min_tickers: int) -> pd.DataFrame:
    """
    Load ml_train 1d price bars.

    Returns DataFrame sorted by (ticker, timestamp) with columns:
        ticker, timestamp, close, vix_term
    Only tickers with >= 252*3 (~3yr) of data are included.
    """
    print(f"Loading price_bars from {from_date}…")
    conn = sqlite3.connect(db)

    # Load VIX / VIX3M for regime tagging
    vix_df = pd.read_sql_query(
        """
        SELECT DATE(timestamp) as date,
               MAX(CASE WHEN ticker='^VIX'  THEN close END) as vix,
               MAX(CASE WHEN ticker='^VIX3M' THEN close END) as vix3m
        FROM price_bars
        WHERE tenant_id='ml_train' AND timeframe='1d'
          AND ticker IN ('^VIX','^VIX3M')
          AND DATE(timestamp) >= ?
        GROUP BY DATE(timestamp)
        """,
        conn,
        params=(from_date,),
    )
    vix_df["vix_term"] = vix_df["vix"] - vix_df["vix3m"]

    # Load price bars (exclude index tickers)
    df = pd.read_sql_query(
        """
        SELECT ticker, DATE(timestamp) as date, close
        FROM price_bars
        WHERE tenant_id='ml_train' AND timeframe='1d'
          AND DATE(timestamp) >= ?
          AND ticker NOT LIKE '^%'
          AND close > 0
        ORDER BY ticker, date
        """,
        conn,
        params=(from_date,),
    )
    conn.close()

    print(f"  Raw rows: {len(df):,}  Tickers: {df['ticker'].nunique():,}")

    # Filter tickers with < 3 years of data (can't compute stable rolling signals)
    counts = df.groupby("ticker")["date"].count()
    qualified = counts[counts >= 252 * 3].index
    df = df[df["ticker"].isin(qualified)]
    print(f"  After 3yr filter: {len(df):,} rows, {df['ticker'].nunique():,} tickers")

    if df["ticker"].nunique() < min_tickers:
        print(f"  WARNING: only {df['ticker'].nunique()} tickers qualify (min={min_tickers})")

    # Merge VIX term
    df = df.merge(vix_df[["date", "vix_term"]], on="date", how="left")
    return df


# ─── Signal computation ───────────────────────────────────────────────── #

def _compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-ticker: compute RSI(14), zscore(20), momentum(5), and forward returns.
    Uses vectorised pandas groupby-apply.
    """
    print("Computing signals and forward returns…")

    def _per_ticker(g: pd.DataFrame) -> pd.DataFrame:
        closes = g["close"].astype(float)
        g = g.copy()
        g["rsi_14"]       = _rsi_rolling(closes, 14)
        g["zscore_20"]    = _zscore_rolling(closes, 20)
        g["short_trend"]  = _momentum_rolling(closes, 5)
        # Forward returns: shift closes BACKWARD by N bars
        for h in HORIZONS:
            future_close = closes.shift(-h)
            g[f"fwd_{h}d"] = (future_close - closes) / closes
        return g

    result = (
        df.groupby("ticker", sort=False, group_keys=False)
          .apply(_per_ticker)
    )
    print(f"  Signal rows: {len(result):,}")
    return result


# ─── IC test ─────────────────────────────────────────────────────────── #

def _run_ic(
    name: str,
    signal_col: str,
    signed_signal: pd.Series,
    gate_mask: pd.Series,
    df: pd.DataFrame,
    regime_col: str = "vix_term",
) -> None:
    """
    Print IC table for one strategy: full universe + regime splits + time periods.
    """
    print(f"\n{'='*72}")
    print(f"  {name}")
    print(f"{'='*72}")
    print(f"  Signal: {signal_col}   Gate events: {gate_mask.sum():,}")
    print()

    sub = df[gate_mask].copy()
    sub["_sig"] = signed_signal[gate_mask]

    # ── Full universe × horizon ─────────────────────────────────────── #
    print(f"  {'Horizon':<10} {'IC':>8} {'t-stat':>8} {'n':>8}  Result")
    print(f"  {'-'*50}")
    best_horizon = None
    best_ic_abs = 0.0
    for h in HORIZONS:
        fwd_col = f"fwd_{h}d"
        ic, t, n = _spearman_ic(sub["_sig"].values, sub[fwd_col].values)
        verdict = _pass_str(ic, t, n)
        flag = " [PASS]" if verdict == "PASS" else ""
        print(f"  {h}d{'':<8} {ic:>+8.4f} {t:>+8.2f} {n:>8,}  {verdict}{flag}")
        if abs(ic) > best_ic_abs and n >= N_PASS:
            best_ic_abs = abs(ic)
            best_horizon = h
    print()

    if best_horizon is None:
        best_horizon = HORIZONS[1]  # default to 5d for drill-down
    fwd_col = f"fwd_{best_horizon}d"

    # ── Regime conditioning ─────────────────────────────────────────── #
    vt = sub[regime_col]
    has_regime = vt.notna().any()
    if has_regime:
        print(f"  Regime conditioning (best horizon={best_horizon}d, VIX term = VIX - VIX3M):")
        print(f"  {'Regime':<18} {'IC':>8} {'t-stat':>8} {'n':>8}  Result")
        print(f"  {'-'*54}")
        for label, mask in [("fear (vix_term>0)", vt > 0), ("calm (vix_term<=0)", vt <= 0)]:
            m = mask & sub[fwd_col].notna()
            ic, t, n = _spearman_ic(sub.loc[m, "_sig"].values, sub.loc[m, fwd_col].values)
            verdict = _pass_str(ic, t, n)
            print(f"  {label:<18} {ic:>+8.4f} {t:>+8.2f} {n:>8,}  {verdict}")
        print()

    # ── Temporal stability ──────────────────────────────────────────── #
    print(f"  Temporal stability (best horizon={best_horizon}d):")
    print(f"  {'Period':<14} {'IC':>8} {'t-stat':>8} {'n':>8}  Result")
    print(f"  {'-'*50}")
    for label, start, end in PERIODS:
        m = (sub["date"] >= start) & (sub["date"] <= end) & sub[fwd_col].notna()
        ic, t, n = _spearman_ic(sub.loc[m, "_sig"].values, sub.loc[m, fwd_col].values)
        verdict = _pass_str(ic, t, n)
        print(f"  {label:<14} {ic:>+8.4f} {t:>+8.2f} {n:>8,}  {verdict}")
    print()


# ─── Main ─────────────────────────────────────────────────────────────── #

def main() -> int:
    parser = argparse.ArgumentParser(description="IC validation for continuous technical strategies")
    parser.add_argument("--db", default=DB)
    parser.add_argument("--from-date", default="2010-01-01",
                        help="Earliest date to include (default: 2010-01-01)")
    parser.add_argument("--strategy", default="all",
                        choices=["all", "rsi", "bollinger", "momentum"],
                        help="Which strategy to test (default: all)")
    parser.add_argument("--min-tickers", type=int, default=200,
                        help="Warn if qualifying tickers < this (default: 200)")
    args = parser.parse_args()

    print(f"Technical strategy IC validation")
    print(f"  DB:        {args.db}")
    print(f"  From date: {args.from_date}")
    print(f"  Strategy:  {args.strategy}")
    print(f"  Pass:      |IC| >= {IC_PASS}, |t| >= {T_PASS}, n >= {N_PASS:,}")
    print()

    df = _load_bars(args.db, from_date=args.from_date, min_tickers=args.min_tickers)
    df = _compute_signals(df)

    # Drop rows where forward return is undefined (last N bars per ticker)
    # and rows missing signal values (first N warm-up bars)
    print(f"  Rows with signals computed: {df['rsi_14'].notna().sum():,}")
    print()

    run_all = (args.strategy == "all")

    # ── RSI ──────────────────────────────────────────────────────────── #
    if run_all or args.strategy == "rsi":
        gate = df["rsi_14"].notna() & (
            (df["rsi_14"] <= RSI_OVERSOLD) | (df["rsi_14"] >= RSI_OVERBOUGHT)
        )
        # signed signal: positive → predict up (oversold), negative → predict down (overbought)
        signed = 50.0 - df["rsi_14"]
        _run_ic(
            "RSI Mean Reversion (technical_rsi_v1)",
            "rsi_14  [gate: RSI<=30 or RSI>=70]",
            signed,
            gate,
            df,
        )

    # ── Bollinger ────────────────────────────────────────────────────── #
    if run_all or args.strategy == "bollinger":
        gate = df["zscore_20"].notna() & (df["zscore_20"].abs() >= ZSCORE_THRESH)
        # signed signal: negative z-score → predict up (below band)
        signed = -df["zscore_20"]
        _run_ic(
            "Bollinger Mean Reversion (technical_bollinger_v1)",
            "zscore_20  [gate: |z| >= 2.0]",
            signed,
            gate,
            df,
        )

    # ── Momentum ─────────────────────────────────────────────────────── #
    if run_all or args.strategy == "momentum":
        gate = df["short_trend"].notna() & (df["short_trend"].abs() >= MOMENTUM_MIN)
        signed = df["short_trend"]
        _run_ic(
            "Baseline Momentum (baseline_momentum_v1)",
            "short_trend  [gate: |5d-momentum| >= 0.4%]",
            signed,
            gate,
            df,
        )

    print("Validation complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
