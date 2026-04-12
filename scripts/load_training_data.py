#!/usr/bin/env python
"""
Load historical price data for ML factor training.

Two sources:
  1. data/raw_dumps/full_history/*.csv  — already on disk, load for free
  2. yfinance                           — fetch symbols not in full_history

Then runs a fresh walk-forward training pass on NVDA and prints
before/after factor coverage for all three horizons.

Usage:
    python scripts/load_training_data.py
    python scripts/load_training_data.py --dry-run   # show what would be loaded
    python scripts/load_training_data.py --no-train  # skip training pass
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

DB_PATH = _ROOT / "data" / "alpha.db"
FULL_HISTORY = _ROOT / "data" / "raw_dumps" / "full_history"
TENANT = "default"
TRAIN_TENANT = "ml_train"

def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        s = str(x).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out

# Symbols available in full_history/ that matter to the factor set
_FULL_HISTORY_BASE = [
    "NVDA", "SPY", "QQQ", "IWM", "TLT", "IWD", "GLD",
    "AAPL", "AMD", "TSLA", "META", "MSFT", "AMZN", "GOOGL", "NFLX",
    # Benchmarks used by cross-asset strategies (if present in full_history dumps)
    "XLK", "XLF", "XLE", "XLV", "HYG", "LQD",
]
try:
    from app.core.price_context import default_benchmark_tickers
    _FULL_HISTORY_BASE += list(default_benchmark_tickers())
except Exception:
    pass
FULL_HISTORY_SYMBOLS = _unique(_FULL_HISTORY_BASE)

# Symbols missing from full_history — fetch via yfinance
_YFINANCE_BASE = [
    "^VIX",       # VIX volatility index (vix_level, vix_change, vix_percentile)
    "BTC-USD",    # Bitcoin (btc_return_5d)
    "CL=F",       # Crude oil futures (oil_r20)
    "DX-Y.NYB",   # US Dollar index (dxy_return_20d)
]
YFINANCE_SYMBOLS = _unique(_YFINANCE_BASE)

# Training config
TRAIN_SYMBOLS = ["NVDA"]
TRAIN_HORIZONS = ["7d"]
TRAIN_START = date(2020, 1, 1)
TRAIN_END = date(2025, 12, 31)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _count_bars(conn: sqlite3.Connection, ticker: str, tenant: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) as n FROM price_bars WHERE tenant_id=? AND ticker=? AND timeframe='1d'",
        (tenant, ticker),
    ).fetchone()
    return int(row["n"])


def _insert_bars(
    conn: sqlite3.Connection,
    ticker: str,
    df: pd.DataFrame,
    tenant: str,
    dry_run: bool,
) -> int:
    """Insert OHLCV rows; skip existing (INSERT OR IGNORE). Returns rows inserted."""
    if df.empty:
        return 0

    rows = [
        (
            tenant, ticker, "1d",
            str(row["date"]) if len(str(row["date"])) == 10 else str(row["date"])[:10],
            float(row["open"]), float(row["high"]), float(row["low"]),
            float(row["close"]), float(row["volume"]),
        )
        for _, row in df.iterrows()
    ]

    if dry_run:
        return len(rows)

    conn.executemany(
        "INSERT OR IGNORE INTO price_bars "
        "(tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return len(rows)


def _load_full_history_csv(path: Path) -> pd.DataFrame | None:
    """Read a full_history CSV handling both column orderings."""
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"    read error: {e}")
        return None

    # Normalise column names (lower, strip)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(set(df.columns)):
        print(f"    unexpected columns: {list(df.columns)}")
        return None

    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"], format="ISO8601").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.sort_values("date")
    return df


def _fetch_yfinance(symbol: str) -> pd.DataFrame | None:
    """Download daily bars via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        print("    yfinance not installed: pip install yfinance")
        return None

    try:
        raw = yf.download(
            symbol,
            start="2020-01-01",
            end=date.today().isoformat(),
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        print(f"    yfinance error: {e}")
        return None

    if raw.empty:
        return None

    raw = raw.reset_index()
    raw.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in raw.columns]
    raw = raw.rename(columns={"date": "date"})
    raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y-%m-%d")

    for col in ("open", "high", "low", "close", "volume"):
        if col in raw.columns:
            # Handle MultiIndex columns from older yfinance
            if hasattr(raw[col], "iloc"):
                raw[col] = pd.to_numeric(raw[col], errors="coerce")

    return raw[["date", "open", "high", "low", "close", "volume"]].dropna(subset=["close"])


# ── Coverage report ───────────────────────────────────────────────────────────

def _coverage_report(db_path: Path, as_of: date = date(2025, 6, 1)) -> None:
    from app.ml.feature_builder import FeatureBuilder

    print(f"\nFactor coverage at {as_of} (tenant=default):")
    print(f"{'Horizon':<8} {'Present':>8} {'Total':>6} {'Coverage':>10}  Missing")
    print("-" * 72)

    for horizon, hdays in [("1d", 1.0), ("7d", 7.0), ("30d", 30.0)]:
        fb = FeatureBuilder(db_path=str(db_path), tenant_id=TENANT)
        feats, cov = fb.build("NVDA", as_of, horizon)
        fb.close()

        from app.ml.factor_spec import load_factor_config
        cfg = load_factor_config()
        eligible = cfg.get_eligible_specs(horizon, hdays)
        missing = [s.name for s in eligible if s.name not in feats]
        print(f"{horizon:<8} {len(feats):>8} {len(eligible):>6} {cov:>9.1%}  {', '.join(missing) or 'none'}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Show what would be loaded without writing")
    parser.add_argument("--no-train", action="store_true", help="Skip training pass")
    args = parser.parse_args()

    conn = _ensure_conn(DB_PATH)
    mode = "[DRY RUN] " if args.dry_run else ""

    # ── Before snapshot ───────────────────────────────────────────────────────
    print("Before:")
    _coverage_report(DB_PATH)
    print()

    # ── 1. Load full_history CSVs ─────────────────────────────────────────────
    print(f"{mode}Loading full_history/ CSVs -> tenant='{TENANT}'")
    fh_total = 0
    for sym in FULL_HISTORY_SYMBOLS:
        path = FULL_HISTORY / f"{sym}.csv"
        if not path.exists():
            print(f"  skip  {sym:<12}  (not in full_history/)")
            continue

        before = _count_bars(conn, sym, TENANT)
        df = _load_full_history_csv(path)
        if df is None:
            continue

        inserted = _insert_bars(conn, sym, df, TENANT, dry_run=args.dry_run)
        after = before + inserted if args.dry_run else _count_bars(conn, sym, TENANT)
        net = after - before
        date_range = f"{df['date'].min()} -> {df['date'].max()}"
        print(f"  {'would add' if args.dry_run else 'loaded':7}  {sym:<12}  {net:+5d} bars  ({date_range})")
        fh_total += net

    # Also copy full_history symbols into ml_train tenant for training consistency
    if not args.dry_run:
        print(f"\n{mode}Copying same symbols -> tenant='{TRAIN_TENANT}'")
        for sym in FULL_HISTORY_SYMBOLS:
            path = FULL_HISTORY / f"{sym}.csv"
            if not path.exists():
                continue
            df = _load_full_history_csv(path)
            if df is None:
                continue
            before = _count_bars(conn, sym, TRAIN_TENANT)
            inserted = _insert_bars(conn, sym, df, TRAIN_TENANT, dry_run=False)
            after = _count_bars(conn, sym, TRAIN_TENANT)
            net = after - before
            if net > 0:
                print(f"  loaded   {sym:<12}  {net:+5d} bars")

    # ── 2. Fetch missing symbols via yfinance ─────────────────────────────────
    print(f"\n{mode}Fetching missing symbols via yfinance -> tenant='{TENANT}'")
    yf_total = 0
    for sym in YFINANCE_SYMBOLS:
        before = _count_bars(conn, sym, TENANT)
        if before > 0 and not args.dry_run:
            print(f"  skip    {sym:<12}  (already has {before} bars)")
            continue
        print(f"  fetching {sym}...", end=" ", flush=True)
        df = _fetch_yfinance(sym)
        if df is None or df.empty:
            print("no data")
            continue

        inserted = _insert_bars(conn, sym, df, TENANT, dry_run=args.dry_run)
        after = before + inserted if args.dry_run else _count_bars(conn, sym, TENANT)
        net = after - before
        date_range = f"{df['date'].min()} -> {df['date'].max()}"
        print(f"{'would add' if args.dry_run else 'loaded':7}  {net:+5d} bars  ({date_range})")
        yf_total += net

        # Also into ml_train
        if not args.dry_run:
            _insert_bars(conn, sym, df, TRAIN_TENANT, dry_run=False)

    conn.close()

    print(f"\nTotal new bars: full_history={fh_total}  yfinance={yf_total}")

    # ── After snapshot ────────────────────────────────────────────────────────
    if not args.dry_run:
        print("\nAfter:")
        _coverage_report(DB_PATH)

    # ── 3. Re-train ───────────────────────────────────────────────────────────
    if args.dry_run or args.no_train:
        return

    print("\n" + "-" * 60)
    print("Re-training walk-forward models on NVDA...")
    print("-" * 60)

    from app.ml.train import run_training_pipeline

    result = run_training_pipeline(
        symbols=TRAIN_SYMBOLS,
        horizons=TRAIN_HORIZONS,
        data_start=TRAIN_START,
        data_end=TRAIN_END,
        db_path=str(DB_PATH),
        train_days=180,
        predict_days=30,
        step_days=90,
        tenant_id=TRAIN_TENANT,
        min_feature_coverage=0.5,
    )

    total = sum(len(v) for v in result.values())
    print(f"\nTraining complete: {total} models passed gate")
    for h, ids in result.items():
        print(f"  {h}: {len(ids)} model(s)")


if __name__ == "__main__":
    main()
