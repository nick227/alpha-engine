#!/usr/bin/env python
"""
Phase: Data Expansion

Loads all available price data into price_bars (tenants: default + ml_train)
and FRED macro parquets, then rebuilds ml_learning_rows and retrains.

Sources (in order):
  1. data/raw_dumps/full_history/*.csv  -- all 50 ETFs + curated equities
  2. yfinance                           -- symbols not covered or with gaps
  3. FRED API                           -- expanded macro series

Tagging:
  ticker_metadata table: ticker -> source, asset_type
  This allows downstream filtering by source or asset class.

Usage:
    python scripts/expand_training_data.py
    python scripts/expand_training_data.py --no-train   # skip retrain
    python scripts/expand_training_data.py --dry-run    # audit only
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

DB_PATH = _ROOT / "data" / "alpha.db"
FULL_HISTORY = _ROOT / "data" / "raw_dumps" / "full_history"
FRED_DIR = _ROOT / "data" / "raw_dumps" / "fred"
TENANTS = ["default", "ml_train"]

# ── Asset classification ───────────────────────────────────────────────────────

ETF_SET = {
    "SPY", "QQQ", "IWM", "TLT", "GLD", "SLV", "IWD", "IWF", "IWB",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC",
    "HYG", "LQD", "AGG", "BND", "EMB", "MUB",
    "EEM", "EFA", "VEA", "VWO", "EWJ", "EWZ", "EWY", "FXI",
    "VXX", "UVXY", "SQQQ", "SPXU", "SH",
    "SMH", "SOXX", "XBI", "IBB", "ARKK",
    "IEF", "SHY", "TIP", "SCHP",
    "USO", "DBC", "PDBC", "IAU",
    "VTI", "VOO", "IVV", "MDY", "IJR",
    "DIA", "SCHE", "SCHA",
}

MACRO_SET = {"^VIX", "BTC-USD", "CL=F", "DX-Y.NYB", "GC=F"}

# Curated equity list: liquid names we want in the training set
EQUITY_UNIVERSE = {
    # Mega-cap tech (highest factor relevance)
    "AAPL", "MSFT", "NVDA", "AMD", "GOOGL", "META", "AMZN", "TSLA", "NFLX",
    "ORCL", "CRM", "ADBE", "INTC", "QCOM", "AVGO", "TXN", "AMAT", "MU",
    # Semis (directly relevant to NVDA)
    "TSM", "ASML", "KLAC", "LRCX", "MRVL", "SMCI",
    # Financials
    "JPM", "GS", "MS", "BAC", "WFC",
    # Consumer / other large-cap
    "AMZN", "WMT", "PG", "KO", "JNJ", "UNH", "XOM", "CVX",
}

# FRED series to expand beyond the 4 we already have
FRED_EXPANSION = {
    # Rates
    "DGS10": "10-Year Treasury Constant Maturity Rate",
    "DGS2": "2-Year Treasury Constant Maturity Rate",
    "DGS1MO": "1-Month Treasury Constant Maturity Rate",
    # Volatility
    "VIXCLS": "CBOE Volatility Index (daily close, back to 1990)",
    # Credit
    "BAMLH0A0HYM2": "ICE BofA High Yield OAS Spread",
    "BAMLC0A0CM": "ICE BofA Investment Grade OAS Spread",
    # Inflation
    "CPIAUCSL": "CPI All Urban Consumers",
    "T5YIE": "5-Year Breakeven Inflation Rate",
    # Labor
    "ICSA": "Initial Claims (weekly)",
    # Money supply / liquidity
    "M2SL": "M2 Money Supply",
}

# yfinance symbols with no full_history dump
YFINANCE_ONLY = [
    "^VIX",      # VIX index (spot)
    "^VIX3M",    # VIX 3-month index — vix_term factor (VIX - VIX3M fear proxy)
    "BTC-USD",   # Bitcoin
    "CL=F",      # WTI crude futures
    "DX-Y.NYB",  # Dollar index
    "GC=F",      # Gold futures (complements GLD ETF)
    "^GSPC",     # S&P 500 index (for beta calculations vs ETF)
    "^TNX",      # 10Y yield as price series
    "XLE",       # Energy sector (not in full_history)
]

TRAIN_SYMBOLS = sorted(EQUITY_UNIVERSE)
TRAIN_HORIZONS = ["7d"]
TRAIN_START = date(2018, 1, 1)
TRAIN_END = date(2025, 12, 31)


# ── Schema helpers ─────────────────────────────────────────────────────────────

def _ensure_ticker_metadata(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_metadata (
            ticker       TEXT NOT NULL,
            source       TEXT NOT NULL,
            asset_type   TEXT NOT NULL,
            description  TEXT,
            loaded_at    TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (ticker, source)
        )
    """)
    conn.commit()


def _tag(conn: sqlite3.Connection, ticker: str, source: str, asset_type: str, description: str = "") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO ticker_metadata (ticker, source, asset_type, description) VALUES (?,?,?,?)",
        (ticker, source, asset_type, description),
    )


def _asset_type(ticker: str) -> str:
    if ticker in ETF_SET:
        return "etf"
    if ticker in MACRO_SET or ticker.startswith("^") or "=" in ticker:
        return "macro"
    return "equity"


# ── Price bar helpers ──────────────────────────────────────────────────────────

def _count(conn: sqlite3.Connection, ticker: str, tenant: str) -> int:
    r = conn.execute(
        "SELECT COUNT(*) FROM price_bars WHERE tenant_id=? AND ticker=? AND timeframe='1d'",
        (tenant, ticker),
    ).fetchone()
    return int(r[0])


def _insert(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame, tenant: str) -> int:
    if df.empty:
        return 0
    rows = [
        (tenant, ticker, "1d", str(r["date"])[:10],
         float(r["open"]), float(r["high"]), float(r["low"]),
         float(r["close"]), float(r["volume"]))
        for _, r in df.iterrows()
        if pd.notna(r.get("close"))
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO price_bars "
        "(tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    return len(rows)


def _read_full_history(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path)
    except Exception:
        return None
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    needed = {"date", "open", "high", "low", "close", "volume"}
    if not needed.issubset(df.columns):
        return None
    df = df[list(needed)].copy()
    df["date"] = pd.to_datetime(df["date"], format="ISO8601", utc=False).dt.strftime("%Y-%m-%d")
    return df.dropna(subset=["close"]).sort_values("date")


def _fetch_yfinance(symbol: str, start: str = "2018-01-01") -> pd.DataFrame | None:
    try:
        import yfinance as yf
        raw = yf.download(symbol, start=start, end=date.today().isoformat(),
                          interval="1d", progress=False, auto_adjust=True)
        if raw.empty:
            return None
        raw = raw.reset_index()
        raw.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in raw.columns]
        raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y-%m-%d")
        for col in ("open", "high", "low", "close", "volume"):
            if col in raw.columns:
                raw[col] = pd.to_numeric(raw[col], errors="coerce")
        return raw[["date", "open", "high", "low", "close", "volume"]].dropna(subset=["close"])
    except Exception as e:
        print(f"    yfinance error for {symbol}: {e}")
        return None


# ── FRED helpers ──────────────────────────────────────────────────────────────

def _fetch_fred(series_id: str, api_key: str) -> pd.DataFrame | None:
    try:
        resp = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": series_id, "api_key": api_key,
                    "file_type": "json", "observation_start": "1990-01-01", "sort_order": "asc"},
            timeout=30,
        )
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        rows = []
        for o in obs:
            v = o.get("value", ".")
            if v == ".":
                continue
            try:
                rows.append({"date": pd.Timestamp(o["date"], tz="UTC"),
                              "series_id": series_id, "value": float(v)})
            except (ValueError, KeyError):
                continue
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["date"] = df["date"].astype("datetime64[us, UTC]")
        return df
    except Exception as e:
        print(f"    FRED error for {series_id}: {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-train", action="store_true")
    parser.add_argument("--train-start", default=None, help="override train start (YYYY-MM-DD)")
    parser.add_argument("--train-end", default=None, help="override train end (YYYY-MM-DD)")
    args = parser.parse_args()
    dry = args.dry_run

    def _parse_day(x: str | None) -> date | None:
        if not x:
            return None
        try:
            return date.fromisoformat(str(x))
        except Exception:
            return None

    train_start = _parse_day(str(args.train_start)) if args.train_start else None
    train_end = _parse_day(str(args.train_end)) if args.train_end else None
    if (args.train_start and train_start is None) or (args.train_end and train_end is None):
        raise SystemExit("ERROR: --train-start/--train-end must be YYYY-MM-DD")
    if train_start is None:
        train_start = TRAIN_START
    if train_end is None:
        train_end = TRAIN_END
    if train_end < train_start:
        raise SystemExit("ERROR: --train-end must be >= --train-start")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    _ensure_ticker_metadata(conn)

    fred_api_key = os.getenv("FRED_API_KEY", "").strip()

    total_new = 0

    # ── 1. full_history ETFs ──────────────────────────────────────────────────
    print("=" * 60)
    print("1. Loading ETFs from full_history/")
    print("=" * 60)
    etf_files = sorted([f for f in FULL_HISTORY.glob("*.csv") if f.stem in ETF_SET])
    for path in etf_files:
        sym = path.stem
        df = _read_full_history(path)
        if df is None:
            continue
        before = _count(conn, sym, "ml_train")
        if not dry:
            for tenant in TENANTS:
                _insert(conn, sym, df, tenant)
            conn.commit()
            _tag(conn, sym, "full_history", _asset_type(sym))
            conn.commit()
        net = _count(conn, sym, "ml_train") - before if not dry else len(df)
        if net > 0 or before == 0:
            print(f"  {sym:<8}  {net:+5d} bars  {df['date'].min()} -> {df['date'].max()}")
        total_new += net

    # ── 2. full_history equities ──────────────────────────────────────────────
    print()
    print("=" * 60)
    print("2. Loading equities from full_history/")
    print("=" * 60)
    equity_files = sorted([
        f for f in FULL_HISTORY.glob("*.csv")
        if f.stem in EQUITY_UNIVERSE and f.stem not in ETF_SET
    ])
    for path in equity_files:
        sym = path.stem
        df = _read_full_history(path)
        if df is None:
            continue
        before = _count(conn, sym, "ml_train")
        if not dry:
            for tenant in TENANTS:
                _insert(conn, sym, df, tenant)
            conn.commit()
            _tag(conn, sym, "full_history", "equity")
            conn.commit()
        net = _count(conn, sym, "ml_train") - before if not dry else len(df)
        if net > 0 or before == 0:
            print(f"  {sym:<8}  {net:+5d} bars  {df['date'].min()} -> {df['date'].max()}")
        total_new += net

    # ── 3. yfinance: symbols without full_history or needing recent data ──────
    print()
    print("=" * 60)
    print("3. Fetching via yfinance (recent gaps + symbols not in full_history)")
    print("=" * 60)
    # All ETFs and equity universe — top up to today (full_history ends 2023-12-28)
    yf_topup = sorted(ETF_SET | EQUITY_UNIVERSE)
    yf_all = sorted(set(YFINANCE_ONLY) | set(yf_topup))
    for sym in yf_all:
        print(f"  {sym}...", end=" ", flush=True)
        df = _fetch_yfinance(sym)
        if df is None or df.empty:
            print("no data")
            continue
        before = _count(conn, sym, "ml_train")
        if not dry:
            for tenant in TENANTS:
                _insert(conn, sym, df, tenant)
            conn.commit()
            _tag(conn, sym, "yfinance", _asset_type(sym))
            conn.commit()
        net = _count(conn, sym, "ml_train") - before if not dry else len(df)
        print(f"{net:+5d} bars  {df['date'].min()} -> {df['date'].max()}")
        total_new += net
        time.sleep(0.3)  # polite pacing

    # ── 4. FRED expansion ─────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("4. FRED macro expansion")
    print("=" * 60)
    if not fred_api_key:
        print("  FRED_API_KEY not set — skipping")
    else:
        FRED_DIR.mkdir(parents=True, exist_ok=True)
        for series_id, description in FRED_EXPANSION.items():
            path = FRED_DIR / f"{series_id}.parquet"
            if path.exists():
                print(f"  {series_id:<22}  already present — skipping")
                continue
            print(f"  {series_id:<22}  fetching...", end=" ", flush=True)
            df = _fetch_fred(series_id, fred_api_key)
            if df is None:
                print("failed")
                continue
            if not dry:
                df.to_parquet(path, index=False)
                conn.execute(
                    "INSERT OR REPLACE INTO ticker_metadata (ticker, source, asset_type, description) VALUES (?,?,?,?)",
                    (series_id, "fred", "macro", description),
                )
                conn.commit()
            print(f"OK  {len(df):5d} obs  {str(df['date'].min())[:10]} -> {str(df['date'].max())[:10]}")

    conn.close()
    print()
    print(f"Total new price_bar rows added to ml_train: ~{total_new:,}")

    if dry or args.no_train:
        print("\n[dry-run / --no-train] Skipping rebuild + retrain.")
        return

    # ── 5. Rebuild ml_learning_rows ───────────────────────────────────────────
    print()
    print("=" * 60)
    print("5. Rebuilding ml_learning_rows")
    print("=" * 60)
    from app.ml.dataset import build_dataset

    # Only train on equities that have enough history (SPY needed for excess return)
    trainable = [s for s in TRAIN_SYMBOLS
                 if (FULL_HISTORY / f"{s}.csv").exists()
                 or s in {"NVDA", "AAPL", "MSFT", "AMD", "TSLA", "AMZN", "GOOGL", "META"}]
    print(f"Building dataset for {len(trainable)} symbols: {trainable}")

    inserted = build_dataset(
        symbols=trainable,
        date_range=(train_start, train_end),
        horizons=TRAIN_HORIZONS,
        db_path=str(DB_PATH),
        min_feature_coverage=0.5,
        tenant_id="ml_train",
        split="train",
    )
    print(f"ml_learning_rows inserted: {inserted:,}")

    # ── 6. Retrain ────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("6. Walk-forward retrain")
    print("=" * 60)
    from app.ml.train import run_training_pipeline

    result = run_training_pipeline(
        symbols=trainable,
        horizons=TRAIN_HORIZONS,
        data_start=train_start,
        data_end=train_end,
        db_path=str(DB_PATH),
        train_days=180,
        predict_days=30,
        step_days=90,
        tenant_id="ml_train",
        min_feature_coverage=0.5,
    )
    total_models = sum(len(v) for v in result.values())
    print(f"\nRetrain complete: {total_models} models passed gate")
    for h, ids in result.items():
        print(f"  {h}: {len(ids)} model(s)")


if __name__ == "__main__":
    main()
