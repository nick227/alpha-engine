#!/usr/bin/env python
"""
Load non-price raw dumps (options, earnings, shorts, internals) into `price_bars`
as derived daily series, so FeatureBuilder can consume them via `source: price`.

This avoids adding new DB tables and keeps ingestion idempotent:
  INSERT OR IGNORE on (tenant_id, ticker, timeframe, timestamp).

Conventions (derived tickers):
  OPT:{SYM}:{METRIC}    (options / volatility / positioning)
  EARN:{SYM}:{METRIC}   (earnings + revisions)
  SHORT:{SYM}:{METRIC}  (short interest / borrow / utilization)
  INT:{VENUE}:{METRIC}  (market internals / breadth)

Input CSV formats are intentionally simple and additive.
See: docs/internal/raw-dumps-derived-series.md
"""
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DerivedSeriesSpec:
    prefix: str
    key_cols: list[str]
    date_col: str
    venue: str | None = None  # for internals


SPECS: dict[str, DerivedSeriesSpec] = {
    "options": DerivedSeriesSpec(prefix="OPT", key_cols=["ticker"], date_col="date"),
    "earnings": DerivedSeriesSpec(prefix="EARN", key_cols=["ticker"], date_col="date"),
    "shorts": DerivedSeriesSpec(prefix="SHORT", key_cols=["ticker"], date_col="date"),
    "internals": DerivedSeriesSpec(prefix="INT", key_cols=[], date_col="date", venue="NYSE"),
}


def _safe_float(x: Any) -> float | None:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return f


def _norm_date(x: Any) -> str | None:
    if x is None:
        return None
    try:
        # normalize to YYYY-MM-DD
        return pd.to_datetime(x, utc=True).strftime("%Y-%m-%d")
    except Exception:
        return None


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_bars (
          tenant_id TEXT NOT NULL,
          ticker TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL,
          PRIMARY KEY (tenant_id, ticker, timeframe, timestamp)
        );
        """
    )


def _insert_rows(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    rows: list[tuple[str, float]],
    dry_run: bool,
) -> int:
    # rows: [(date_str, value)]
    payload = [
        (
            str(tenant_id),
            str(ticker),
            "1d",
            str(d),
            float(v),
            float(v),
            float(v),
            float(v),
            0.0,
        )
        for d, v in rows
    ]
    if dry_run:
        return len(payload)
    conn.executemany(
        """
        INSERT OR IGNORE INTO price_bars
          (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        payload,
    )
    return int(conn.total_changes)


def _forward_fill(obs: list[tuple[str, float]], max_days: int) -> list[tuple[str, float]]:
    if max_days <= 0 or len(obs) < 2:
        return obs
    out: list[tuple[str, float]] = []
    # obs sorted by date asc
    for i, (d, v) in enumerate(obs):
        out.append((d, v))
        if i == len(obs) - 1:
            break
        d0 = date.fromisoformat(d)
        d1 = date.fromisoformat(obs[i + 1][0])
        span = min(max_days, max(0, (d1 - d0).days - 1))
        for k in range(1, span + 1):
            out.append(((d0 + timedelta(days=k)).isoformat(), v))
    return out


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]
    # Accept `symbol` as an alias for `ticker` (downloaders often emit `symbol`).
    if "ticker" not in df.columns and "symbol" in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    # Avoid having both (can happen if a user merges files).
    if "ticker" in df.columns and "symbol" in df.columns:
        df = df.drop(columns=["symbol"])
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/alpha.db")
    ap.add_argument("--tenant", default="ml_train", help="tenant to write into (default: ml_train)")
    ap.add_argument("--kind", required=True, choices=sorted(SPECS.keys()))
    ap.add_argument("--input", required=True, help="input CSV file path OR directory of CSVs")
    ap.add_argument("--forward-fill-days", type=int, default=0, help="forward-fill last observation for N days")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    spec = SPECS[str(args.kind)]
    input_path = Path(str(args.input))
    if not input_path.exists():
        raise SystemExit(f"missing input: {input_path}")

    if input_path.is_dir():
        paths = sorted([p for p in input_path.glob("*.csv") if p.is_file()])
        if not paths:
            raise SystemExit(f"no CSV files found under: {input_path}")
        dfs: list[pd.DataFrame] = []
        for p in paths:
            try:
                dfs.append(load_csv(p))
            except Exception:
                continue
        if not dfs:
            raise SystemExit(f"no readable CSVs under: {input_path}")
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = load_csv(input_path)
    if spec.date_col not in df.columns:
        raise SystemExit(f"missing date col '{spec.date_col}' in {input_path.name}")

    # metric columns are all non-key columns except the date
    ignore = set([spec.date_col] + spec.key_cols)
    metric_cols = [c for c in df.columns if c not in ignore]
    if not metric_cols:
        raise SystemExit("no metric columns found (expected at least one numeric column)")

    # normalize date column
    df["_date"] = df[spec.date_col].map(_norm_date)
    df = df[df["_date"].notna()].copy()
    if df.empty:
        print("no rows after date normalization")
        return 0

    conn = sqlite3.connect(str(Path(args.db)))
    try:
        _ensure_schema(conn)
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA journal_mode=WAL;")

        written = 0

        if args.kind == "internals":
            venue = str(spec.venue or "NYSE").strip().upper()
            for metric in metric_cols:
                obs: list[tuple[str, float]] = []
                for _, row in df.iterrows():
                    v = _safe_float(row.get(metric))
                    if v is None:
                        continue
                    obs.append((str(row["_date"]), float(v)))
                obs.sort(key=lambda x: x[0])
                obs2 = _forward_fill(obs, int(args.forward_fill_days))
                t = f"{spec.prefix}:{venue}:{metric.upper()}"
                written += _insert_rows(conn, tenant_id=str(args.tenant), ticker=t, rows=obs2, dry_run=bool(args.dry_run))
            if not args.dry_run:
                conn.commit()
            print(f"{'would write' if args.dry_run else 'wrote'} ~{written} derived bars from {input_path.name}")
            return 0

        # Per-ticker derived series
        if "ticker" not in df.columns:
            raise SystemExit("missing 'ticker' (or 'symbol') column")

        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        for sym, g in df.groupby("ticker", sort=False):
            if not sym:
                continue
            g2 = g.sort_values("_date")
            for metric in metric_cols:
                obs: list[tuple[str, float]] = []
                for _, row in g2.iterrows():
                    v = _safe_float(row.get(metric))
                    if v is None:
                        continue
                    obs.append((str(row["_date"]), float(v)))
                if not obs:
                    continue
                obs2 = _forward_fill(obs, int(args.forward_fill_days))
                t = f"{spec.prefix}:{sym}:{metric.upper()}"
                written += _insert_rows(conn, tenant_id=str(args.tenant), ticker=t, rows=obs2, dry_run=bool(args.dry_run))

        if not args.dry_run:
            conn.commit()
        print(f"{'would write' if args.dry_run else 'wrote'} ~{written} derived bars from {input_path.name}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
