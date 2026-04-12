#!/usr/bin/env python
"""
Download FRED macro series and save as parquet files.

Usage:
    python scripts/download_fred_dump.py
    python scripts/download_fred_dump.py --series FEDFUNDS T10Y2Y DFII10 UNRATE
    python scripts/download_fred_dump.py --out data/raw_dumps/fred

Output: data/raw_dumps/fred/{SERIES_ID}.parquet
Schema: date (timestamp[us, tz=UTC]), series_id (string), value (float64)

FRED API key is read from the FRED_API_KEY environment variable or .env file.
Free keys: https://fred.stlouisfed.org/docs/api/api_key.html
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Load .env so FRED_API_KEY is available when running as a script
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

import requests
import pandas as pd

# Series required by config/factors.yaml
DEFAULT_SERIES = [
    "FEDFUNDS",
    "T10Y2Y",
    "DFII10",
    "UNRATE",
    # Credit spreads
    "BAMLH0A0HYM2",
    "BAMLC0A0CM",
    # Rates
    "DGS10",
    "DGS2",
    "DGS1MO",
    # Inflation / labor / liquidity
    "T5YIE",
    "CPIAUCSL",
    "ICSA",
    "M2SL",
    # VIX close series (macro)
    "VIXCLS",
]

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def fetch_series(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch all observations for a FRED series and return as DataFrame."""
    resp = requests.get(
        FRED_BASE,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": "2000-01-01",
            "sort_order": "asc",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    observations = data.get("observations", [])
    if not observations:
        raise ValueError(f"No observations returned for {series_id}")

    rows = []
    for obs in observations:
        val_str = obs.get("value", ".")
        if val_str == ".":
            continue  # FRED uses "." for missing values
        try:
            rows.append({
                "date": pd.Timestamp(obs["date"], tz="UTC"),
                "series_id": series_id,
                "value": float(val_str),
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        raise ValueError(f"All observations missing for {series_id}")

    df = pd.DataFrame(rows)
    df["date"] = df["date"].dt.tz_convert("UTC").astype("datetime64[us, UTC]")
    df["series_id"] = df["series_id"].astype("string")
    df["value"] = df["value"].astype("float64")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Download FRED macro series to parquet")
    parser.add_argument(
        "--series", nargs="+", default=DEFAULT_SERIES,
        help=f"FRED series IDs to download (default: {DEFAULT_SERIES})",
    )
    parser.add_argument(
        "--out", default="data/raw_dumps/fred",
        help="Output directory (default: data/raw_dumps/fred)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite existing parquet files (default: skip existing)",
    )
    args = parser.parse_args()

    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        print("Error: FRED_API_KEY not set. Add it to .env or set the environment variable.")
        print("Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")
        sys.exit(1)

    out_dir = Path(args.out)
    if not out_dir.is_absolute():
        out_dir = _ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output: {out_dir}")
    print()

    ok, failed = 0, 0
    # De-dupe while preserving order
    seen: set[str] = set()
    series_list: list[str] = []
    for s in args.series:
        sid = str(s).upper().strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        series_list.append(sid)

    for sid in series_list:
        sid = sid.upper().strip()
        path = out_dir / f"{sid}.parquet"
        if path.exists() and not args.force:
            print(f"  SKIP {sid:12s}  already exists  -> {path.name}")
            ok += 1
            continue
        try:
            df = fetch_series(sid, api_key)
            df.to_parquet(path, index=False)
            print(f"  OK  {sid:12s}  {len(df):5d} obs  {str(df['date'].min())[:10]} -> {str(df['date'].max())[:10]}  -> {path.name}")
            ok += 1
        except Exception as e:
            print(f"  FAIL {sid:12s}  {e}")
            failed += 1

    print()
    print(f"Done: {ok} downloaded, {failed} failed")


if __name__ == "__main__":
    main()
