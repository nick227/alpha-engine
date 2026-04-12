#!/usr/bin/env python
"""
Download historical short-interest style metrics from Financial Modeling Prep (FMP)
and append to a per-symbol CSV.

Output:
  data/raw_dumps/shorts/{symbol}_shorts.csv

Schema:
  date,symbol,short_float,days_to_cover,short_volume

Dedup:
  - Skips if (date,symbol) already exists unless --force.

Notes:
  - FMP has multiple API "families" (stable vs api/v3 vs api/v4) and naming differs.
    This script tries a small set of candidate endpoints and uses the first that works.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date as _date
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

try:
    from dotenv import load_dotenv

    load_dotenv(override=False)
except Exception:
    pass


DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "SPY", "QQQ", "IWM", "TSLA", "AMD", "META", "AMZN"]


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _safe_float(x: Any) -> float | None:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    return f


def _norm_date(x: Any) -> str | None:
    if not x:
        return None
    s = str(x).strip()
    # fast path: YYYY-MM-DD
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def _read_existing(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["date", "symbol", "short_float", "days_to_cover", "short_volume"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in cols})


def _get_json(url: str, *, params: dict[str, Any]) -> Any:
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def _extract_rows(symbol: str, payload: Any) -> list[dict[str, Any]]:
    # payload can be list[dict] or {historical:[...]} etc.
    items: list[Any] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for k in ("historical", "data", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                items = v
                break
        if not items and all(isinstance(v, (int, float, str, type(None))) for v in payload.values()):
            # single record dict
            items = [payload]

    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        d = _norm_date(it.get("date") or it.get("settlementDate") or it.get("reportDate"))
        if not d:
            continue

        short_float = _safe_float(it.get("shortFloat") or it.get("short_float") or it.get("shortFloatPercent"))
        days_to_cover = _safe_float(it.get("daysToCover") or it.get("days_to_cover"))
        short_volume = _safe_float(it.get("shortVolume") or it.get("short_volume"))

        out.append(
            {
                "date": d,
                "symbol": symbol.upper(),
                "short_float": None if short_float is None else round(float(short_float), 6),
                "days_to_cover": None if days_to_cover is None else round(float(days_to_cover), 6),
                "short_volume": None if short_volume is None else round(float(short_volume), 6),
            }
        )

    out.sort(key=lambda r: r["date"])
    return out


def _candidate_endpoints(symbol: str) -> list[tuple[str, dict[str, Any]]]:
    # Keep the list small and deterministic.
    api_key = _env("FMP_API_KEY")
    return [
        (f"https://financialmodelingprep.com/stable/short-interest", {"symbol": symbol.upper(), "apikey": api_key}),
        (f"https://financialmodelingprep.com/stable/short-interest?symbol={symbol.upper()}", {"apikey": api_key}),
        (f"https://financialmodelingprep.com/api/v4/short_interest", {"symbol": symbol.upper(), "apikey": api_key}),
        (f"https://financialmodelingprep.com/api/v4/short-interest", {"symbol": symbol.upper(), "apikey": api_key}),
        (f"https://financialmodelingprep.com/api/v3/short_interest/{symbol.upper()}", {"apikey": api_key}),
        (f"https://financialmodelingprep.com/api/v3/short-interest/{symbol.upper()}", {"apikey": api_key}),
    ]


def download_symbol(symbol: str) -> list[dict[str, Any]]:
    last_err: str | None = None
    for url, params in _candidate_endpoints(symbol):
        try:
            payload = _get_json(url, params=params)
            rows = _extract_rows(symbol, payload)
            if rows:
                return rows
            last_err = f"no rows from {url}"
        except Exception as e:
            last_err = f"{url}: {e}"
            continue
    raise RuntimeError(last_err or "no working endpoint")


def run_one(symbol: str, *, out_dir: Path, force: bool) -> bool:
    out_path = out_dir / f"{symbol.upper()}_shorts.csv"
    existing = _read_existing(out_path)
    existing_keys = {(r.get("date", "").strip(), r.get("symbol", "").strip().upper()) for r in existing}

    rows = download_symbol(symbol)

    if not force:
        rows = [r for r in rows if (str(r["date"]), symbol.upper()) not in existing_keys]

    if not rows:
        print(f"  {symbol}: no new rows -> skip")
        return True

    # Merge and de-dupe (prefer new if force is set).
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for r in existing:
        k = (str(r.get("date", "")).strip(), str(r.get("symbol", "")).strip().upper())
        if k[0] and k[1]:
            merged[k] = r
    for r in rows:
        k = (str(r["date"]), symbol.upper())
        merged[k] = r

    out_rows = list(merged.values())
    out_rows.sort(key=lambda r: (str(r.get("date", "")), str(r.get("symbol", ""))))
    _write_rows(out_path, out_rows)
    print(f"  {symbol}: wrote {out_path} (+{len(rows)} rows)")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="comma-separated symbols")
    ap.add_argument("--out", default="data/raw_dumps/shorts", help="output directory")
    ap.add_argument("--force", action="store_true", help="overwrite existing rows if present")
    args = ap.parse_args()

    if not _env("FMP_API_KEY"):
        print("ERROR: FMP_API_KEY not found in environment")
        return 2

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    out_dir = Path(str(args.out))

    ok = 0
    bad = 0
    print(f"FMP shorts -> {out_dir}")
    for sym in symbols:
        try:
            if run_one(sym, out_dir=out_dir, force=bool(args.force)):
                ok += 1
            else:
                bad += 1
        except Exception as e:
            bad += 1
            print(f"  {sym}: ERROR - {e}")

    print(f"Done: {ok} ok, {bad} failed")
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
