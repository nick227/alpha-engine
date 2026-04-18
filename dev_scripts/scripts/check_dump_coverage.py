#!/usr/bin/env python
"""Report raw_dumps coverage: full_history price (with aliases), FRED, analyst CSVs, FNSPID."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from app.core.full_history_csv import resolve_full_history_csv_path  # noqa: E402

DEFAULT_TICKERS = (
    "AAPL MSFT SPY NVDA TSLA AMZN META AMD QQQ IWM".split()
)
FRED_SERIES = ("FEDFUNDS", "T10Y2Y", "UNRATE", "CPIAUCSL")


def _fred_complete(fred_dir: Path) -> bool:
    return all((fred_dir / f"{s}.parquet").is_file() for s in FRED_SERIES)


def _fnspid_present(fnspid_dir: Path) -> bool:
    if (fnspid_dir / "news.parquet").is_file():
        return True
    return any(fnspid_dir.glob("*.csv"))


def _ticker_in_analyst_file(path: Path, ticker: str, chunksize: int) -> bool:
    u = ticker.upper()
    for chunk in pd.read_csv(path, usecols=["stock"], chunksize=chunksize):
        if chunk["stock"].astype(str).str.upper().eq(u).any():
            return True
    return False


def main() -> int:
    p = argparse.ArgumentParser(description="Audit data/raw_dumps coverage for target tickers.")
    p.add_argument("tickers", nargs="*", default=DEFAULT_TICKERS, help="Symbols (default: core basket)")
    p.add_argument(
        "--deep",
        action="store_true",
        help="Scan raw_partner_headlines + raw_analyst_ratings for each ticker (slow on large CSVs).",
    )
    p.add_argument("--strict", action="store_true", help="Exit 1 if any ticker lacks a full_history price file")
    p.add_argument("--chunksize", type=int, default=200_000)
    args = p.parse_args()

    raw = _ROOT / "data" / "raw_dumps"
    fh = raw / "full_history"
    fred = raw / "fred"
    headlines = raw / "raw_partner_headlines.csv"
    ratings = raw / "raw_analyst_ratings.csv"
    fnspid_dir = raw / "fnspid"

    macro_ok = _fred_complete(fred)
    analyst_files_ok = headlines.is_file() and ratings.is_file()
    fnspid_ok = _fnspid_present(fnspid_dir)

    print("Global:")
    print(f"  macro (FRED {','.join(FRED_SERIES)}): {'OK' if macro_ok else 'MISSING'}")
    print(f"  analyst (raw_partner + raw_analyst files): {'OK' if analyst_files_ok else 'MISSING'}")
    print(f"  FNSPID (csv or news.parquet): {'OK' if fnspid_ok else 'MISSING'}")
    print()

    failed = False
    for t in args.tickers:
        path = resolve_full_history_csv_path(fh, t)
        price_ok = path is not None
        alias = ""
        if path is not None and t.upper() != path.stem.upper():
            alias = f" [file {path.stem}.csv]"

        if args.deep and analyst_files_ok:
            h_ok = _ticker_in_analyst_file(headlines, t, args.chunksize) if headlines.is_file() else False
            r_ok = _ticker_in_analyst_file(ratings, t, args.chunksize) if ratings.is_file() else False
            ev = "OK" if (h_ok or r_ok) else "absent"
        elif analyst_files_ok:
            ev = "not_scanned"
        else:
            ev = "n/a"

        pr = "OK" if price_ok else "MISS"
        print(f"  {t:<6}  price {pr}{alias}  events {ev}")
        if args.strict and not price_ok:
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
