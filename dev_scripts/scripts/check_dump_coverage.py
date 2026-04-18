#!/usr/bin/env python
"""Report raw_dumps coverage: full_history price (with aliases), FRED, analyst CSVs, FNSPID."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import yaml

from app.core.event_ticker_aliases import EVENT_STOCK_ALIASES  # noqa: E402
from app.core.full_history_csv import resolve_full_history_csv_path  # noqa: E402

FALLBACK_TICKERS = (
    "AAPL MSFT SPY NVDA TSLA AMZN META AMD QQQ IWM".split()
)
FALLBACK_FRED = ("FEDFUNDS", "T10Y2Y", "UNRATE", "CPIAUCSL")
DEFAULT_CONFIG = _ROOT / "config" / "coverage_gates.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def _fred_complete(fred_dir: Path, series: tuple[str, ...]) -> bool:
    return all((fred_dir / f"{s}.parquet").is_file() for s in series)


def _fnspid_present(fnspid_dir: Path) -> bool:
    if (fnspid_dir / "news.parquet").is_file():
        return True
    return any(fnspid_dir.glob("*.csv"))


def _analyst_row_mask(su: pd.Series, ticker: str) -> pd.Series:
    sym = ticker.upper()
    if sym in EVENT_STOCK_ALIASES:
        return su.isin(EVENT_STOCK_ALIASES[sym])
    return su == sym


def _count_analyst_rows(path: Path, ticker: str, chunksize: int) -> int:
    n = 0
    for chunk in pd.read_csv(path, usecols=["stock"], chunksize=chunksize):
        su = chunk["stock"].astype(str).str.strip().str.upper()
        n += int(_analyst_row_mask(su, ticker).sum())
    return n


def main() -> int:
    p = argparse.ArgumentParser(
        description="Audit data/raw_dumps coverage. Defaults load config/coverage_gates.yaml.",
    )
    p.add_argument(
        "tickers",
        nargs="*",
        default=None,
        help="Symbols (default: tickers from --config)",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"YAML with tickers and thresholds (default: {DEFAULT_CONFIG})",
    )
    p.add_argument(
        "--no-config",
        action="store_true",
        help="Ignore --config; use --tickers-fallback list and CLI flags only.",
    )
    p.add_argument(
        "--report",
        action="store_true",
        help="Never exit nonzero (print only). Overrides gate.exit_nonzero_on_failure.",
    )
    p.add_argument(
        "--deep",
        action="store_true",
        help="Force analyst row scan (normally on when gate.require_analyst_rows is true).",
    )
    p.add_argument(
        "--deep-min-rows",
        type=int,
        default=None,
        metavar="N",
        help="Override analyst_rows_min_default; per-ticker analyst_rows_min still applies when set.",
    )
    p.add_argument("--strict", action="store_true", help="Force price-file requirement.")
    p.add_argument("--chunksize", type=int, default=200_000)
    args = p.parse_args()

    cfg = {} if args.no_config else _load_yaml(args.config)
    if not args.no_config and not args.config.is_file():
        print(f"check_dump_coverage: config not found {args.config} — using built-in fallbacks", file=sys.stderr)
    gate = cfg.get("gate") or {}
    tickers: list[str]
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
    else:
        tickers = [str(x).strip().upper() for x in (cfg.get("tickers") or FALLBACK_TICKERS)]

    fred_series = tuple(cfg.get("fred_series") or FALLBACK_FRED)
    min_default = int(cfg.get("analyst_rows_min_default", 100))
    min_by_ticker = {str(k).upper(): int(v) for k, v in (cfg.get("analyst_rows_min") or {}).items()}

    require_price = bool(gate.get("require_price_file", True))
    require_analyst = bool(gate.get("require_analyst_rows", True))
    exit_nonzero = bool(gate.get("exit_nonzero_on_failure", True))
    if args.report:
        exit_nonzero = False

    do_deep = require_analyst or args.deep
    if args.deep_min_rows is not None:
        min_default = args.deep_min_rows
    if args.strict:
        require_price = True

    raw = _ROOT / "data" / "raw_dumps"
    fh = raw / "full_history"
    fred = raw / "fred"
    headlines = raw / "raw_partner_headlines.csv"
    ratings = raw / "raw_analyst_ratings.csv"
    fnspid_dir = raw / "fnspid"

    macro_ok = _fred_complete(fred, fred_series)
    analyst_files_ok = headlines.is_file() and ratings.is_file()
    fnspid_ok = _fnspid_present(fnspid_dir)

    cfg_label = "none" if args.no_config else str(args.config)
    print(f"Coverage config: {cfg_label}")
    print("Global:")
    print(f"  macro (FRED {','.join(fred_series)}): {'OK' if macro_ok else 'MISSING'}")
    print(f"  analyst (raw_partner + raw_analyst files): {'OK' if analyst_files_ok else 'MISSING'}")
    print(f"  FNSPID (csv or news.parquet): {'OK' if fnspid_ok else 'MISSING'}")
    print()

    failed = False
    if not macro_ok:
        failed = True
    if require_analyst and not analyst_files_ok:
        failed = True

    for t in tickers:
        path = resolve_full_history_csv_path(fh, t)
        price_ok = path is not None
        alias = ""
        if path is not None and t.upper() != path.stem.upper():
            alias = f" [file {path.stem}.csv]"

        row_floor = min_by_ticker.get(t, min_default)

        if do_deep and analyst_files_ok:
            h_ct = _count_analyst_rows(headlines, t, args.chunksize) if headlines.is_file() else 0
            r_ct = _count_analyst_rows(ratings, t, args.chunksize) if ratings.is_file() else 0
            total_ev = h_ct + r_ct
            ev = f"rows={total_ev:,}  min={row_floor}"
            if require_analyst and total_ev < row_floor:
                ev = f"{ev}  WARN<threshold"
                failed = True
        elif analyst_files_ok:
            ev = "not_scanned"
        else:
            ev = "n/a"

        pr = "OK" if price_ok else "MISS"
        print(f"  {t:<6}  price {pr}{alias}  events {ev}")
        if require_price and not price_ok:
            failed = True

    code = 1 if (failed and exit_nonzero) else 0
    return code


if __name__ == "__main__":
    sys.exit(main())
