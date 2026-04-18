"""
CSVPriceDumpAdapter - reads pre-downloaded stock price CSV files.

Expected layout:
    data/raw_dumps/full_history/{SYMBOL}.csv

CSV schema:
    date    : YYYY-MM-DD
    open    : float
    high    : float
    low     : float
    close   : float
    adj close: float
    volume  : int
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from app.core.full_history_csv import resolve_full_history_csv_path
from app.ingest.adapters.dump_adapter import DumpAdapter


class CSVPriceDumpAdapter(DumpAdapter):
    """Reads stock price data from CSV files in full_history directory."""

    def __init__(self, dump_dir: str = "data/raw_dumps/full_history"):
        self.dump_dir = Path(dump_dir)
        # Key: resolved CSV path string (META and FB share one file when aliased)
        self._date_cache: dict[str, tuple[datetime, datetime]] = {}

    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        """Check if any symbol has data in the date range."""
        for symbol in symbols:
            symbol_file = resolve_full_history_csv_path(self.dump_dir, symbol)
            if symbol_file is None:
                continue
            symbol_start, symbol_end = self._get_date_range(symbol_file)
            if symbol_start <= end and symbol_end >= start:
                return True
        return False

    def _get_date_range(self, symbol_file: Path) -> tuple[datetime, datetime]:
        """Get min/max dates for a resolved CSV (cached by path)."""
        key = str(symbol_file.resolve())
        if key in self._date_cache:
            return self._date_cache[key]

        try:
            df = pd.read_csv(symbol_file, usecols=["date"])
            if df.empty:
                return (datetime.max, datetime.min)
            min_date = pd.to_datetime(df["date"].min()).to_pydatetime()
            max_date = pd.to_datetime(df["date"].max()).to_pydatetime()
            self._date_cache[key] = (min_date, max_date)
            return (min_date, max_date)
        except Exception:
            return (datetime.max, datetime.min)

    def load_slice(
        self,
        start: datetime,
        end: datetime,
        symbols: list[str],
        chunk_size: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        """Load CSV data for symbols in date range."""
        for symbol in symbols:
            symbol_file = resolve_full_history_csv_path(self.dump_dir, symbol)
            if symbol_file is None:
                continue

            try:
                df = pd.read_csv(symbol_file)
                df["date"] = pd.to_datetime(df["date"], utc=True)

                # Convert start/end to timezone-aware for comparison
                start_ts = pd.Timestamp(start, tz="UTC") if start.tzinfo is None else start
                end_ts = pd.Timestamp(end, tz="UTC") if end.tzinfo is None else end

                mask = (df["date"] >= start_ts) & (df["date"] < end_ts)
                df = df[mask]

                if not df.empty:
                    df["ticker"] = symbol.upper()
                    yield df
            except Exception as e:
                continue

    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert DataFrame to raw dicts for the Extractor pipeline."""
        return df.rename(columns={
            "date": "timestamp",
            "open": "price_open",
            "high": "price_high",
            "low": "price_low",
            "close": "price_close",
            "adj close": "price_adj_close",
            "volume": "volume",
        }).to_dict("records")
