"""
AlphaVantageDumpAdapter - reads pre-downloaded Alpha Vantage CSV files.

Expected layout:
    data/raw_dumps/alpha_vantage/{SYMBOL}.csv

CSV schema:
    timestamp: YYYY-MM-DD
    open     : float
    high     : float
    low      : float
    close    : float
    volume   : int
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from app.ingest.adapters.dump_adapter import DumpAdapter


class AlphaVantageDumpAdapter(DumpAdapter):
    """Reads stock price data from Alpha Vantage CSV files."""

    def __init__(self, dump_dir: str = "data/raw_dumps/alpha_vantage"):
        self.dump_dir = Path(dump_dir)
        self._date_cache: dict[str, tuple[datetime, datetime]] = {}

    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        """Check if any symbol has data in the date range."""
        for symbol in symbols:
            symbol_file = self.dump_dir / f"{symbol.upper()}.csv"
            if not symbol_file.exists():
                continue
            symbol_start, symbol_end = self._get_date_range(symbol)
            if symbol_start <= end and symbol_end >= start:
                return True
        return False

    def _get_date_range(self, symbol: str) -> tuple[datetime, datetime]:
        """Get min/max dates for a symbol (cached)."""
        if symbol in self._date_cache:
            return self._date_cache[symbol]

        symbol_file = self.dump_dir / f"{symbol.upper()}.csv"
        if not symbol_file.exists():
            return (datetime.max, datetime.min)

        try:
            df = pd.read_csv(symbol_file, usecols=["timestamp"])
            if df.empty:
                return (datetime.max, datetime.min)
            min_date = pd.to_datetime(df["timestamp"].min()).to_pydatetime()
            max_date = pd.to_datetime(df["timestamp"].max()).to_pydatetime()
            self._date_cache[symbol] = (min_date, max_date)
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
            symbol_file = self.dump_dir / f"{symbol.upper()}.csv"
            if not symbol_file.exists():
                continue

            try:
                df = pd.read_csv(symbol_file)
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

                # Convert start/end to timezone-aware for comparison
                start_ts = pd.Timestamp(start, tz="UTC") if start.tzinfo is None else start
                end_ts = pd.Timestamp(end, tz="UTC") if end.tzinfo is None else end

                mask = (df["timestamp"] >= start_ts) & (df["timestamp"] < end_ts)
                df = df[mask]

                if not df.empty:
                    df["ticker"] = symbol.upper()
                    yield df
            except Exception as e:
                continue

    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert DataFrame to raw dicts for the Extractor pipeline."""
        return df.rename(columns={
            "timestamp": "timestamp",
            "open": "price_open",
            "high": "price_high",
            "low": "price_low",
            "close": "price_close",
            "volume": "volume",
        }).to_dict("records")
