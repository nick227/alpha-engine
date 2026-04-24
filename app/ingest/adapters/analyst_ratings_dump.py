"""
AnalystRatingsDumpAdapter - reads pre-downloaded analyst ratings CSV files.

Expected layout:
    data/raw_dumps/raw_partner_headlines.csv
    data/raw_dumps/raw_analyst_ratings.csv

CSV schema (raw_partner_headlines):
    headline : str
    url      : str
    publisher: str
    date     : YYYY-MM-DD HH:MM:SS
    stock    : str (ticker)

CSV schema (raw_analyst_ratings):
    headline : str
    url      : str
    publisher: str
    date     : YYYY-MM-DD HH:MM:SS
    stock    : str (ticker)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd

from app.core.event_ticker_aliases import EVENT_STOCK_ALIASES
from app.ingest.adapters.dump_adapter import DumpAdapter


class AnalystRatingsDumpAdapter(DumpAdapter):
    """Reads analyst ratings/news data from CSV files."""

    def __init__(
        self,
        headlines_file: str = "data/raw_dumps/raw_partner_headlines.csv",
        ratings_file: str = "data/raw_dumps/raw_analyst_ratings.csv",
    ):
        self.headlines_file = Path(headlines_file)
        self.ratings_file = Path(ratings_file)
        self._date_cache: tuple[datetime, datetime] | None = None

    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        """Check if data exists in the date range."""
        min_date, max_date = self._get_date_range()
        return min_date <= end and max_date >= start

    def _get_date_range(self) -> tuple[datetime, datetime]:
        """Get min/max dates from the files (cached)."""
        if self._date_cache:
            return self._date_cache

        min_date = datetime.max
        max_date = datetime.min

        for file_path in [self.headlines_file, self.ratings_file]:
            if not file_path.exists():
                continue
            try:
                df = pd.read_csv(file_path, usecols=["date"], nrows=100)
                if not df.empty:
                    file_min = pd.to_datetime(df["date"], utc=True).min()
                    file_max = pd.to_datetime(df["date"], utc=True).max()
                    min_date = min(min_date, file_min)
                    max_date = max(max_date, file_max)
            except Exception:
                continue

        self._date_cache = (min_date.to_pydatetime(), max_date.to_pydatetime())
        return self._date_cache

    def load_slice(
        self,
        start: datetime,
        end: datetime,
        symbols: list[str],
        chunk_size: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        """Load news data for symbols in date range."""
        symbols_upper = [s.upper() for s in symbols]

        for file_path in [self.headlines_file, self.ratings_file]:
            if not file_path.exists():
                continue

            try:
                df = pd.read_csv(file_path)
                df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

                mask = (df["date"] >= start) & (df["date"] < end)
                su = df["stock"].astype(str).str.strip().str.upper()
                if symbols_upper:
                    parts: list[pd.Series] = []
                    for sym in symbols_upper:
                        if sym in EVENT_STOCK_ALIASES:
                            parts.append(su.isin(EVENT_STOCK_ALIASES[sym]))
                        else:
                            parts.append(su == sym)
                    tick_mask = parts[0]
                    for q in parts[1:]:
                        tick_mask = tick_mask | q
                    mask = mask & tick_mask

                df = df.loc[mask].copy()
                if df.empty:
                    continue

                up = df["stock"].astype(str).str.strip().str.upper()
                df["ticker"] = np.where(up == "FB", "META", up)
                df = df.drop(columns=["stock"])
                yield df

            except Exception as e:
                continue

    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert DataFrame to raw dicts for the Extractor pipeline."""
        return df.rename(columns={
            "headline": "text",
            "date": "timestamp",
            "publisher": "source",
            "url": "url",
        }).to_dict("records")
