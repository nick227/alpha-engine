"""
FnspidDumpAdapter — reads pre-downloaded FNSPID financial news.

Expected layout::

    data/raw_dumps/fnspid/news.parquet
    data/raw_dumps/fnspid/*.csv (from HuggingFace)

Parquet schema::

    date      : timestamp[us, tz=UTC]
    ticker    : string
    headline  : string

CSV schema (HuggingFace)::

    Date           : timestamp
    Article_title  : string
    Stock_symbol   : string
    Url            : string
    Publisher      : string
    Author         : string
    Article        : string

FNSPID source:
    https://github.com/Zdong104/FNSPID_Financial_News_Dataset
    https://huggingface.co/datasets/sabareesh88/FNSPID_nasdaq

Download helper::

    python scripts/download_fnspid_dump.py
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from app.ingest.adapters.dump_adapter import DumpAdapter, _coerce_utc


class FnspidDumpAdapter(DumpAdapter):
    """
    Streams FNSPID ticker-mapped news headlines from parquet or CSV files.

    Supports:
    - Parquet: data/raw_dumps/fnspid/news.parquet
    - CSV: data/raw_dumps/fnspid/*.csv (HuggingFace format)

    Uses PyArrow row-group streaming + vectorised pandas filtering.
    """

    _COLUMNS = ["date", "ticker", "headline"]

    def __init__(self, dump_dir: str | Path = Path("data/raw_dumps/fnspid")):
        self.dump_dir = Path(dump_dir)

    @property
    def _news_path(self) -> Path:
        return self.dump_dir / "news.parquet"

    def _resolve_dir(self, spec: Any) -> Path:
        override = (getattr(spec, "options", None) or {}).get("dump_dir")
        return Path(override) if override else self.dump_dir

    def _get_data_path(self) -> Path | None:
        """Find available data file (parquet or CSV)."""
        if self._news_path.exists():
            return self._news_path
        # Look for CSV files in dump_dir and parent (for fnspid_sample.csv)
        csv_files = list(self.dump_dir.glob("*.csv"))
        if not csv_files:
            csv_files = list(self.dump_dir.parent.glob("fnspid*.csv"))
        return csv_files[0] if csv_files else None

    # ------------------------------------------------------------------ #
    # DumpAdapter interface                                                #
    # ------------------------------------------------------------------ #

    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        return self._get_data_path() is not None

    def load_slice(
        self,
        start: datetime,
        end: datetime,
        symbols: list[str],
        chunk_size: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        path = self._get_data_path()
        if path is None:
            return

        start_ts = pd.Timestamp(start, tz="UTC") if getattr(start, "tzinfo", None) is None else pd.Timestamp(start).tz_convert("UTC")
        end_ts = pd.Timestamp(end, tz="UTC") if getattr(end, "tzinfo", None) is None else pd.Timestamp(end).tz_convert("UTC")
        sym_set = set(symbols) if symbols else None

        if path.suffix == ".csv":
            yield from _stream_news_csv(path, start_ts, end_ts, sym_set, chunk_size)
        else:
            try:
                import pyarrow.parquet as pq
                _use_pyarrow = True
            except ImportError:
                _use_pyarrow = False

            if _use_pyarrow:
                yield from _stream_news_pyarrow(path, start_ts, end_ts, sym_set, chunk_size)
            else:
                yield from _stream_news_pandas(path, start_ts, end_ts, sym_set, chunk_size)

    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Vectorised conversion — pandas C-layer, no Python row loops."""
        out = df[[c for c in self._COLUMNS if c in df.columns]].copy()
        if "date" in out.columns and pd.api.types.is_datetime64_any_dtype(out["date"]):
            out["date"] = out["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return out.to_dict("records")

    async def fetch_raw(self, spec: Any, ctx: Any) -> list[dict[str, Any]]:
        original_dir = self.dump_dir
        self.dump_dir = self._resolve_dir(spec)
        try:
            return await super().fetch_raw(spec, ctx)
        finally:
            self.dump_dir = original_dir


# ------------------------------------------------------------------ #
# Streaming helpers                                                    #
# ------------------------------------------------------------------ #

def _stream_news_pyarrow(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    sym_set: set[str] | None,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(path))
    for batch in pf.iter_batches(batch_size=chunk_size):
        df = batch.to_pandas()
        if "date" not in df.columns:
            continue
        df["date"] = _coerce_utc(df["date"])
        mask = (df["date"] >= start_ts) & (df["date"] < end_ts)
        if sym_set and "ticker" in df.columns:
            mask &= df["ticker"].isin(sym_set)
        filtered = df.loc[mask]
        if not filtered.empty:
            yield filtered


def _stream_news_pandas(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    sym_set: set[str] | None,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    df = pd.read_parquet(str(path))
    if "date" not in df.columns:
        return
    df["date"] = _coerce_utc(df["date"])
    mask = (df["date"] >= start_ts) & (df["date"] < end_ts)
    if sym_set and "ticker" in df.columns:
        mask &= df["ticker"].isin(sym_set)
    filtered = df.loc[mask]
    if filtered.empty:
        return
    for i in range(0, len(filtered), chunk_size):
        yield filtered.iloc[i : i + chunk_size]


def _stream_news_csv(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    sym_set: set[str] | None,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    """Stream news from CSV files (HuggingFace format)."""
    df = pd.read_csv(path)
    if "Date" not in df.columns:
        return

    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    mask = (df["Date"] >= start_ts) & (df["Date"] < end_ts)

    if sym_set and "Stock_symbol" in df.columns:
        mask &= df["Stock_symbol"].isin(sym_set)

    filtered = df.loc[mask]
    if filtered.empty:
        return

    filtered = filtered.rename(columns={
        "Date": "date",
        "Stock_symbol": "ticker",
        "Article_title": "headline"
    })

    for i in range(0, len(filtered), chunk_size):
        yield filtered.iloc[i : i + chunk_size]
