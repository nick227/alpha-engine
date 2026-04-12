"""
StooqDumpAdapter — reads pre-downloaded STOOQ OHLCV price history.

Expected layout::

    data/raw_dumps/stooq/{SYMBOL}.parquet

Parquet schema::

    date    : timestamp[us, tz=UTC]
    symbol  : string
    open    : float32
    high    : float32
    low     : float32
    close   : float32
    volume  : int64

Download helper (run once)::

    python scripts/download_stooq_dump.py --symbols AAPL MSFT SPY
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from app.ingest.adapters.dump_adapter import DumpAdapter, _coerce_utc


class StooqDumpAdapter(DumpAdapter):
    """
    Streams STOOQ daily OHLCV parquet files.

    Each symbol has its own parquet file.  Row-group streaming via
    PyArrow prevents full-file loads; date filtering is vectorised.
    """

    _COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]

    def __init__(self, dump_dir: str | Path = Path("data/raw_dumps/stooq")):
        self.dump_dir = Path(dump_dir)

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _path(self, symbol: str) -> Path:
        return self.dump_dir / f"{symbol.upper()}.parquet"

    def _resolve_dir(self, spec: Any) -> Path:
        override = (getattr(spec, "options", None) or {}).get("dump_dir")
        return Path(override) if override else self.dump_dir

    # ------------------------------------------------------------------ #
    # DumpAdapter interface                                                #
    # ------------------------------------------------------------------ #

    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        if not self.dump_dir.exists():
            return False
        return any(self._path(sym).exists() for sym in symbols)

    def load_slice(
        self,
        start: datetime,
        end: datetime,
        symbols: list[str],
        chunk_size: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        start_ts = pd.Timestamp(start, tz="UTC") if getattr(start, "tzinfo", None) is None else pd.Timestamp(start).tz_convert("UTC")
        end_ts = pd.Timestamp(end, tz="UTC") if getattr(end, "tzinfo", None) is None else pd.Timestamp(end).tz_convert("UTC")

        try:
            import pyarrow.parquet as pq
            _use_pyarrow = True
        except ImportError:
            _use_pyarrow = False

        for sym in symbols:
            path = self._path(sym)
            if not path.exists():
                continue

            if _use_pyarrow:
                yield from _stream_parquet_pyarrow(
                    path, start_ts, end_ts, sym, chunk_size
                )
            else:
                yield from _stream_parquet_pandas(
                    path, start_ts, end_ts, sym, chunk_size
                )

    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Vectorised conversion — uses pandas C-layer, no Python row loops."""
        out = df[[c for c in self._COLUMNS if c in df.columns]].copy()
        if "date" in out.columns and pd.api.types.is_datetime64_any_dtype(out["date"]):
            out["date"] = out["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return out.to_dict("records")

    # Override fetch_raw to plumb dump_dir from spec.options
    async def fetch_raw(self, spec: Any, ctx: Any) -> list[dict[str, Any]]:
        original_dir = self.dump_dir
        self.dump_dir = self._resolve_dir(spec)
        try:
            return await super().fetch_raw(spec, ctx)
        finally:
            self.dump_dir = original_dir


# ------------------------------------------------------------------ #
# Streaming helpers (module-level, no class overhead)                  #
# ------------------------------------------------------------------ #

def _stream_parquet_pyarrow(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    sym: str,
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
        filtered = df.loc[mask]
        if filtered.empty:
            continue
        if "symbol" not in filtered.columns:
            filtered = filtered.copy()
            filtered["symbol"] = sym
        yield filtered


def _stream_parquet_pandas(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    sym: str,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    df = pd.read_parquet(str(path))
    if "date" not in df.columns:
        return
    df["date"] = _coerce_utc(df["date"])
    mask = (df["date"] >= start_ts) & (df["date"] < end_ts)
    filtered = df.loc[mask]
    if filtered.empty:
        return
    if "symbol" not in filtered.columns:
        filtered = filtered.copy()
        filtered["symbol"] = sym
    for i in range(0, len(filtered), chunk_size):
        yield filtered.iloc[i : i + chunk_size]
