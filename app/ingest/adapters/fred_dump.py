"""
FredDumpAdapter — reads pre-downloaded FRED bulk macro series.

Expected layout::

    data/raw_dumps/fred/{SERIES_ID}.parquet

Parquet schema::

    date      : timestamp[us, tz=UTC]
    series_id : string
    value     : float64

Download helper::

    python scripts/download_fred_dump.py --series FEDFUNDS T10Y2Y UNRATE CPIAUCSL
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from app.ingest.adapters.dump_adapter import DumpAdapter, _coerce_utc

# Default series to load when the spec doesn't specify.
_DEFAULT_SERIES = ["FEDFUNDS", "T10Y2Y", "UNRATE", "CPIAUCSL"]


class FredDumpAdapter(DumpAdapter):
    """
    Streams FRED macro indicator parquet files.

    One parquet file per series.  Series names are read from
    ``spec.options["series"]`` (list) or the ``_DEFAULT_SERIES`` fallback.
    """

    _COLUMNS = ["date", "series_id", "value"]

    def __init__(self, dump_dir: str | Path = Path("data/raw_dumps/fred")):
        self.dump_dir = Path(dump_dir)

    def _path(self, series_id: str) -> Path:
        return self.dump_dir / f"{series_id.upper()}.parquet"

    def _resolve_dir(self, spec: Any) -> Path:
        override = (getattr(spec, "options", None) or {}).get("dump_dir")
        return Path(override) if override else self.dump_dir

    def _series_list(self, spec: Any) -> list[str]:
        opts = getattr(spec, "options", None) or {}
        raw = opts.get("series", _DEFAULT_SERIES)
        if isinstance(raw, list):
            return [str(s).strip().upper() for s in raw if s]
        if isinstance(raw, str):
            return [s.strip().upper() for s in raw.split(",") if s.strip()]
        return _DEFAULT_SERIES

    # ------------------------------------------------------------------ #
    # DumpAdapter interface                                                #
    # ------------------------------------------------------------------ #

    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        if not self.dump_dir.exists():
            return False
        return any(self._path(sid).exists() for sid in _DEFAULT_SERIES)

    def load_slice(
        self,
        start: datetime,
        end: datetime,
        symbols: list[str],
        chunk_size: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        # symbols arg is unused for FRED (global macro, not per-ticker)
        start_ts = pd.Timestamp(start, tz="UTC") if getattr(start, "tzinfo", None) is None else pd.Timestamp(start).tz_convert("UTC")
        end_ts = pd.Timestamp(end, tz="UTC") if getattr(end, "tzinfo", None) is None else pd.Timestamp(end).tz_convert("UTC")

        try:
            import pyarrow.parquet as pq
            _use_pyarrow = True
        except ImportError:
            _use_pyarrow = False

        for sid in _DEFAULT_SERIES:
            path = self._path(sid)
            if not path.exists():
                continue

            if _use_pyarrow:
                yield from _stream_series_pyarrow(path, start_ts, end_ts, sid, chunk_size)
            else:
                yield from _stream_series_pandas(path, start_ts, end_ts, sid, chunk_size)

    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Vectorised conversion."""
        out = df[[c for c in self._COLUMNS if c in df.columns]].copy()
        if "date" in out.columns and pd.api.types.is_datetime64_any_dtype(out["date"]):
            out["date"] = out["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return out.to_dict("records")

    async def fetch_raw(self, spec: Any, ctx: Any) -> list[dict[str, Any]]:
        original_dir = self.dump_dir
        self.dump_dir = self._resolve_dir(spec)
        # Use series from spec if available
        original_default = list(_DEFAULT_SERIES)
        series = self._series_list(spec)
        _DEFAULT_SERIES.clear()
        _DEFAULT_SERIES.extend(series)
        try:
            return await super().fetch_raw(spec, ctx)
        finally:
            self.dump_dir = original_dir
            _DEFAULT_SERIES.clear()
            _DEFAULT_SERIES.extend(original_default)


# ------------------------------------------------------------------ #
# Streaming helpers                                                    #
# ------------------------------------------------------------------ #

def _stream_series_pyarrow(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    series_id: str,
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
        if "series_id" not in filtered.columns:
            filtered = filtered.copy()
            filtered["series_id"] = series_id
        yield filtered


def _stream_series_pandas(
    path: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    series_id: str,
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
    if "series_id" not in filtered.columns:
        filtered = filtered.copy()
        filtered["series_id"] = series_id
    for i in range(0, len(filtered), chunk_size):
        yield filtered.iloc[i : i + chunk_size]
