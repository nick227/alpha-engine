"""
DumpAdapter — base class for static-dump adapters.

Reads pre-downloaded parquet files instead of making live API calls.
Streams data in row-group chunks; never loads the full dataset into memory.
"""
from __future__ import annotations

import abc
from datetime import datetime
from typing import Any, Iterator, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from app.ingest.source_spec import SourceSpec
    from app.ingest.fetch_context import FetchContext


class DumpAdapter(abc.ABC):
    """
    Abstract base for dump-file adapters.

    Subclasses must implement:
        has_data(start, end, symbols) -> bool
        load_slice(start, end, symbols, chunk_size) -> Iterator[pd.DataFrame]
        to_raw_dicts(df) -> list[dict]

    The concrete ``fetch_raw`` wires these together and satisfies the
    ``SourceAdapter`` protocol used by the rest of the ingest pipeline.
    """

    # ------------------------------------------------------------------ #
    # Abstract interface                                                   #
    # ------------------------------------------------------------------ #

    @abc.abstractmethod
    def has_data(self, start: datetime, end: datetime, symbols: list[str]) -> bool:
        """Return True if this dump covers [start, end) for at least one symbol."""
        ...

    @abc.abstractmethod
    def load_slice(
        self,
        start: datetime,
        end: datetime,
        symbols: list[str],
        chunk_size: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        """
        Yield DataFrame chunks for [start, end) filtered to *symbols*.

        Requirements:
        - Stream row-groups; never read the whole file at once.
        - Vectorised date filtering (no Python loops over rows).
        - Each yielded chunk has at most *chunk_size* rows.
        """
        ...

    @abc.abstractmethod
    def to_raw_dicts(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """
        Convert a DataFrame chunk to raw dicts for the Extractor pipeline.

        Use vectorised pandas operations (``to_dict("records")``, etc.).
        No Python loops over individual rows.
        """
        ...

    # ------------------------------------------------------------------ #
    # SourceAdapter-compatible entrypoint                                  #
    # ------------------------------------------------------------------ #

    async def fetch_raw(self, spec: Any, ctx: Any) -> list[dict[str, Any]]:
        """
        Wire ``load_slice`` / ``to_raw_dicts`` into the ``SourceAdapter``
        protocol.  Returns an empty list when no dump data is available
        (the backfill runner will decide whether to fall back to API).
        """
        start: datetime | None = getattr(ctx, "start_date", None)
        end: datetime | None = getattr(ctx, "end_date", None)
        if start is None or end is None:
            return []

        symbols: list[str] = _extract_symbols(spec)

        if not self.has_data(start, end, symbols):
            return []

        results: list[dict[str, Any]] = []
        for chunk in self.load_slice(start, end, symbols):
            if not chunk.empty:
                results.extend(self.to_raw_dicts(chunk))
        return results


# ------------------------------------------------------------------ #
# Shared helpers                                                       #
# ------------------------------------------------------------------ #

def _extract_symbols(spec: Any) -> list[str]:
    """Pull symbols list from a SourceSpec (handles str or list)."""
    raw = getattr(spec, "symbols", None)
    if isinstance(raw, list):
        return [str(s).strip().upper() for s in raw if s]
    if isinstance(raw, str) and raw.strip():
        return [s.strip().upper() for s in raw.split(",") if s.strip()]
    return []


def _coerce_utc(ts_series: pd.Series) -> pd.Series:
    """Vectorised: convert a datetime Series to UTC-aware timestamps."""
    if not pd.api.types.is_datetime64_any_dtype(ts_series):
        ts_series = pd.to_datetime(ts_series, utc=True, errors="coerce")
    elif ts_series.dt.tz is None:
        ts_series = ts_series.dt.tz_localize("UTC")
    else:
        ts_series = ts_series.dt.tz_convert("UTC")
    return ts_series
