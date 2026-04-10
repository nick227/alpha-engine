from __future__ import annotations
from typing import Any

from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class YahooFinanceAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        """
        Historical-friendly market feed using yfinance OHLCV as synthetic "market events".

        Emits rows compatible with sources.yaml for `yahoo_market_watch`:
          - symbol
          - published_at
          - headline
          - open/high/low/close/volume (optional)
        """
        try:
            import yfinance as yf  # type: ignore
            import pandas as pd  # type: ignore
        except Exception as e:
            raise ImportError("yfinance is not installed; cannot use yahoo_finance adapter.") from e

        symbols = spec.symbols if isinstance(spec.symbols, list) else ["SPY", "QQQ"]
        start_dt = ctx.start_date or ctx.run_timestamp
        end_dt = ctx.end_date or start_dt
        if end_dt <= start_dt:
            end_dt = start_dt

        # yfinance `end` is exclusive-ish; keep slices inclusive by adding a small buffer.
        end_dt_fetch = end_dt
        if hasattr(end_dt_fetch, "replace"):
            end_dt_fetch = end_dt_fetch.replace(microsecond=0)

        interval = str((spec.options or {}).get("interval") or "1h").strip() or "1h"

        try:
            df = yf.download(
                tickers=symbols,
                start=start_dt,
                end=end_dt_fetch,
                interval=interval,
                progress=False,
                auto_adjust=False,
                actions=False,
                threads=False,
            )
        except Exception:
            return []

        if df is None or getattr(df, "empty", True):
            return []

        df = df.reset_index()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(x) for x in col if x]) for col in df.columns]

        # Handle different date column names.
        date_col = "Datetime" if "Datetime" in df.columns else ("Date" if "Date" in df.columns else None)
        if date_col and date_col != "Datetime":
            df["Datetime"] = df[date_col]

        # Determine available symbols by column scanning (e.g. Close_SPY).
        found_symbols: set[str] = set()
        for col in df.columns:
            if "_" not in col:
                continue
            head, tail = col.split("_", 1)
            if head in {"Open", "High", "Low", "Close", "Volume", "Adj Close"} and tail:
                found_symbols.add(tail)

        results: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            ts = row.get("Datetime")
            if ts is None:
                continue
            for sym in sorted(found_symbols):
                close = row.get(f"Close_{sym}")
                record: dict[str, Any] = {
                    "symbol": sym,
                    "published_at": ts,
                    "headline": f"{sym} close {close}" if close is not None else f"{sym} market update",
                }
                for k in ("Open", "High", "Low", "Close", "Volume"):
                    v = row.get(f"{k}_{sym}")
                    if v is not None:
                        record[k.lower()] = v
                results.append(record)

        return results
