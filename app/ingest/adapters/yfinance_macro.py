from __future__ import annotations
from typing import Any

from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext


class YFinanceMacroAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        try:
            import yfinance as yf  # type: ignore
            import pandas as pd
        except Exception as e:
            raise ImportError("yfinance is not installed; cannot use yfinance_macro adapter.") from e

        symbols = spec.options.get("symbols", [])
        if not symbols:
            return []

        results = []
        for symbol in symbols:
            try:
                df = yf.download(
                    tickers=symbol,
                    period="10d",
                    interval="1d",
                    progress=False,
                    auto_adjust=False,
                    actions=False,
                    threads=False,
                )
                
                if df is not None and not getattr(df, "empty", True):
                    df = df.reset_index()
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = [
                            "_".join([str(x) for x in col if x])
                            for col in df.columns
                        ]
                    records = df.to_dict("records")
                    for record in records:
                        record["symbol"] = symbol
                        results.append(record)
            except Exception:
                continue
        
        return results

