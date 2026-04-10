from __future__ import annotations
from typing import Any

from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext


class MarketBaselineAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        try:
            import yfinance as yf  # type: ignore
            import pandas as pd
        except Exception as e:
            raise ImportError("yfinance is not installed; cannot use market_baseline adapter.") from e

        symbols = spec.symbols if isinstance(spec.symbols, list) else ["SPY", "QQQ", "IWM"]
        
        results = []
        try:
            df = yf.download(
                tickers=symbols,
                period="5d",
                interval="1h",
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
                
                # Handle different date column names
                date_col = 'Datetime' if 'Datetime' in df.columns else 'Date'
                if date_col != 'Datetime':
                    df['Datetime'] = df[date_col]
                
                records = df.to_dict("records")
                for record in records:
                    record["symbol"] = "market_baseline"
                    results.append(record)
        except Exception:
            pass
        
        return results
