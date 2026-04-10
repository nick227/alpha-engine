from __future__ import annotations
from typing import Any

from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext


class CrossAssetAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        try:
            import yfinance as yf  # type: ignore
            import pandas as pd
        except Exception as e:
            raise ImportError("yfinance is not installed; cannot use cross_asset adapter.") from e

        symbols = spec.options.get("symbols", ["CL=F", "GC=F", "DX-Y.NYB", "^VIX", "^TNX"])
        
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
                
                # Convert wide row to row-per-symbol format
                for _, row in df.iterrows():
                    timestamp = row['Datetime']
                    
                    # Extract symbols from column names
                    symbols = set()
                    for col in df.columns:
                        if '_' in col and any(col.startswith(prefix) for prefix in ['Close_', 'Open_', 'High_', 'Low_', 'Volume_', 'Adj Close_']):
                            symbol_part = col.split('_', 1)[1]
                            symbols.add(symbol_part)
                    
                    # Create one row per symbol
                    for symbol in symbols:
                        record = {'timestamp': timestamp, 'symbol': symbol}
                        
                        # Map OHLCV fields for this symbol
                        field_mappings = {
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low', 
                            'Close': 'close',
                            'Adj Close': 'adj_close',
                            'Volume': 'volume'
                        }
                        
                        for yf_field, normal_field in field_mappings.items():
                            col_name = f"{yf_field}_{symbol}"
                            if col_name in row and pd.notna(row[col_name]):
                                record[normal_field] = row[col_name]
                        
                        results.append(record)
        except Exception:
            pass
        
        return results
