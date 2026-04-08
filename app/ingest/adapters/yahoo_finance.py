from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class YahooFinanceAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        symbols = spec.symbols if isinstance(spec.symbols, list) else ["SPY", "QQQ"]
        
        return [
            {
                "published_at": ctx.run_timestamp,
                "symbol": symbol,
                "headline": f"{symbol} Yahoo Finance placeholder event",
                "provider": "yahoo",
            }
            for symbol in symbols
        ]

