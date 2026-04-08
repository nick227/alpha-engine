from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class AlpacaNewsAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        # This will eventually be an aiohttp call.
        await ctx.rate_limiter.throttle()
        _keys = ctx.key_manager.get("alpaca")
        
        symbols = spec.symbols if isinstance(spec.symbols, list) else ["NVDA", "AAPL"]
        raw_events: list[dict[str, Any]] = []
        for symbol in symbols:
            raw_events.append({
                "created_at": ctx.run_timestamp,
                "symbols": symbol,
                "headline": f"Alpaca headline for {symbol}",
                "summary": "This is a sample summary for Alpaca news.",
                "provider": "alpaca"
            })
        return raw_events
