from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class EtfFlowsAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        # In a real impl, we would use FMP_API_KEY and fetch from /v3/etf-holder/{symbol}
        # fmp_key = ctx.key_manager.get("fmp")
        
        symbols = spec.symbols if isinstance(spec.symbols, list) else ["SPY", "QQQ"]
        ts = ctx.start_date or ctx.run_timestamp
        
        return [
            {
                "symbol": symbol,
                "inflow": 150000000.0, # Mock $150M inflow
                "timestamp": ts,
                "provider": "fmp_etf"
            }
            for symbol in symbols
        ]
