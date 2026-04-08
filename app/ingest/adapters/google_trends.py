from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class GoogleTrendsAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        # In a real impl, we would use SERPAPI_KEY from ctx.key_manager and fetch via aiohttp
        # serp_key = ctx.key_manager.get("serpapi")
        
        keywords = spec.options.get("keywords", ["bitcoin", "stock market", "inflation"])
        ts = ctx.start_date or ctx.run_timestamp
        
        results = []
        for kw in keywords:
            results.append({
                "keyword": kw,
                "value": 75, # Mock interest level 0-100
                "timestamp": ts,
                "provider": "google_trends"
            })
        return results
