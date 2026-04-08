from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class FearGreedAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        # In a real environment, we would use aiohttp to hit https://api.alternative.me/fng/
        # For this POC, we'll return a deterministic mock based on the run timestamp
        # to ensure backfill/replay cycles produce data.
        
        await ctx.rate_limiter.throttle()
        ts = ctx.start_date or ctx.run_timestamp
        
        # Mock sentiment score loop 0-100
        score = 45 
        label = "Neutral"
        extreme = 0
        
        if score > 75: 
            label = "Extreme Greed"
            extreme = 1
        elif score > 55: label = "Greed"
        elif score > 45: label = "Neutral"
        elif score < 25: 
            label = "Extreme Fear"
            extreme = -1
        
        return [
            {
                "fear_greed": score,
                "classification": label,
                "extreme": extreme,
                "delta": 0, # Mock delta
                "timestamp": ts,
                "provider": "alternative_me"
            }
        ]
