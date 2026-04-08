from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class MarketBreadthAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        ts = ctx.start_date or ctx.run_timestamp
        
        # In a real impl, we would fetch a universe snapshot (e.g. from Alpaca/Polygon)
        # and count tickers with close > prev_close.
        
        # Mock breadth stats
        advancers = 312
        decliners = 188
        total = advancers + decliners
        ratio = advancers / decliners if decliners > 0 else 1.0
        participation = total / 500.0 # vs S&P 500
        
        return [
            {
                "timestamp": ts,
                "advancers": advancers,
                "decliners": decliners,
                "breadth_ratio": ratio,
                "participation": participation,
                "risk_on_score": 0.65 if ratio > 1.2 else 0.4,
                "provider": "breadth_calculator"
            }
        ]
