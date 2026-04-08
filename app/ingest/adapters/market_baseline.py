from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class MarketBaselineAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        ts = ctx.start_date or ctx.run_timestamp
        
        # In a real impl, fetch SPY, QQQ, IWM returns for the requested period.
        return [
            {
                "timestamp": ts,
                "spy_return_1h": 0.002,
                "qqq_return_1h": 0.0035,
                "iwm_return_1h": -0.001,
                "spy_return_1d": 0.012,
                "provider": "market_baseline"
            }
        ]
