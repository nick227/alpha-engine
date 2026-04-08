from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class EarningsCalendarAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        ts = ctx.start_date or ctx.run_timestamp
        
        # Mock earnings events
        return [
            {"symbol": "AAPL", "date": ts, "provider": "fmp_earnings"},
            {"symbol": "TSLA", "date": ts, "provider": "fmp_earnings"},
        ]
