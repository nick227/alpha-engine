from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class FredMacroAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        _keys = ctx.key_manager.get("fred")
        series = spec.options.get("series", "FEDFUNDS")
        
        return [
            {
                "date": ctx.run_timestamp,
                "series_value": 4.25,
                "series_name": series,
                "provider": "fred",
            }
        ]
