from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class FredMacroAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        _keys = ctx.key_manager.get("fred")
        
        ts = ctx.start_date or ctx.run_timestamp
        
        if spec.id == "yield_curve_spread":
            # Mock DGS10 - DGS2
            dgs10 = 4.2
            dgs2 = 4.5
            spread = dgs10 - dgs2
            regime = "inverted" if spread < 0 else "steepening"
            
            return [
                {
                    "timestamp": ts,
                    "spread": spread,
                    "regime": regime,
                    "provider": "fred_yield_curve",
                }
            ]
            
        series = spec.options.get("series", "FEDFUNDS")
        return [
            {
                "date": ts,
                "series_value": 4.25,
                "series_name": series,
                "provider": "fred",
            }
        ]
