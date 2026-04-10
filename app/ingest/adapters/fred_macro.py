from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers

class FredMacroAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        fred_cfg = ctx.key_manager.get("fred")
        api_key = None
        if isinstance(fred_cfg, dict):
            api_key = str(fred_cfg.get("api_key") or fred_cfg.get("key") or fred_cfg.get("token") or "").strip() or None
        elif isinstance(fred_cfg, str):
            api_key = fred_cfg.strip() or None

        if not api_key:
            return []
            
        ts = ctx.start_date or ctx.run_timestamp
        date_str = ts.strftime("%Y-%m-%d")
        
        if spec.id == "yield_curve_spread":
            # Fetch DGS10 and DGS2 series from FRED
            dgs10_fetch = {
                "kind": "http_json",
                "url": "https://api.stlouisfed.org/fred/series/observations",
                "params": {
                    "series_id": "DGS10",
                    "api_key": api_key,
                    "file_type": "json",
                    "observation_start": date_str,
                },
                "timeout_s": 10,
            }
            
            dgs2_fetch = {
                "kind": "http_json",
                "url": "https://api.stlouisfed.org/fred/series/observations",
                "params": {
                    "series_id": "DGS2",
                    "api_key": api_key,
                    "file_type": "json",
                    "observation_start": date_str,
                },
                "timeout_s": 10,
            }
            
            dgs10_data = await adapter_helpers.fetch_json(dgs10_fetch, ctx)
            dgs2_data = await adapter_helpers.fetch_json(dgs2_fetch, ctx)
            
            # Return raw API responses for extractor to handle
            return [{"dgs10_data": dgs10_data, "dgs2_data": dgs2_data}]
            
        series = spec.options.get("series", "FEDFUNDS")
        series_fetch = {
            "kind": "http_json",
            "url": "https://api.stlouisfed.org/fred/series/observations",
            "params": {
                "series_id": series,
                "api_key": api_key,
                "file_type": "json",
                "observation_start": date_str,
            },
            "timeout_s": 10,
        }
        
        series_data = await adapter_helpers.fetch_json(series_fetch, ctx)
        
        # Return raw API response for extractor to handle
        return series_data or []
