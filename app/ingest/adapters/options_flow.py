from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers

class OptionsFlowAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        poly_cfg = ctx.key_manager.get("polygon")
        api_key = None
        if isinstance(poly_cfg, dict):
            api_key = str(poly_cfg.get("api_key") or poly_cfg.get("apikey") or poly_cfg.get("key") or "").strip() or None
        elif isinstance(poly_cfg, str):
            api_key = poly_cfg.strip() or None

        if not api_key:
            return []
        
        symbols = spec.symbols if isinstance(spec.symbols, list) else ["SPY"]
        
        results = []
        for symbol in symbols:
            options_fetch = {
                "kind": "http_json",
                "url": "https://api.polygon.io/v3/reference/options/contracts",
                "params": {
                    "underlying_ticker": symbol,
                    "as_of": (ctx.start_date or ctx.run_timestamp).strftime("%Y-%m-%d"),
                    "limit": 100,
                    "sort": "trade_volume",
                    "order": "desc",
                    "apiKey": api_key,
                },
                "timeout_s": 15,
            }
            
            options_data = await adapter_helpers.fetch_json(options_fetch, ctx)
            if options_data:
                results.extend(options_data)
        
        return results
