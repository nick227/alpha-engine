from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers

class GoogleTrendsAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        serp_cfg = ctx.key_manager.get("serpapi")
        api_key = None
        if isinstance(serp_cfg, dict):
            api_key = str(serp_cfg.get("api_key") or serp_cfg.get("key") or serp_cfg.get("token") or "").strip() or None
        elif isinstance(serp_cfg, str):
            api_key = serp_cfg.strip() or None

        if not api_key:
            return []
        
        keywords = spec.options.get("keywords", ["bitcoin", "stock market", "inflation"])
        
        results = []
        for keyword in keywords:
            trends_fetch = {
                "kind": "http_json",
                "url": "https://serpapi.com/search.json",
                "params": {"engine": "google_trends", "q": keyword, "api_key": api_key, "date": "now 7-d"},
                "timeout_s": 15,
            }
            
            trends_data = await adapter_helpers.fetch_json(trends_fetch, ctx)
            if trends_data:
                results.extend(trends_data)
        
        return results
