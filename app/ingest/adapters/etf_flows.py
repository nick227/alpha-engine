from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers

class EtfFlowsAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        fmp_cfg = ctx.key_manager.get("fmp")
        api_key = None
        if isinstance(fmp_cfg, dict):
            api_key = str(fmp_cfg.get("apikey") or fmp_cfg.get("api_key") or fmp_cfg.get("key") or "").strip() or None
        elif isinstance(fmp_cfg, str):
            api_key = fmp_cfg.strip() or None

        if not api_key:
            return []
        
        symbols = spec.symbols if isinstance(spec.symbols, list) else ["SPY", "QQQ"]
        
        results = []
        for symbol in symbols:
            etf_fetch = {
                "kind": "http_json",
                "url": "https://financialmodelingprep.com/stable/etf/holdings",
                "params": {"symbol": symbol, "apikey": api_key},
                "timeout_s": 15,
            }
            
            etf_data = await adapter_helpers.fetch_json(etf_fetch, ctx)
            if etf_data:
                results.extend(etf_data)
        
        return results
