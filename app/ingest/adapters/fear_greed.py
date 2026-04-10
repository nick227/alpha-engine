from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers

class FearGreedAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        fng_fetch = {
            "kind": "http_json",
            "url": "https://api.alternative.me/fng/",
            "params": {"limit": 2, "format": "json"},
            "timeout_s": 10,
        }

        fng_data = await adapter_helpers.fetch_json(fng_fetch, ctx)
        
        return fng_data or []
