from __future__ import annotations
from datetime import timedelta
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers
from app.core.time_utils import to_utc_datetime

class AlpacaNewsAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        alpaca_key = ctx.key_manager.get("alpaca")
        key_id = None
        secret = None
        if isinstance(alpaca_key, dict):
            key_id = str(alpaca_key.get("key_id") or alpaca_key.get("APCA_API_KEY_ID") or "").strip() or None
            secret = str(alpaca_key.get("secret_key") or alpaca_key.get("APCA_API_SECRET_KEY") or "").strip() or None
        elif isinstance(alpaca_key, str):
            key_id = alpaca_key.strip() or None
            secret = ""

        # Treat empty values as "not configured" to keep offline runs fast.
        if not key_id or secret is None:
            return []
        
        symbols = spec.symbols if isinstance(spec.symbols, list) else ["NVDA", "AAPL"]
        
        start_dt = to_utc_datetime(ctx.start_date or ctx.run_timestamp).replace(microsecond=0)
        end_dt = to_utc_datetime(ctx.end_date or start_dt).replace(microsecond=0)
        if end_dt <= start_dt:
            end_dt = start_dt + timedelta(seconds=1)
        
        results = []
        fetch = {
            "kind": "http_json",
            "url": "https://data.alpaca.markets/v1beta1/news",
            "params": {
                "symbols": ",".join([str(s).strip().upper() for s in symbols if str(s).strip()]),
                "start": start_dt.isoformat().replace("+00:00", "Z"),
                "end": end_dt.isoformat().replace("+00:00", "Z"),
                "limit": 50,
                "sort": "asc",
            },
            "headers": {
                "APCA-API-KEY-ID": str(key_id),
                "APCA-API-SECRET-KEY": str(secret or ""),
            },
            "rows_path": "news",
            "timeout_s": 15,
        }

        news_rows = await adapter_helpers.fetch_json(fetch, ctx)
        if news_rows:
            results.extend(news_rows)
        
        return results
