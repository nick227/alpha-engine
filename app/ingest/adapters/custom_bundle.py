from __future__ import annotations
import json
import os
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.core.time_utils import to_utc_datetime

class CustomBundleAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        # Synchronous internal IO is fine here per user constraints
        path = spec.options.get("file")
        if not path or not os.path.exists(path):
            return []

        cache_key = f"custom_bundle:{os.path.abspath(path)}"
        rows: list[dict[str, Any]] | None = None
        if isinstance(ctx.cache_handle, dict):
            rows = ctx.cache_handle.get(cache_key)

        if rows is None:
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            rows = loaded if isinstance(loaded, list) else []
            if isinstance(ctx.cache_handle, dict):
                ctx.cache_handle[cache_key] = rows

        # Optional date filtering for backfill slices.
        if ctx.start_date and ctx.end_date:
            start_utc = to_utc_datetime(ctx.start_date)
            end_utc = to_utc_datetime(ctx.end_date)
            out: list[dict[str, Any]] = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                ts_val = None
                for key in ("timestamp", "created_at", "published_at", "created_utc", "date"):
                    if key in r and r.get(key) is not None:
                        ts_val = r.get(key)
                        break
                if ts_val is None:
                    continue
                ts = to_utc_datetime(ts_val)
                if start_utc <= ts < end_utc:
                    out.append(r)
            return out

        return rows
