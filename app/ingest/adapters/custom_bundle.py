from __future__ import annotations
import json
import os
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class CustomBundleAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        # Synchronous internal IO is fine here per user constraints
        path = spec.options.get("file")
        if not path or not os.path.exists(path):
            return []

        with open(path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        return rows
