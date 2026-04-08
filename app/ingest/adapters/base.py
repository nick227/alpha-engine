from __future__ import annotations
from typing import Protocol, Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class SourceAdapter(Protocol):
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]: ...
