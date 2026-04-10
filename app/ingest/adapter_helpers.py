from __future__ import annotations

import asyncio
from typing import Any

from app.ingest.fetch_context import FetchContext
from app.ingest.source_spec import SourceSpec

from app.ingest import fetchers


def _coerce_rows(rows: Any) -> list[dict[str, Any]]:
    if not rows:
        return []
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    if isinstance(rows, dict):
        return [rows]
    return []


def _timeout_s(spec_or_fetch: Any) -> float:
    fetch = None
    if isinstance(spec_or_fetch, SourceSpec):
        fetch = spec_or_fetch.fetch
    elif isinstance(spec_or_fetch, dict):
        fetch = spec_or_fetch
    else:
        fetch = getattr(spec_or_fetch, "fetch", None)

    # fetch can be a Pydantic model or a dict-like. Both commonly expose timeout_s.
    t = None
    try:
        if isinstance(fetch, dict):
            t = fetch.get("timeout_s")
        else:
            t = getattr(fetch, "timeout_s", None)
    except Exception:
        t = None

    try:
        t_f = float(t or 30)
    except Exception:
        t_f = 30.0
    return max(0.1, t_f)


async def fetch_json(spec: Any, ctx: FetchContext, *, retries: int = 2) -> list[dict[str, Any]]:
    """
    Shared adapter helper: fetch JSON rows with consistent behavior.

    - Automatic rate limiting (ctx.rate_limiter)
    - Automatic retries (max 2 by default)
    - Safe timeout bounding (asyncio.wait_for)
    - Always returns list[dict]
    """
    attempt = 0
    while True:
        try:
            await ctx.rate_limiter.throttle()
            # spec can be SourceSpec, a FetchSpec-like object, or a dict fetch payload.
            rows = await asyncio.wait_for(fetchers.fetch_rows(spec, ctx), timeout=_timeout_s(spec))
            return _coerce_rows(rows)
        except asyncio.CancelledError:
            raise
        except Exception:
            if attempt >= int(retries):
                return []
            attempt += 1
            # Keep backoff tiny; adapters should remain fast and deterministic in tests.
            await asyncio.sleep(0)


async def fetch_json_paginated(
    spec: SourceSpec,
    ctx: FetchContext,
    *,
    page_param: str = "page",
    start_page: int = 1,
    max_pages: int = 10,
    retries: int = 2,
) -> list[dict[str, Any]]:
    """
    Optional pagination helper for page-based JSON endpoints.

    This is intentionally small and generic: it increments `page_param` and stops on the
    first empty page or when `max_pages` is reached.
    """
    if spec.fetch is None:
        return await fetch_json(spec, ctx, retries=retries)

    all_rows: list[dict[str, Any]] = []
    for page in range(int(start_page), int(start_page) + int(max_pages)):
        params = dict(spec.fetch.params or {})
        params[str(page_param)] = page

        fetch_spec = spec.fetch.model_copy(update={"params": params})
        page_spec = spec.model_copy(update={"fetch": fetch_spec})

        rows = await fetch_json(page_spec, ctx, retries=retries)
        if not rows:
            break
        all_rows.extend(rows)
    
    return all_rows
