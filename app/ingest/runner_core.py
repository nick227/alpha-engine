from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from app.ingest.fetch_context import FetchContext
from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter


_rate_limiters: dict[str, RateLimiter] = {}


def get_limiter(provider: str) -> RateLimiter:
    key = str(provider or "unknown").strip().lower() or "unknown"
    if key not in _rate_limiters:
        _rate_limiters[key] = RateLimiter(key)
    return _rate_limiters[key]


def provider_for_adapter(adapter_name: str, *, source_id: str | None = None) -> str:
    a = str(adapter_name or "").strip().lower()
    if "alpaca" in a:
        return "alpaca"
    if "reddit" in a:
        return "reddit"
    if "fred" in a:
        return "fred"
    if "yahoo" in a or "yfinance" in a:
        return "yahoo"
    return a or str(source_id or "unknown")


def build_ctx(
    *,
    adapter_name: str,
    source_id: str,
    key_manager: KeyManager,
    cache_handle: Any | None = None,
    mode: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> FetchContext:
    provider = provider_for_adapter(adapter_name, source_id=source_id)
    return FetchContext(
        provider=provider,
        key_manager=key_manager,
        rate_limiter=get_limiter(provider),
        cache_handle=cache_handle,
        run_timestamp=datetime.now(timezone.utc),
        start_date=start_date,
        end_date=end_date,
        run_metadata={"mode": str(mode or "").strip().lower(), "source_id": str(source_id)},
    )


def coerce_rows(rows: Any) -> list[dict[str, Any]]:
    if not rows:
        return []
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    if isinstance(rows, dict):
        return [rows]
    return []


async def safe_adapter_fetch(
    adapter: Any,
    spec: Any,
    ctx: FetchContext,
    *,
    timeout_s: float = 10.0,
    retries: int = 0,
) -> list[dict[str, Any]]:
    """
    Runner-side safety wrapper.

    Adapters are responsible for HTTP retry/rate-limit behavior via shared helpers.
    This wrapper only prevents a single adapter from blocking the entire run.
    """
    if timeout_s <= 0:
        timeout_s = 10.0

    async def _run_once() -> list[dict[str, Any]]:
        task = asyncio.create_task(adapter.fetch_raw(spec, ctx))
        done, _pending = await asyncio.wait({task}, timeout=float(timeout_s))
        if task in done:
            if task.cancelled():
                return []
            try:
                rows = coerce_rows(task.result())
                adapter_name = str(getattr(spec, "adapter", None) or getattr(adapter, "__class__", type("x", (), {})).__name__)
                source_id = str(getattr(spec, "id", "") or "")
                # Fetch-layer diagnostic log (no validation, no branching).
                try:
                    print(f"[ingest] {adapter_name} -> {len(rows)} rows (source={source_id})")
                except Exception:
                    pass
                return rows
            except asyncio.CancelledError:
                return []
            except Exception:
                return []

        # Hard timeout: cancel but do not await completion (adapters may be stuck in threads).
        task.cancel()

        def _drain(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            try:
                _ = t.exception()
            except asyncio.CancelledError:
                return
            except Exception:
                return

        task.add_done_callback(_drain)
        return []

    attempt = 0
    while True:
        out = await _run_once()
        if out or attempt >= int(retries):
            return out
        attempt += 1
        await asyncio.sleep(0)
