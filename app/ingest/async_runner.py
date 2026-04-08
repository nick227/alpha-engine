from __future__ import annotations
import asyncio
from typing import Any

from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter
from app.ingest.fetch_context import FetchContext
from app.ingest.extractor import Extractor
from app.ingest.validator import validate_sources_yaml, validate_events
from app.ingest.registry import resolve_adapter
from app.ingest.event_model import Event

# Global rate limiters cache to persist state across fetches in a long-running app
_rate_limiters: dict[str, RateLimiter] = {}

def get_limiter(provider: str) -> RateLimiter:
    if provider not in _rate_limiters:
        _rate_limiters[provider] = RateLimiter(provider)
    return _rate_limiters[provider]

async def _fetch_and_process(spec, key_manager: KeyManager, extractor: Extractor) -> list[Event]:
    adapter = resolve_adapter(spec.adapter)
    if not adapter:
        print(f"Skipping unknown adapter: {spec.adapter}")
        return []

    # Some basic heuristic to determine provider or default to spec id
    provider = "unknown"
    if "alpaca" in spec.adapter:
        provider = "alpaca"
    elif "reddit" in spec.adapter:
        provider = "reddit"
    elif "fred" in spec.adapter:
        provider = "fred"
    elif "yahoo" in spec.adapter:
        provider = "yahoo"
    else:
        provider = spec.id

    limiter = get_limiter(provider)
    ctx = FetchContext(
        provider=provider,
        key_manager=key_manager,
        rate_limiter=limiter,
    )

    try:
        raw_rows = await adapter.fetch_raw(spec, ctx)
        events = extractor.normalize_many(raw_rows, spec)
        return validate_events(events)
    except Exception as e:
        print(f"Error fetching from {spec.id} using {spec.adapter}: {e}")
        return []

async def fetch_all_sources_async(path: str = "config/sources.yaml") -> list[Event]:
    specs = validate_sources_yaml(path)
    key_manager = KeyManager()
    extractor = Extractor()

    tasks = []
    for spec in specs:
        if not spec.enabled:
            continue
        tasks.append(_fetch_and_process(spec, key_manager, extractor))

    results = await asyncio.gather(*tasks)
    
    # Flatten the list of lists
    all_events = []
    for events_batch in results:
        all_events.extend(events_batch)
        
    return all_events
