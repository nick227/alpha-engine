from __future__ import annotations
import asyncio
from typing import Dict

from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter
from app.ingest.fetch_context import FetchContext
from app.ingest.extractor import Extractor
from app.ingest.validator import validate_sources_yaml, validate_events
from app.ingest.registry import resolve_adapter
from app.ingest.event_model import Event
from app.ingest.metrics import metrics_registry
from app.ingest.dedupe import Deduper
from app.ingest.event_store import EventStore
from app.ingest.router import EventRouter

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
        valid_events = validate_events(events)
        
        return valid_events
    except Exception as e:
        print(f"Error fetching from {spec.id} using {spec.adapter}: {e}")
        metrics_registry.record_error(spec.id)
        return []

async def fetch_all_sources_async(path: str = "config/sources.yaml") -> Dict[str, list[Event]]:
    specs = validate_sources_yaml(path)
    key_manager = KeyManager()
    extractor = Extractor()
    
    # State singletons for the run
    deduper = Deduper()
    store = EventStore()
    router = EventRouter()

    tasks = []
    for spec in specs:
        if not spec.enabled:
            continue
        tasks.append((spec, _fetch_and_process(spec, key_manager, extractor)))

    # Gather tasks
    gather_results = await asyncio.gather(*(t[1] for t in tasks), return_exceptions=True)
    
    all_unique_events = []
    
    for i, (spec, _) in enumerate(tasks):
        result = gather_results[i]
        
        if isinstance(result, Exception):
            metrics_registry.record_error(spec.id)
            continue
            
        events: list[Event] = result
        
        # 1. Dedupe
        unique_events, dropped_count = deduper.process(events)
        all_unique_events.extend(unique_events)
        
        # 2. Record metrics
        metrics_registry.record_fetch_success(
            source_id=spec.id,
            new_events=len(unique_events),
            dropped=dropped_count,
            latency_ms=10.0 # Placeholder latency tracking for now
        )

    # 3. Persistence
    inserted_db = store.save_batch(all_unique_events)
    print(f"Ingestion run complete. {len(all_unique_events)} unique, {inserted_db} new to DB.")

    # 4. Routing
    routed = router.route(all_unique_events)
    return routed
