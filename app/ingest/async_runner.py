from __future__ import annotations
import asyncio
import time
from typing import Dict

from app.ingest.key_manager import KeyManager
from app.ingest.extractor import Extractor
from app.ingest.validator import validate_sources_yaml, validate_events
from app.ingest.registry import resolve_adapter
from app.ingest.event_model import Event
from app.ingest.metrics import metrics_registry
from app.ingest.dedupe import Deduper
from app.ingest.event_store import EventStore
from app.ingest.router import EventRouter
from app.ingest.runner_core import build_ctx, safe_adapter_fetch


async def _fetch_and_process(spec, key_manager: KeyManager, extractor: Extractor) -> tuple[list[Event], float, str | None, int]:
    adapter = resolve_adapter(spec.adapter)
    if not adapter:
        print(f"[ingest] mode=live source={spec.id} adapter={spec.adapter} error=unknown_adapter")
        return [], 0.0, "unknown_adapter", 0

    try:
        ctx = build_ctx(
            adapter_name=str(spec.adapter),
            source_id=str(spec.id),
            key_manager=key_manager,
            mode="live",
        )
        t0 = time.perf_counter()
        raw_rows = await safe_adapter_fetch(adapter, spec, ctx, timeout_s=10.0, retries=0)
        events = extractor.normalize_many(raw_rows, spec)
        valid_events = validate_events(events)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        return valid_events, latency_ms, None, len(raw_rows)
    except Exception as e:
        print(f"[ingest] mode=live source={spec.id} adapter={spec.adapter} error={type(e).__name__}:{e}")
        return [], 0.0, "exception", 0

async def fetch_all_sources_async(path: str = "config/sources.yaml") -> Dict[str, list[Event]]:
    specs = validate_sources_yaml(path)
    key_manager = KeyManager()
    extractor = Extractor()
    
    # State singletons for the run
    deduper = Deduper()
    store = EventStore()
    router = EventRouter()

    tasks: list[tuple[object, asyncio.Task[tuple[list[Event], float, str | None, int]]]] = []
    for spec in specs:
        if not spec.enabled:
            continue
        tasks.append((spec, asyncio.create_task(_fetch_and_process(spec, key_manager, extractor))))

    # Gather tasks
    gather_results = await asyncio.gather(*(t[1] for t in tasks), return_exceptions=True)
    
    all_unique_events = []
    
    for i, (spec, _) in enumerate(tasks):
        result = gather_results[i]
        
        if isinstance(result, Exception):
            metrics_registry.record_error(spec.id)
            continue
            
        events, latency_ms, err_kind, raw_rows_count = result
        if err_kind is not None:
            metrics_registry.record_error(spec.id)
            continue
        
        # 1. Dedupe
        unique_events, dropped_count = deduper.process(events)
        all_unique_events.extend(unique_events)
        
        # 2. Record metrics
        metrics_registry.record_fetch_success(
            source_id=spec.id,
            new_events=len(unique_events),
            dropped=dropped_count,
            latency_ms=float(latency_ms),
        )

        # 3. Per-source timing log (single line; safe under concurrency)
        print(
            f"[ingest] mode=live source={spec.id} adapter={spec.adapter} raw={raw_rows_count} "
            f"valid={len(events)} unique={len(unique_events)} dropped={dropped_count} "
            f"total_ms={latency_ms:.2f}"
        )

    # 4. Persistence
    inserted_db = store.save_batch(all_unique_events)
    print(f"[ingest] mode=live complete unique={len(all_unique_events)} inserted={inserted_db}")

    # 5. Routing
    routed = router.route(all_unique_events)
    return routed


def main() -> int:
    path = "config/sources.yaml"
    try:
        import os

        path = str(os.getenv("ALPHA_SOURCES_YAML", path) or path)
    except Exception:
        pass

    try:
        asyncio.run(fetch_all_sources_async(path))
        return 0
    except KeyboardInterrupt:
        print("[ingest] mode=live interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
