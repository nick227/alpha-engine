from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from app.ingest.dedupe import Deduper
from app.ingest.event_store import EventStore
from app.ingest.extractor import Extractor
from app.ingest.fetch_context import FetchContext
from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter
from app.ingest.registry import resolve_adapter
from app.ingest.router import EventRouter
from app.ingest.validator import validate_events_with_reasons, validate_sources_yaml


def test_ingest_smoke_no_network(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """
    Fast adapter integrity check (~50ms).

    Validates:
    - adapters load (registry)
    - config/sources.yaml parses + validates
    - at least one adapter returns raw rows without network
    - extractor produces valid Event(s)
    - validation drops nothing
    - dedupe drops duplicates
    - store insert works + is idempotent
    - router produces a non-empty route
    """

    def _no_network(*_args, **_kwargs):
        raise AssertionError("Network usage is not allowed in this smoke test.")

    # Guardrail: if anything tries to use the shared declarative HTTP fetcher, fail fast.
    import app.ingest.fetchers as fetchers

    monkeypatch.setattr(fetchers, "urlopen", _no_network)

    specs = validate_sources_yaml("config/sources.yaml")
    assert specs, "Expected at least one source spec in config/sources.yaml"

    # Adapters load: every adapter referenced by config resolves in the registry.
    missing = sorted({s.adapter for s in specs if resolve_adapter(s.adapter) is None})
    assert not missing, f"Missing adapters in registry: {missing}"

    # Pick a known-offline spec and run it through the full pipeline (no network required).
    spec = next((s for s in specs if s.id == "developer_bundle_ai_supply"), specs[0])
    adapter = resolve_adapter(spec.adapter)
    assert adapter is not None, f"Failed to resolve adapter: {spec.adapter}"

    ctx = FetchContext(
        provider="alpaca",
        key_manager=KeyManager(),
        rate_limiter=RateLimiter("alpaca"),
        cache_handle={},
        run_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    raw_rows = asyncio.run(adapter.fetch_raw(spec, ctx))
    assert isinstance(raw_rows, list), "Adapter must return a list"
    assert raw_rows, f"Adapter returned no rows: {spec.id} ({spec.adapter})"
    assert isinstance(raw_rows[0], dict), "Adapter rows must be dict payloads"

    extractor = Extractor()
    events = extractor.normalize_many([raw_rows[0]], spec)
    assert events, "Extractor must produce at least one Event"
    assert events[0].timestamp is not None, "Event.timestamp must not be None (bad extract mapping)"
    assert isinstance(events[0].numeric_features, dict), "Event.numeric_features must be a dict"

    valid, dropped = validate_events_with_reasons(events)
    assert valid, f"Expected at least one valid event; dropped={dropped}"
    assert sum(dropped.values()) == 0, f"Smoke event unexpectedly failed validation: dropped={dropped}"

    deduper = Deduper()
    unique, dropped_dupes = deduper.process(valid + valid)
    assert unique, "Deduper must return at least one unique event"
    assert dropped_dupes >= 1, "Deduper must drop at least one duplicate when given duplicates"

    store = EventStore(db_path=tmp_path / "ingest_smoke.db")
    inserted_1 = store.save_batch(unique)
    inserted_2 = store.save_batch(unique)
    assert inserted_1 == len(unique), "Store should insert unique events on first write"
    assert inserted_2 == 0, "Store should ignore duplicates on second write"

    router = EventRouter()
    routed = router.route(unique)
    assert routed, "Router must return a non-empty route map"
    assert sum(len(v) for v in routed.values()) == len(unique), "Router must not drop events"
