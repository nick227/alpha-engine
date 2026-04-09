from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone, timedelta

import pytest

from app.ingest.event_store import EventStore
from app.ingest.source_spec import SourceSpec, ExtractSpec
from app.ingest.backfill_runner import _derive_ingest_status


def test_begin_ingest_window_locking_and_stale_takeover(tmp_path):
    db = tmp_path / "alpha.db"
    store = EventStore(db_path=db)
    source_id = "s1"
    start_ts = "2026-04-01T00:00:00+00:00"
    end_ts = "2026-04-02T00:00:00+00:00"
    spec_hash = "deadbeefdeadbeef"
    provider = "alpaca"

    assert store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )
    # Second worker should skip while it's running and not stale.
    assert not store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )

    # Make it stale by pushing updated_at far into the past.
    stale = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE ingest_runs SET updated_at = ? WHERE source_id=? AND start_ts=? AND end_ts=? AND spec_hash=?",
            (stale, source_id, start_ts, end_ts, spec_hash),
        )

    # Now takeover should be allowed, and retry_count should increment.
    assert store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT retry_count, status FROM ingest_runs WHERE source_id=? AND start_ts=? AND end_ts=? AND spec_hash=?",
            (source_id, start_ts, end_ts, spec_hash),
        ).fetchone()
    assert row is not None
    assert int(row[0]) >= 1
    assert str(row[1]) == "running"


def test_complete_window_never_reverts_to_running(tmp_path):
    db = tmp_path / "alpha.db"
    store = EventStore(db_path=db)
    source_id = "s1"
    start_ts = "2026-04-01T00:00:00+00:00"
    end_ts = "2026-04-02T00:00:00+00:00"
    spec_hash = "deadbeefdeadbeef"
    provider = "alpaca"

    assert store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )
    store.record_ingest_run(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        ok=True,
        fetched_count=10,
        emitted_count=10,
        last_error=None,
    )
    assert store.is_ingest_window_completed(
        source_id=source_id, start_ts=start_ts, end_ts=end_ts, spec_hash=spec_hash
    )
    # Must never revert to running after completion.
    assert not store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )


def test_empty_window_is_recorded_and_skipped(tmp_path):
    db = tmp_path / "alpha.db"
    store = EventStore(db_path=db)
    source_id = "s1"
    start_ts = "2026-04-01T00:00:00+00:00"
    end_ts = "2026-04-02T00:00:00+00:00"
    spec_hash = "deadbeefdeadbeef"
    provider = "alpaca"

    assert store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )
    store.record_ingest_run(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        ok=True,
        fetched_count=0,
        emitted_count=0,
        last_error=None,
    )

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT status, empty_count, last_error FROM ingest_runs WHERE source_id=? AND start_ts=? AND end_ts=? AND spec_hash=?",
            (source_id, start_ts, end_ts, spec_hash),
        ).fetchone()
    assert row is not None
    assert row[0] == "complete"
    assert int(row[1]) == 1
    assert row[2] == "empty"
    assert store.is_ingest_window_completed(
        source_id=source_id, start_ts=start_ts, end_ts=end_ts, spec_hash=spec_hash
    )


def test_crash_mid_window_leaves_running_status(tmp_path):
    db = tmp_path / "alpha.db"
    store = EventStore(db_path=db)
    source_id = "s1"
    start_ts = "2026-04-01T00:00:00+00:00"
    end_ts = "2026-04-02T00:00:00+00:00"
    spec_hash = "deadbeefdeadbeef"
    provider = "alpaca"

    assert store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash,
        provider=provider,
        running_ttl_s=3600,
    )
    assert store.get_ingest_window_status(
        source_id=source_id, start_ts=start_ts, end_ts=end_ts, spec_hash=spec_hash
    ) == "running"


def test_spec_change_forces_refetch(tmp_path):
    db = tmp_path / "alpha.db"
    store = EventStore(db_path=db)
    source_id = "s1"
    start_ts = "2026-04-01T00:00:00+00:00"
    end_ts = "2026-04-02T00:00:00+00:00"
    provider = "alpaca"
    spec_hash_v1 = store.stable_spec_hash({"id": "s1", "fetch": {"url": "x"}, "extract": {"text": "a"}})
    spec_hash_v2 = store.stable_spec_hash({"id": "s1", "fetch": {"url": "x"}, "extract": {"text": "b"}})

    assert store.begin_ingest_window(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash_v1,
        provider=provider,
        running_ttl_s=3600,
    )
    store.record_ingest_run(
        source_id=source_id,
        start_ts=start_ts,
        end_ts=end_ts,
        spec_hash=spec_hash_v1,
        provider=provider,
        ok=True,
        fetched_count=1,
        emitted_count=1,
        last_error=None,
    )

    assert store.is_ingest_window_completed(
        source_id=source_id, start_ts=start_ts, end_ts=end_ts, spec_hash=spec_hash_v1
    )
    assert not store.is_ingest_window_completed(
        source_id=source_id, start_ts=start_ts, end_ts=end_ts, spec_hash=spec_hash_v2
    )


def test_failed_schema_transition_for_zero_emission_nonempty_response():
    ok_override, status_override, err_override = _derive_ingest_status(
        raw_rows_count=5,
        emitted_count=0,
        ok=True,
        error=None,
    )
    assert ok_override is False
    assert status_override == "failed_schema"
    assert err_override == "zero_emission_nonempty_response"


def test_request_cache_overlapping_slice_reuse(monkeypatch):
    from app.ingest import registry
    from app.ingest.backfill_runner import _fetch_slice
    from app.ingest.key_manager import KeyManager
    from app.ingest.extractor import Extractor

    calls = {"n": 0}

    class DummyAdapter:
        async def fetch_raw(self, spec, ctx):
            calls["n"] += 1
            return [{"ts": ctx.start_date, "headline": "hello", "symbol": "AAPL"}]

    monkeypatch.setitem(registry.ADAPTERS, "dummy_adapter", DummyAdapter())

    spec = SourceSpec(
        id="dummy_source",
        type="news",
        adapter="dummy_adapter",
        enabled=True,
        weight=1.0,
        priority=1,
        symbols=["AAPL"],
        extract=ExtractSpec(text="headline", timestamp="ts", ticker="symbol"),
        options={},
    )

    start = datetime(2026, 4, 1, tzinfo=timezone.utc)
    end = datetime(2026, 4, 2, tzinfo=timezone.utc)
    cache_handle: dict = {}

    r1 = asyncio.run(_fetch_slice(spec, start, end, KeyManager(), Extractor(), cache_handle=cache_handle))
    r2 = asyncio.run(_fetch_slice(spec, start, end, KeyManager(), Extractor(), cache_handle=cache_handle))

    assert calls["n"] == 1
    assert r1.ok and r2.ok
    assert r1.request_cache_hit is False
    assert r2.request_cache_hit is True
    assert r1.response_fingerprint == r2.response_fingerprint
