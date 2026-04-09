from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from app.core.repository import Repository
from app.ingest.backfill_runner import BackfillRunner
from app.ingest.event_model import Event


def test_replay_range_skips_when_no_bars_provider(tmp_path, monkeypatch):
    db_path = tmp_path / "alpha.db"
    runner = BackfillRunner(db_path=str(db_path))

    # Seed one stored event (no ticker => bypass Target Stocks gating).
    evt = Event(
        id="evt-1",
        source_id="test",
        source_type="unit",
        timestamp="2023-12-02T00:00:00Z",
        ticker=None,
        text="hello",
        tags=[],
        weight=1.0,
        numeric_features={},
    )
    runner.store.save_batch([evt])

    # Force bars provider to be unavailable.
    monkeypatch.setattr(runner, "_bars_cache", lambda: None)

    repo = Repository(str(db_path))
    summary = asyncio.run(
        runner.replay_range(
            start_time=datetime(2023, 12, 2, tzinfo=timezone.utc),
            end_time=datetime(2023, 12, 3, tzinfo=timezone.utc),
            repo=repo,
        )
    )

    assert summary.replayed == 0
    assert summary.deferred == 1
    assert summary.deferred_reasons["no_bars_provider"] == 1

