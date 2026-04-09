from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from app.ingest.backfill_runner import BackfillRunner, ReplaySummary


class _Repo:
    def __init__(self, kv: dict[str, str]):
        self.kv = dict(kv)

    def get_kv(self, key: str, tenant_id: str = "default") -> str | None:
        return self.kv.get(f"{tenant_id}:{key}")


def test_replay_unseen_uses_cursor_when_bounds_are_stale(monkeypatch):
    runner = BackfillRunner(db_path="data/_tmp_unused.db")

    calls: list[tuple] = []

    async def fake_replay_range(*, start_time, end_time, repo, start_exclusive=False, cursor_id=None):
        calls.append((start_time, end_time, start_exclusive, cursor_id))
        return ReplaySummary(replayed=0, deferred=1)

    monkeypatch.setattr(runner, "replay_range", fake_replay_range)

    repo = _Repo(
        {
            # Stale "replayed" bounds in the future relative to the requested window.
            "backfill:backfill_replayed_min_ts": "2023-12-02T00:00:00Z",
            "backfill:backfill_replayed_max_ts": "2026-02-01T00:00:00Z",
            "backfill:backfill_replayed_max_id": "zzz",
            # Cursor shows we only got through the first day.
            "backfill:backfill_replay_cursor_ts": "2023-12-03T00:00:00Z",
            "backfill:backfill_replay_cursor_id": "ccc",
        }
    )

    out = asyncio.run(
        runner.replay_unseen(
            start_time=datetime(2023, 12, 2, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            repo=repo,  # type: ignore[arg-type]
        )
    )

    assert out.deferred == 1
    assert len(calls) == 1
    start_time, end_time, start_exclusive, cursor_id = calls[0]
    assert start_time == datetime(2023, 12, 3, tzinfo=timezone.utc)
    assert end_time == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert start_exclusive is True
    assert cursor_id == "ccc"

