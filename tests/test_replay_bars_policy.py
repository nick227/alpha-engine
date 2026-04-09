from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd

import app.ingest.backfill_runner as br
from app.ingest.backfill_runner import BackfillRunner
from app.ingest.event_model import Event


class _FakeBarsCache:
    def __init__(self) -> None:
        self.ensure_calls: list[dict] = []

    def ensure_policy(self, *, tickers, start, end, now=None):
        self.ensure_calls.append(
            {
                "tickers": sorted([str(t) for t in tickers if t]),
                "start": start,
                "end": end,
                "now": now,
            }
        )
        return {}

    def fetch_bars_df(self, *, timeframe: str, tickers, start, end) -> pd.DataFrame:
        # Return an empty df with the expected columns; this forces the replay path to defer,
        # but still validates the bars policy inputs.
        return pd.DataFrame(columns=["ticker", "timestamp", "open", "high", "low", "close", "volume"])


class _DummyRepo:
    def __init__(self) -> None:
        self.kv: dict[tuple[str, str], str] = {}

    def set_kv(self, key: str, value: str, tenant_id: str = "default") -> None:
        self.kv[(tenant_id, key)] = str(value)

    def get_kv(self, key: str, tenant_id: str = "default") -> str | None:
        return self.kv.get((tenant_id, key))

    def persist_missing_price_context_events(self, rows) -> None:
        _ = rows

    def transaction(self):
        from contextlib import contextmanager

        @contextmanager
        def cm():
            yield

        return cm()


def test_replay_uses_slice_end_for_bars_policy_now(tmp_path, monkeypatch):
    db_path = tmp_path / "alpha.db"
    runner = BackfillRunner(db_path=str(db_path))

    # Avoid SQLite flakiness by stubbing the store read path.
    evt = Event(
        id="evt-1",
        source_id="test",
        source_type="unit",
        timestamp="2023-12-02T00:00:00Z",
        ticker="AAPL",
        text="hello",
        tags=[],
        weight=1.0,
        numeric_features={},
    )
    monkeypatch.setattr(runner.store, "get_events_chronological", lambda *a, **k: [evt])

    monkeypatch.setattr(br, "get_target_stocks", lambda asof=None: ["AAPL", "MSFT", "NVDA"])
    monkeypatch.setenv("BACKFILL_ENSURE_TARGET_UNIVERSE_BARS", "false")

    fake = _FakeBarsCache()
    monkeypatch.setattr(runner, "_bars_cache", lambda: fake)

    repo = _DummyRepo()
    asyncio.run(
        runner.replay_range(
            start_time=datetime(2023, 12, 2, tzinfo=timezone.utc),
            end_time=datetime(2023, 12, 3, tzinfo=timezone.utc),
            repo=repo,
        )
    )

    assert fake.ensure_calls, "expected ensure_policy() to be called"
    call = fake.ensure_calls[0]
    assert call["tickers"] == ["AAPL"]
    assert call["now"] == datetime(2023, 12, 3, tzinfo=timezone.utc)
