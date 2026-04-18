from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd

from app.ingest.backfill_runner import BackfillRunner
from app.ingest.event_model import Event


class _FakeBarsCache:
    def ensure_policy(self, *, tickers, start, end, now=None):
        return {}

    def fetch_bars_df(self, *, timeframe: str, tickers, start, end) -> pd.DataFrame:
        # Minimal bars coverage so price_context is non-empty.
        rows = []
        for t in tickers:
            rows.append(
                {
                    "tenant_id": "backfill",
                    "ticker": str(t),
                    "timeframe": str(timeframe),
                    "timestamp": "2024-04-05T00:00:00Z",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                }
            )
        return pd.DataFrame(rows)


class _DummyRepo:
    def set_kv(self, *a, **k):
        return None

    def get_kv(self, *a, **k):
        return None

    def persist_missing_price_context_events(self, rows):
        _ = rows

    def transaction(self):
        from contextlib import contextmanager

        @contextmanager
        def cm():
            yield

        return cm()


def test_replay_injects_macro_snapshot_into_price_context(monkeypatch, tmp_path):
    runner = BackfillRunner(db_path=str(tmp_path / "alpha.db"))

    tick = Event(
        id="e1",
        source_id="alpaca_news_main",
        source_type="news",
        timestamp="2024-04-05T00:00:00Z",
        ticker="NVDA",
        text="hello",
        tags=["news"],
        weight=1.0,
        numeric_features={},
    )

    monkeypatch.setattr(runner.store, "get_events_chronological", lambda *a, **k: [tick])
    monkeypatch.setattr(runner, "_bars_cache", lambda: _FakeBarsCache())
    monkeypatch.setattr(
        "app.ingest.backfill_runner.get_active_universe_tickers",
        lambda **kwargs: ["NVDA"],
    )
    monkeypatch.setattr(
        runner,
        "_macro_snapshot_for_slice",
        lambda asof: {"oil_return_1d": 0.01, "btc_return_1d": -0.02},
    )

    captured = {}

    def fake_run_pipeline(*, raw_events, price_contexts, **kwargs):
        captured["raw_events"] = list(raw_events)
        captured["price_contexts"] = dict(price_contexts)
        return {}

    monkeypatch.setattr("app.ingest.backfill_runner.run_pipeline", fake_run_pipeline)

    asyncio.run(
        runner.replay_range(
            start_time=datetime(2024, 4, 5, tzinfo=timezone.utc),
            end_time=datetime(2024, 4, 6, tzinfo=timezone.utc),
            repo=_DummyRepo(),  # type: ignore[arg-type]
        )
    )

    assert [e.id for e in captured["raw_events"]] == ["e1"]
    ctx = captured["price_contexts"]["e1"]
    assert ctx["macro"]["oil_return_1d"] == 0.01
    assert ctx["macro"]["btc_return_1d"] == -0.02

