from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from app.core.bars.cache import BarsCache, bar_window_for_events
from app.core.bars.providers import OHLCVBar


class _FakeProvider:
    name = "fake"

    def __init__(self, bars: list[OHLCVBar]) -> None:
        self._bars = list(bars)
        self.calls = 0

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        self.calls += 1
        return list(self._bars)


def test_bars_cache_inserts_then_reuses_coverage(tmp_path) -> None:
    db_path = tmp_path / "data" / "alpha.db"
    bars = [
        OHLCVBar(timestamp="2026-01-01T00:00:00Z", open=1, high=1, low=1, close=1, volume=1),
        OHLCVBar(timestamp="2026-01-02T00:00:00Z", open=2, high=2, low=2, close=2, volume=2),
    ]
    provider = _FakeProvider(bars)
    cache = BarsCache(db_path=str(db_path), provider=provider, min_rows_expected_per_day=1)

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 3, tzinfo=timezone.utc)

    inserted = cache.ensure_bars(ticker="NVDA", timeframe="1d", start=start, end=end)
    assert inserted == 2
    assert provider.calls == 1

    inserted2 = cache.ensure_bars(ticker="NVDA", timeframe="1d", start=start, end=end)
    assert inserted2 == 0
    assert provider.calls == 1

    df = cache.fetch_bars_df(timeframe="1d", tickers=["NVDA"], start=start, end=end)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


def test_bar_window_for_events_empty_defaults_to_utc_dayish() -> None:
    w = bar_window_for_events(event_times=[])
    assert w.start.tzinfo is not None
    assert w.end.tzinfo is not None
    assert w.start < w.end

