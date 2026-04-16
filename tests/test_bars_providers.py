from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.bars.providers import FallbackBarsProvider, OHLCVBar, _chunk_range


class _ProviderEmpty:
    name = "empty"

    def __init__(self) -> None:
        self.calls = 0

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        self.calls += 1
        return []


class _ProviderRaises:
    name = "raises"

    def __init__(self) -> None:
        self.calls = 0

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        self.calls += 1
        raise RuntimeError("boom")


class _ProviderBars:
    name = "bars"

    def __init__(self, bars: list[OHLCVBar]) -> None:
        self.calls = 0
        self._bars = list(bars)

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        self.calls += 1
        return list(self._bars)


def test_chunk_range_splits_and_covers_interval() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 20, tzinfo=timezone.utc)
    chunks = _chunk_range(start, end, timedelta(days=7))
    assert chunks[0][0] == start
    assert chunks[-1][1] == end
    # Non-overlap and contiguous.
    for (a0, a1), (b0, b1) in zip(chunks, chunks[1:]):
        assert a0 < a1
        assert a1 == b0
        assert b0 < b1


def test_chunk_range_empty_when_start_equals_end() -> None:
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert _chunk_range(t, t, timedelta(days=1)) == []


def test_fallback_provider_skips_failures_and_returns_bars() -> None:
    bars = [OHLCVBar(timestamp="2026-01-01T00:00:00Z", open=1, high=2, low=1, close=2, volume=10)]
    p1 = _ProviderRaises()
    p2 = _ProviderEmpty()
    p3 = _ProviderBars(bars)
    fb = FallbackBarsProvider([p1, p2, p3])

    out = fb.fetch_bars(
        timeframe="1d",
        ticker="NVDA",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    assert out == bars
    assert p1.calls == 1
    assert p2.calls == 1
    assert p3.calls == 1


def test_fallback_provider_returns_empty_when_all_fail() -> None:
    p1 = _ProviderRaises()
    p2 = _ProviderRaises()
    fb = FallbackBarsProvider([p1, p2])
    out = fb.fetch_bars(
        timeframe="1d",
        ticker="NVDA",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    assert out == []

