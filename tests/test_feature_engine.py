from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from app.core.feature_engine import FeatureEngine


def _make_minute_bars(n: int) -> pd.DataFrame:
    start = datetime(2026, 1, 1, 14, 30, tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        ts = start + timedelta(minutes=i)
        close = 100.0 + float(i)
        rows.append(
            {
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000.0 + 10.0 * i,
            }
        )
    # Intentionally reverse to validate _prepare_bars sorting.
    return pd.DataFrame(list(reversed(rows)))


def test_prepare_bars_sorts_and_coerces_timestamp(tmp_path) -> None:
    fe = FeatureEngine()
    bars = _make_minute_bars(3)
    prepared = fe._prepare_bars(bars)  # noqa: SLF001
    assert prepared["timestamp"].is_monotonic_increasing
    assert str(prepared["timestamp"].dt.tz) == "UTC"
    assert float(prepared.iloc[0]["close"]) == 100.0


def test_find_event_index_picks_last_bar_at_or_before_event_time() -> None:
    fe = FeatureEngine()
    bars = fe._prepare_bars(_make_minute_bars(5))  # noqa: SLF001
    # Event between bars[1] and bars[2] -> should pick bars[1]
    event_ts = (bars.iloc[1]["timestamp"] + timedelta(seconds=30)).to_pydatetime()
    idx = fe._find_event_index(bars, event_ts)  # noqa: SLF001
    assert idx == 1


def test_build_feature_set_separates_features_from_outcomes() -> None:
    fe = FeatureEngine()
    bars = _make_minute_bars(400)
    prepared = fe._prepare_bars(bars)  # noqa: SLF001
    event_idx = 240
    event_ts = prepared.iloc[event_idx]["timestamp"].to_pydatetime()

    features, outcomes = fe.build_feature_set(prepared, event_ts, cross_asset_data=None)
    assert features
    assert outcomes
    # Outcomes are forward-looking; features should not contain future_return keys.
    assert "future_return_1m" in outcomes
    assert "future_return_1m" not in features
    assert "max_runup_15m" in outcomes


def test_compute_returns_matches_expected_for_1m_and_4h() -> None:
    fe = FeatureEngine()
    prepared = fe._prepare_bars(_make_minute_bars(500))  # noqa: SLF001
    event_idx = 240
    returns = fe._compute_returns(prepared, event_idx)  # noqa: SLF001

    # close at idx=240 is 340, idx=239 is 339, idx=0 is 100.
    assert abs(returns["return_1m"] - ((340.0 - 339.0) / 339.0)) < 1e-12
    assert abs(returns["return_4h"] - ((340.0 - 100.0) / 100.0)) < 1e-12


def test_trend_strength_and_volatility_features_do_not_error_and_return_keys() -> None:
    fe = FeatureEngine()
    prepared = fe._prepare_bars(_make_minute_bars(200))  # noqa: SLF001
    event_idx = 100
    vol = fe._compute_volatility_features(prepared, event_idx)  # noqa: SLF001
    # Windows present when event_idx >= window.
    assert "parkinson_vol_50" in vol
    assert "gk_vol_20" in vol

    trend = fe._compute_trend_strength(prepared, event_idx)  # noqa: SLF001
    assert "adx_14" in trend
    assert "trend_strength" in trend

