from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from app.core.macro.yfinance_series import build_macro_snapshot_for_asof


def test_macro_snapshot_derives_requested_features():
    # 30 "trading days" of closes: 100..129
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    idx = pd.to_datetime([start + timedelta(days=i) for i in range(30)], utc=True)
    closes = pd.Series([100.0 + i for i in range(30)], index=idx, dtype="float64")

    asof = datetime(2024, 2, 1, tzinfo=timezone.utc)
    snap = build_macro_snapshot_for_asof(name="oil", closes=closes, asof=asof)

    feats = snap.features
    assert "oil_close" in feats
    assert "oil_return_1d" in feats
    assert "oil_return_5d" in feats
    assert "oil_volatility_10d" in feats
    assert "oil_trend_20d" in feats

