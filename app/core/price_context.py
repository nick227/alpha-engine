from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import numpy as np
import pandas as pd

from app.core.types import RawEvent


def _to_utc(ts: str | datetime) -> datetime:
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    parsed = pd.to_datetime(ts, utc=True)
    return parsed.to_pydatetime().astimezone(timezone.utc)


def _rsi(series: pd.Series, period: int = 14) -> float:
    series = series.astype(float)
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)

    roll_up = up.rolling(period).mean()
    roll_down = down.rolling(period).mean()
    rs = roll_up / roll_down.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    value = float(rsi.iloc[-1]) if not rsi.empty else 50.0
    if np.isnan(value):
        return 50.0
    return max(0.0, min(100.0, value))


def _nearest_bar_index(bars: pd.DataFrame, ts: datetime) -> int | None:
    if bars.empty:
        return None
    # Find first bar at or after event time.
    idx = bars["timestamp"].searchsorted(pd.Timestamp(ts), side="left")
    if idx >= len(bars):
        return None
    return int(idx)


def build_price_context_for_event(
    *,
    ticker_bars: pd.DataFrame,
    event_ts: datetime,
    horizons_minutes: Iterable[int] = (1, 5, 15, 60),
) -> dict:
    """
    Produces a feature-rich price_context dict from 1-minute OHLCV bars.
    Designed to satisfy current strategy + MRA + evaluation expectations.
    """
    bars = ticker_bars.sort_values("timestamp").reset_index(drop=True).copy()
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)

    idx = _nearest_bar_index(bars, event_ts)
    if idx is None:
        return {}

    row = bars.iloc[idx]
    entry_price = float(row["close"])
    entry_volume = float(row.get("volume", 0.0))

    lookback_20 = bars.iloc[max(0, idx - 20) : idx + 1]
    lookback_14 = bars.iloc[max(0, idx - 14) : idx + 1]

    # Returns / horizons (also populate future_return_* for evaluate_prediction).
    ctx: dict[str, float | bool | list[float]] = {"entry_price": entry_price}

    for minutes in horizons_minutes:
        target_ts = _to_utc(event_ts + timedelta(minutes=int(minutes)))
        j = _nearest_bar_index(bars, target_ts)
        if j is None:
            continue
        exit_price = float(bars.iloc[j]["close"])
        r = (exit_price - entry_price) / entry_price if entry_price else 0.0
        if minutes == 1:
            ctx["return_1m"] = r
            ctx["future_return_1m"] = r
        elif minutes == 5:
            ctx["return_5m"] = r
            ctx["future_return_5m"] = r
        elif minutes == 15:
            ctx["return_15m"] = r
            ctx["future_return_15m"] = r
        elif minutes == 60:
            ctx["return_1h"] = r
            ctx["future_return_1h"] = r

    # Short trend (5m lookback).
    if idx >= 5:
        prev = float(bars.iloc[idx - 5]["close"])
        ctx["short_trend"] = (entry_price - prev) / prev if prev else 0.0
    else:
        ctx["short_trend"] = 0.0

    # Realized volatility proxy (20-bar std of 1m returns).
    returns_1m = lookback_20["close"].astype(float).pct_change().dropna()
    realized_vol = float(returns_1m.std(ddof=0)) if len(returns_1m) > 1 else 0.0
    ctx["realized_volatility"] = realized_vol
    hist = returns_1m.abs().tolist()
    ctx["historical_volatility_window"] = [float(x) for x in (hist[-20:] if hist else [0.0] * 20)]

    # Volume ratio.
    avg_vol = float(lookback_20["volume"].astype(float).mean()) if not lookback_20.empty else 0.0
    ctx["volume_ratio"] = (entry_volume / avg_vol) if avg_vol else 1.0

    # VWAP (20 bars).
    v = lookback_20["volume"].astype(float)
    typical = (lookback_20["high"].astype(float) + lookback_20["low"].astype(float) + lookback_20["close"].astype(float)) / 3.0
    vwap = float((typical * v).sum() / v.sum()) if float(v.sum()) > 0 else float(lookback_20["close"].astype(float).mean()) if not lookback_20.empty else entry_price
    ctx["vwap_distance"] = (entry_price - vwap) / vwap if vwap else 0.0

    # VWAP reclaim/reject (simple 1-step cross).
    if idx >= 1:
        prev_close = float(bars.iloc[idx - 1]["close"])
        prev_window = bars.iloc[max(0, idx - 21) : idx].copy()
        prev_window["timestamp"] = pd.to_datetime(prev_window["timestamp"], utc=True)
        pv = prev_window["volume"].astype(float)
        pt = (prev_window["high"].astype(float) + prev_window["low"].astype(float) + prev_window["close"].astype(float)) / 3.0
        prev_vwap = float((pt * pv).sum() / pv.sum()) if float(pv.sum()) > 0 else float(prev_window["close"].astype(float).mean()) if not prev_window.empty else vwap
        ctx["vwap_reclaim"] = (entry_price > vwap) and (prev_close <= prev_vwap)
        ctx["vwap_reject"] = (entry_price < vwap) and (prev_close >= prev_vwap)
    else:
        ctx["vwap_reclaim"] = False
        ctx["vwap_reject"] = False

    # Range expansion.
    current_range = float(row["high"]) - float(row["low"])
    prior_ranges = (lookback_20["high"].astype(float) - lookback_20["low"].astype(float)).replace(0.0, np.nan)
    avg_range = float(prior_ranges.mean()) if len(prior_ranges.dropna()) else 0.0
    ctx["range_expansion"] = (current_range / avg_range) if avg_range else 1.0

    # Z-score (20 bars) and RSI (14).
    closes_20 = lookback_20["close"].astype(float)
    mean_20 = float(closes_20.mean()) if not closes_20.empty else entry_price
    std_20 = float(closes_20.std(ddof=0)) if len(closes_20) > 1 else 0.0
    ctx["zscore_20"] = (entry_price - mean_20) / std_20 if std_20 else 0.0
    ctx["rsi_14"] = _rsi(lookback_14["close"].astype(float), period=14) if not lookback_14.empty else 50.0

    # Continuation slope + pullback depth over next 15 minutes (if available).
    horizon_15 = int(_nearest_bar_index(bars, _to_utc(event_ts + timedelta(minutes=15))) or idx)
    window = bars.iloc[idx : horizon_15 + 1]
    if len(window) >= 2:
        last_close = float(window.iloc[-1]["close"])
        ctx["continuation_slope"] = (last_close - entry_price) / entry_price if entry_price else 0.0
        peak = float(window["close"].astype(float).max())
        trough = float(window["close"].astype(float).min())
        ctx["pullback_depth"] = (peak - trough) / entry_price if entry_price else 0.0

        # For outcome stats.
        best_high = float(window["high"].astype(float).max())
        best_low = float(window["low"].astype(float).min())
        ctx["max_runup"] = (best_high - entry_price) / entry_price if entry_price else 0.0
        ctx["max_drawdown"] = (best_low - entry_price) / entry_price if entry_price else 0.0
    else:
        ctx["continuation_slope"] = 0.0
        ctx["pullback_depth"] = 0.0

    return dict(ctx)


def build_price_contexts_from_bars(
    *,
    raw_events: list[RawEvent],
    bars: pd.DataFrame,
    horizons_minutes: Iterable[int] = (1, 5, 15, 60),
) -> dict[str, dict]:
    """
    Builds a price_context dict keyed by raw_event.id from a multi-ticker bars DataFrame.
    """
    df = bars.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    ctxs: dict[str, dict] = {}

    for evt in raw_events:
        ticker = evt.tickers[0] if evt.tickers else None
        if not ticker:
            continue
        ticker_bars = df[df["ticker"] == ticker]
        if ticker_bars.empty:
            continue
        ctxs[evt.id] = build_price_context_for_event(
            ticker_bars=ticker_bars,
            event_ts=_to_utc(evt.timestamp),
            horizons_minutes=horizons_minutes,
        )
    return ctxs

