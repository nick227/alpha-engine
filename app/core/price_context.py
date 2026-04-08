from __future__ import annotations

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


def _prior_bar_index(bars: pd.DataFrame, ts: datetime) -> int | None:
    """
    Find last bar at or before `ts`.
    """
    if bars.empty:
        return None
    idx = bars["timestamp"].searchsorted(pd.Timestamp(ts), side="right") - 1
    if idx < 0:
        return None
    return int(idx)


def build_price_context_for_event(
    *,
    ticker_bars: pd.DataFrame,
    event_ts: datetime,
    horizons_minutes: Iterable[int] = (1, 5, 15, 60, 240, 1440),
    assume_sorted: bool = False,
) -> dict:
    """
    Produces a feature-rich price_context dict from 1-minute OHLCV bars.
    Designed to satisfy current strategy + MRA + evaluation expectations.
    """
    if assume_sorted:
        bars = ticker_bars.reset_index(drop=True)
    else:
        bars = ticker_bars.sort_values("timestamp").reset_index(drop=True)

    ts = bars["timestamp"]
    if not pd.api.types.is_datetime64_any_dtype(ts):
        bars = bars.copy(deep=False)
        bars["timestamp"] = pd.to_datetime(ts, utc=True)
    else:
        tz = getattr(ts.dtype, "tz", None)
        if tz is None or str(tz) != "UTC":
            bars = bars.copy(deep=False)
            bars["timestamp"] = pd.to_datetime(ts, utc=True)

    # IMPORTANT: Use the last bar at or before the event timestamp to avoid look-ahead.
    idx = _prior_bar_index(bars, event_ts)
    if idx is None:
        return {}

    row = bars.iloc[idx]
    entry_price = float(row["close"])
    entry_volume = float(row.get("volume", 0.0))

    lookback_20 = bars.iloc[max(0, idx - 20) : idx + 1]
    lookback_14 = bars.iloc[max(0, idx - 14) : idx + 1]

    # Feature returns are strictly backward-looking (avoid look-ahead bias).
    # Outcomes use future_return_* keys and are intended only for evaluation.
    ctx: dict[str, float | bool | list[float]] = {"entry_price": entry_price}

    for minutes in horizons_minutes:
        past_ts = _to_utc(event_ts - timedelta(minutes=int(minutes)))
        p = _prior_bar_index(bars, past_ts)
        if p is not None:
            past_price = float(bars.iloc[p]["close"])
            past_r = (entry_price - past_price) / past_price if past_price else 0.0
        else:
            past_r = 0.0

        future_ts = _to_utc(event_ts + timedelta(minutes=int(minutes)))
        j = _nearest_bar_index(bars, future_ts)
        if j is not None:
            exit_price = float(bars.iloc[j]["close"])
            future_r = (exit_price - entry_price) / entry_price if entry_price else 0.0
        else:
            future_r = 0.0

        if minutes == 1:
            ctx["return_1m"] = past_r
            ctx["future_return_1m"] = future_r
        elif minutes == 5:
            ctx["return_5m"] = past_r
            ctx["future_return_5m"] = future_r
        elif minutes == 15:
            ctx["return_15m"] = past_r
            ctx["future_return_15m"] = future_r
        elif minutes == 60:
            ctx["return_1h"] = past_r
            ctx["future_return_1h"] = future_r
        elif minutes == 240:
            ctx["return_4h"] = past_r
            ctx["future_return_4h"] = future_r
        elif minutes == 1440:
            ctx["return_1d"] = past_r
            ctx["future_return_1d"] = future_r

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
        prev_window = bars.iloc[max(0, idx - 21) : idx]
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

    # Feature: slope/pullback over the prior 15 minutes (past-only).
    past_15_idx = _prior_bar_index(bars, _to_utc(event_ts - timedelta(minutes=15)))
    if past_15_idx is None:
        past_window = bars.iloc[max(0, idx - 15) : idx + 1]
    else:
        past_window = bars.iloc[past_15_idx : idx + 1]
    if len(past_window) >= 2:
        first_close = float(past_window.iloc[0]["close"])
        ctx["continuation_slope"] = (entry_price - first_close) / first_close if first_close else 0.0
        peak = float(past_window["close"].astype(float).max())
        trough = float(past_window["close"].astype(float).min())
        ctx["pullback_depth"] = (peak - trough) / entry_price if entry_price else 0.0
    else:
        ctx["continuation_slope"] = 0.0
        ctx["pullback_depth"] = 0.0

    # Outcomes: max runup/drawdown over the next 15 minutes (future-only, evaluation use).
    horizon_15 = _nearest_bar_index(bars, _to_utc(event_ts + timedelta(minutes=15)))
    if horizon_15 is None:
        horizon_15 = idx
    outcome_window = bars.iloc[idx : int(horizon_15) + 1]
    if len(outcome_window) >= 2:
        best_high = float(outcome_window["high"].astype(float).max())
        best_low = float(outcome_window["low"].astype(float).min())
        ctx["max_runup"] = (best_high - entry_price) / entry_price if entry_price else 0.0
        ctx["max_drawdown"] = (best_low - entry_price) / entry_price if entry_price else 0.0
    else:
        ctx["max_runup"] = 0.0
        ctx["max_drawdown"] = 0.0

    return dict(ctx)


def build_price_context_for_event_with_resolution(
    *,
    ticker_bars: pd.DataFrame,
    event_ts: datetime,
    timeframe: str,
    horizons_minutes: Iterable[int] = (1, 5, 15, 60, 240, 1440),
) -> dict:
    """
    Build a price context from bars that may be 1m, 1h, or 1d.

    Contract notes:
    - Always returns the same keys as the 1m builder (when possible).
    - For resolutions coarser than 1m, short-horizon keys are set to 0.0 to avoid
      implying precision the data cannot support.
    - Adds `bar_resolution_used` for debugging/observability.
    """
    tf = str(timeframe).strip().lower()
    if tf not in {"1m", "1h", "1d"}:
        tf = "1m"

    # Reuse the existing 1m path unchanged.
    if tf == "1m":
        out = build_price_context_for_event(
            ticker_bars=ticker_bars,
            event_ts=event_ts,
            horizons_minutes=horizons_minutes,
            assume_sorted=False,
        )
        if out:
            out["bar_resolution_used"] = "1m"
        return out

    # Generic coarse-resolution builder.
    bar_minutes = 60 if tf == "1h" else 1440
    bars = ticker_bars.sort_values("timestamp").reset_index(drop=True)
    ts = bars["timestamp"]
    if not pd.api.types.is_datetime64_any_dtype(ts):
        bars = bars.copy(deep=False)
        bars["timestamp"] = pd.to_datetime(ts, utc=True)
    else:
        tz = getattr(ts.dtype, "tz", None)
        if tz is None or str(tz) != "UTC":
            bars = bars.copy(deep=False)
            bars["timestamp"] = pd.to_datetime(ts, utc=True)

    idx = _prior_bar_index(bars, event_ts)
    if idx is None:
        return {}

    row = bars.iloc[idx]
    entry_price = float(row["close"])
    entry_volume = float(row.get("volume", 0.0))

    lookback_20 = bars.iloc[max(0, idx - 20) : idx + 1]
    lookback_14 = bars.iloc[max(0, idx - 14) : idx + 1]

    ctx: dict[str, float | bool | list[float] | str] = {"entry_price": entry_price, "bar_resolution_used": tf}

    for minutes in horizons_minutes:
        # Avoid claiming minute-level precision when the data is hourly/daily.
        if minutes < bar_minutes:
            past_r = 0.0
            future_r = 0.0
        else:
            past_ts = _to_utc(event_ts - timedelta(minutes=int(minutes)))
            p = _prior_bar_index(bars, past_ts)
            if p is not None:
                past_price = float(bars.iloc[p]["close"])
                past_r = (entry_price - past_price) / past_price if past_price else 0.0
            else:
                past_r = 0.0

            future_ts = _to_utc(event_ts + timedelta(minutes=int(minutes)))
            j = _nearest_bar_index(bars, future_ts)
            if j is not None:
                exit_price = float(bars.iloc[j]["close"])
                future_r = (exit_price - entry_price) / entry_price if entry_price else 0.0
            else:
                future_r = 0.0

        if minutes == 1:
            ctx["return_1m"] = past_r
            ctx["future_return_1m"] = future_r
        elif minutes == 5:
            ctx["return_5m"] = past_r
            ctx["future_return_5m"] = future_r
        elif minutes == 15:
            ctx["return_15m"] = past_r
            ctx["future_return_15m"] = future_r
        elif minutes == 60:
            ctx["return_1h"] = past_r
            ctx["future_return_1h"] = future_r
        elif minutes == 240:
            ctx["return_4h"] = past_r
            ctx["future_return_4h"] = future_r
        elif minutes == 1440:
            ctx["return_1d"] = past_r
            ctx["future_return_1d"] = future_r

    # Coarse default for short-trend + microstructure features.
    ctx["short_trend"] = 0.0
    ctx["vwap_reclaim"] = False
    ctx["vwap_reject"] = False
    ctx["continuation_slope"] = 0.0
    ctx["pullback_depth"] = 0.0
    ctx["max_runup"] = 0.0
    ctx["max_drawdown"] = 0.0

    # Realized volatility proxy (20 bars std of returns).
    returns = lookback_20["close"].astype(float).pct_change().dropna()
    realized_vol = float(returns.std(ddof=0)) if len(returns) > 1 else 0.0
    ctx["realized_volatility"] = realized_vol
    hist = returns.abs().tolist()[-20:]
    ctx["historical_volatility"] = [float(x) for x in hist] if hist else []

    # Volume ratio (coarse).
    avg_vol = float(lookback_20["volume"].astype(float).mean()) if not lookback_20.empty else 0.0
    ctx["volume_ratio"] = (entry_volume / avg_vol) if avg_vol else 1.0

    # VWAP (20 bars).
    v = lookback_20["volume"].astype(float)
    typical = (lookback_20["high"].astype(float) + lookback_20["low"].astype(float) + lookback_20["close"].astype(float)) / 3.0
    vwap = float((typical * v).sum() / v.sum()) if float(v.sum()) > 0 else float(lookback_20["close"].astype(float).mean()) if not lookback_20.empty else entry_price
    ctx["vwap_distance"] = (entry_price - vwap) / vwap if vwap else 0.0

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

    return dict(ctx)


def build_price_contexts_from_bars_multi(
    *,
    raw_events: list[RawEvent],
    bars_by_timeframe: dict[str, pd.DataFrame],
    horizons_minutes: Iterable[int] = (1, 5, 15, 60),
) -> dict[str, dict]:
    """
    Build price contexts from multi-timeframe bars.

    bars_by_timeframe: mapping like {'1m': df, '1h': df, '1d': df} where each df
    contains at least [ticker, timestamp, open, high, low, close, volume].
    """
    # Pre-group by timeframe/ticker once.
    grouped: dict[str, dict[str, pd.DataFrame]] = {}
    for tf, df in (bars_by_timeframe or {}).items():
        if df is None or getattr(df, "empty", True):
            continue
        d = df.copy(deep=False)
        if "timestamp" in d.columns:
            d["timestamp"] = pd.to_datetime(d["timestamp"], utc=True)
        grouped[str(tf).strip().lower()] = {
            str(ticker): grp.reset_index(drop=True)
            for ticker, grp in d.groupby("ticker", sort=False)
        }

    ctxs: dict[str, dict] = {}
    tf_order = ["1m", "1h", "1d"]

    for evt in raw_events:
        ticker = evt.tickers[0] if evt.tickers else None
        if not ticker:
            continue
        t = str(ticker)
        chosen_tf = None
        chosen_bars = None
        for tf in tf_order:
            bars = grouped.get(tf, {}).get(t)
            if bars is not None and not bars.empty:
                chosen_tf = tf
                chosen_bars = bars
                break
        if chosen_tf is None or chosen_bars is None:
            continue
        ctx = build_price_context_for_event_with_resolution(
            ticker_bars=chosen_bars,
            event_ts=_to_utc(evt.timestamp),
            timeframe=chosen_tf,
            horizons_minutes=horizons_minutes,
        )
        if ctx:
            ctxs[evt.id] = ctx

    return ctxs

def build_price_contexts_from_bars(
    *,
    raw_events: list[RawEvent],
    bars: pd.DataFrame,
    horizons_minutes: Iterable[int] = (1, 5, 15, 60),
    bars_already_utc: bool = False,
) -> dict[str, dict]:
    """
    Builds a price_context dict keyed by raw_event.id from a multi-ticker bars DataFrame.
    """
    df = bars if bars_already_utc else bars.copy(deep=False)
    if not bars_already_utc:
        df = df.copy(deep=False)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Avoid repeated DataFrame filtering per event (O(events * bars)).
    # Group once and reuse per ticker.
    bars_by_ticker: dict[str, pd.DataFrame] = {
        str(ticker): grp.reset_index(drop=True)
        for ticker, grp in df.groupby("ticker", sort=False)
    }
    ctxs: dict[str, dict] = {}

    for evt in raw_events:
        ticker = evt.tickers[0] if evt.tickers else None
        if not ticker:
            continue
        ticker_bars = bars_by_ticker.get(str(ticker))
        if ticker_bars is None or ticker_bars.empty:
            continue
        ctxs[evt.id] = build_price_context_for_event(
            ticker_bars=ticker_bars,
            event_ts=_to_utc(evt.timestamp),
            horizons_minutes=horizons_minutes,
            assume_sorted=True,
        )
    return ctxs
