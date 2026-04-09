from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from app.core.time_utils import to_utc_datetime


@dataclass(frozen=True, slots=True)
class MacroSnapshot:
    asof: datetime
    features: dict[str, float]


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


def _pct_change(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b) - 1.0


def fetch_daily_closes(*, symbol: str, start: datetime, end: datetime) -> pd.Series:
    """
    Fetch daily close prices from yfinance for [start, end) (UTC).

    Returns a Series indexed by UTC datetime (normalized to midnight UTC).
    """
    try:
        import yfinance as yf  # type: ignore
    except Exception as e:
        raise ImportError("yfinance is not installed; cannot fetch macro series.") from e

    start_utc = to_utc_datetime(start).replace(microsecond=0)
    end_utc = to_utc_datetime(end).replace(microsecond=0)
    if start_utc >= end_utc:
        return pd.Series(dtype="float64")

    df = yf.download(
        tickers=str(symbol),
        start=start_utc,
        end=end_utc,
        interval="1d",
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=False,
    )
    if df is None or getattr(df, "empty", True):
        return pd.Series(dtype="float64")
    try:
        df = df.reset_index()
    except Exception:
        return pd.Series(dtype="float64")

    ts_col = "Date" if "Date" in df.columns else ("Datetime" if "Datetime" in df.columns else None)
    if ts_col is None or "Close" not in df.columns:
        return pd.Series(dtype="float64")

    items: list[tuple[datetime, float]] = []
    for _, row in df.iterrows():
        try:
            ts = row[ts_col]
            if not isinstance(ts, datetime):
                ts = datetime.fromisoformat(str(ts))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ts = ts.astimezone(timezone.utc).replace(microsecond=0)
            close = _safe_float(row["Close"])
            if close is None:
                continue
            items.append((ts, close))
        except Exception:
            continue

    if not items:
        return pd.Series(dtype="float64")

    items.sort(key=lambda x: x[0])
    idx = [t for t, _ in items]
    vals = [v for _, v in items]
    return pd.Series(vals, index=pd.to_datetime(idx, utc=True), dtype="float64")


def build_macro_snapshot_for_asof(
    *,
    name: str,
    closes: pd.Series,
    asof: datetime,
) -> MacroSnapshot:
    """
    Derive macro context features (close, returns, volatility, trend) from a close series.

    - return_1d: pct change vs prior close
    - return_5d: pct change vs 5 trading closes ago
    - volatility_10d: std dev of daily returns over last 10 closes
    - trend_20d: pct change vs 20 trading closes ago
    """
    asof_utc = to_utc_datetime(asof).replace(microsecond=0)
    if closes is None or closes.empty:
        return MacroSnapshot(asof=asof_utc, features={})

    s = closes.sort_index()
    s = s[s.index < pd.Timestamp(asof_utc)]
    if s.empty:
        return MacroSnapshot(asof=asof_utc, features={})

    # last close prior to asof
    last_close = _safe_float(s.iloc[-1])
    if last_close is None:
        return MacroSnapshot(asof=asof_utc, features={})

    prev_close = _safe_float(s.iloc[-2]) if len(s) >= 2 else None
    close_5 = _safe_float(s.iloc[-6]) if len(s) >= 6 else None
    close_20 = _safe_float(s.iloc[-21]) if len(s) >= 21 else None

    ret_1d = _pct_change(last_close, prev_close)
    ret_5d = _pct_change(last_close, close_5)
    trend_20d = _pct_change(last_close, close_20)

    vol_10d: float | None = None
    if len(s) >= 11:
        window = s.iloc[-11:]
        rets = window.pct_change().dropna()
        try:
            v = float(rets.std(ddof=0))
            if not pd.isna(v):
                vol_10d = v
        except Exception:
            vol_10d = None

    prefix = str(name).strip().lower()
    feats: dict[str, float] = {}
    feats[f"{prefix}_close"] = float(last_close)
    if ret_1d is not None:
        feats[f"{prefix}_return_1d"] = float(ret_1d)
    if ret_5d is not None:
        feats[f"{prefix}_return_5d"] = float(ret_5d)
    if vol_10d is not None:
        feats[f"{prefix}_volatility_10d"] = float(vol_10d)
    if trend_20d is not None:
        feats[f"{prefix}_trend_20d"] = float(trend_20d)

    return MacroSnapshot(asof=asof_utc, features=feats)


def fetch_and_build_macro_features(
    *,
    specs: list[tuple[str, str]],
    asof: datetime,
    lookback_days: int = 60,
) -> MacroSnapshot:
    """
    Fetch yfinance daily closes and build a combined macro feature dict.

    specs: list of (name, symbol)
    """
    asof_utc = to_utc_datetime(asof).replace(microsecond=0)
    start = asof_utc - timedelta(days=int(lookback_days))
    end = asof_utc + timedelta(days=1)

    combined: dict[str, float] = {}
    for name, symbol in specs:
        closes = fetch_daily_closes(symbol=str(symbol), start=start, end=end)
        snap = build_macro_snapshot_for_asof(name=str(name), closes=closes, asof=asof_utc)
        combined.update(snap.features)

    return MacroSnapshot(asof=asof_utc, features=combined)

