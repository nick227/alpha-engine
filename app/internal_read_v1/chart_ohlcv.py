"""OHLCV reads, resampling, history and candles payloads."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import sqlite3

from app.core.time_utils import to_utc_datetime
from app.internal_read_v1.chart_range_interval import window_start_end

UTC = timezone.utc
MAX_POINTS_CAP = 1500


def read_ohlcv_df(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    timeframe: str,
    start: datetime | None,
    end: datetime,
) -> pd.DataFrame:
    tf = str(timeframe).strip().lower()
    sql = """
        SELECT timestamp, open, high, low, close, volume
        FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = ?
    """
    params: list[Any] = [tenant_id, ticker, tf]
    if start is not None:
        sql += " AND timestamp >= ? AND timestamp <= ?"
        params.extend([start.isoformat(), end.isoformat()])
    else:
        sql += " AND timestamp <= ?"
        params.append(end.isoformat())
    sql += " ORDER BY timestamp ASC"
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("timestamp")


def _has_tf(conn: sqlite3.Connection, tenant_id: str, ticker: str, tf: str) -> bool:
    r = conn.execute(
        "SELECT 1 FROM price_bars WHERE tenant_id=? AND ticker=? AND timeframe=? LIMIT 1",
        (tenant_id, ticker, tf),
    ).fetchone()
    return r is not None


def pick_timeframe(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    range_key: str,
    interval_key: str,
) -> str:
    if range_key == "1D":
        if interval_key in ("1m", "5m", "30m", "1h"):
            if _has_tf(conn, tenant_id, ticker, "1m"):
                return "1m"
            if _has_tf(conn, tenant_id, ticker, "1h"):
                return "1h"
        return "1d"
    if range_key == "1W":
        if interval_key in ("30m", "1h", "1m", "5m"):
            if _has_tf(conn, tenant_id, ticker, "1h"):
                return "1h"
            if _has_tf(conn, tenant_id, ticker, "1m"):
                return "1m"
        return "1d"
    return "1d"


def _downsample_close(df: pd.DataFrame, interval_key: str, _range_key: str) -> list[dict[str, Any]]:
    if df.empty:
        return []
    s = df.set_index("timestamp").sort_index()["close"].dropna()
    if s.empty:
        return []
    if interval_key == "5m" and len(s) > 10:
        out = s.resample("5min").last().dropna()
    elif interval_key == "30m":
        out = s.resample("30min").last().dropna()
    elif interval_key == "1h":
        out = s.resample("1h").last().dropna()
    elif interval_key == "1m":
        out = s
    elif interval_key == "1D":
        out = s.resample("1D").last().dropna()
    elif interval_key == "1W":
        out = s.resample("1W").last().dropna()
    elif interval_key == "1Mo":
        out = s.resample("1ME").last().dropna()
    else:
        out = s.resample("1D").last().dropna()

    points: list[dict[str, Any]] = []
    for ts, val in out.items():
        if pd.isna(val):
            continue
        dt = ts.to_pydatetime()
        if interval_key in ("1m", "5m", "30m", "1h"):
            t_str = dt.isoformat()
        else:
            t_str = dt.strftime("%Y-%m-%d")
        points.append({"t": t_str, "c": float(val)})
    if len(points) > MAX_POINTS_CAP:
        step = max(1, math.ceil(len(points) / MAX_POINTS_CAP))
        points = points[::step]
    return points


def build_history_payload(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    range_key: str,
    interval_key: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = to_utc_datetime(now or datetime.now(UTC))
    start, end = window_start_end(range_key=range_key, now=now)
    tf = pick_timeframe(conn, tenant_id=tenant_id, ticker=ticker, range_key=range_key, interval_key=interval_key)
    df = read_ohlcv_df(conn, tenant_id=tenant_id, ticker=ticker, timeframe=tf, start=start, end=end)
    if df.empty and tf != "1d":
        df = read_ohlcv_df(conn, tenant_id=tenant_id, ticker=ticker, timeframe="1d", start=start, end=end)
        tf = "1d"
    points = _downsample_close(df, interval_key, range_key)
    return {
        "ticker": ticker,
        "range": range_key,
        "interval": interval_key,
        "timeframe_used": tf,
        "points": points,
    }


def build_candles_payload(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    range_key: str,
    interval_key: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = to_utc_datetime(now or datetime.now(UTC))
    start, end = window_start_end(range_key=range_key, now=now)
    tf = pick_timeframe(conn, tenant_id=tenant_id, ticker=ticker, range_key=range_key, interval_key=interval_key)
    df = read_ohlcv_df(conn, tenant_id=tenant_id, ticker=ticker, timeframe=tf, start=start, end=end)
    if df.empty:
        df = read_ohlcv_df(conn, tenant_id=tenant_id, ticker=ticker, timeframe="1d", start=start, end=end)
        tf = "1d"
    if df.empty:
        return {"ticker": ticker, "range": range_key, "interval": interval_key, "timeframe_used": tf, "candles": []}
    s = df.set_index("timestamp").sort_index()
    if interval_key == "1Mo":
        rule = "1ME"
        o = s["open"].resample(rule).first()
        h = s["high"].resample(rule).max()
        l = s["low"].resample(rule).min()
        c = s["close"].resample(rule).last()
        v = s["volume"].resample(rule).sum()
    elif interval_key == "1W":
        o = s["open"].resample("1W").first()
        h = s["high"].resample("1W").max()
        l = s["low"].resample("1W").min()
        c = s["close"].resample("1W").last()
        v = s["volume"].resample("1W").sum()
    else:
        o = s["open"].resample("1D").first()
        h = s["high"].resample("1D").max()
        l = s["low"].resample("1D").min()
        c = s["close"].resample("1D").last()
        v = s["volume"].resample("1D").sum()
    candles: list[dict[str, Any]] = []
    for ts in c.dropna().index:
        candles.append(
            {
                "t": ts.strftime("%Y-%m-%d"),
                "o": float(o.loc[ts]) if ts in o.index and pd.notna(o.loc[ts]) else None,
                "h": float(h.loc[ts]) if ts in h.index and pd.notna(h.loc[ts]) else None,
                "l": float(l.loc[ts]) if ts in l.index and pd.notna(l.loc[ts]) else None,
                "c": float(c.loc[ts]) if pd.notna(c.loc[ts]) else None,
                "v": float(v.loc[ts]) if ts in v.index and pd.notna(v.loc[ts]) else None,
            }
        )
    if len(candles) > MAX_POINTS_CAP:
        step = max(1, math.ceil(len(candles) / MAX_POINTS_CAP))
        candles = candles[::step]
    return {
        "ticker": ticker,
        "range": range_key,
        "interval": interval_key,
        "timeframe_used": tf,
        "candles": candles,
    }
