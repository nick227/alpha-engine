"""
Chart-ready reads from SQLite `price_bars` (pandas resampling). No network I/O.
"""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import sqlite3

from app.core.time_utils import to_utc_datetime

UTC = timezone.utc
MAX_POINTS_CAP = 1500

RANGE_ALIASES = {
    "1d": "1D",
    "1w": "1W",
    "1mo": "1M",
    "3mo": "3M",
    "1y": "1Y",
    "5y": "5Y",
    "max": "MAX",
}

# Second letter distinguishes minute (m) vs month (Mo) in defaults
DEFAULT_INTERVAL: dict[str, str] = {
    "1D": "5m",
    "1W": "30m",
    "1M": "1D",
    "3M": "1D",
    "1Y": "1D",
    "5Y": "1W",
    "MAX": "1Mo",
}


def normalize_ticker(ticker: str) -> str:
    return str(ticker or "").strip().upper()


def parse_range_key(raw: str | None) -> str:
    if not raw:
        return "1Y"
    low = str(raw).strip().lower()
    if low in RANGE_ALIASES:
        return RANGE_ALIASES[low]
    s = str(raw).strip().upper()
    if s in ("1D", "1W", "1M", "3M", "1Y", "5Y", "MAX"):
        return s
    raise ValueError(f"invalid range: {raw}")


_INTERVAL_RE = re.compile(r"^(?P<n>\d+)(?P<u>m|mo|min|h|d|w|y)$", re.I)


def parse_interval_key(raw: str | None, range_key: str) -> str:
    if raw is None or str(raw).strip() == "":
        return DEFAULT_INTERVAL[range_key]
    s = str(raw).strip()
    u = s.upper()
    # explicit tokens
    if u in ("1MO", "1MO.", "MONTH", "MONTHLY"):
        return "1Mo"
    if u in ("1W", "WEEK", "WEEKLY"):
        return "1W"
    if u in ("1D", "DAY", "DAILY"):
        return "1D"
    if u in ("1H", "60M", "60MIN"):
        return "1h"
    if u in ("30M", "30MIN"):
        return "30m"
    if u in ("5M", "5MIN"):
        return "5m"
    if u == "1M":
        if range_key in ("MAX", "5Y", "1Y", "3M"):
            return "1Mo"
        return "1m"
    m = _INTERVAL_RE.match(s.strip())
    if m:
        n, unit = m.group("n"), m.group("u").lower()
        if unit == "mo":
            return "1Mo"
        if unit == "m" and n == "1":
            return "1m"
        if unit == "m" and n == "5":
            return "5m"
        if unit == "m" and n == "30":
            return "30m"
        if unit == "h" and n == "1":
            return "1h"
        if unit == "d" and n == "1":
            return "1D"
        if unit == "w" and n == "1":
            return "1W"
    raise ValueError(f"invalid interval: {raw}")


def _window_start_end(*, range_key: str, now: datetime) -> tuple[datetime | None, datetime]:
    end = to_utc_datetime(now)
    if range_key == "MAX":
        return None, end
    deltas = {
        "1D": timedelta(days=2),
        "1W": timedelta(days=8),
        "1M": timedelta(days=32),
        "3M": timedelta(days=95),
        "1Y": timedelta(days=370),
        "5Y": timedelta(days=365 * 5 + 5),
    }
    return end - deltas[range_key], end


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


def _downsample_close(df: pd.DataFrame, interval_key: str, range_key: str) -> list[dict[str, Any]]:
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
    start, end = _window_start_end(range_key=range_key, now=now)
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


def build_quote_payload(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    now = to_utc_datetime(now or datetime.now(UTC))
    end_iso = now.isoformat()
    for tf in ("1m", "1h", "1d"):
        row = conn.execute(
            """
            SELECT timestamp, close FROM price_bars
            WHERE tenant_id = ? AND ticker = ? AND timeframe = ? AND timestamp <= ?
            ORDER BY timestamp DESC LIMIT 1
            """,
            (tenant_id, ticker, tf, end_iso),
        ).fetchone()
        if row:
            return {
                "ticker": ticker,
                "price": float(row["close"]),
                "time": str(row["timestamp"]),
                "timeframe": tf,
            }
    return None


def load_company_profile_json(ticker: str) -> dict[str, Any]:
    root = Path(os.environ.get("COMPANY_PROFILES_DIR", "data/company_profiles"))
    path = root / f"{normalize_ticker(ticker)}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_company_payload(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
) -> dict[str, Any]:
    prof = load_company_profile_json(ticker)
    row = conn.execute(
        """
        SELECT sector, industry FROM fundamentals_snapshot
        WHERE tenant_id = ? AND ticker = ?
        ORDER BY as_of_date DESC LIMIT 1
        """,
        (tenant_id, ticker),
    ).fetchone()
    sec = row["sector"] if row else None
    ind = row["industry"] if row else None
    return {
        "ticker": ticker,
        "shortName": prof.get("shortName"),
        "longName": prof.get("longName"),
        "sector": sec or prof.get("sector"),
        "industry": ind or prof.get("industry"),
        "website": prof.get("website"),
        "country": prof.get("country"),
        "marketCap": prof.get("marketCap"),
        "beta": prof.get("beta"),
        "profile_loaded": bool(prof),
    }


def _first_bar_date(conn: sqlite3.Connection, *, tenant_id: str, ticker: str) -> str | None:
    row = conn.execute(
        "SELECT MIN(timestamp) FROM price_bars WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'",
        (tenant_id, ticker),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return to_utc_datetime(row[0]).strftime("%Y-%m-%d")


def build_stats_payload(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    now = to_utc_datetime(now or datetime.now(UTC))
    q = build_quote_payload(conn, tenant_id=tenant_id, ticker=ticker, now=now)
    if q is None:
        return None
    price = float(q["price"])
    start_52w = now - timedelta(days=372)
    row_52 = conn.execute(
        """
        SELECT MAX(high) AS mx, MIN(low) AS mn FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
          AND timestamp >= ? AND timestamp <= ?
        """,
        (tenant_id, ticker, start_52w.isoformat(), now.isoformat()),
    ).fetchone()
    high52 = float(row_52["mx"]) if row_52 and row_52["mx"] is not None else price
    low52 = float(row_52["mn"]) if row_52 and row_52["mn"] is not None else price
    row_ath = conn.execute(
        """
        SELECT MAX(high) AS mx FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
        """,
        (tenant_id, ticker),
    ).fetchone()
    ath = float(row_ath["mx"]) if row_ath and row_ath["mx"] is not None else price
    prof = load_company_profile_json(ticker)
    ipo_raw = prof.get("ipoDate") if isinstance(prof.get("ipoDate"), str) else None
    if not ipo_raw:
        ipo_raw = _first_bar_date(conn, tenant_id=tenant_id, ticker=ticker)
    years_listed: int | None = None
    if ipo_raw:
        try:
            ipo_d = datetime.strptime(ipo_raw[:10], "%Y-%m-%d").replace(tzinfo=UTC)
            years_listed = max(0, (now - ipo_d).days // 365)
        except Exception:
            years_listed = None
    return {
        "ticker": ticker,
        "price": round(price, 4),
        "high52": round(high52, 4),
        "low52": round(low52, 4),
        "ath": round(ath, 4),
        "ipoDate": ipo_raw,
        "yearsListed": years_listed,
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
    start, end = _window_start_end(range_key=range_key, now=now)
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
