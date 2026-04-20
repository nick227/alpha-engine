"""Quote, company profile, and stats payloads from SQLite + optional JSON profile."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import sqlite3

from app.core.time_utils import to_utc_datetime
from app.internal_read_v1.chart_symbols import normalize_ticker

UTC = timezone.utc


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


def _avg_daily_volume(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    now: datetime,
    sessions: int = 30,
) -> float | None:
    rows = conn.execute(
        """
        SELECT volume FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d' AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (tenant_id, ticker, now.isoformat(), int(sessions)),
    ).fetchall()
    if not rows:
        return None
    vols = [float(r["volume"]) for r in rows if r["volume"] is not None]
    if not vols:
        return None
    return float(sum(vols) / len(vols))


def _day_change_pct_daily(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    now: datetime,
) -> float | None:
    rows = conn.execute(
        """
        SELECT close FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d' AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT 2
        """,
        (tenant_id, ticker, now.isoformat()),
    ).fetchall()
    if len(rows) < 2:
        return None
    last = float(rows[0]["close"])
    prev = float(rows[1]["close"])
    if prev == 0:
        return None
    return (last / prev - 1.0) * 100.0


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

    day_pct = _day_change_pct_daily(conn, tenant_id=tenant_id, ticker=ticker, now=now)
    avg_vol = _avg_daily_volume(conn, tenant_id=tenant_id, ticker=ticker, now=now, sessions=30)
    mc = prof.get("marketCap")
    market_cap: int | float | None
    if mc is None:
        market_cap = None
    elif isinstance(mc, (int, float)):
        market_cap = int(mc) if isinstance(mc, float) and mc == int(mc) else mc
    else:
        try:
            market_cap = float(mc)
        except (TypeError, ValueError):
            market_cap = None

    return {
        "ticker": ticker,
        "price": round(price, 4),
        "dayChangePct": None if day_pct is None else round(day_pct, 4),
        "high52": round(high52, 4),
        "low52": round(low52, 4),
        "avgVolume": None if avg_vol is None else round(avg_vol, 2),
        "marketCap": market_cap,
        "ath": round(ath, 4),
        "ipoDate": ipo_raw,
        "yearsListed": years_listed,
    }
