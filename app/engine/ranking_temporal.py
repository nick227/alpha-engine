"""
Lightweight temporal modifiers for queue / prediction ranking.

No insights engine — VIX from price_bars, regime from thresholds, optional sentiment stub.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import date
from typing import Any

# Set ALPHA_RANK_TEMPORAL=0 to disable multipliers (base rank only).
_RANK_TEMPORAL = os.getenv("ALPHA_RANK_TEMPORAL", "1").strip().lower() not in ("0", "false", "no")


def _calendar_date_from_bar_ts(ts_raw: str | None) -> date | None:
    if not ts_raw:
        return None
    s = str(ts_raw).strip()
    if len(s) >= 10:
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            pass
    return None


def fetch_vix_row_on_or_before(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    as_of_date: str,
) -> tuple[float, str] | None:
    """
    Latest daily ^VIX bar on or before as_of_date.
    Returns (close, timestamp raw string from DB) or None if missing.
    """
    row = conn.execute(
        """
        SELECT close, timestamp FROM price_bars
        WHERE tenant_id = ?
          AND ticker = '^VIX'
          AND timeframe = '1d'
          AND date(timestamp) <= date(?)
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (tenant_id, as_of_date),
    ).fetchone()
    if not row or row["close"] is None:
        return None
    ts = str(row["timestamp"] or "")
    return (float(row["close"]), ts)


def fetch_vix_close_on_or_before(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    as_of_date: str,
) -> float | None:
    """Latest daily ^VIX close on or before as_of_date (calendar)."""
    got = fetch_vix_row_on_or_before(conn, tenant_id=tenant_id, as_of_date=as_of_date)
    return None if got is None else got[0]


def infer_regime(vix: float) -> str:
    if vix < 15.0:
        return "low"
    if vix > 30.0:
        return "high"
    return "normal"


def normalize_strategy_type(strategy_key: str) -> str:
    k = (strategy_key or "").strip().lower()
    if k.startswith("discovery_"):
        k = k[len("discovery_") :]
    for suf in ("_v1_paper", "_paper"):
        if k.endswith(suf):
            k = k[: -len(suf)]
    return k


def build_market_context(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    as_of_date: str,
    sentiment: str | None = None,
    vix_override: float | None = None,
) -> dict[str, Any]:
    """
    Thin snapshot for ranking adjustments and FE trust signals.
    If VIX is missing in DB, uses 20.0 and regime "normal" (neutral default);
    sets vix_fallback_used and context_warning.
    """
    vix_fallback_used = False
    vix_ts_raw: str | None = None
    vix_bar_day: date | None = None

    try:
        as_of_day = date.fromisoformat(as_of_date[:10])
    except ValueError:
        as_of_day = date.today()

    if vix_override is not None:
        vix = float(vix_override)
        vix_bar_day = as_of_day
        vix_ts_iso = as_of_day.isoformat()
    else:
        row = fetch_vix_row_on_or_before(conn, tenant_id=tenant_id, as_of_date=as_of_date)
        if row is None:
            vix = 20.0
            vix_fallback_used = True
            vix_ts_iso = ""
        else:
            vix, vix_ts_raw = row
            vix_bar_day = _calendar_date_from_bar_ts(vix_ts_raw)
            vix_ts_iso = vix_bar_day.isoformat() if vix_bar_day else (vix_ts_raw[:10] if vix_ts_raw and len(vix_ts_raw) >= 10 else "")

    reg = infer_regime(vix)
    try:
        _y, m, _ = as_of_date.split("-", 2)
        month = int(m)
    except (ValueError, IndexError):
        month = date.today().month
    sent = (sentiment or "neutral").strip().lower()
    if sent not in ("positive", "neutral", "negative"):
        sent = "neutral"

    vix_age_days: int | None = None
    if vix_fallback_used or not vix_bar_day:
        vix_age_days = None
    else:
        vix_age_days = (as_of_day - vix_bar_day).days

    context_warning = bool(vix_fallback_used or (vix_age_days is not None and vix_age_days > 1))

    out: dict[str, Any] = {
        "vix": vix,
        "regime": reg,
        "sentiment": sent,
        "month": month,
        "vix_timestamp": vix_ts_iso if vix_ts_iso else None,
        "vix_fallback_used": vix_fallback_used,
        "vix_age_days": vix_age_days,
        "context_warning": context_warning,
    }
    return out


def apply_temporal_adjustment(strategy_key: str, context: dict[str, Any]) -> float:
    """
    Multiplier applied to base rank score. Safe defaults — no hard filters.
    strategy_key: discovery strategy id or type (normalized internally).
    """
    if not _RANK_TEMPORAL:
        return 1.0
    st = normalize_strategy_type(strategy_key)
    vix = float(context.get("vix") or 20.0)
    month = int(context.get("month") or 0)

    m = 1.0

    if vix > 30.0:
        if st in ("mean_reversion", "vol_crush"):
            m *= 0.7
        if st in ("breakout", "volatility_breakout"):
            m *= 1.2

    if vix < 15.0 and st == "silent_compounder":
        m *= 1.15

    if month == 9:
        m *= 0.9

    return m
