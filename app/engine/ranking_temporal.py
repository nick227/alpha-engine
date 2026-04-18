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


def fetch_vix_close_on_or_before(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    as_of_date: str,
) -> float | None:
    """Latest daily ^VIX close on or before as_of_date (calendar)."""
    row = conn.execute(
        """
        SELECT close FROM price_bars
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
    return float(row["close"])


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
    Thin snapshot for ranking adjustments.
    If VIX is missing in DB, uses 20.0 and regime "normal" (neutral default).
    """
    vix = float(vix_override) if vix_override is not None else fetch_vix_close_on_or_before(conn, tenant_id=tenant_id, as_of_date=as_of_date)
    if vix is None:
        vix = 20.0
    reg = infer_regime(vix)
    try:
        y, m, _ = as_of_date.split("-", 2)
        month = int(m)
    except (ValueError, IndexError):
        month = date.today().month
    sent = (sentiment or "neutral").strip().lower()
    if sent not in ("positive", "neutral", "negative"):
        sent = "neutral"
    return {
        "vix": vix,
        "regime": reg,
        "sentiment": sent,
        "month": month,
    }


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
