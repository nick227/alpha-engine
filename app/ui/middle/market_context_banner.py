"""Streamlit strip: VIX + regime; stale warning when context_warning is true."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import streamlit as st


@st.cache_data(ttl=120)
def _snapshot_market_context(db_path_resolved: str, as_of: str) -> dict[str, Any]:
    from app.db.repository import AlphaRepository
    from app.engine.ranking_temporal import build_market_context

    p = Path(db_path_resolved)
    if not p.is_file():
        return {}
    repo = AlphaRepository(db_path=str(p))
    try:
        return build_market_context(repo.conn, tenant_id="default", as_of_date=as_of)
    finally:
        repo.close()


def render_market_context_strip(*, db_path: str | None = None) -> None:
    """Shows VIX and regime; subtle stale warning for FE trust."""
    raw = db_path or os.getenv("ALPHA_DB_PATH", "data/alpha.db")
    try:
        resolved = str(Path(raw).resolve())
    except OSError:
        return
    as_of = date.today().isoformat()
    ctx = _snapshot_market_context(resolved, as_of)
    if not ctx:
        return
    vix = ctx.get("vix")
    reg = str(ctx.get("regime") or "?")
    warn = bool(ctx.get("context_warning", False))
    if vix is None:
        return
    try:
        vx = float(vix)
    except (TypeError, ValueError):
        return
    line = f"VIX {vx:.1f} · Regime {reg}"
    if warn:
        st.caption(f"{line} — ⚠ market context may be stale")
    else:
        st.caption(line)
