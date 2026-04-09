from __future__ import annotations

import streamlit as st

from app.ui.shell.chat_panel import render_chat_panel
from app.ui.shell.nav import ROUTES, get_route, set_route


def render_top_bar(*, filters: dict) -> str:
    current_route = get_route()

    st.markdown(
        """
        <div style="
            display:flex;
            justify-content:space-between;
            align-items:flex-end;
            gap:16px;
            padding:16px 0 8px;
            border-bottom:1px solid var(--color-border);
            margin-bottom:20px;
        ">
            <div>
                <div style="font-size:12px;color:var(--color-text-secondary);text-transform:uppercase;letter-spacing:0.08em;">
                    Alpha Engine
                </div>
                <div style="font-size:28px;font-weight:700;color:var(--color-text-primary);line-height:1.1;">
                    Unified Research Shell
                </div>
            </div>
            <div style="font-size:12px;color:var(--color-text-secondary);text-align:right;">
                Shared context: ticker / horizon / run travel with you
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    nav_col, context_col, chat_col = st.columns([7, 4, 2])
    with nav_col:
        route_cols = st.columns(len(ROUTES))
        for idx, (route_key, label) in enumerate(ROUTES):
            with route_cols[idx]:
                if st.button(
                    label,
                    key=f"route_{route_key}",
                    type="primary" if current_route == route_key else "secondary",
                    use_container_width=True,
                ):
                    set_route(route_key)
                    current_route = route_key

    with context_col:
        ticker = filters.get("ticker") or "All tickers"
        horizon = filters.get("horizon_days")
        run_id = filters.get("run_id")
        st.caption(
            f"Context: {ticker} | {horizon}d horizon | "
            f"{run_id[:8] if isinstance(run_id, str) and run_id else 'latest run'}"
        )

    with chat_col:
        with st.popover("Assistant", use_container_width=True):
            render_chat_panel()

    return current_route
