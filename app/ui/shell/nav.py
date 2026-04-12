from __future__ import annotations

import streamlit as st


ROUTES: list[tuple[str, str]] = [
    ("dashboard", "Dashboard"),
    ("paper", "Paper Trades"),
    ("ops", "Ops / Data"),
    ("ih", "Intelligence Hub"),
    ("audit", "Signal Audit"),
]

DEFAULT_ROUTE = ROUTES[0][0]


def get_route() -> str:
    route = st.session_state.get("ui_route", DEFAULT_ROUTE)
    if route not in {key for key, _ in ROUTES}:
        route = DEFAULT_ROUTE
    st.session_state.ui_route = route
    return route


def set_route(route: str) -> None:
    if route in {key for key, _ in ROUTES}:
        st.session_state.ui_route = route
