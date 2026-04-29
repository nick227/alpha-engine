from __future__ import annotations

import streamlit as st

from app.services.dashboard_service import DashboardService

# Routes where the sidebar adds no value.
_NO_SIDEBAR_ROUTES = {"ih", "audit"}

# Routes where Ticker is relevant.
_TICKER_ROUTES = {"dashboard", "explain", "paper", "ops"}

# Routes where Horizon is relevant.
_HORIZON_ROUTES = {"dashboard"}


def _selectbox(label: str, options: list[str], *, key: str, default: str | None = None) -> str:
    if not options:
        options = ["—"]
    current = st.session_state.get(key, default)
    if current not in options:
        current = options[0]
    idx = options.index(current)
    return st.selectbox(label, options=options, index=idx, key=key)


def render_sidebar_filters(service: DashboardService, *, route: str) -> dict:
    if route in _NO_SIDEBAR_ROUTES:
        # Nothing to control — collapse the sidebar.
        st.sidebar.empty()
        return {
            "tenant_id": "default",
            "ticker": None,
            "horizon_days": 7,
        }

    with st.sidebar:
        st.markdown("### Data Controls")

        tenants = service.list_tenants() or ["default"]
        default_tenant = "default" if "default" in tenants else tenants[0]
        tenant_id = _selectbox("Tenant", tenants, key="ui_tenant", default=default_tenant)

        ticker: str | None = None
        if route in _TICKER_ROUTES:
            tickers = service.list_tickers(tenant_id=tenant_id)
            ticker_raw = _selectbox("Ticker", tickers, key="ui_ticker", default=(tickers[0] if tickers else None))
            ticker = ticker_raw if ticker_raw != "—" else None

        horizon_days: int = 7
        if route in _HORIZON_ROUTES:
            horizon_days = st.selectbox("Horizon", options=[1, 7, 30], index=1, key="ui_horizon")

    filters = {
        "tenant_id": tenant_id,
        "ticker": ticker,
        "horizon_days": horizon_days,
    }
    st.session_state.ui_filters = filters
    return filters
