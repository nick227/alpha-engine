from __future__ import annotations

import streamlit as st

from app.ui.middle.dashboard_service import DashboardService


def _selectbox(label: str, options: list[str], *, key: str, default: str | None = None) -> str:
    if not options:
        options = ["â€”"]
    current = st.session_state.get(key, default)
    if current not in options:
        current = options[0]
    idx = options.index(current)
    return st.selectbox(label, options=options, index=idx, key=key)


def render_sidebar_filters(service: DashboardService) -> dict:
    with st.sidebar:
        st.markdown("### Data Controls")
        tenants = service.list_tenants() or ["default"]
        # Prefer 'default' tenant if it exists, otherwise use the first one
        default_tenant = "default" if "default" in tenants else tenants[0]
        tenant_id = _selectbox("Tenant", tenants, key="ui_tenant", default=default_tenant)

        runs = sorted(service.list_prediction_runs(tenant_id=tenant_id), key=lambda r: r.created_at, reverse=True)
        run_labels = ["Latest"] + [r.label for r in runs]
        run_label = _selectbox("Run", run_labels, key="ui_run", default="Latest")
        run_id = None if run_label == "Latest" else next((r.id for r in runs if r.label == run_label), None)

        tickers = (
            service.list_run_tickers(run_id=run_id, tenant_id=tenant_id)
            if run_id
            else service.list_tickers(tenant_id=tenant_id)
        )
        ticker = _selectbox("Ticker", tickers, key="ui_ticker", default=(tickers[0] if tickers else None))

        timeframe = _selectbox("Timeframe", ["1M", "3M", "6M", "1Y"], key="ui_timeframe", default="3M")
        horizon_days = st.selectbox("Horizon", options=[1, 7, 30], index=1, key="ui_horizon")

        strategies: list[str] = []
        if run_id:
            strategies = service.list_run_strategies(run_id=run_id, tenant_id=tenant_id)
        strategy_options = ["(Any)"] + strategies
        strategy_id = _selectbox("Strategy", strategy_options, key="ui_strategy", default="(Any)")
        strategy_id = None if strategy_id == "(Any)" else strategy_id

        st.divider()
        st.caption("Shared filters apply across Dashboard, Intelligence Hub, and Signal Audit.")

    filters = {
        "tenant_id": tenant_id,
        "run_id": run_id,
        "ticker": ticker if ticker != "â€”" else None,
        "timeframe": timeframe,
        "horizon_days": horizon_days,
        "strategy_id": strategy_id,
    }
    st.session_state.ui_filters = filters
    return filters
