from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from app.services.dashboard_service import DashboardService, PredictionAnalyticsQuery


def backtest_strategy_analysis_main(
    service: DashboardService,
    *,
    tenant_id: str,
    run_id: str | None,
    ticker: str | None,
    strategy_id: str | None,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Backtest / Strategy Analysis")
        st.caption("Run-scoped ranking + variance chart + core metrics.")
    else:
        st.markdown("### Backtest / Strategy Analysis")
        st.caption("Run-scoped ranking + variance chart + core metrics.")

    if not run_id:
        st.info("Select a prediction run in the sidebar to enable this view.")
        return

    # Efficiency rankings
    st.markdown("### Strategy ranking (efficiency)")
    horizon_days = st.session_state.get("ui_filters", {}).get("horizon_days")
    forecast_days = int(horizon_days) if horizon_days else None
    rankings = service.get_efficiency_rankings(
        tenant_id=tenant_id,
        ticker=ticker,
        forecast_days=forecast_days,
        limit=25,
    )
    st.dataframe(
        [
            {
                "strategy": r.strategy_id,
                "horizon_days": r.forecast_days,
                "efficiency": r.avg_efficiency_rating,
                "alpha": r.alpha_strategy,
                "win_rate": r.win_rate,
                "avg_return": r.avg_return,
                "drawdown": r.drawdown,
                "samples": r.samples,
            }
            for r in rankings
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Forecast variance (predicted vs actual)")
    q = PredictionAnalyticsQuery(
        tenant_id=tenant_id,
        run_id=run_id,
        ticker=ticker,
        strategy_id=strategy_id,
    )
    result = service.get_prediction_analytics(q)

    if result.metric_cards:
        cols = st.columns(min(4, len(result.metric_cards)))
        for idx, card in enumerate(result.metric_cards[:4]):
            with cols[idx]:
                st.metric(card["label"], card["value"])

    if result.chart_card and result.chart_card.get("predicted") and result.chart_card.get("actual"):
        fig = go.Figure()
        pred = result.chart_card["predicted"]
        act = result.chart_card["actual"]
        fig.add_trace(go.Scatter(x=[p["x"] for p in act], y=[p["y"] for p in act], mode="lines", name="actual"))
        fig.add_trace(go.Scatter(x=[p["x"] for p in pred], y=[p["y"] for p in pred], mode="lines", name="predicted"))
        fig.update_layout(height=420, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select both a ticker and a strategy (sidebar) to view the variance chart.")

    if result.leaderboard_card and result.leaderboard_card.get("data"):
        st.markdown("### Champion vs challenger (top rows)")
        st.dataframe(result.leaderboard_card["data"], use_container_width=True, hide_index=True)
