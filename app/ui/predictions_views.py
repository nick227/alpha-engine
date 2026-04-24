from __future__ import annotations

import streamlit as st

from app.services.dashboard_service import DashboardService, arrow
from app.ui.theme import COLORS


def _consensus_row(consensus) -> dict:
    return {
        "ticker": consensus.ticker,
        "direction": f"{arrow(consensus.direction)} {consensus.direction}",
        "confidence": f"{consensus.confidence:.0%}",
        "trust": f"{consensus.trust:.0%}" if getattr(consensus, "trust", None) is not None else "â€”",
        "strategies": consensus.participating_strategies,
        "regime": consensus.active_regime or "—",
    }


def predictions_views_main(
    service: DashboardService,
    *,
    tenant_id: str,
    ticker: str | None,
    run_id: str | None,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Intelligence / Predictions")
        st.caption("Views over forward-looking picks plus quick compare and overlays.")
    else:
        st.markdown("### Intelligence / Predictions")
        st.caption("Views over forward-looking picks plus quick compare and overlays.")

    tabs = st.tabs(["Best Picks", "Dips / Reversals", "Compare Tickers", "Strategy Overlays", "Timeline"])

    with tabs[0]:
        rankings = service.get_target_rankings(tenant_id=tenant_id, limit=20)
        rows = [
            {
                "ticker": r.ticker,
                "score": r.score,
                "conviction": r.conviction,
                "regime": r.regime,
                "timestamp": r.timestamp,
            }
            for r in rankings
            if r.score > 0
        ]
        rows.sort(key=lambda x: (x["score"] * x["conviction"]), reverse=True)
        st.dataframe(rows, use_container_width=True, hide_index=True)

    with tabs[1]:
        rankings = service.get_target_rankings(tenant_id=tenant_id, limit=20)
        rows = [
            {
                "ticker": r.ticker,
                "score": r.score,
                "conviction": r.conviction,
                "regime": r.regime,
                "timestamp": r.timestamp,
            }
            for r in rankings
            if r.score < 0
        ]
        rows.sort(key=lambda x: (x["score"] * x["conviction"]))
        st.dataframe(rows, use_container_width=True, hide_index=True)

    with tabs[2]:
        tickers = service.list_tickers(tenant_id=tenant_id)
        default = [ticker] if ticker in tickers else (tickers[:3] if tickers else [])
        selected = st.multiselect("Tickers", options=tickers, default=default)
        if not selected:
            st.info("Select at least one ticker.")
        else:
            rows = []
            for t in selected:
                by_h = service.get_consensus_by_horizon(tenant_id=tenant_id, ticker=t, horizons=["1d", "7d", "30d"])
                for h, c in by_h.items():
                    if c:
                        rows.append({**_consensus_row(c), "horizon": h})
                    else:
                        rows.append({"ticker": t, "horizon": h, "direction": "—", "confidence": "—", "strategies": "—", "regime": "—"})
            if rows:
                st.dataframe(rows, use_container_width=True, hide_index=True)
            else:
                st.info("No consensus rows available for selected tickers.")

    with tabs[3]:
        st.markdown("### Efficiency rankings")
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
                    "samples": r.samples,
                    "stability": r.stability,
                }
                for r in rankings
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.caption("For full overlays, use Intelligence Hub (matrix + overlays + timeline).")

    with tabs[4]:
        if not run_id:
            st.info("Select a run in the sidebar to view run-scoped tickers/strategies/timeline.")
        else:
            st.markdown("### Recent signals (run context)")
            recent = service.get_recent_signals(tenant_id=tenant_id, ticker=ticker, limit=50)
            st.dataframe(
                [
                    {
                        "time": s.time,
                        "ticker": s.ticker,
                        "direction": s.direction,
                        "confidence": s.confidence,
                        "trust": s.trust,
                        "strategy": s.strategy,
                        "regime": s.regime,
                    }
                    for s in recent
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.caption("Timeline overlays are available in Intelligence Hub for the selected ticker/run.")
