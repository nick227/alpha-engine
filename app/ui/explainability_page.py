"""Explainability: why ticker, performance, matrix, daily diffs, top-N health — read-only over existing DB."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.ui.middle.dashboard_service import DashboardService


def explainability_main(
    service: DashboardService,
    *,
    tenant_id: str,
    ticker: str | None,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Explainability")
        st.caption("Read models only: predictions, candidate_queue, outcomes, admission_metrics.")
    else:
        st.markdown("### Explainability")

    tabs = st.tabs(["Why / Admission", "Performance & Matrix", "What changed", "Top-N health"])

    tickers = service.list_tickers(tenant_id=tenant_id)
    default_t = ticker if ticker and ticker in tickers else (tickers[0] if tickers else None)
    _idx = tickers.index(default_t) if default_t and default_t in tickers else 0

    with tabs[0]:
        t = st.selectbox("Ticker", options=tickers, index=_idx) if tickers else None
        if not t:
            st.info("No tickers in active universe.")
        else:
            panel = service.get_explain_ticker_panel(tenant_id=tenant_id, ticker=t)
            st.subheader("candidate_queue")
            cq = panel.get("candidate_queue")
            if cq:
                st.json(cq)
            else:
                st.caption("No row (not seen in discovery pipeline yet, or different tenant).")

            st.subheader("Recent predictions + ranking context")
            rows = panel.get("recent_predictions") or []
            if not rows:
                st.caption("No predictions for this ticker.")
            else:
                slim = []
                for r in rows[:15]:
                    slim.append(
                        {
                            "strategy_id": r.get("strategy_id"),
                            "timestamp": r.get("timestamp"),
                            "prediction": r.get("prediction"),
                            "confidence": r.get("confidence"),
                            "rank_score": r.get("rank_score"),
                            "rank_score_base": r.get("rank_score_base"),
                            "temporal_multiplier": r.get("temporal_multiplier"),
                            "vix_age_days": r.get("vix_age_days"),
                            "context_warning": r.get("context_warning"),
                            "vix_fallback_used": r.get("vix_fallback_used"),
                        }
                    )
                st.dataframe(pd.DataFrame(slim), use_container_width=True, hide_index=True)

    with tabs[1]:
        t2 = st.selectbox("Ticker (performance)", options=tickers, key="perf_t", index=_idx) if tickers else None
        if t2:
            perf = service.get_explain_per_ticker_performance(tenant_id=tenant_id, ticker=t2)
            for wname, block in (perf.get("windows") or {}).items():
                st.markdown(f"**{wname}**")
                st.caption(f"best: {block.get('best_strategy')} | worst: {block.get('worst_strategy')}")
                df = pd.DataFrame(block.get("by_strategy") or [])
                if not df.empty:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No outcomes in window.")

        st.subheader("Strategy × ticker matrix (win_rate, n)")
        lb = st.slider("Lookback days", 30, 365, 90, key="mx_lb")
        mx = service.get_explain_strategy_ticker_matrix(tenant_id=tenant_id, tickers=None, lookback_days=int(lb))
        if mx:
            dfm = pd.DataFrame(mx)
            dfm["low_sample"] = dfm["n"].fillna(0) < 5
            disp = dfm.drop(columns=["low_sample"])
            styler = disp.style.apply(
                lambda row: [
                    "background-color: #e8e8e8" if bool(dfm.loc[row.name, "low_sample"]) else ""
                ]
                * len(row),
                axis=1,
            )
            st.dataframe(styler, use_container_width=True, hide_index=True)
        else:
            st.caption("No matrix rows (need prediction_outcomes in lookback).")

    with tabs[2]:
        hrs = st.slider("Hours", 6, 72, 24, key="chg_h")
        ch = service.get_explain_what_changed(tenant_id=tenant_id, hours=int(hrs))
        st.subheader("Admission runs (metrics)")
        st.dataframe(pd.DataFrame(ch.get("admission_runs") or []), use_container_width=True, hide_index=True)
        st.subheader("Overrule swaps (from detail json)")
        st.json(ch.get("swaps_recent") or [])
        st.subheader("candidate_queue touches")
        st.dataframe(pd.DataFrame(ch.get("candidate_touch_recent") or []), use_container_width=True, hide_index=True)

    with tabs[3]:
        lim = st.number_input("Top N from rankings", min_value=5, max_value=50, value=20)
        q = service.get_explain_topn_quality(tenant_id=tenant_id, limit=int(lim))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tickers", q.get("n"))
        c2.metric("Avg multiplier (cq)", f"{q.get('avg_multiplier'):.3f}" if q.get("avg_multiplier") is not None else "—")
        c3.metric("Lens buckets", len(q.get("lens_counts") or {}))
        c4.metric("% context_warning", f"{float(q.get('pct_context_warning') or 0):.0%}")
        st.json(q.get("lens_counts") or {})
        st.subheader("Head rankings")
        st.dataframe(pd.DataFrame(q.get("rankings_head") or []), use_container_width=True, hide_index=True)
