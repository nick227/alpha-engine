"""Explainability: why ticker, performance, matrix, daily diffs, top-N health — read-only over existing DB."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.ui.middle.dashboard_service import DashboardService
from app.ui.middle.explainability_constants import MIN_SAMPLE_N


def _style_low_sample_rows(df: pd.DataFrame, *, n_col: str = "n"):
    """Gray background when n < MIN_SAMPLE_N (matches strategy×ticker matrix)."""
    if n_col not in df.columns:
        return df.style
    low = df[n_col].fillna(0).astype(float) < float(MIN_SAMPLE_N)
    return df.style.apply(
        lambda row: ["background-color: #e8e8e8" if bool(low.loc[row.name]) else ""] * len(row),
        axis=1,
    )


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

    tabs = st.tabs(
        [
            "Why / Admission",
            "Performance & Matrix",
            "Rank movers",
            "What changed",
            "Top-N health",
        ]
    )

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
                    cd = r.get("confidence_decomposition") or {}
                    slim.append(
                        {
                            "strategy_id": r.get("strategy_id"),
                            "timestamp": r.get("timestamp"),
                            "prediction": r.get("prediction"),
                            "model_confidence": cd.get("model_confidence"),
                            "rank_score_base": cd.get("rank_score_base"),
                            "temporal_mult": cd.get("temporal_multiplier"),
                            "base_times_temporal": cd.get("base_times_temporal"),
                            "rank_score": cd.get("rank_score_after_temporal"),
                            "vix_age_days": r.get("vix_age_days"),
                            "context_warning": r.get("context_warning"),
                            "vix_fallback_used": r.get("vix_fallback_used"),
                        }
                    )
                st.dataframe(pd.DataFrame(slim), use_container_width=True, hide_index=True)

            ot_n = st.slider("Outcomes for trend", 5, 10, 10, key="ot_n")
            trend = service.get_explain_outcome_trend(
                tenant_id=tenant_id, ticker=t, last_n=int(ot_n)
            )
            st.subheader("Recent outcome trend")
            if trend.get("low_sample"):
                st.warning(
                    f"Low sample: need at least **{MIN_SAMPLE_N}** outcomes per total window and each half "
                    f"(have n={trend.get('n_actual')}, halves {trend.get('half_first_n')}/"
                    f"{trend.get('half_second_n')}). Treat trend as noisy."
                )
            c1, c2, c3 = st.columns(3)
            c1.metric("Trend", trend.get("trend") or "—")
            w1 = trend.get("win_rate_first_half")
            w2 = trend.get("win_rate_second_half")
            c2.metric("Win rate (older half)", f"{w1:.0%}" if w1 is not None else "—")
            c3.metric("Win rate (newer half)", f"{w2:.0%}" if w2 is not None else "—")
            olist = trend.get("outcomes") or []
            if olist:
                st.dataframe(pd.DataFrame(olist), use_container_width=True, hide_index=True)
            else:
                st.caption("No evaluated outcomes yet for this ticker.")

    with tabs[1]:
        wk = service.get_explain_weekly_performance(tenant_id=tenant_id)
        st.subheader("Weekly performance (last 7 days, evaluated outcomes)")
        oa = wk.get("overall") or {}
        if oa.get("low_sample"):
            st.warning(
                f"Weekly aggregate is low sample (n={oa.get('n')} < **{MIN_SAMPLE_N}**). "
                "Win rate / return are noisy."
            )
        elif int(oa.get("n") or 0) == 0:
            st.caption("No evaluated outcomes in the 7-day window.")
        w1, w2, w3 = st.columns(3)
        w1.metric("N outcomes", int(oa.get("n") or 0))
        wr = oa.get("win_rate")
        w2.metric("Win rate", f"{float(wr):.1%}" if wr is not None else "—")
        ar = oa.get("avg_return")
        w3.metric("Avg return", f"{float(ar):.3f}" if ar is not None else "—")
        bs = wk.get("by_strategy") or []
        if bs:
            df_bs = pd.DataFrame(bs)
            st.dataframe(_style_low_sample_rows(df_bs), use_container_width=True, hide_index=True)
        else:
            st.caption("No outcomes in the 7-day window.")

        t2 = st.selectbox("Ticker (performance)", options=tickers, key="perf_t", index=_idx) if tickers else None
        if t2:
            perf = service.get_explain_per_ticker_performance(tenant_id=tenant_id, ticker=t2)
            for wname, block in (perf.get("windows") or {}).items():
                st.markdown(f"**{wname}**")
                st.caption(f"best: {block.get('best_strategy')} | worst: {block.get('worst_strategy')}")
                df = pd.DataFrame(block.get("by_strategy") or [])
                if not df.empty:
                    st.dataframe(_style_low_sample_rows(df), use_container_width=True, hide_index=True)
                else:
                    st.caption("No outcomes in window.")

        st.subheader("Strategy × ticker matrix (win_rate, n)")
        lb = st.slider("Lookback days", 30, 365, 90, key="mx_lb")
        mx = service.get_explain_strategy_ticker_matrix(tenant_id=tenant_id, tickers=None, lookback_days=int(lb))
        if mx:
            dfm = pd.DataFrame(mx)
            dfm["low_sample"] = dfm["n"].fillna(0) < MIN_SAMPLE_N
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
        mv = service.get_explain_ranking_movers(tenant_id=tenant_id, top_n=25)
        mrd = int(mv.get("max_rank_depth") or 800)
        st.info(
            "**Rank #** — **lower is better** (1 ≈ top). "
            "**rank Δ** = rank_today − rank_yesterday: "
            "**negative ⇒ ↓ improving** (rank # went down); "
            "**positive ⇒ ↑ weakening** (rank # went up). "
            f"Ranks are computed within the top **{mrd}** names per snapshot."
        )
        st.caption(
            "Compares the two latest distinct `ranking_snapshots` timestamps."
        )
        st.markdown(
            f"**Latest snapshot:** `{mv.get('snapshot_ts_latest')}`  \n"
            f"**Previous:** `{mv.get('snapshot_ts_previous')}`"
        )
        if mv.get("message"):
            st.info(mv["message"])
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top risers (improved rank)")
            st.dataframe(
                pd.DataFrame(mv.get("risers") or []),
                use_container_width=True,
                hide_index=True,
            )
        with c2:
            st.subheader("Top fallers")
            st.dataframe(
                pd.DataFrame(mv.get("fallers") or []),
                use_container_width=True,
                hide_index=True,
            )
        e1, e2 = st.columns(2)
        with e1:
            st.subheader("New in latest snapshot")
            st.dataframe(
                pd.DataFrame(mv.get("new_in_latest") or []),
                use_container_width=True,
                hide_index=True,
            )
        with e2:
            st.subheader("Dropped since previous")
            st.dataframe(
                pd.DataFrame(mv.get("dropped_from_latest") or []),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Rank persistence (last snapshots)")
        st.caption(
            f"Uses up to 10 distinct `ranking_snapshots` times already in the DB (no extra storage). "
            f"**rank_norm** = rank / max_depth (≈ **0 best**, **1 worst**). "
            f"Line **down** = improving. Gray rows elsewhere = n < {MIN_SAMPLE_N}."
        )
        rh_t = st.selectbox(
            "Ticker (rank history)",
            options=tickers,
            key="rh_ticker",
            index=_idx,
        ) if tickers else None
        if rh_t:
            hist = service.get_explain_rank_history(tenant_id=tenant_id, ticker=rh_t, max_snapshots=10)
            if hist.get("message"):
                st.info(hist["message"])
            srows = hist.get("series") or []
            if srows:
                dfh = pd.DataFrame(srows)
                st.dataframe(dfh, use_container_width=True, hide_index=True)
                chart = dfh.dropna(subset=["rank_norm"])
                if len(chart) >= 2:
                    st.caption("Chart: **rank_norm** (0 ≈ best, 1 ≈ worst within max depth).")
                    st.line_chart(chart.set_index("snapshot_ts")["rank_norm"])
                elif len(chart) == 1:
                    st.caption("Only one snapshot includes this ticker at current depth — add runs for a trend line.")
            else:
                st.caption("No series rows.")

    with tabs[3]:
        hrs = st.slider("Hours", 6, 72, 24, key="chg_h")
        ch = service.get_explain_what_changed(tenant_id=tenant_id, hours=int(hrs))
        st.subheader("Admission runs (metrics)")
        st.dataframe(pd.DataFrame(ch.get("admission_runs") or []), use_container_width=True, hide_index=True)
        st.subheader("Overrule swaps (from detail json)")
        st.json(ch.get("swaps_recent") or [])
        st.subheader("candidate_queue touches")
        st.dataframe(pd.DataFrame(ch.get("candidate_touch_recent") or []), use_container_width=True, hide_index=True)

    with tabs[4]:
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
