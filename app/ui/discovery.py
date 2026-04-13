from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from app.ui.middle.dashboard_service import DashboardService


def _arrow_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            continue
        out[col] = out[col].astype("string")
    return out


def discovery_main(
    service: DashboardService,
    *,
    tenant_id: str,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Discovery")
    else:
        st.markdown("### Discovery")

    st.caption("Discovery is read-only here. Run jobs from Ops / Data → Run Jobs (Discovery controls).")

    # Core discovery strategies
    st.markdown("## 🧠 Core Discovery Strategies")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Realness + Repricing", "🎯", help="misclassified real company")
    with col2:
        st.metric("Silent Compounder", "📈", help="ignored growth") 
    with col3:
        st.metric("Narrative Lag", "⏰", help="late theme adoption")
    with col4:
        st.metric("Survivor", "🛡️", help="avoided collapse")
    with col5:
        st.metric("Ownership Vacuum", "🏗️", help="early accumulation")

    dates = service.list_discovery_dates(tenant_id=tenant_id, limit=120)
    strategies = service.list_discovery_strategy_types(tenant_id=tenant_id)

    tab_strategies, tab_candidates, tab_stats, tab_watchlist = st.tabs(
        ["🎯 Strategies", "📋 Candidates", "📊 Stats", "⭐ Watchlist"]
    )

    with tab_strategies:
        if not dates:
            st.info("No discovery history found yet.")
        else:
            st.subheader("Strategy Performance Overview")
            
            # Strategy selection
            s1, s2, s3 = st.columns([2, 2, 2])
            with s1:
                end_date = st.selectbox("End date", options=dates, index=0, key="strat_end_date")
            with s2:
                window_days = int(st.selectbox("Window (days)", options=[7, 14, 30, 60], index=1, key="strat_window"))
            with s3:
                horizon = int(st.selectbox("Horizon (days)", options=[1, 5, 20], index=1, key="strat_horizon"))

            # Get strategy stats
            strategy_stats = service.list_discovery_stats(
                tenant_id=tenant_id,
                end_date=end_date,
                window_days=window_days,
                horizon_days=horizon,
                group_type="strategy",
                latest_only=True,
                limit=100,
            )

            if strategy_stats:
                df = pd.DataFrame(strategy_stats)
                
                # Display strategy cards
                strategies_info = {
                    "realness_repricing": {"name": "Realness + Repricing", "emoji": "🎯", "desc": "misclassified real company"},
                    "silent_compounder": {"name": "Silent Compounder", "emoji": "📈", "desc": "ignored growth"},
                    "narrative_lag": {"name": "Narrative Lag", "emoji": "⏰", "desc": "late theme adoption"},
                    "survivor": {"name": "Survivor", "emoji": "🛡️", "desc": "avoided collapse"},
                    "ownership_vacuum": {"name": "Ownership Vacuum", "emoji": "🏗️", "desc": "early accumulation"}
                }

                for strat_key, info in strategies_info.items():
                    strat_data = df[df["group_value"] == strat_key]
                    if not strat_data.empty:
                        row = strat_data.iloc[0]
                        with st.container():
                            col1, col2, col3, col4 = st.columns([1, 2, 2, 2])
                            with col1:
                                st.markdown(f"### {info['emoji']}")
                            with col2:
                                st.markdown(f"**{info['name']}**")
                                st.caption(info['desc'])
                            with col3:
                                if 'avg_return' in row:
                                    st.metric(f"{horizon}d Return", f"{row['avg_return']*100:.2f}%")
                                if 'win_rate' in row:
                                    st.metric("Win Rate", f"{row['win_rate']*100:.1f}%")
                            with col4:
                                if 'n' in row:
                                    st.metric("Candidates", f"{int(row['n'])}")
                                if 'lift' in row:
                                    st.metric("Lift", f"{row['lift']*10000:.0f} bps")
                            st.divider()
                else:
                    st.info("No strategy performance data available for selected period.")
            else:
                st.info("No strategy stats found. Run discovery jobs to generate strategy performance data.")

    with tab_candidates:
        c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
        with c1:
            if dates:
                as_of_date = st.selectbox("As-of date", options=dates, index=0, key="disc_asof_candidates")
            else:
                as_of_date = st.text_input("As-of date (YYYY-MM-DD)", value=date.today().isoformat(), key="disc_asof_candidates_txt")
        with c2:
            strategy = st.selectbox("Strategy", options=["(all)"] + strategies, index=0, key="disc_strategy_candidates")
        with c3:
            bucket = st.selectbox(
                "Price bucket",
                options=["(all)", "<1", "1-2", "2-5", "5-10", "10-20", "20+"],
                index=0,
                key="disc_bucket_candidates",
            )
        with c4:
            min_score = st.slider("Min score", min_value=0.0, max_value=1.0, value=0.0, step=0.01, key="disc_min_score_candidates")
        with c5:
            limit = int(st.selectbox("Limit", options=[50, 100, 200, 500, 1000], index=1, key="disc_limit_candidates"))

        sym_filter = st.text_input("Symbol filter (optional)", value="", key="disc_symbol_filter").strip().upper() or None

        rows = service.list_discovery_candidates(
            tenant_id=tenant_id,
            as_of_date=as_of_date,
            strategy_type=None if strategy == "(all)" else strategy,
            price_bucket=None if bucket == "(all)" else bucket,
            min_score=float(min_score),
            symbol=sym_filter,
            limit=int(limit),
        )

        if not rows:
            st.info("No discovery candidates found for this selection.")
        else:
            df = pd.DataFrame(rows)
            # Pull drivers for quick UX (string list stored in metadata_json)
            if "metadata_json" in df.columns:
                import json as _json

                def _drivers_from_md(s: Any) -> str:
                    try:
                        md = _json.loads(str(s or "{}"))
                        drivers = md.get("drivers") if isinstance(md, dict) else None
                        if isinstance(drivers, list):
                            return " | ".join([str(x) for x in drivers[:3] if str(x).strip()])
                    except Exception:
                        return ""
                    return ""

                df["drivers"] = df["metadata_json"].map(_drivers_from_md)

            show_cols = [
                c
                for c in [
                    "as_of_date",
                    "symbol",
                    "strategy_type",
                    "score",
                    "reason",
                    "drivers",
                    "close",
                    "price_bucket",
                    "avg_dollar_volume_20d",
                    "sector",
                    "industry",
                ]
                if c in df.columns
            ]
            
            # Card-based feed (primary view)
            for _, row in df.iterrows():
                symbol = row.get("symbol", "N/A")
                score = row.get("score", 0)
                strategy = row.get("strategy_type", "N/A")
                why = row.get("drivers") or row.get("reason") or ""
                
                st.markdown(f"**{symbol}** <span style='color: #4CAF50; font-weight: 600;'>{score:.2f}</span> <span style='color: #666;'>|</span> **{strategy}**", unsafe_allow_html=True)
                st.caption(why if why else "No reason provided")
                st.divider()
            
            with st.expander("View raw table", expanded=False):
                st.dataframe(_arrow_safe_df(df[show_cols]), use_container_width=True, hide_index=True)

    with tab_watchlist:
        wl_dates = service.list_watchlist_dates(tenant_id=tenant_id, limit=120)
        if not wl_dates:
            st.info("No watchlist history yet. Run `discovery_cli promote` from Ops / Data.")
        else:
            st.subheader("Daily Top Picks")
            p1, p2, p3 = st.columns([2, 2, 2])
            with p1:
                wl_date = st.selectbox("Watchlist date", options=wl_dates, index=0, key="wl_date")
            with p2:
                picks_n = int(st.selectbox("Top picks", options=[5, 10, 15, 20], index=2, key="wl_picks_n"))
            with p3:
                view_mode = st.selectbox("View", options=["Table", "Text"], index=0, key="wl_view_mode")

            picks = service.list_daily_top_picks(tenant_id=tenant_id, as_of_date=wl_date, limit=picks_n)
            if not picks:
                st.info("No picks for this date.")
            else:
                dfp = pd.DataFrame(picks)
                if view_mode == "Table":
                    st.dataframe(_arrow_safe_df(dfp), use_container_width=True, hide_index=True)
                else:
                    lines = []
                    for row in picks:
                        lines.append(
                            f"{row['symbol']} — {row['side']} — {row['conviction']} | "
                            f"playbook={row.get('playbook_id','')} | "
                            f"overlap={row['overlap_count']} days_seen={row['days_seen']} avg_score={row['avg_score']:.2f} | "
                            f"{row['strategies']} | {row['why']}"
                        )
                    st.text_area("Daily Watchlist (copy/paste)", value="\n".join(lines), height=280)

            st.divider()
            st.subheader("Watchlist (raw rows)")
            w1, w2 = st.columns([2, 2])
            with w1:
                wl_date_raw = wl_date
            with w2:
                limit = int(st.selectbox("Limit", options=[20, 50, 100, 200], index=1, key="wl_limit"))
            wl = service.list_watchlist(tenant_id=tenant_id, as_of_date=wl_date_raw, limit=limit)
            if not wl:
                st.info("Watchlist is empty for this date.")
            else:
                df = pd.DataFrame(wl)
                st.dataframe(_arrow_safe_df(df), use_container_width=True, hide_index=True)

    with tab_stats:
        if not dates:
            st.info("No discovery dates found yet.")
        else:
            st.subheader("📊 Discovery Performance Stats")
            
            # Simplified controls
            s1, s2, s3 = st.columns([2, 2, 2])
            with s1:
                end_date = st.selectbox("End date", options=dates, index=0, key="stats_end_date")
            with s2:
                window_days = int(st.selectbox("Window (days)", options=[7, 14, 30], index=1, key="stats_window"))
            with s3:
                horizon = int(st.selectbox("Horizon (days)", options=[1, 5, 20], index=1, key="stats_horizon"))

            # Get overall performance metrics
            overall_stats = service.list_discovery_stats(
                tenant_id=tenant_id,
                end_date=end_date,
                window_days=window_days,
                horizon_days=horizon,
                group_type="cohort",
                latest_only=True,
                limit=100,
            )

            # Get strategy-specific stats
            strategy_stats = service.list_discovery_stats(
                tenant_id=tenant_id,
                end_date=end_date,
                window_days=window_days,
                horizon_days=horizon,
                group_type="strategy",
                latest_only=True,
                limit=100,
            )

            if overall_stats:
                df_overall = pd.DataFrame(overall_stats)
                cohort_df = df_overall[df_overall["group_type"].astype(str) == "cohort"] if "group_type" in df_overall.columns else pd.DataFrame()
                
                if not cohort_df.empty:
                    st.markdown("### 🎯 Overall Performance")
                    by = {str(r["group_value"]): r for _, r in cohort_df.iterrows()}
                    w = float(by.get("watchlist", {}).get("avg_return")) if "watchlist" in by else None
                    c = float(by.get("candidates", {}).get("avg_return")) if "candidates" in by else None
                    ww = float(by.get("watchlist", {}).get("win_rate")) if "watchlist" in by else None
                    cw = float(by.get("candidates", {}).get("win_rate")) if "candidates" in by else None

                    def _bp(x: float | None) -> str:
                        if x is None:
                            return "n/a"
                        return f"{x*10000:.0f} bps"

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Watchlist Return", f"{(w*100):.2f}%" if w is not None else "n/a")
                    col2.metric("Candidates Return", f"{(c*100):.2f}%" if c is not None else "n/a")
                    col3.metric("Lift", _bp(w - c) if (w is not None and c is not None) else "n/a")
                    col4.metric("Watchlist Win Rate", f"{(ww*100):.1f}%" if ww is not None else "n/a")

            if strategy_stats:
                df_strategies = pd.DataFrame(strategy_stats)
                st.markdown("### 🧠 Strategy Performance")
                
                strategies_info = {
                    "realness_repricing": {"name": "Realness + Repricing", "emoji": "🎯"},
                    "silent_compounder": {"name": "Silent Compounder", "emoji": "📈"},
                    "narrative_lag": {"name": "Narrative Lag", "emoji": "⏰"},
                    "survivor": {"name": "Survivor", "emoji": "🛡️"},
                    "ownership_vacuum": {"name": "Ownership Vacuum", "emoji": "🏗️"}
                }

                strategy_cols = st.columns(5)
                for i, (strat_key, info) in enumerate(strategies_info.items()):
                    with strategy_cols[i]:
                        strat_data = df_strategies[df_strategies["group_value"] == strat_key]
                        if not strat_data.empty:
                            row = strat_data.iloc[0]
                            st.markdown(f"### {info['emoji']}")
                            st.markdown(f"**{info['name']}**")
                            if 'avg_return' in row:
                                st.metric(f"{horizon}d Return", f"{row['avg_return']*100:.2f}%")
                            if 'win_rate' in row:
                                st.metric("Win Rate", f"{row['win_rate']*100:.1f}%")
                        else:
                            st.markdown(f"### {info['emoji']}")
                            st.markdown(f"**{info['name']}**")
                            st.caption("No data")

            if not overall_stats and not strategy_stats:
                st.info("No stats stored for this selection. Run `discovery_cli stats` from Ops / Data.")

            # Show detailed data table
            if overall_stats or strategy_stats:
                with st.expander("📋 Detailed Data", expanded=False):
                    all_stats = []
                    if overall_stats:
                        all_stats.extend(overall_stats)
                    if strategy_stats:
                        all_stats.extend(strategy_stats)
                    if all_stats:
                        df_all = pd.DataFrame(all_stats)
                        st.dataframe(_arrow_safe_df(df_all), use_container_width=True, hide_index=True)
