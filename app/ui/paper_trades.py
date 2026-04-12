from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from app.ui.middle.dashboard_service import DashboardService


def _arrow_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit Arrow transport is strict about mixed dtypes.
    Keep numbers as numbers and coerce everything else to string.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            continue
        out[col] = out[col].astype("string")
    return out


def _to_since_iso(d: date) -> str:
    # Interpret the chosen date in America/Chicago at midnight, then convert to UTC ISO.
    central = ZoneInfo("America/Chicago")
    dt = datetime.combine(d, time.min, tzinfo=central).astimezone(timezone.utc)
    return dt.isoformat()


def paper_trades_main(
    service: DashboardService,
    *,
    tenant_id: str,
    ticker: str | None,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Paper Trades")
        st.caption("Validate that improved predictions can translate into profitable execution (without risking capital).")
    else:
        st.caption("Validate prediction quality through execution outcomes (paper mode).")

    with st.expander("How this page fits the pipeline", expanded=False):
        st.markdown(
            """
Paper trading is the bridge between **prediction quality** and **tradable outcomes**.

Use this page to answer:
- Are trades being opened/closed as expected from the signals?
- Are winners/losers consistent with confidence and strategy selection?
- Is realized P&L stable (not dominated by a few outliers)?
- Are there operational issues (stale prices, missing exits, unexpected statuses)?
            """.strip()
        )

    controls = st.container()
    with controls:
        c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
        with c1:
            mode_label = st.selectbox("Mode", options=["paper", "backtest", "live", "(Any)"], index=0)
            mode = None if mode_label == "(Any)" else mode_label
        with c2:
            status_label = st.selectbox("Status", options=["(Any)", "Open", "Closed"], index=0)
            status = None
            if status_label == "Closed":
                status = "CLOSED"
            elif status_label == "Open":
                # Stored statuses today are typically EXECUTED/CLOSED; treat "Open" as a UI-only filter.
                status = None
        with c3:
            since_enabled = st.toggle("Since filter", value=False)
            since_date = st.date_input("Since", value=(date.today()), disabled=not since_enabled)
            since_iso = _to_since_iso(since_date) if since_enabled else None
        with c4:
            limit = int(st.slider("Max rows", min_value=50, max_value=2000, value=250, step=50))
        with c5:
            auto_refresh = st.toggle("Auto-refresh", value=False)
            refresh_s = int(st.selectbox("Refresh (s)", options=[3, 5, 10, 30], index=1, disabled=not auto_refresh))

        if auto_refresh:
            st_autorefresh(interval=refresh_s * 1000, key="paper_trades_autorefresh")

    overview = service.get_paper_overview(tenant_id=tenant_id, mode=mode, ticker=ticker, limit=limit)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Open trades", int(overview.get("open_trades") or 0))
    m2.metric("Closed trades", int(overview.get("closed_trades") or 0))
    m3.metric("Realized P&L", f"${float(overview.get('realized_pnl') or 0.0):,.2f}")
    win_rate = overview.get("win_rate")
    m4.metric("Win rate", "—" if win_rate is None else f"{float(win_rate):.1%}")

    tab_monitor, tab_history, tab_analytics = st.tabs(["Monitor", "History", "Analytics"])

    with tab_monitor:
        with st.expander("What to look for in Monitor", expanded=False):
            st.markdown(
                """
- Positions should have plausible sizes and directions; large/unbounded exposure usually signals risk control issues.
- `last_price` is sourced from the latest close in `price_bars` (if available). Missing last prices can hide risk.
- Open trades should have recent timestamps; long-lived open trades can indicate missing lifecycle updates.
                """.strip()
            )
        st.subheader("Positions (last known)")
        positions = service.list_paper_positions(tenant_id=tenant_id, mode=mode, ticker=ticker)
        if positions:
            pos_df = pd.DataFrame(positions)
            pos_df = _arrow_safe_df(pos_df)
            st.dataframe(pos_df, use_container_width=True, hide_index=True)
        else:
            st.info("No positions found in `positions` table for the current filters.")

        st.subheader("Open trades")
        trades = service.list_paper_trades(
            tenant_id=tenant_id,
            mode=mode,
            ticker=ticker,
            since_iso=since_iso,
            limit=limit,
        )
        open_trades = [t for t in trades if str(t.get("status", "")).upper() != "CLOSED"]
        if open_trades:
            open_df = _arrow_safe_df(pd.DataFrame(open_trades))
            show_cols = [
                "timestamp",
                "ticker",
                "direction",
                "quantity",
                "entry_price",
                "status",
                "strategy_id",
            ]
            st.dataframe(open_df[[c for c in show_cols if c in open_df.columns]], use_container_width=True, hide_index=True)
        else:
            st.caption("No open trades in `trades` for the current filters.")

    with tab_history:
        st.subheader("Trade history")
        with st.expander("How to use History", expanded=False):
            st.markdown(
                """
History is where you audit whether the system is learning the right lessons:
- Sort/scan by `pnl` and `pnl_pct` to see if returns are consistent or dominated by tails.
- Compare outcomes by `strategy_id` to spot which strategies are actually contributing alpha.
- Use Trade details to inspect `analysis` / LLM fields if you’re validating the decision chain.
                """.strip()
            )

        all_trades = service.list_paper_trades(
            tenant_id=tenant_id,
            mode=mode,
            ticker=ticker,
            status=("CLOSED" if status_label == "Closed" else None),
            since_iso=since_iso,
            limit=limit,
        )
        if status_label == "Open":
            all_trades = [t for t in all_trades if str(t.get("status", "")).upper() != "CLOSED"]

        if not all_trades:
            st.info("No trades found for the current filters.")
        else:
            df = _arrow_safe_df(pd.DataFrame(all_trades))
            default_cols = [
                "timestamp",
                "ticker",
                "direction",
                "quantity",
                "entry_price",
                "exit_price",
                "pnl",
                "pnl_pct",
                "status",
                "strategy_id",
            ]
            st.dataframe(df[[c for c in default_cols if c in df.columns]], use_container_width=True, hide_index=True)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", data=csv, file_name="paper_trades.csv", mime="text/csv")

            with st.expander("Trade details", expanded=False):
                ids = [t.get("id") for t in all_trades if t.get("id")]
                if not ids:
                    st.caption("No trade ids available.")
                else:
                    selected = st.selectbox("Trade", options=ids, index=0)
                    match = next((t for t in all_trades if t.get("id") == selected), None)
                    if match:
                        st.json(match)

    with tab_analytics:
        st.subheader("Realized P&L")
        with st.expander("How to interpret Analytics", expanded=False):
            st.markdown(
                """
Analytics is the “so what?” view:
- A steadily rising cumulative curve suggests the predictions are translating into tradable edge.
- A noisy curve with big spikes suggests concentration risk or unstable strategy selection.
- The P&L histogram should not be overwhelmingly negative or overly dependent on a few wins.
                """.strip()
            )
        closed = [
            t
            for t in service.list_paper_trades(
                tenant_id=tenant_id,
                mode=mode,
                ticker=ticker,
                status="CLOSED",
                since_iso=since_iso,
                limit=max(limit, 500),
            )
            if t.get("pnl") is not None
        ]
        if not closed:
            st.info("No closed trades with P&L available.")
            return

        dfc = pd.DataFrame(closed)
        dfc["timestamp"] = pd.to_datetime(dfc["timestamp"], errors="coerce", utc=True)
        dfc = dfc.dropna(subset=["timestamp"]).sort_values("timestamp")
        dfc["pnl"] = pd.to_numeric(dfc["pnl"], errors="coerce").fillna(0.0)
        dfc["cum_pnl"] = dfc["pnl"].cumsum()

        fig = px.line(dfc, x="timestamp", y="cum_pnl", title="Cumulative realized P&L")
        fig.update_layout(margin=dict(l=20, r=20, t=50, b=20), height=320)
        st.plotly_chart(fig, use_container_width=True)

        h = px.histogram(dfc, x="pnl", nbins=40, title="Trade P&L distribution")
        h.update_layout(margin=dict(l=20, r=20, t=50, b=20), height=320)
        st.plotly_chart(h, use_container_width=True)
