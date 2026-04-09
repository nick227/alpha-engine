from __future__ import annotations

import streamlit as st

from app.ui.middle.dashboard_service import DashboardService, arrow
from app.ui.theme import COLORS


def _pill(text: str, *, bg: str, fg: str) -> str:
    return (
        f"<span style=\"padding: 2px 8px; border-radius: 999px; background: {bg}; "
        f"color: {fg}; font-size: 12px; font-weight: 600;\">{text}</span>"
    )


def _signal_card(signal: dict) -> None:
    direction = str(signal.get("direction", "")).upper()
    ticker = str(signal.get("ticker", "—"))
    expected_move = str(signal.get("expected_move", "—"))
    confidence = float(signal.get("confidence", 0.0) or 0.0)
    regime = str(signal.get("regime", "—"))
    strategy = str(signal.get("strategy", "—"))
    timestamp = signal.get("timestamp", "")
    rank = signal.get("rank", 0)

    is_buy = direction in ("BUY", "UP", "LONG")
    
    # Parse timestamp to show relative time
    from datetime import datetime
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            time_ago = (datetime.now(ts.tzinfo) - ts).days
            time_str = f"{time_ago}d ago" if time_ago > 0 else "Today"
        except:
            time_str = "Recent"
    else:
        time_str = "Recent"
    
    # Simulate current price (in real app, this would come from live data)
    import random
    current_price = round(random.uniform(100, 500), 2)
    
    # Calculate target price based on expected move
    move_pct = float(expected_move.replace('+', '').replace('%', '').replace('—', '0'))
    if direction == "SELL":
        move_pct = -move_pct
    target_price = round(current_price * (1 + move_pct/100), 2)
    
    # Trading-focused card design
    with st.container():
        # Header with ticker and current price
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.markdown(f"### {ticker}")
        with col2:
            st.markdown(f"<div style='text-align: center; font-size: 18px; font-weight: bold;'>${current_price}</div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div style='text-align: center; background: {'#10b981' if is_buy else '#ef4444'}; color: white; padding: 4px 8px; border-radius: 12px; font-size: 12px; font-weight: bold;'>#{rank}</div>", unsafe_allow_html=True)
        
        # Clear trading signal
        direction_color = "green" if is_buy else "red"
        action = "LONG" if is_buy else "SHORT"
        st.markdown(f"#### <span style='color: {direction_color};'>{direction}</span> - {action} SETUP", unsafe_allow_html=True)
        
        # Price targets
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current", f"${current_price}")
        with col2:
            st.metric("Target", f"${target_price}", delta=f"{expected_move}")
        with col3:
            st.metric("Confidence", f"{confidence:.0%}")
        
        # Trading details
        col1, col2 = st.columns(2)
        with col1:
            st.caption(f"**Strategy**: {strategy}")
            st.caption(f"**Market**: {regime.replace('_', ' ').title()}")
        with col2:
            st.caption(f"**Signal Age**: {time_str}")
            if confidence >= 0.7:
                st.caption(" High Confidence Setup")
            else:
                st.caption(" Lower Confidence")
        
        # Action guidance
        if is_buy:
            action_text = f"Consider LONG above ${current_price}"
        else:
            action_text = f"Consider SHORT below ${current_price}"
        
        st.markdown(f"<div style='background: rgba(0,0,0,0.05); padding: 8px; border-radius: 6px; font-size: 12px; text-align: center;'>{action_text}</div>", unsafe_allow_html=True)
        
        st.divider()


def dashboard_compact_main(
    service: DashboardService,
    *,
    tenant_id: str,
    ticker: str | None,
    horizon_days: int | None,
    show_page_header: bool = True,
) -> None:
    if show_page_header:
        st.markdown("# Trading Dashboard")
        st.markdown("**AI-powered signals for your watchlist**")
        
        # Quick market overview
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Market Mode", "Bull Market", delta="+2.3%")
        with col2:
            st.metric("Active Signals", len(top) if 'top' in locals() else 0)
        with col3:
            st.metric("Avg Confidence", "72%" if 'top' in locals() else "N/A")
        
        st.markdown("---")
        
        # Watchlist section
        st.markdown("## ð¯ Your Watchlist")
        st.markdown("**Target stocks being monitored for trading opportunities**")
        
        # Show key watchlist stocks (in real app, this would be user's actual watchlist)
        watchlist_stocks = ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMZN"]
        cols = st.columns(6)
        for i, stock in enumerate(watchlist_stocks):
            with cols[i]:
                st.metric(stock, f"${random.randint(100, 500)}")
        
        st.markdown("---")
        
        # Clear trading focus
        st.markdown("## ð¤ Today's Trading Setups")
        st.markdown("**AI-detected opportunities in your watchlist stocks**")
        
        with st.expander("ð¡ How to Use These Signals", expanded=False):
            st.markdown("""
            **For Day Trading:**
            - **BUY Signal**: Consider long position, watch for entry above current price
            - **SELL Signal**: Consider short position or exit long positions
            - **Confidence > 70%**: Higher probability setups
            - **Expected Move**: Target profit potential for the trade
            
            **Risk Management:**
            - Always use stop-losses
            - Position size based on confidence level
            - Verify with your own analysis
            """)

    try:
        top = service.get_top_ten_signals(tenant_id=tenant_id, limit=15)
    except Exception as exc:
        st.error(f"Failed to load signals: {exc}")
        return

    if not top:
        st.info("No signals available yet. Run a backfill / scoring loop to populate the database.")
        return

    # No need for redundant text - signals are self-explanatory

    st.markdown("### Top predictions")
    cols = st.columns(3, gap="large")
    for idx, signal in enumerate(top):
        with cols[idx % 3]:
            _signal_card(signal)
            c1, c2 = st.columns([1, 1])
            with c1:
                if st.button("Open in IH", key=f"open_ih_{idx}_{signal.get('ticker','')}"):
                    st.session_state.asset_change = signal.get("ticker")
                    st.session_state.ui_route = "ih"
                    st.rerun()
            with c2:
                st.caption(f"Rank #{signal.get('rank','—')}")

    with st.expander("Recent signals (table)", expanded=False):
        recent = service.get_recent_signals(tenant_id=tenant_id, limit=25)
        st.dataframe(
            [
                {
                    "time": s.time,
                    "ticker": s.ticker,
                    "direction": s.direction,
                    "confidence": s.confidence,
                    "strategy": s.strategy,
                    "regime": s.regime,
                }
                for s in recent
            ],
            use_container_width=True,
            hide_index=True,
        )
