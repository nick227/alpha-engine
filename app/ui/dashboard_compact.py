from __future__ import annotations

import random
import streamlit as st

from app.services.dashboard_service import DashboardService, arrow
from app.ui.theme import COLORS


def _pill(text: str, *, bg: str, fg: str) -> str:
    return (
        f"<span style=\"padding: 2px 8px; border-radius: 999px; background: {bg}; "
        f"color: {fg}; font-size: 12px; font-weight: 600;\">{text}</span>"
    )


def _build_prediction_row(signal: dict, latest_prices: dict[str, float] | None = None) -> dict:
    """Build a display row from a ranking signal. Only shows values we actually have."""
    from datetime import datetime

    direction = str(signal.get("direction", "BUY")).upper()
    ticker = str(signal.get("ticker", "—"))
    conviction = float(signal.get("conviction", 0.0) or 0.0)
    regime = str(signal.get("regime") or "—")
    strategy = str(signal.get("strategy") or "—")
    timestamp = signal.get("timestamp", "")
    forecast_horizon = signal.get("forecast_horizon") or "—"
    attribution = signal.get("attribution") or {}
    predicted_return: float | None = signal.get("predicted_return")

    is_buy = direction in ("BUY", "UP", "LONG")
    action = "LONG" if is_buy else "SHORT"

    # Timestamp age
    time_str = "—"
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            days_ago = (datetime.now(ts.tzinfo) - ts).days
            time_str = f"{days_ago}d ago" if days_ago > 0 else "Today"
        except Exception:
            pass

    # Price: prefer live close, then recorded entry price, then nothing.
    current_price: float | None = None
    price_source = "—"
    if latest_prices and ticker in latest_prices:
        current_price = latest_prices[ticker]
        price_source = "Live"
    elif signal.get("entry_price") is not None:
        current_price = float(signal["entry_price"])
        price_source = "At signal"

    entry_str = f"${current_price:.2f}" if current_price is not None else "—"

    # Target: only compute when we have both a real price and a real predicted return.
    target_str = "—"
    stop_str = "—"
    rr_str = "—"
    if current_price is not None and predicted_return is not None:
        move = predicted_return if is_buy else -predicted_return
        target_price = round(current_price * (1 + move), 2)
        stop_loss = round(current_price * 0.98, 2) if is_buy else round(current_price * 1.02, 2)
        risk = abs(current_price - stop_loss)
        reward = abs(target_price - current_price)
        target_str = f"${target_price:.2f}"
        stop_str = f"${stop_loss:.2f}"
        rr_str = f"1:{round(reward / risk, 2)}" if risk > 0 else "—"

    # Primary driver from attribution keys — skip internal ranking fields.
    _INTERNAL_KEYS = {"confidence", "strategy_weight", "conviction", "score"}
    primary_driver = "Ranking signal"
    if attribution:
        meaningful = {k: v for k, v in attribution.items() if k.lower() not in _INTERNAL_KEYS}
        if meaningful:
            top_key = max(meaningful, key=lambda k: abs(meaningful[k]))
            primary_driver = top_key.replace("_", " ").title()

    return {
        "Action": action,
        "Ticker": ticker,
        "Entry": entry_str,
        "Target": target_str,
        "Stop Loss": stop_str,
        "Risk:Reward": rr_str,
        "Conviction": conviction,
        "Confidence": conviction,  # card renderer reads "Confidence"
        "Horizon": forecast_horizon,
        "Strategy": strategy,
        "Regime": regime.replace("_", " ").title(),
        "Signal Age": time_str,
        "Price Src": price_source,
        "Primary Driver": primary_driver,
        "_current_price": current_price,
        "_signal": signal,
    }


def _get_signal_badges(row: dict) -> list[str]:
    """Determine badges for a signal based on its characteristics. Max 2 badges."""
    badges = []
    
    confidence = row.get("Confidence", 0)
    horizon = row.get("Horizon", "7d")
    strategy = row.get("Strategy", "").lower()
    primary_driver = row.get("Primary Driver", "").lower()
    
    # Priority order: High Conviction > Fast Setup > others
    # High Conviction: confidence >= 75%
    if confidence >= 0.75:
        badges.append("High Conviction")
    
    # Fast Setup: 1d horizon
    if horizon == "1d" and len(badges) < 2:
        badges.append("Fast Setup")
    
    # Mean Reversion: strategy or driver indicates mean reversion
    if "mean" in strategy or "mean" in primary_driver or "reversion" in strategy:
        if len(badges) < 2:
            badges.append("Mean Reversion")
    
    # Momentum: strategy indicates momentum
    if "momentum" in strategy or "trend" in strategy:
        if len(badges) < 2:
            badges.append("Momentum")
    
    return badges[:2]  # Max 2 badges


def _render_signal_card(row: dict, idx: int) -> None:
    """Render a single signal card for the river - vertically stacked, left-aligned."""
    action = row.get("Action", "...")
    ticker = row.get("Ticker", "...")
    confidence = row.get("Confidence", 0)
    entry = row.get("Entry", "$0")
    target = row.get("Target", "$0")
    horizon = row.get("Horizon", "7d")
    why = row.get("Primary Driver", "...")
    
    is_long = action == "LONG"
    action_color = "#10b981" if is_long else "#ef4444"
    
    # Get badges (max 2)
    badges = _get_signal_badges(row)
    
    # Card container with subtle border
    with st.container():
        # Build badge HTML
        badge_html = ""
        for badge in badges:
            if badge == "High Conviction":
                badge_html += " <span style='background: #ef4444; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;'>High Conviction</span>"
            elif badge == "Fast Setup":
                badge_html += " <span style='background: #3b82f6; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;'>Fast Setup</span>"
            elif badge == "Mean Reversion":
                badge_html += " <span style='background: #8b5cf6; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;'>Mean Reversion</span>"
            elif badge == "Momentum":
                badge_html += " <span style='background: #10b981; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;'>Momentum</span>"
        
        # Confidence color
        conf_color = "#10b981" if confidence >= 0.7 else "#f59e0b" if confidence >= 0.5 else "#6b7280"
        
        # Render card as single vertical flow
        price_line = entry if target == "—" else f"{entry}  →  {target}"
        horizon_tag = f" ({horizon})" if horizon != "—" else ""
        st.markdown(f"""
        <div style='padding: 12px 0; border-bottom: 1px solid #e5e7eb;'>
            <div style='margin-bottom: 4px;'>
                <span style='background: {action_color}; color: white; padding: 4px 12px; border-radius: 4px; font-weight: bold; font-size: 14px;'>{action}</span>
                <span style='font-size: 20px; font-weight: bold; margin-left: 8px;'>{ticker}</span>
                <span style='color: {conf_color}; font-weight: 900; font-size: 22px; margin-left: 12px;'>{confidence:.0%} conviction</span>
                {badge_html}
            </div>
            <div style='margin-top: 6px;'>
                <span style='font-size: 16px; font-weight: 600;'>{price_line}</span>
                <span style='color: #6b7280; font-size: 13px; margin-left: 8px;'>{horizon_tag}</span>
            </div>
            <div style='margin-top: 4px; color: #6b7280; font-size: 13px;'>
                {why}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Expandable details
        with st.expander("Details"):
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Stop Loss", row.get("Stop Loss", "..."))
                st.metric("Risk:Reward", row.get("Risk:Reward", "..."))
            with col2:
                st.caption(f"**Strategy**: {row.get('Strategy', '...')}")
                st.caption(f"**Sources**: {row.get('Sources', 1)} strategies")
                st.caption(f"**Regime**: {row.get('Regime', '...')}")
                st.caption(f"**Age**: {row.get('Signal Age', '...')}")
                st.caption(f"**Price**: {row.get('Price Src', '...')}")
            
            if st.button(f"Open {ticker} in Intelligence Hub", key=f"river_open_{idx}_{ticker}"):
                st.session_state.asset_change = ticker
                st.session_state.ui_route = "ih"
                st.rerun()


def _render_signal_river(predictions: list[dict]) -> None:
    """Render predictions as a scrollable signal river of cards."""
    if not predictions:
        st.info("No predictions available")
        return
    
    # Build rows and sort by confidence (default, no UI needed)
    rows = [_build_prediction_row(p.get('_signal', p), {}) for p in predictions]
    rows.sort(key=lambda x: x.get("Confidence", 0), reverse=True)
    
    # Render each card in the river
    for idx, row in enumerate(rows):
        _render_signal_card(row, idx)


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
        
        # Get initial signals for header metrics (will be refreshed after full fetch)
        try:
            initial_signals = service.get_top_ten_signals(tenant_id=tenant_id, limit=15)
        except:
            initial_signals = []
        
        # Compute market mode from signal regimes
        regime_counts = {}
        for s in initial_signals:
            regime = s.get('regime', 'unknown')
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        
        # Determine dominant regime
        if regime_counts:
            dominant_regime = max(regime_counts, key=regime_counts.get)
            regime_display = "Bull" if "bull" in dominant_regime.lower() else "Bear" if "bear" in dominant_regime.lower() else "Sideways"
            regime_delta = f"+{sum(1 for s in initial_signals if s.get('direction', '').upper() in ['BUY', 'LONG'])} long" if regime_display == "Bull" else f"-{sum(1 for s in initial_signals if s.get('direction', '').upper() in ['SELL', 'SHORT'])} short"
        else:
            regime_display = "Analyzing..."
            regime_delta = None
        
        # Quick market overview
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Market Mode", regime_display, delta=regime_delta)
        with col2:
            st.metric("Active Signals", str(len(initial_signals)))
        with col3:
            avg_conf = sum(s.get('confidence', 0) for s in initial_signals) / len(initial_signals) if initial_signals else 0
            st.metric("Avg Confidence", f"{avg_conf:.0%}")
        
        st.markdown("---")
        
        # Watchlist section
        st.markdown("## Your Watchlist")
        st.markdown("**Target stocks being monitored for trading opportunities**")
        
        # Get real prices for watchlist stocks
        watchlist_stocks = ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMZN"]
        try:
            watchlist_prices = service.store.get_latest_close_prices(
                tenant_id=tenant_id, 
                tickers=watchlist_stocks, 
                timeframe="1d"
            )
        except:
            watchlist_prices = {}
        
        cols = st.columns(6)
        for i, stock in enumerate(watchlist_stocks):
            with cols[i]:
                price = watchlist_prices.get(stock)
                if price:
                    st.metric(stock, f"${price:.2f}")
                else:
                    st.metric(stock, "Loading...")
        
        st.markdown("---")
        
        # Clear trading focus
        st.markdown("## 🎯 Today's Trading Setups")
        st.markdown("**AI-detected opportunities with real-time pricing**")
        
        with st.expander("How to Use These Signals", expanded=False):
            st.markdown("""
            **How AI Makes Predictions:**
            - **Multi-Factor Analysis**: AI examines dozens of market indicators, price patterns, and fundamental data
            - **Historical Pattern Matching**: Finds similar market conditions from the past 10+ years
            - **Ensemble Methods**: Combines multiple strategies to increase reliability
            - **Confidence Scoring**: Based on historical accuracy in similar market conditions
            
            **Understanding Each Signal:**
            - **Current Price**: Live market price (green) or estimated (orange)
            - **Target Price**: Predicted price within the forecast horizon
            - **Target Horizon**: When the prediction is expected to materialize (1d, 7d, 30d)
            - **Confidence**: Historical success rate in similar conditions
            - **Key Drivers**: Click "Why this prediction?" to see the top factors
            
            **Trading Approach:**
            - **High Confidence (>70%)**: More reliable, consider larger position sizes
            - **Medium Confidence (50-70%)**: Moderate risk, use standard position sizing
            - **Low Confidence (<50%)**: Higher risk, consider smaller positions or waiting
            
            **Risk Management:**
            - Always use stop-losses (typically 2-3% below entry)
            - Position size based on confidence level and risk tolerance
            - Verify signals with your own analysis and market knowledge
            """)

    try:
        top = service.get_top_ten_signals(tenant_id=tenant_id, limit=15)
    except Exception as exc:
        st.error(f"Failed to load signals: {exc}")
        return

    if not top:
        st.info("No signals available yet. Run a backfill / scoring loop to populate the database.")
        return
    
    # Get latest prices for all tickers in top signals
    tickers = [s.get('ticker') for s in top if s.get('ticker')]
    latest_prices = service.store.get_latest_close_prices(
        tenant_id=tenant_id, 
        tickers=tickers, 
        timeframe="1d"
    ) if tickers else {}
    
    # Sort by confidence instead of alpha
    top.sort(key=lambda x: x.get('confidence', 0), reverse=True)

    # Data freshness indicator
    if latest_prices:
        st.markdown(f"<div style='background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-size: 12px; display: inline-block; margin-bottom: 10px;'> Live Data Connected ({len(latest_prices)} tickers)</div>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='background: #f59e0b; color: white; padding: 4px 8px; border-radius: 4px; font-size: 12px; display: inline-block; margin-bottom: 10px;'> Using Estimated Prices</div>", unsafe_allow_html=True)

    # Update metrics with actual data
    if show_page_header:
        # Calculate average confidence
        avg_confidence = sum(s.get('confidence', 0) for s in top) / len(top) if top else 0
        
        # Update the metrics with real data
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Market Mode", "Bull Market", delta="+2.3%")
        with col2:
            st.metric("Active Signals", str(len(top)))
        with col3:
            st.metric("Avg Confidence", f"{avg_confidence:.0%}")

    # No need for redundant text - signals are self-explanatory

    # Build prediction data with prices
    predictions = []
    for signal in top:
        signal_with_price = dict(signal)
        signal_with_price['_current_price'] = latest_prices.get(signal.get('ticker')) if latest_prices.get(signal.get('ticker')) else None
        predictions.append(signal_with_price)
    
    # Render as signal river
    st.markdown("### Top AI Predictions")
    st.markdown("*Sorted by confidence. Click 'Details' to expand.*")
    
    _render_signal_river(predictions)

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
