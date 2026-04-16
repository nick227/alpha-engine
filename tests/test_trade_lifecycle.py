from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.trading.trade_lifecycle import OrderType, TradeLeg, TradeLifecycleManager, TradePosition, TradeState


def test_create_trade_from_signal_builds_defaults_and_state_history() -> None:
    mgr = TradeLifecycleManager({"stop_loss_pct": 0.02, "target_pct": 0.04, "max_hold_time_hours": 1})
    trade = mgr.create_trade_from_signal(
        signal_id="sig1",
        strategy_id="strat1",
        ticker="NVDA",
        direction="long",
        entry_price=100.0,
        quantity=10.0,
        confidence=0.9,
        regime="NORMAL",
    )
    assert trade.state == TradeState.SIGNAL
    assert trade.stop_loss_price is not None and abs(trade.stop_loss_price - 98.0) < 1e-9
    assert trade.target_price is not None and abs(trade.target_price - 104.0) < 1e-9
    assert len(trade.state_history) == 1
    assert trade.state_history[0][1] == TradeState.SIGNAL


def test_check_exit_conditions_target_stop_time_and_partial_exit() -> None:
    mgr = TradeLifecycleManager(
        {"stop_loss_pct": 0.02, "target_pct": 0.04, "max_hold_time_hours": 1, "partial_exit_levels": [0.5]}
    )
    trade = mgr.create_trade_from_signal(
        signal_id="sig1",
        strategy_id="strat1",
        ticker="NVDA",
        direction="long",
        entry_price=100.0,
        quantity=10.0,
        confidence=0.9,
    )

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trade.entry_timestamp = now - timedelta(hours=2)
    trade.position = TradePosition(
        ticker="NVDA",
        direction="long",
        total_quantity=10.0,
        filled_quantity=10.0,
        remaining_quantity=10.0,
        average_entry_price=100.0,
        current_price=100.0,
        unrealized_pnl=0.0,
        unrealized_pnl_pct=0.0,
        last_updated=now,
    )

    actions = mgr._check_exit_conditions(trade, current_price=150.0, timestamp=now)  # noqa: SLF001
    types = {a.get("type") for a in actions}
    assert "target_reached" in types
    assert "time_exit" in types
    assert "partial_exit" in types

    stop_actions = mgr._check_exit_conditions(trade, current_price=90.0, timestamp=now)  # noqa: SLF001
    assert any(a.get("type") == "stop_loss" for a in stop_actions)


def test_calculate_leg_pnl_long_exit_leg() -> None:
    mgr = TradeLifecycleManager({})
    trade = mgr.create_trade_from_signal(
        signal_id="sig1",
        strategy_id="strat1",
        ticker="NVDA",
        direction="long",
        entry_price=100.0,
        quantity=10.0,
        confidence=0.9,
    )
    trade.position = TradePosition(
        ticker="NVDA",
        direction="long",
        total_quantity=10.0,
        filled_quantity=10.0,
        remaining_quantity=10.0,
        average_entry_price=100.0,
        current_price=100.0,
        unrealized_pnl=0.0,
        unrealized_pnl_pct=0.0,
        last_updated=datetime.now(timezone.utc),
    )
    exit_leg = TradeLeg(
        id="leg1",
        trade_id=trade.id,
        leg_type="final_exit",
        direction="short",
        quantity=5.0,
        price=110.0,
        timestamp=datetime.now(timezone.utc),
        order_type=OrderType.MARKET,
    )
    pnl = mgr._calculate_leg_pnl(trade, exit_leg)  # noqa: SLF001
    assert abs(pnl - 50.0) < 1e-9

