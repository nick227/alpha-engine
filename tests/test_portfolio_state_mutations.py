from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from app.trading.paper_trader import PortfolioState
from app.trading.trade_lifecycle import OrderType, Trade, TradePosition, TradeState


def _mk_trade(ticker: str, direction: str, fill_qty: float, fill_price: float) -> Trade:
    now = datetime.now(timezone.utc)
    position = TradePosition(
        ticker=ticker,
        direction=direction,
        total_quantity=fill_qty,
        filled_quantity=fill_qty,
        remaining_quantity=0.0,
        average_entry_price=fill_price,
        current_price=fill_price,
        unrealized_pnl=0.0,
        unrealized_pnl_pct=0.0,
        last_updated=now,
    )
    return Trade(
        id=str(uuid4()),
        signal_id=str(uuid4()),
        strategy_id="test_strategy",
        ticker=ticker,
        direction=direction,
        entry_price=fill_price,
        target_price=None,
        stop_loss_price=None,
        trailing_stop_price=None,
        quantity=fill_qty,
        order_type=OrderType.MARKET,
        signal_timestamp=now,
        entry_timestamp=now,
        exit_timestamp=None,
        duration_minutes=None,
        state=TradeState.ENTERED,
        position=position,
    )


def test_portfolio_long_entry_and_close_updates_cash_and_pnl() -> None:
    now = datetime.now(timezone.utc)
    portfolio = PortfolioState(
        cash=100000.0,
        positions={},
        pending_orders=[],
        last_updated=now,
        initial_capital=100000.0,
        peak_value=100000.0,
    )

    trade = _mk_trade("AAPL", "long", 10.0, 100.0)
    portfolio.open_position_from_trade(trade)
    assert portfolio.cash == 99000.0
    assert portfolio.positions["AAPL"].quantity == 10.0

    pnl = portfolio.close_position_for_trade(trade, 110.0)
    assert pnl == 100.0
    assert portfolio.cash == 100100.0
    assert "AAPL" not in portfolio.positions
    assert portfolio.realized_pnl == 100.0


def test_portfolio_short_entry_and_close_updates_cash_and_pnl() -> None:
    now = datetime.now(timezone.utc)
    portfolio = PortfolioState(
        cash=100000.0,
        positions={},
        pending_orders=[],
        last_updated=now,
        initial_capital=100000.0,
        peak_value=100000.0,
    )

    trade = _mk_trade("TSLA", "short", 10.0, 100.0)
    portfolio.open_position_from_trade(trade)
    assert portfolio.cash == 101000.0
    assert portfolio.positions["TSLA"].quantity == -10.0

    pnl = portfolio.close_position_for_trade(trade, 90.0)
    assert pnl == 100.0
    assert portfolio.cash == 100100.0
    assert "TSLA" not in portfolio.positions
    assert portfolio.realized_pnl == 100.0


def test_portfolio_partial_close_reduces_position_and_realizes_pnl() -> None:
    now = datetime.now(timezone.utc)
    portfolio = PortfolioState(
        cash=100000.0,
        positions={},
        pending_orders=[],
        last_updated=now,
        initial_capital=100000.0,
        peak_value=100000.0,
    )

    trade = _mk_trade("MSFT", "long", 10.0, 100.0)
    portfolio.open_position_from_trade(trade)
    assert portfolio.cash == 99000.0

    pnl1 = portfolio.partial_close_for_trade(trade, exit_quantity=4.0, exit_price=105.0)
    assert pnl1 == 20.0
    assert portfolio.cash == 99420.0
    assert portfolio.positions["MSFT"].quantity == 6.0

    pnl2 = portfolio.close_position_for_trade(trade, 95.0)
    assert pnl2 == -30.0
    assert portfolio.cash == 99990.0
    assert "MSFT" not in portfolio.positions
    assert portfolio.realized_pnl == -10.0


def test_portfolio_direction_flip_realizes_and_opens_new_side() -> None:
    now = datetime.now(timezone.utc)
    portfolio = PortfolioState(
        cash=100000.0,
        positions={},
        pending_orders=[],
        last_updated=now,
        initial_capital=100000.0,
        peak_value=100000.0,
    )

    long_trade = _mk_trade("NFLX", "long", 10.0, 100.0)
    portfolio.open_position_from_trade(long_trade)
    assert portfolio.cash == 99000.0
    assert portfolio.positions["NFLX"].quantity == 10.0
    assert portfolio.realized_pnl == 0.0

    flip_trade = _mk_trade("NFLX", "short", 15.0, 110.0)  # sell 15 -> flip to short 5
    portfolio.open_position_from_trade(flip_trade)

    # Cash: -1000 (buy 10) then +1650 (sell 15) = +650 net
    assert portfolio.cash == 100650.0
    assert portfolio.positions["NFLX"].quantity == -5.0
    assert portfolio.positions["NFLX"].entry_price == 110.0
    assert portfolio.realized_pnl == 100.0
    assert portfolio.daily_pnl == 100.0
