from __future__ import annotations

from datetime import datetime, timezone

from app.trading.risk_engine import RiskAction, RiskEngine


def _has_action(results, action: RiskAction) -> bool:
    return any(r.action == action for r in results)


def test_risk_engine_halts_when_daily_loss_exceeds_limit() -> None:
    engine = RiskEngine({"risk_limits": {"max_daily_loss_pct": 0.02, "emergency_halt_loss_pct": 0.05}})
    results = engine.check_trade_risk(
        ticker="NVDA",
        direction="BUY",
        position_size=10,
        entry_price=100.0,
        confidence=0.9,
        strategy_id="s1",
        portfolio_value=100_000.0,
        daily_pnl=-3_000.0,  # 3% loss
        current_positions={},
        current_exposure={},
    )
    assert _has_action(results, RiskAction.HALT_TRADING)


def test_risk_engine_reduces_when_position_size_exceeds_limit() -> None:
    engine = RiskEngine({"risk_limits": {"max_position_size": 0.01}})
    results = engine.check_trade_risk(
        ticker="NVDA",
        direction="BUY",
        position_size=20,
        entry_price=100.0,  # $2k position on $100k = 2%
        confidence=0.9,
        strategy_id="s1",
        portfolio_value=100_000.0,
        daily_pnl=0.0,
        current_positions={},
        current_exposure={},
    )
    assert _has_action(results, RiskAction.REDUCE)


def test_risk_engine_enforces_cooldown_after_consecutive_losses() -> None:
    engine = RiskEngine({"risk_limits": {"consecutive_loss_limit": 3, "loss_cooldown_minutes": 30}})
    engine.consecutive_losses = 3
    engine.last_trade_time = datetime.now(timezone.utc)
    results = engine.check_trade_risk(
        ticker="NVDA",
        direction="BUY",
        position_size=1,
        entry_price=100.0,
        confidence=0.9,
        strategy_id="s1",
        portfolio_value=100_000.0,
        daily_pnl=0.0,
        current_positions={},
        current_exposure={},
    )
    assert _has_action(results, RiskAction.REJECT)


def test_risk_engine_update_metrics_halts_on_drawdown() -> None:
    engine = RiskEngine({"risk_limits": {"max_drawdown_pct": 0.01}})
    engine.update_portfolio_metrics(
        portfolio_value=100_000.0,
        cash_available=100_000.0,
        positions={},
        current_prices={},
    )
    engine.update_portfolio_metrics(
        portfolio_value=98_000.0,  # 2% drawdown
        cash_available=98_000.0,
        positions={},
        current_prices={},
    )
    assert engine.trading_halted is True
    assert engine.halt_reason is not None
    assert "Drawdown" in engine.halt_reason


def test_risk_engine_alerts_include_high_exposure() -> None:
    engine = RiskEngine({"risk_limits": {"max_total_exposure": 0.80}})
    # $85k exposure / ($85k + $15k) = 85% > 0.9 * 80% => alert
    engine.update_portfolio_metrics(
        portfolio_value=100_000.0,
        cash_available=15_000.0,
        positions={"NVDA": 850.0},
        current_prices={"NVDA": 100.0},
    )
    alerts = engine.get_risk_alerts()
    assert any(a.get("type") == "total_exposure" for a in alerts)

