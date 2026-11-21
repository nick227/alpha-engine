import os
import pytest
from datetime import datetime, timezone
from app.core.types import Prediction
from app.engine.evaluate import evaluate_prediction, summarize_outcomes
from app.core.bars.providers import build_bars_provider, NullBarsProvider, MockBarsProvider


def _make_prediction(direction: str, entry_price: float = 100.0, horizon: str = "7d") -> Prediction:
    return Prediction(
        id="pred_test_123",
        strategy_id="strat_test",
        scored_event_id="evt_test",
        ticker="AAPL",
        timestamp=datetime.now(timezone.utc),
        prediction=direction,
        confidence=0.85,
        horizon=horizon,
        entry_price=entry_price,
        mode="test",
    )


def test_long_prediction_positive_return():
    pred = _make_prediction("up", entry_price=100.0)
    ctx = {"future_return_7d": 0.05}  # +5% asset move
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is True
    assert pytest.approx(outcome.actual_return, 1e-6) == 0.05
    assert pytest.approx(outcome.return_pct, 1e-6) == 0.05  # Strategy made +5%
    assert pytest.approx(outcome.exit_price, 1e-4) == 105.0
    assert outcome.exit_reason == "horizon"


def test_long_prediction_negative_return():
    pred = _make_prediction("up", entry_price=100.0)
    ctx = {"future_return_7d": -0.05}  # -5% asset move
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is False
    assert pytest.approx(outcome.actual_return, 1e-6) == -0.05
    assert pytest.approx(outcome.return_pct, 1e-6) == -0.05  # Strategy lost -5%
    assert pytest.approx(outcome.exit_price, 1e-4) == 95.0
    assert outcome.exit_reason == "horizon"


def test_short_prediction_positive_profit_when_asset_falls():
    pred = _make_prediction("down", entry_price=100.0)
    ctx = {"future_return_7d": -0.08}  # Asset dropped -8%
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is True  # Short was correct!
    assert pytest.approx(outcome.actual_return, 1e-6) == -0.08  # Asset fell -8%
    assert pytest.approx(outcome.return_pct, 1e-6) == 0.08  # Strategy gained +8%!
    assert pytest.approx(outcome.exit_price, 1e-4) == 92.0
    assert outcome.exit_reason == "horizon"


def test_short_prediction_loss_when_asset_rises():
    pred = _make_prediction("down", entry_price=100.0)
    ctx = {"future_return_7d": 0.04}  # Asset rose +4%
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is False  # Short was incorrect
    assert pytest.approx(outcome.actual_return, 1e-6) == 0.04  # Asset rose +4%
    assert pytest.approx(outcome.return_pct, 1e-6) == -0.04  # Strategy lost -4%
    assert pytest.approx(outcome.exit_price, 1e-4) == 104.0
    assert outcome.exit_reason == "horizon"


def test_flat_prediction():
    pred = _make_prediction("flat", entry_price=100.0)
    ctx = {"future_return_7d": 0.0001}  # Virtually flat
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is True
    assert pytest.approx(outcome.return_pct, 1e-6) == 0.0


def test_missing_price_context():
    pred = _make_prediction("up", entry_price=100.0)
    ctx = {}  # Missing future_return_7d
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is False
    assert outcome.return_pct == 0.0
    assert outcome.actual_return == 0.0
    assert outcome.exit_price == 100.0
    assert outcome.exit_reason == "missing_price"


def test_invalid_entry_price():
    pred = _make_prediction("up", entry_price=0.0)
    ctx = {"future_return_7d": 0.05}
    outcome = evaluate_prediction(pred, ctx)

    assert outcome.direction_correct is False
    assert outcome.return_pct == 0.0
    assert outcome.exit_reason == "missing_price"


def test_mock_provider_rejected_without_allow_mock_bars(monkeypatch):
    monkeypatch.delenv("ALLOW_MOCK_BARS", raising=False)
    with pytest.raises(RuntimeError, match="Mock bars provider requested"):
        build_bars_provider("mock")

    # When explicitly allowed
    monkeypatch.setenv("ALLOW_MOCK_BARS", "true")
    provider = build_bars_provider("mock")
    assert isinstance(provider, MockBarsProvider)


def test_null_provider_returns_empty():
    provider = build_bars_provider("null")
    assert isinstance(provider, NullBarsProvider)
    bars = provider.fetch_bars(
        timeframe="1d",
        ticker="AAPL",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 10, tzinfo=timezone.utc),
    )
    assert bars == []
