from __future__ import annotations

from app.db.repository import AlphaRepository
from app.engine.efficiency_champion_promotion import decide_efficiency_champion


def test_decide_promote_when_no_incumbent() -> None:
    decision = decide_efficiency_champion(
        incumbent=None,
        challenger={"strategy_id": "s2", "avg_efficiency_rating": 0.7},
        min_efficiency=0.60,
        min_delta_vs_incumbent=0.05,
    )
    assert decision.action == "promote"
    assert decision.reason == "no_incumbent"


def test_decide_keep_when_delta_below_threshold_and_incumbent_is_good() -> None:
    decision = decide_efficiency_champion(
        incumbent={"strategy_id": "s1", "avg_efficiency_rating": 0.62},
        challenger={"strategy_id": "s2", "avg_efficiency_rating": 0.65},
        min_efficiency=0.60,
        min_delta_vs_incumbent=0.05,
    )
    assert decision.action == "keep"
    assert decision.reason == "delta_below_threshold"


def test_decide_promote_even_with_small_delta_if_incumbent_below_min_efficiency() -> None:
    decision = decide_efficiency_champion(
        incumbent={"strategy_id": "s1", "avg_efficiency_rating": 0.40},
        challenger={"strategy_id": "s2", "avg_efficiency_rating": 0.42},
        min_efficiency=0.60,
        min_delta_vs_incumbent=0.05,
    )
    assert decision.action == "promote"
    assert decision.reason == "better_challenger"


def test_repo_efficiency_champion_record_roundtrip() -> None:
    repo = AlphaRepository(":memory:")
    try:
        rid = repo.upsert_efficiency_champion_record(
            tenant_id="default",
            ticker="AAPL",
            timeframe="1d",
            forecast_days=None,
            regime=None,
            strategy_id="consensus-v1",
            strategy_version=None,
            avg_efficiency_rating=0.61,
            samples=123,
            total_forecast_days=456,
        )
        assert rid
        row = repo.get_efficiency_champion_record(tenant_id="default", ticker="AAPL", timeframe="1d")
        assert row is not None
        assert row["ticker"] == "AAPL"
        assert row["timeframe"] == "1d"
        assert row["forecast_days"] == -1
        assert row["regime"] == ""
        assert row["strategy_id"] == "consensus-v1"
        assert abs(float(row["avg_efficiency_rating"]) - 0.61) < 1e-12
        assert int(row["samples"]) == 123
        assert int(row["total_forecast_days"]) == 456
    finally:
        repo.close()

