from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.mra import compute_mra
from app.core.regime_manager import RegimeManager
from app.core.scoring import score_event
from app.core.types import RawEvent, StrategyConfig
from app.engine.consensus_engine import ConsensusEngine, TrackSignal
from app.engine.mutation_engine import mutate_strategy_config
from app.engine.promotion_gate import passes_forward_gate
from app.engine.reaper_engine import should_reap
from app.engine.runner import run_pipeline
from app.engine.strategy_factory import build_strategy_instance


def test_strategy_produces_prediction_given_event() -> None:
    evt = RawEvent(
        id="evt-1",
        timestamp=datetime.now(timezone.utc),
        source="test",
        text="Nvidia raises guidance after record datacenter demand",
        tickers=["NVDA"],
    )
    scored = score_event(evt)
    price_context = {
        "entry_price": 100.0,
        "return_5m": 0.01,
        "return_15m": 0.02,
        "volume_ratio": 2.0,
        "vwap_distance": 0.01,
        "range_expansion": 1.5,
        "continuation_slope": 0.3,
        "pullback_depth": 0.001,
        "future_return_15m": 0.02,
    }
    mra = compute_mra(scored, price_context)

    cfg = StrategyConfig(
        id="text-mra-test",
        name="text_mra_test",
        version="v1",
        strategy_type="text_mra",
        mode="backtest",
        active=True,
        config={
            "required_categories": [],
            "min_materiality": 0.0,
            "min_company_relevance": 0.0,
            "min_mra_score": 0.0,
            "horizon": "15m",
            "text_weight": 0.6,
            "mra_weight": 0.4,
        },
    )
    strat = build_strategy_instance(cfg)
    assert strat is not None

    pred = strat.maybe_predict(scored, mra, price_context, evt.timestamp)
    assert pred is not None
    assert pred.ticker == "NVDA"
    assert pred.horizon == "15m"


def test_consensus_combines_sentiment_quant_with_agreement_bonus_and_stability() -> None:
    engine = ConsensusEngine(regime_manager=RegimeManager(agreement_bonus=0.05))

    sentiment = TrackSignal(ticker="NVDA", direction="up", confidence=0.8, track="sentiment", metadata={})
    quant = TrackSignal(ticker="NVDA", direction="up", confidence=0.2, track="quant", metadata={})

    # NORMAL volatility regime -> base weights 0.5/0.5, stability weights dominate.
    out = engine.combine(
        sentiment_signal=sentiment,
        quant_signal=quant,
        realized_volatility=0.01,
        historical_volatility_window=[0.01] * 20,
        sentiment_stability=0.9,
        quant_stability=0.1,
    )

    # ws ~ 0.9, wq ~ 0.1; plus agreement_bonus 0.05.
    assert abs(out.confidence - (0.9 * 0.8 + 0.1 * 0.2 + 0.05)) < 1e-6
    assert out.metadata["ws"] == 0.9
    assert out.metadata["wq"] == 0.1


def test_regime_weighting_changes_final_score() -> None:
    engine = ConsensusEngine(regime_manager=RegimeManager(high_vol_z=1.0, low_vol_z=-1.0, agreement_bonus=0.05))
    sentiment = TrackSignal(ticker="NVDA", direction="up", confidence=0.9, track="sentiment", metadata={})
    quant = TrackSignal(ticker="NVDA", direction="up", confidence=0.1, track="quant", metadata={})

    # Normal regime (zscore ~ 0) => ~0.5/0.5 weights.
    normal = engine.combine(
        sentiment_signal=sentiment,
        quant_signal=quant,
        realized_volatility=0.01,
        historical_volatility_window=[0.01] * 20,
    )

    # High vol regime (zscore high) => sentiment heavy.
    high = engine.combine(
        sentiment_signal=sentiment,
        quant_signal=quant,
        realized_volatility=0.10,
        historical_volatility_window=[0.01] * 19 + [0.02],
    )

    assert high.confidence > normal.confidence


def test_replay_scoring_computes_outcome_correctly(tmp_path) -> None:
    from app.core.repository import Repository
    from app.engine.replay_sqlite import SQLiteMetricsUpdater, SQLiteOutcomeWriter, SQLitePredictionRepository, SQLitePriceRepository
    from app.engine.replay_worker import ReplayWorker
    from app.core.types import Prediction

    db_path = tmp_path / "alpha.db"
    repo = Repository(db_path=db_path)

    # Seed strategy + one paper prediction that already expired (1m).
    strat_cfg = StrategyConfig(
        id="strat-test",
        name="test",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={"horizon": "1m"},
    )
    with repo.transaction():
        repo.persist_strategy(strat_cfg)

        pred_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(microsecond=0)
        pred = Prediction(
            id="pred-1",
            strategy_id=strat_cfg.id,
            scored_event_id="scored-1",
            ticker="NVDA",
            timestamp=pred_ts,
            prediction="up",
            confidence=0.6,
            horizon="1m",
            entry_price=100.0,
            mode="paper",
            feature_snapshot={"regime": "HIGH", "trend_strength": "UNKNOWN"},
        )
        repo.persist_prediction(pred)

        # Add an exit bar at/after expiry with close=102.
        expiry = pred_ts + timedelta(minutes=1)
        repo.upsert_price_bar(
            ticker="NVDA",
            timestamp=expiry.isoformat().replace("+00:00", "Z"),
            open_price=101.0,
            high=102.0,
            low=100.5,
            close=102.0,
            volume=1000.0,
        )

    predictions = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)

    scored = worker.run_once(datetime.now(timezone.utc))
    assert scored == 1

    row = repo.conn.execute("SELECT return_pct, direction_correct FROM prediction_outcomes WHERE prediction_id = 'pred-1'").fetchone()
    assert row is not None
    assert float(row["return_pct"]) > 0
    assert int(row["direction_correct"]) == 1
    repo.close()


def test_optimizer_mutation_produces_valid_child() -> None:
    parent = StrategyConfig(
        id="text-parent",
        name="text_parent",
        version="v1",
        strategy_type="text_mra",
        mode="backtest",
        active=True,
        config={"text_weight": 0.6, "mra_weight": 0.4, "min_mra_score": 0.2, "min_materiality": 0.4},
    )
    children = mutate_strategy_config(parent, max_children=10)
    assert children
    for ch in children:
        assert ch.id != parent.id
        assert ch.strategy_type == parent.strategy_type
        assert abs(float(ch.config.get("text_weight", 0.0)) + float(ch.config.get("mra_weight", 0.0)) - 1.0) < 1e-9


def test_promotion_gate_accepts_and_rejects() -> None:
    parent = {"id": "p", "forward_alpha": 0.01, "stability_score": 0.7, "sample_size": 20}
    cand_pass = {"id": "c", "forward_alpha": 0.02, "stability_score": 0.8, "sample_size": 20}
    passed, _ = passes_forward_gate(cand_pass, parent, min_stability_required=0.6, min_sample_size=10)
    assert passed is True

    cand_fail = {"id": "c2", "forward_alpha": -0.01, "stability_score": 0.5, "sample_size": 5}
    passed2, _ = passes_forward_gate(cand_fail, parent, min_stability_required=0.6, min_sample_size=10)
    assert passed2 is False


def test_rollback_triggers_when_stability_drops() -> None:
    reap, reason = should_reap({"stability_score": 0.59, "consecutive_bad_windows": 3, "parent_underperformance_pct": 0.0})
    assert reap is True
    assert "Stability" in reason


def test_run_pipeline_produces_predictions() -> None:
    now = datetime.now(timezone.utc)
    raw_events = [
        RawEvent(id="evt-1", timestamp=now, source="test", text="Tesla secondary offering sparks dilution concern", tickers=["TSLA"]),
        RawEvent(id="evt-2", timestamp=now + timedelta(minutes=1), source="test", text="Nvidia raises guidance after record services growth", tickers=["NVDA"]),
    ]
    price_contexts = {
        "evt-1": {"entry_price": 100.0, "return_5m": -0.01, "return_15m": -0.02, "future_return_15m": -0.02, "volume_ratio": 2.0},
        "evt-2": {"entry_price": 100.0, "return_5m": 0.01, "return_15m": 0.02, "future_return_15m": 0.02, "volume_ratio": 2.0},
    }
    cfgs = [
        StrategyConfig(
            id="text-mra-v1",
            name="text_mra_v1",
            version="v1",
            strategy_type="text_mra",
            mode="backtest",
            active=True,
            config={"min_materiality": 0.0, "min_company_relevance": 0.0, "min_mra_score": 0.0, "horizon": "15m", "text_weight": 0.6, "mra_weight": 0.4},
        )
    ]
    out = run_pipeline(raw_events, price_contexts, persist=False, strategy_configs=cfgs)
    assert len(out["prediction_rows"]) >= 1
