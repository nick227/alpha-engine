from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from app.core.repository import Repository
from app.core.types import RawEvent, StrategyConfig
from app.engine.champion_state import set_active_champion
from app.engine.champion_selector import select_champions
from app.engine.live_loop_service import LiveLoopService


def test_select_champions_prefers_stability_then_return(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = Repository(db_path=db_path)

    sentiment_a = StrategyConfig(
        id="sent-a",
        name="sent_a",
        version="v1",
        strategy_type="text_mra",
        mode="paper",
        active=True,
        config={},
    )
    sentiment_b = StrategyConfig(
        id="sent-b",
        name="sent_b",
        version="v1",
        strategy_type="text_mra",
        mode="paper",
        active=True,
        config={},
    )
    quant_a = StrategyConfig(
        id="q-a",
        name="q_a",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={},
    )
    quant_b = StrategyConfig(
        id="q-b",
        name="q_b",
        version="v1",
        strategy_type="technical_vwap_reclaim",
        mode="paper",
        active=True,
        config={},
    )

    with repo.transaction():
        for cfg in (sentiment_a, sentiment_b, quant_a, quant_b):
            repo.persist_strategy(cfg)

        # Stability dominates: sentiment_b should win even with lower avg_return.
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, ?, ?)",
            ("stab_sent_a", sentiment_a.id, 0.70, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, ?, ?)",
            ("stab_sent_b", sentiment_b.id, 0.90, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, ?, ?)",
            ("stab_q_a", quant_a.id, 0.80, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, ?, ?)",
            ("stab_q_b", quant_b.id, 0.80, repo.now_iso().replace("+00:00", "Z")),
        )

        # Tie on stability for quant -> avg_return breaks tie.
        repo.execute(
            "INSERT OR REPLACE INTO strategy_performance "
            "(id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at) "
            "VALUES (?, 'default', ?, 'ALL', 20, 0.0, ?, 0.0, ?)",
            ("perf_q_a", quant_a.id, 0.001, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_performance "
            "(id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at) "
            "VALUES (?, 'default', ?, 'ALL', 20, 0.0, ?, 0.0, ?)",
            ("perf_q_b", quant_b.id, 0.010, repo.now_iso().replace("+00:00", "Z")),
        )

    champions = select_champions(repo, min_predictions=5)
    assert champions["sentiment"].config.id == "sent-b"
    assert champions["quant"].config.id == "q-b"
    repo.close()


def test_live_loop_persists_champion_snapshot_and_uses_champions(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = Repository(db_path=db_path)

    # One sentiment + one quant strategy, but ensure selector prefers these via perf/stability.
    sent = StrategyConfig(
        id="sent-1",
        name="sent",
        version="v1",
        strategy_type="text_mra",
        mode="paper",
        active=True,
        config={"min_mra_score": 0.0, "min_materiality": 0.0, "min_company_relevance": 0.0},
    )
    quant = StrategyConfig(
        id="quant-1",
        name="quant",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={"min_short_trend": 0.0001, "horizon": "15m"},
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(minutes=10)
    event_ts = start + timedelta(minutes=5)

    evt = RawEvent(id="evt-1", timestamp=event_ts, source="live", text="Test event", tickers=["NVDA"])

    with repo.transaction():
        repo.persist_strategy(sent)
        repo.persist_strategy(quant)
        repo.persist_raw_event(evt)
        repo.enqueue_raw_event(evt.id)

        # Provide enough bars for idx>=5 and a positive short trend.
        # 1-minute bars from start..start+10m.
        for i in range(0, 11):
            ts = (start + timedelta(minutes=i)).isoformat().replace("+00:00", "Z")
            close = 100.0 + (i * 0.2)
            repo.upsert_price_bar(
                ticker="NVDA",
                timestamp=ts,
                open_price=close - 0.05,
                high=close + 0.1,
                low=close - 0.1,
                close=close,
                volume=1000.0,
            )

        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, 0.9, ?)",
            ("stab_sent", sent.id, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, 0.9, ?)",
            ("stab_quant", quant.id, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_performance "
            "(id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at) "
            "VALUES (?, 'default', ?, 'ALL', 10, 0.0, 0.0, 0.0, ?)",
            ("perf_sent", sent.id, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_performance "
            "(id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at) "
            "VALUES (?, 'default', ?, 'ALL', 10, 0.0, 0.0, 0.0, ?)",
            ("perf_quant", quant.id, repo.now_iso().replace("+00:00", "Z")),
        )

    repo.close()

    service = LiveLoopService(db_path=str(db_path))
    out = service.run_once(now)
    assert out["processed_events"] == 1
    assert out["predictions"] >= 1

    repo2 = Repository(db_path=db_path)
    snap_raw = repo2.get_kv("champions:last")
    assert snap_raw is not None
    snap = json.loads(snap_raw)
    assert snap["sentiment"]["strategy_id"] == "sent-1"
    assert snap["quant"]["strategy_id"] == "quant-1"

    # Ensure only champion strategies (plus consensus) emitted predictions for the event.
    rows = repo2.conn.execute("SELECT strategy_id FROM predictions WHERE tenant_id='default' AND ticker='NVDA'").fetchall()
    strategy_ids = {str(r["strategy_id"]) for r in rows}
    assert "sent-1" in strategy_ids
    assert "quant-1" in strategy_ids
    repo2.close()


def test_live_loop_prefers_active_champion_kv(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = Repository(db_path=db_path)

    sent = StrategyConfig(
        id="sent-1",
        name="sent",
        version="v1",
        strategy_type="text_mra",
        mode="paper",
        active=True,
        config={"min_mra_score": 0.0, "min_materiality": 0.0, "min_company_relevance": 0.0},
    )
    quant = StrategyConfig(
        id="quant-1",
        name="quant",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={"min_short_trend": 0.0001, "horizon": "15m"},
    )

    other_quant = StrategyConfig(
        id="quant-2",
        name="quant2",
        version="v1",
        strategy_type="technical_vwap_reclaim",
        mode="paper",
        active=True,
        config={},
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(minutes=10)
    event_ts = start + timedelta(minutes=5)
    evt = RawEvent(id="evt-1", timestamp=event_ts, source="live", text="Test event", tickers=["NVDA"])

    with repo.transaction():
        repo.persist_strategy(sent)
        repo.persist_strategy(quant)
        repo.persist_strategy(other_quant)
        repo.persist_raw_event(evt)
        repo.enqueue_raw_event(evt.id)

        # KV says quant-2 is champion, even though quant-1 could also predict.
        set_active_champion(repo, track="sentiment", strategy_id="sent-1", now=now, reason="test")
        set_active_champion(repo, track="quant", strategy_id="quant-2", now=now, reason="test")

        for i in range(0, 11):
            ts = (start + timedelta(minutes=i)).isoformat().replace("+00:00", "Z")
            close = 100.0 + (i * 0.2)
            repo.upsert_price_bar(
                ticker="NVDA",
                timestamp=ts,
                open_price=close - 0.05,
                high=close + 0.1,
                low=close - 0.1,
                close=close,
                volume=1000.0,
            )

    repo.close()

    service = LiveLoopService(db_path=str(db_path))
    out = service.run_once(now)
    assert out["processed_events"] == 1

    repo2 = Repository(db_path=db_path)
    rows = repo2.conn.execute("SELECT strategy_id FROM predictions WHERE tenant_id='default' AND ticker='NVDA'").fetchall()
    ids = {str(r["strategy_id"]) for r in rows}
    assert "sent-1" in ids
    assert "quant-1" not in ids

    kv = repo2.get_kv("champions:active:quant")
    assert kv is not None
    payload = json.loads(kv)
    assert payload["strategy_id"] == "quant-2"
    repo2.close()
