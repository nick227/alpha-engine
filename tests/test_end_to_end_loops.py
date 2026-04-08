from __future__ import annotations

from datetime import datetime, timezone, timedelta


def test_live_replay_optimizer_tick_smoke(tmp_path, monkeypatch) -> None:
    # Force loops to use a temp db by working in a temp cwd.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "target_stocks.yaml").write_text("- NVDA\n", encoding="utf-8")

    from app.core.repository import Repository
    from app.core.types import RawEvent
    from app.runtime.recursive_runtime import RecursiveRuntime

    repo = Repository("data/alpha.db")
    with repo.transaction():
        # Seed a minimal bar history for NVDA
        repo.upsert_price_bar("NVDA", "2026-01-15T14:30:00Z", 100.0, 100.2, 99.9, 100.1, 1000.0)
        repo.upsert_price_bar("NVDA", "2026-01-15T14:31:00Z", 100.1, 101.0, 100.1, 101.0, 1500.0)
        # Seed one event + queue it
        evt = RawEvent(
            id="raw_evt_001",
            timestamp=datetime(2026, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
            source="sample",
            text="NVIDIA raises guidance after stronger-than-expected datacenter demand.",
            tickers=["NVDA"],
        )
        repo.persist_raw_event(evt)
    repo.close()

    rt = RecursiveRuntime()
    result = rt.tick()

    assert result["live"]["status"] == "ok"
    assert result["replay"]["status"] == "ok"
    assert result["optimizer"]["status"] == "ok"


def test_optimizer_precompute_cache_reuses_windows(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "target_stocks.yaml").write_text("- NVDA\n", encoding="utf-8")

    from app.core.repository import Repository
    from app.core.types import RawEvent, StrategyConfig
    from app.engine.optimizer_loop_service import OptimizerLoopService
    import app.engine.genetic_optimizer_service as gos

    repo = Repository("data/alpha.db")
    with repo.transaction():
        # Seed bars
        repo.upsert_price_bar("NVDA", "2026-01-15T14:30:00Z", 100.0, 100.2, 99.9, 100.1, 1000.0)
        repo.upsert_price_bar("NVDA", "2026-01-15T14:31:00Z", 100.1, 101.0, 100.1, 101.0, 1500.0)
        repo.upsert_price_bar("NVDA", "2026-01-15T14:45:00Z", 101.0, 102.0, 100.8, 101.5, 1200.0)
        # Seed enough events for optimizer
        base = datetime(2026, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        for i in range(6):
            evt = RawEvent(
                id=f"raw_evt_{i}",
                timestamp=base,
                source="sample",
                text="NVIDIA raises guidance after stronger-than-expected datacenter demand.",
                tickers=["NVDA"],
            )
            repo.persist_raw_event(evt)
            base = base + timedelta(minutes=1)

        # Seed one active strategy
        cfg = StrategyConfig(
            id="baseline-momentum-v1",
            name="baseline_momentum_v1",
            version="v1",
            strategy_type="baseline_momentum",
            mode="backtest",
            active=True,
            config={"min_short_trend": 0.0001, "horizon": "15m"},
        )
        repo.persist_strategy(cfg)
    repo.close()

    calls = {"n": 0}
    original = gos.GeneticOptimizerService.precompute_windows

    def wrapped(self, *args, **kwargs):  # type: ignore[no-redef]
        calls["n"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(gos.GeneticOptimizerService, "precompute_windows", wrapped)

    svc = OptimizerLoopService()
    out1 = svc.run_once(datetime.now(timezone.utc))
    out2 = svc.run_once(datetime.now(timezone.utc))

    assert out1["status"] == "ok"
    assert out2["status"] == "ok"
    # Cache should make precompute run only once across two calls with unchanged DB.
    assert calls["n"] == 1
