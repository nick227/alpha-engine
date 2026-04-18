from __future__ import annotations

from app.engine.ranking_temporal import (
    append_market_context_audit,
    apply_temporal_adjustment,
    build_market_context,
    infer_regime,
    market_context_log_line,
    normalize_strategy_type,
    temporal_ranking_config_snapshot,
)


def test_temporal_ranking_config_includes_pipeline_version() -> None:
    cfg = temporal_ranking_config_snapshot()
    assert "pipeline_version" in cfg
    assert isinstance(cfg["pipeline_version"], str) and len(cfg["pipeline_version"]) >= 1


def test_pipeline_version_env_override(monkeypatch) -> None:
    import app.engine.ranking_temporal as rt

    monkeypatch.setattr(rt, "_PIPELINE_VERSION_CACHE", None)
    monkeypatch.setenv("ALPHA_PIPELINE_VERSION", "custom-build-99")
    assert rt.pipeline_version() == "custom-build-99"


def test_infer_regime() -> None:
    assert infer_regime(10.0) == "low"
    assert infer_regime(20.0) == "normal"
    assert infer_regime(35.0) == "high"


def test_normalize_strategy_type() -> None:
    assert normalize_strategy_type("silent_compounder_v1_paper") == "silent_compounder"
    assert normalize_strategy_type("discovery_volatility_breakout") == "volatility_breakout"


def test_market_context_log_line() -> None:
    s = market_context_log_line(
        {
            "vix": 18.4,
            "regime": "normal",
            "vix_age_days": 0,
            "context_warning": False,
        }
    )
    assert "VIX=18.4" in s and "Regime=normal" in s and "Age=0d" in s and "Warning=False" in s


def test_apply_temporal_high_vix_breakout_boost() -> None:
    ctx = {"vix": 32.0, "regime": "high", "sentiment": "neutral", "month": 4, "vix_fallback_used": False}
    m = apply_temporal_adjustment("volatility_breakout", ctx)
    assert abs(m - 1.2) < 1e-9


def test_apply_temporal_low_vix_silent_compounder() -> None:
    ctx = {"vix": 12.0, "regime": "low", "sentiment": "neutral", "month": 4, "vix_fallback_used": False}
    m = apply_temporal_adjustment("silent_compounder", ctx)
    assert abs(m - 1.15) < 1e-9


def test_apply_temporal_september_dampener() -> None:
    ctx = {"vix": 20.0, "regime": "normal", "sentiment": "neutral", "month": 9, "vix_fallback_used": False}
    m = apply_temporal_adjustment("silent_compounder", ctx)
    assert abs(m - 0.9) < 1e-9


def test_apply_temporal_disabled_flag(monkeypatch) -> None:
    import app.engine.ranking_temporal as rt

    monkeypatch.setattr(rt, "_RANK_TEMPORAL", False)
    ctx = {"vix": 32.0, "regime": "high", "sentiment": "neutral", "month": 4, "vix_fallback_used": False}
    assert rt.apply_temporal_adjustment("volatility_breakout", ctx) == 1.0


def test_apply_temporal_vix_fallback_penalty(monkeypatch) -> None:
    import app.engine.ranking_temporal as rt

    monkeypatch.setattr(rt, "_RANK_TEMPORAL", False)
    ctx = {"vix": 20.0, "month": 4, "vix_fallback_used": True}
    assert abs(rt.apply_temporal_adjustment("anything", ctx) - 0.95) < 1e-9


def test_append_market_context_audit(tmp_path, monkeypatch) -> None:
    import app.engine.ranking_temporal as rt

    log = tmp_path / "audit.tsv"
    monkeypatch.setattr(rt, "_AUDIT_LOG", log)
    append_market_context_audit(
        "queue_rank_trim",
        {"context_warning": True, "vix_fallback_used": False, "vix": 18.0, "vix_age_days": 0},
    )
    text = log.read_text(encoding="utf-8")
    assert "queue_rank_trim" in text and "\tTrue\tFalse\t18.0\t0" in text


def test_build_market_context_uses_vix_from_db(tmp_path) -> None:
    from app.db.repository import AlphaRepository

    db_path = tmp_path / "t.db"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        conn = repo.conn
        conn.execute(
            """
            INSERT INTO price_bars
              (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES
              ('default', '^VIX', '1d', '2026-04-16T00:00:00Z', 18, 18, 18, 18, 1e6)
            """,
        )
        conn.commit()
    finally:
        repo.close()

    repo2 = AlphaRepository(db_path=str(db_path))
    try:
        ctx = build_market_context(repo2.conn, tenant_id="default", as_of_date="2026-04-17")
        assert ctx["vix"] == 18.0
        assert ctx["regime"] == "normal"
        assert ctx["month"] == 4
        assert ctx["vix_timestamp"] == "2026-04-16"
        assert ctx["vix_fallback_used"] is False
        assert ctx["vix_age_days"] == 1
        assert ctx["context_warning"] is False
    finally:
        repo2.close()


def test_build_market_context_fallback_when_no_vix_bar(tmp_path) -> None:
    from app.db.repository import AlphaRepository

    db_path = tmp_path / "empty.db"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        ctx = build_market_context(repo.conn, tenant_id="default", as_of_date="2026-04-17")
        assert ctx["vix"] == 20.0
        assert ctx["vix_fallback_used"] is True
        assert ctx["vix_timestamp"] is None
        assert ctx["vix_age_days"] is None
        assert ctx["context_warning"] is True
    finally:
        repo.close()


def test_build_market_context_stale_vix_bar_warns(tmp_path) -> None:
    from app.db.repository import AlphaRepository

    db_path = tmp_path / "stale.db"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        conn = repo.conn
        conn.execute(
            """
            INSERT INTO price_bars
              (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES
              ('default', '^VIX', '1d', '2026-04-14T00:00:00Z', 20, 20, 20, 20, 1e6)
            """,
        )
        conn.commit()
    finally:
        repo.close()

    repo2 = AlphaRepository(db_path=str(db_path))
    try:
        ctx = build_market_context(repo2.conn, tenant_id="default", as_of_date="2026-04-17")
        assert ctx["vix_age_days"] == 3
        assert ctx["context_warning"] is True
    finally:
        repo2.close()
