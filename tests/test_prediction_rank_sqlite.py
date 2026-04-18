from __future__ import annotations

import json

from app.db.repository import AlphaRepository
from app.engine.prediction_rank_sqlite import (
    compute_prediction_rank_score,
    rank_predictions_for_date,
)


def test_compute_prediction_rank_score_mid_values() -> None:
    rs = compute_prediction_rank_score(
        confidence=0.5,
        accuracy=0.5,
        avg_return=0.0,
        live_score=0.5,
        stability_score=0.5,
    )
    expected = 0.35 * 0.5 + 0.20 * 0.5 + 0.20 * 0.5 + 0.15 * 0.5 + 0.10 * 0.5
    assert abs(rs - expected) < 1e-9


def _seed_scored_event(conn, tenant: str) -> None:
    conn.execute(
        """
        INSERT INTO raw_events (id, tenant_id, timestamp, source, text, tickers_json, metadata_json)
        VALUES ('re_pr', ?, '2026-04-17T10:00:00Z', 't', 'x', '[]', '{}')
        """,
        (tenant,),
    )
    conn.execute(
        """
        INSERT INTO scored_events (
          id, tenant_id, raw_event_id, primary_ticker, category, materiality, direction,
          confidence, company_relevance, concept_tags_json, explanation_terms_json,
          scorer_version, taxonomy_version
        )
        VALUES ('se_pr', ?, 're_pr', 'AAA', 'c', 0.5, 'up', 0.5, 0.5, '[]', '[]', 'v1', 'v1')
        """,
        (tenant,),
    )


def _seed_strategy_metrics(conn, tenant: str, sid: str) -> None:
    conn.execute(
        """
        INSERT INTO strategies (
          id, tenant_id, track, name, version, strategy_type, mode, config_json, status, live_score
        )
        VALUES (?, ?, 'paper', 'T', '1', 't', 'paper', '{}', 'CANDIDATE', 0.8)
        """,
        (sid, tenant),
    )
    conn.execute(
        """
        INSERT INTO strategy_performance (
          id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha
        )
        VALUES ('perf_seed', ?, ?, 'ALL', 10, 0.6, 0.02, 0.0)
        """,
        (tenant, sid),
    )
    conn.execute(
        """
        INSERT INTO strategy_stability (
          id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score
        )
        VALUES ('stab_seed', ?, ?, 0.5, 0.5, 0.55)
        """,
        (tenant, sid),
    )


def _insert_prediction(
    conn,
    *,
    pid: str,
    tenant: str,
    sid: str,
    ts: str,
    confidence: float,
    mode: str = "discovery",
) -> None:
    conn.execute(
        """
        INSERT INTO predictions (
          id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction,
          confidence, horizon, entry_price, mode, feature_snapshot_json
        )
        VALUES (?, ?, ?, 'se_pr', 'AAA', ?, 'up', ?, '5d', 10.0, ?, ?)
        """,
        (pid, tenant, sid, ts, confidence, mode, json.dumps({"strategy": "silent_compounder"})),
    )


def test_rank_predictions_for_date_sets_rank_score(tmp_path) -> None:
    db_path = tmp_path / "r.db"
    tenant = "default"
    as_of = "2026-04-17"
    ts = f"{as_of}T14:00:00Z"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        conn = repo.conn
        _seed_scored_event(conn, tenant)
        _seed_strategy_metrics(conn, tenant, "silent_compounder_v1_paper")
        _insert_prediction(conn, pid="p1", tenant=tenant, sid="silent_compounder_v1_paper", ts=ts, confidence=0.7)
        conn.commit()
    finally:
        repo.close()

    out = rank_predictions_for_date(
        db_path=str(db_path),
        as_of_date=as_of,
        tenant_id=tenant,
        apply_trim=False,
    )
    assert out["updated"] == 1

    repo2 = AlphaRepository(db_path=str(db_path))
    try:
        row = repo2.conn.execute(
            "SELECT rank_score, ranking_context_json FROM predictions WHERE id='p1'"
        ).fetchone()
        assert row is not None and row["rank_score"] is not None
        assert float(row["rank_score"]) > 0.0
        snap = json.loads(str(row["ranking_context_json"] or "{}"))
        assert snap.get("market_context", {}).get("vix") is not None
        assert "rank_score_base" in snap and "temporal_multiplier" in snap
        assert snap.get("config", {}).get("pipeline_version")
    finally:
        repo2.close()


def test_rank_trim_keeps_higher_score_same_strategy(tmp_path) -> None:
    db_path = tmp_path / "r2.db"
    tenant = "default"
    as_of = "2026-04-17"
    ts = f"{as_of}T14:00:00Z"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        conn = repo.conn
        _seed_scored_event(conn, tenant)
        _seed_strategy_metrics(conn, tenant, "silent_compounder_v1_paper")
        _insert_prediction(conn, pid="p_hi", tenant=tenant, sid="silent_compounder_v1_paper", ts=ts, confidence=0.95)
        _insert_prediction(conn, pid="p_lo", tenant=tenant, sid="silent_compounder_v1_paper", ts=ts, confidence=0.05)
        conn.commit()
    finally:
        repo.close()

    out = rank_predictions_for_date(
        db_path=str(db_path),
        as_of_date=as_of,
        tenant_id=tenant,
        apply_trim=True,
        global_top_n=5,
        max_per_strategy=1,
    )
    assert out["trimmed"] == 1

    repo2 = AlphaRepository(db_path=str(db_path))
    try:
        ids = {str(r["id"]) for r in repo2.conn.execute("SELECT id FROM predictions").fetchall()}
        assert ids == {"p_hi"}
    finally:
        repo2.close()


def test_prediction_rank_sqlite_cli_help() -> None:
    from app.engine.prediction_rank_sqlite import build_parser

    p = build_parser()
    assert "--as-of" in str(p.format_help())
