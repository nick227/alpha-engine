from __future__ import annotations

from app.db.repository import AlphaRepository
from app.discovery.admission import run_diversity_admission


def _row(
    ticker: str,
    *,
    status: str,
    lens: str,
    mcap: str,
    mult: float,
    dscore: float,
) -> tuple:
    now = "2026-04-01T00:00:00Z"
    return (
        "default",
        ticker,
        status,
        now,
        now,
        5,
        None,
        "test_strat",
        "[]",
        lens,
        dscore,
        None,
        mcap,
        None,
        None,
        mult,
        "{}",
    )


def test_diversity_admission_per_lens_and_cap(tmp_path) -> None:
    db = tmp_path / "t.db"
    repo = AlphaRepository(db_path=db)
    try:
        rows = [
            _row("AAA", status="recurring", lens="undervalued", mcap="large", mult=0.9, dscore=0.8),
            _row("BBB", status="recurring", lens="undervalued", mcap="large", mult=0.85, dscore=0.7),
            _row("CCC", status="recurring", lens="top_signal", mcap="small", mult=0.88, dscore=0.75),
            _row("DDD", status="recurring", lens="top_signal", mcap="small", mult=0.82, dscore=0.72),
        ]
        repo.conn.executemany(
            """
            INSERT INTO candidate_queue (
              tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count,
              rejection_reason, primary_strategy, strategy_tags_json, discovery_lens, discovery_score,
              price_bucket, market_cap_bucket, sector, industry, multiplier_score, metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        repo.conn.commit()

        out = run_diversity_admission(
            repo,
            max_admitted=3,
            per_lens_cap=1,
            per_mcap_cap=2,
        )
        assert out["ok"] is True
        assert len(out["newly_admitted"]) == 3
        assert set(out["newly_admitted"]) >= {"AAA", "CCC"}

        n = repo.conn.execute(
            "SELECT COUNT(*) AS n FROM candidate_queue WHERE status='admitted'"
        ).fetchone()["n"]
        assert int(n) == 3
        mrows = repo.conn.execute("SELECT COUNT(*) AS n FROM admission_metrics").fetchone()["n"]
        assert int(mrows) >= 1
    finally:
        repo.close()


def _admitted_row(
    ticker: str,
    *,
    mult: float,
    dscore: float,
    mcap: str = "large",
) -> tuple:
    now = "2026-04-01T00:00:00Z"
    return (
        "default",
        ticker,
        "admitted",
        now,
        now,
        5,
        None,
        "s",
        "[]",
        "undervalued",
        dscore,
        None,
        mcap,
        None,
        None,
        mult,
        "{}",
    )


def test_overrule_at_cap_swaps_weakest(tmp_path) -> None:
    db = tmp_path / "t.db"
    repo = AlphaRepository(db_path=db)
    try:
        rows = []
        for i in range(20):
            sym = f"W{i:02d}"
            rows.append(_admitted_row(sym, mult=0.1 + i * 0.001, dscore=0.1))
        rows.append(_row("STAR", status="recurring", lens="top_signal", mcap="large", mult=0.95, dscore=0.9))
        repo.conn.executemany(
            """
            INSERT INTO candidate_queue (
              tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count,
              rejection_reason, primary_strategy, strategy_tags_json, discovery_lens, discovery_score,
              price_bucket, market_cap_bucket, sector, industry, multiplier_score, metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        repo.conn.commit()

        out = run_diversity_admission(
            repo,
            max_admitted=20,
            per_lens_cap=1,
            per_mcap_cap=None,
            overrule_at_cap=True,
            overrule_min_multiplier=0.85,
            overrule_min_discovery_score=0.8,
            max_overrule_swaps=2,
        )
        assert out["overrule"]["ran"] is True
        assert out["overrule"]["count"] >= 1
        star = repo.conn.execute(
            "SELECT status FROM candidate_queue WHERE ticker='STAR'"
        ).fetchone()
        assert str(star["status"]) == "admitted"
    finally:
        repo.close()
