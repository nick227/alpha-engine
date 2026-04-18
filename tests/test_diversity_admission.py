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
    finally:
        repo.close()
