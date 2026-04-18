from __future__ import annotations

import json

from app.db.repository import AlphaRepository
from app.engine.queue_rank_trim import compute_rank_score, rank_trim_pending_queue


def test_compute_rank_score_prefers_high_win_strategy() -> None:
    meta = {"strategy": "silent_compounder", "avg_score": 0.6, "raw_score": 0.6}
    low = {"silent_compounder": {"win_rate": 0.4, "avg_return": -0.01}}
    high = {"silent_compounder": {"win_rate": 0.75, "avg_return": 0.02}}
    assert compute_rank_score(meta, high) > compute_rank_score(meta, low)


def test_rank_trim_keeps_top_n(tmp_path) -> None:
    db_path = tmp_path / "t.db"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        conn = repo.conn
        as_of = "2026-04-17"
        conn.execute(
            """
            INSERT INTO discovery_stats
              (tenant_id, end_date, window_days, horizon_days, group_type, group_value, n, avg_return, win_rate, lift, status)
            VALUES
              ('default', ?, 30, 5, 'candidate_strategy', 'a_strat', 50, 0.01, 0.7, 0.0, ''),
              ('default', ?, 30, 5, 'candidate_strategy', 'b_strat', 50, -0.01, 0.45, 0.0, '')
            """,
            (as_of, as_of),
        )
        for i in range(5):
            sym = f"S{i}"
            strat = "a_strat" if i < 3 else "b_strat"
            score = 0.9 - i * 0.01
            meta = {"strategy": strat, "avg_score": score, "raw_score": score}
            conn.execute(
                """
                INSERT INTO prediction_queue
                  (tenant_id, as_of_date, symbol, source, priority, status, metadata_json)
                VALUES ('default', ?, ?, ?, 0, 'pending', ?)
                """,
                (as_of, sym, f"discovery_{strat}", json.dumps(meta)),
            )
        conn.commit()
    finally:
        repo.close()

    out = rank_trim_pending_queue(
        db_path=str(db_path),
        as_of_date=as_of,
        tenant_id="default",
        global_top_n=2,
    )
    assert out["pending_before"] == 5
    assert out["kept"] == 2
    assert out["deleted"] == 3

    repo2 = AlphaRepository(db_path=str(db_path))
    try:
        n = repo2.conn.execute(
            "SELECT COUNT(*) as n FROM prediction_queue WHERE status='pending' AND as_of_date=?",
            (as_of,),
        ).fetchone()["n"]
        assert int(n) == 2
    finally:
        repo2.close()


def test_queue_rank_trim_cli_help() -> None:
    from app.engine.queue_rank_trim import build_parser

    p = build_parser()
    assert "--as-of" in str(p.format_help())
