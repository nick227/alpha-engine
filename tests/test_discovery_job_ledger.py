from __future__ import annotations

from app.db.repository import AlphaRepository


def test_discovery_job_ledger_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = AlphaRepository(db_path=db_path)
    try:
        job_id = repo.start_discovery_job(job_type="nightly", tenant_id="default")
        row = repo.conn.execute("SELECT job_type, status FROM discovery_jobs WHERE id = ?", (job_id,)).fetchone()
        assert row["job_type"] == "nightly"
        assert row["status"] == "running"
        repo.finish_discovery_job(job_id=job_id, status="success", tenant_id="default")
        row2 = repo.conn.execute("SELECT status, completed_at FROM discovery_jobs WHERE id = ?", (job_id,)).fetchone()
        assert row2["status"] == "success"
        assert row2["completed_at"] is not None
    finally:
        repo.close()

