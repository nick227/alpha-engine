from __future__ import annotations


from app.ingest.backfill_runner import BackfillRunner


def test_backfill_runner_wires_promotion_engine_to_db(tmp_path):
    db_path = tmp_path / "alpha.db"

    runner = BackfillRunner(db_path=str(db_path))

    engine = runner._get_promotion_engine()

    assert str(engine.repo.db_path) == str(db_path)
    assert str(engine.store.db_path) == str(db_path)
