from __future__ import annotations

from app.analytics.learning_feedback_report import format_oneline, run_report


def test_run_report_empty_db(tmp_path) -> None:
    from app.db.repository import AlphaRepository

    db = tmp_path / "a.db"
    AlphaRepository(db_path=str(db)).close()
    s = run_report(db_path=str(db), tenant_id="default")
    assert s["by_source"] == {}
    line = format_oneline(s)
    assert "no matched" in line.lower()


def test_save_trade_warns_missing_prediction_when_sourced(caplog) -> None:
    import logging

    from app.db.repository import AlphaRepository

    caplog.set_level(logging.WARNING)
    db = ":memory:"
    repo = AlphaRepository(db_path=db)
    try:
        repo.save_trade(
            {
                "id": "x1",
                "ticker": "A",
                "direction": "long",
                "quantity": 1.0,
                "entry_price": 10.0,
                "status": "CLOSED",
                "mode": "paper",
                "source": "paper",
            }
        )
    finally:
        repo.close()
    assert any("missing prediction_id" in r.message for r in caplog.records)
