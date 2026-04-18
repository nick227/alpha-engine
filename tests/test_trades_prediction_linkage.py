from __future__ import annotations

from app.db.repository import AlphaRepository


def test_save_trade_persists_prediction_linkage(tmp_path) -> None:
    db_path = tmp_path / "t.db"
    repo = AlphaRepository(db_path=str(db_path))
    try:
        repo.save_trade(
            {
                "id": "trade-1",
                "ticker": "NVDA",
                "direction": "long",
                "quantity": 1.0,
                "entry_price": 100.0,
                "exit_price": 102.0,
                "pnl": 2.0,
                "status": "CLOSED",
                "mode": "paper",
                "strategy_id": "s1",
                "prediction_id": "pred-abc",
                "broker_order_id": "ord-xyz",
                "source": "alpaca",
            }
        )
        row = repo.conn.execute(
            "SELECT prediction_id, broker_order_id, source FROM trades WHERE id = ?",
            ("trade-1",),
        ).fetchone()
        assert row is not None
        assert str(row["prediction_id"]) == "pred-abc"
        assert str(row["broker_order_id"]) == "ord-xyz"
        assert str(row["source"]) == "alpaca"
    finally:
        repo.close()
