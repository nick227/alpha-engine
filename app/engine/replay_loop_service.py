from __future__ import annotations

from datetime import datetime, timezone

from app.core.repository import Repository
from app.engine.replay_sqlite import SQLiteMetricsUpdater, SQLiteOutcomeWriter, SQLitePredictionRepository, SQLitePriceRepository
from app.engine.replay_worker import ReplayWorker

class ReplayLoopService:
    """Replay scoring loop scaffold."""

    def run_once(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        repo = Repository("data/alpha.db")
        predictions = SQLitePredictionRepository(repo)
        prices = SQLitePriceRepository(repo)
        outcomes = SQLiteOutcomeWriter(repo)
        metrics = SQLiteMetricsUpdater(repo)
        worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)
        scored = worker.run_once(now)
        repo.add_heartbeat("replay", "ok", f"scored {scored} predictions")
        repo.close()
        return {"mode": "replay", "ran_at": now.isoformat(), "status": "ok", "scored": scored}
