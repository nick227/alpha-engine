from __future__ import annotations

from datetime import datetime, timezone

from app.core.repository import Repository
from app.engine.champion_state import refresh_active_champions_from_ranked
from app.engine.replay_sqlite import SQLiteMetricsUpdater, SQLiteOutcomeWriter, SQLitePredictionRepository, SQLitePriceRepository
from app.engine.replay_worker import ReplayWorker

class ReplayLoopService:
    """Replay scoring loop scaffold."""

    def __init__(self, db_path: str = "data/alpha.db", tenant_id: str = "default") -> None:
        self.db_path = str(db_path)
        self.tenant_id = str(tenant_id)

    def run_once(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        repo = Repository(self.db_path)
        predictions = SQLitePredictionRepository(repo)
        prices = SQLitePriceRepository(repo)
        outcomes = SQLiteOutcomeWriter(repo)
        metrics = SQLiteMetricsUpdater(repo)
        worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)
        scored = worker.run_once(now)
        snapshot = refresh_active_champions_from_ranked(repo, tenant_id=self.tenant_id, min_predictions=5, now=now)
        repo.add_heartbeat("replay", "ok", f"scored {scored} predictions")
        repo.close()
        return {"mode": "replay", "ran_at": now.isoformat(), "status": "ok", "scored": scored, "champions": snapshot}
