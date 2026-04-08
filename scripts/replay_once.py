from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

from dateutil.parser import isoparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository
from app.engine.replay_sqlite import (
    SQLiteMetricsUpdater,
    SQLiteOutcomeWriter,
    SQLitePredictionRepository,
    SQLitePriceRepository,
)
from app.engine.replay_worker import ReplayWorker


def main(
    *,
    db_path: str | Path = "data/alpha.db",
    now_iso: str | None = None,
) -> None:
    now = datetime.now(timezone.utc) if now_iso is None else isoparse(now_iso).astimezone(timezone.utc)

    repo = Repository(db_path=db_path)
    predictions = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)

    worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)
    scored = worker.run_once(now)
    repo.close()

    print(f"Replay scored {scored} predictions at {now.isoformat()}.")


if __name__ == "__main__":
    main()

