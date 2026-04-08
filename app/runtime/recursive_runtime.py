
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from app.engine.live_loop_service import LiveLoopService
from app.engine.optimizer_loop_service import OptimizerLoopService
from app.engine.replay_loop_service import ReplayLoopService


class RecursiveRuntime:
    """
    Minimal autonomous runtime: runs live/replay/optimizer loops in parallel.

    Each loop opens its own SQLite connection (important for thread safety).
    """

    def __init__(self) -> None:
        self.live = LiveLoopService()
        self.replay = ReplayLoopService()
        self.optimizer = OptimizerLoopService()

    def tick(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)

        tasks = {
            "live": lambda: self.live.run_once(now),
            "replay": lambda: self.replay.run_once(now),
            "optimizer": lambda: self.optimizer.run_once(now),
        }

        results: dict[str, Any] = {"timestamp": now.isoformat()}
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(fn): name for name, fn in tasks.items()}
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    results[name] = fut.result()
                except Exception as e:
                    results[name] = {"status": "error", "error": repr(e)}

        return results
