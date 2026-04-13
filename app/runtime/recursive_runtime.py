
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from app.engine.live_loop_service import LiveLoopService
from app.engine.optimizer_loop_service import OptimizerLoopService
from app.engine.replay_loop_service import ReplayLoopService


def _env_bool(name: str, default: bool) -> bool:
    raw = str(__import__("os").getenv(name, "") or "").strip().lower()
    if raw == "":
        return bool(default)
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


class RecursiveRuntime:
    """
    Minimal autonomous runtime: runs live/replay/optimizer loops in parallel.

    Each loop opens its own SQLite connection (important for thread safety).
    """

    def __init__(self) -> None:
        # Default is backward-compatible: run all loops.
        self.enable_live = _env_bool("ALPHA_RUNTIME_ENABLE_LIVE", True)
        self.enable_replay = _env_bool("ALPHA_RUNTIME_ENABLE_REPLAY", True)
        self.enable_optimizer = _env_bool("ALPHA_RUNTIME_ENABLE_OPTIMIZER", True)

        self.live = LiveLoopService() if self.enable_live else None
        self.replay = ReplayLoopService() if self.enable_replay else None
        self.optimizer = OptimizerLoopService() if self.enable_optimizer else None

    def tick(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)

        tasks: dict[str, Any] = {}
        if self.live is not None:
            tasks["live"] = lambda: self.live.run_once(now)
        if self.replay is not None:
            tasks["replay"] = lambda: self.replay.run_once(now)
        if self.optimizer is not None:
            tasks["optimizer"] = lambda: self.optimizer.run_once(now)

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
