
from __future__ import annotations

import time

from app.runtime.recursive_runtime import RecursiveRuntime


class RuntimeScheduler:
    def __init__(self, interval_seconds: int = 5) -> None:
        self.runtime = RecursiveRuntime()
        self.interval = int(interval_seconds)

    def start(self, max_ticks: int | None = None) -> None:
        ticks = 0
        while True:
            result = self.runtime.tick()
            print("tick:", result["timestamp"], "live:", result.get("live", {}).get("status"), "replay:", result.get("replay", {}).get("status"), "optimizer:", result.get("optimizer", {}).get("status"))
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                return
            time.sleep(self.interval)
