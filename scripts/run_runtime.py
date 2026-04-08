from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.runtime.scheduler import RuntimeScheduler


def main() -> None:
    import os

    interval = int(os.environ.get("ALPHA_RUNTIME_INTERVAL", "5"))
    max_ticks_raw = os.environ.get("ALPHA_RUNTIME_MAX_TICKS")
    max_ticks = int(max_ticks_raw) if max_ticks_raw else None
    RuntimeScheduler(interval_seconds=interval).start(max_ticks=max_ticks)


if __name__ == "__main__":
    main()
