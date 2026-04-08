from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.runtime.scheduler import RuntimeScheduler


def main() -> None:
    RuntimeScheduler(interval_seconds=5).start()


if __name__ == "__main__":
    main()

