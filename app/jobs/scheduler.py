"""
Idempotent daily scheduler for Alpha Engine pipeline.

- Runs as a background daemon loop, executing once per calendar day at the configured UTC hour.
- Respects fail-closed preflight (ALPHA_PIPELINE_ENABLED, storage headroom guard).
- Uses kernel advisory flock via start_guarded to guarantee zero concurrent writers.
- Records idempotent run state in pipeline-status.json so restarts never re-trigger.
- Controlled by ALPHA_SCHEDULER_ENABLED (default "false", keeping Railway staging safely disabled).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.jobs import pipeline_control

logger = logging.getLogger("pipeline_scheduler")

_scheduler_instance: DailyScheduler | None = None


class DailyScheduler:
    def __init__(
        self,
        db_path: str = "data/alpha.db",
        *,
        utc_target_hour: int = 21,
        interval_seconds: int = 60,
    ) -> None:
        self.db_path = db_path
        self.utc_target_hour = int(os.environ.get("ALPHA_SCHEDULER_UTC_HOUR") or utc_target_hour)
        self.interval_seconds = int(os.environ.get("ALPHA_SCHEDULER_INTERVAL_SECONDS") or interval_seconds)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_attempt: str | None = None
        self._last_attempt_status: str | None = None

    def is_enabled(self) -> bool:
        return os.environ.get("ALPHA_SCHEDULER_ENABLED", "").strip().lower() == "true"

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="alpha_daily_scheduler")
        self._thread.start()
        logger.info(
            f"DailyScheduler started for db='{self.db_path}' (target_hour={self.utc_target_hour} UTC, enabled={self.is_enabled()})"
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("DailyScheduler stopped.")

    def _should_trigger_today(self, now: datetime) -> bool:
        today_iso = now.date().isoformat()
        status = pipeline_control.read_status(self.db_path) or {}

        # If today already succeeded, do not run again
        if status.get("state") == "succeeded" and status.get("finished_at", "").startswith(today_iso):
            return False

        # Only trigger at or after the scheduled UTC hour
        return now.hour >= self.utc_target_hour

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self.is_enabled():
                    now = datetime.now(timezone.utc)
                    if self._should_trigger_today(now):
                        logger.info(f"Triggering scheduled daily pipeline at {now.isoformat()}")
                        self._last_attempt = now.isoformat()
                        res = pipeline_control.start_guarded(
                            self.db_path,
                            trigger="scheduler",
                            background=False,
                        )
                        self._last_attempt_status = res.get("status")
                        logger.info(f"Scheduled pipeline finished with result: {res}")
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")

            # Sleep in small increments for clean shutdown
            for _ in range(max(1, self.interval_seconds)):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.is_enabled(),
            "target_hour_utc": self.utc_target_hour,
            "interval_seconds": self.interval_seconds,
            "running": self._thread is not None and self._thread.is_alive(),
            "last_attempt": self._last_attempt,
            "last_attempt_status": self._last_attempt_status,
        }


def get_scheduler(db_path: str = "data/alpha.db") -> DailyScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = DailyScheduler(db_path=db_path)
    return _scheduler_instance
