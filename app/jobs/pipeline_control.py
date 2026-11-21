"""Single-writer control for the daily pipeline.

- Lock: an advisory ``flock`` on a file next to the database (persistent volume).
  The kernel releases it if the process dies, so a crash never leaves a stale lock.
- Status: JSON next to the database, written atomically, so the last run's outcome
  survives restarts and redeploys.
- Preflight (fail-closed): runs only when ALPHA_PIPELINE_ENABLED=true and the
  database volume has at least ALPHA_PIPELINE_MIN_FREE_MB free.
"""
from __future__ import annotations

import fcntl
import json
import os
import shutil
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

DEFAULT_MIN_FREE_MB = 3072


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _data_dir(db_path: str) -> Path:
    return Path(db_path).resolve().parent


def lock_path(db_path: str) -> Path:
    return Path(os.environ.get("ALPHA_PIPELINE_LOCK_PATH") or _data_dir(db_path) / "pipeline.lock")


def status_path(db_path: str) -> Path:
    return Path(os.environ.get("ALPHA_PIPELINE_STATUS_PATH") or _data_dir(db_path) / "pipeline-status.json")


def read_status(db_path: str) -> dict[str, Any] | None:
    p = status_path(db_path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_status(db_path: str, record: dict[str, Any]) -> None:
    p = status_path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def _record_skip(db_path: str, reason: str, trigger: str, **extra: Any) -> dict[str, Any]:
    """Records a skipped attempt without overwriting the last real run."""
    prev = read_status(db_path) or {}
    prev["last_skip"] = {"reason": reason, "at": _now(), "trigger": trigger, **extra}
    _write_status(db_path, prev)
    return {"status": "skipped", "reason": reason, **extra}


def free_mb(db_path: str) -> int:
    return int(shutil.disk_usage(_data_dir(db_path)).free / (1024 * 1024))


def preflight(db_path: str) -> tuple[bool, str | None, dict[str, Any]]:
    min_free = int(os.environ.get("ALPHA_PIPELINE_MIN_FREE_MB") or DEFAULT_MIN_FREE_MB)
    info = {"free_mb": free_mb(db_path), "min_free_mb": min_free}
    if os.environ.get("ALPHA_PIPELINE_ENABLED", "").strip().lower() != "true":
        return False, "disabled", info
    if info["free_mb"] < min_free:
        return False, "insufficient_storage", info
    return True, None, info


def try_lock(db_path: str):
    """Returns an open, exclusively locked file object, or None if another run holds it."""
    p = lock_path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fh = open(p, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    fh.seek(0)
    fh.truncate()
    fh.write(json.dumps({"pid": os.getpid(), "host": socket.gethostname(), "locked_at": _now()}))
    fh.flush()
    return fh


def is_locked(db_path: str) -> bool:
    fh = try_lock(db_path)
    if fh is None:
        return True
    _release(fh)
    return False


def _release(fh) -> None:
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        fh.close()


def _execute(fh, db_path: str, trigger: str, runner: Callable[..., int]) -> int:
    started = _now()
    base = {"state": "running", "started_at": started, "trigger": trigger, "host": socket.gethostname(), "pid": os.getpid(), "db_path": db_path}
    _write_status(db_path, base)
    try:
        rc = runner(db_path=db_path)
        _write_status(db_path, {**base, "state": "succeeded" if rc == 0 else "failed", "exit_code": rc, "finished_at": _now()})
        return rc
    except Exception as exc:  # noqa: BLE001 — record any crash as a failed run
        _write_status(db_path, {**base, "state": "failed", "exit_code": None, "error": f"{type(exc).__name__}: {exc}"[:500], "finished_at": _now()})
        return 1
    finally:
        _release(fh)


def start_guarded(db_path: str, *, trigger: str, background: bool, runner: Callable[..., int] | None = None) -> dict[str, Any]:
    """Preflight, take the lock, then run (in a thread when background=True)."""
    if runner is None:
        from app.jobs.daily_pipeline_runner import run_daily_pipeline as runner  # noqa: PLW0127
    ok, reason, info = preflight(db_path)
    if not ok:
        return _record_skip(db_path, reason, trigger, **info)
    fh = try_lock(db_path)
    if fh is None:
        return _record_skip(db_path, "lock_held", trigger)
    if background:
        threading.Thread(target=_execute, args=(fh, db_path, trigger, runner), daemon=True, name="daily_pipeline_worker").start()
        return {"status": "started", "mode": "background", **info}
    rc = _execute(fh, db_path, trigger, runner)
    return {"status": "succeeded" if rc == 0 else "failed", "exit_code": rc, **info}


def summary(db_path: str) -> dict[str, Any]:
    ok, reason, info = preflight(db_path)
    return {"last_run": read_status(db_path), "locked": is_locked(db_path), "would_run": ok, "blocked_reason": reason, **info}


def sentinel_text(db_path: str) -> str | None:
    """One-line status for health endpoints, from the persistent status file."""
    s = read_status(db_path)
    if not s or "state" not in s:
        return None
    when = s.get("finished_at") or s.get("started_at")
    return f"{s['state'].upper()} at={when} exit_code={s.get('exit_code')}"
