"""Single-writer pipeline control: preflight, flock, persistent status, API wiring."""
from __future__ import annotations

import threading
import time

import pytest
from starlette.testclient import TestClient

from app.jobs import pipeline_control as pc


@pytest.fixture
def db(tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.delenv("ALPHA_PIPELINE_LOCK_PATH", raising=False)
    monkeypatch.delenv("ALPHA_PIPELINE_STATUS_PATH", raising=False)
    monkeypatch.setenv("ALPHA_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("ALPHA_PIPELINE_MIN_FREE_MB", "1")
    return str(tmp_path / "alpha.db")


def test_disabled_by_default_is_skipped_and_recorded(db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHA_PIPELINE_ENABLED", raising=False)
    calls = []
    res = pc.start_guarded(db, trigger="test", background=False, runner=lambda db_path: calls.append(1) or 0)
    assert res["status"] == "skipped" and res["reason"] == "disabled"
    assert calls == []
    assert pc.read_status(db)["last_skip"]["reason"] == "disabled"


def test_insufficient_storage_blocks_run(db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPHA_PIPELINE_MIN_FREE_MB", str(10**9))
    calls = []
    res = pc.start_guarded(db, trigger="test", background=False, runner=lambda db_path: calls.append(1) or 0)
    assert res["reason"] == "insufficient_storage"
    assert res["free_mb"] < res["min_free_mb"]
    assert calls == []


def test_success_persists_status_and_releases_lock(db: str) -> None:
    res = pc.start_guarded(db, trigger="test", background=False, runner=lambda db_path: 0)
    assert res["status"] == "succeeded"
    s = pc.read_status(db)
    assert s["state"] == "succeeded" and s["exit_code"] == 0 and s["trigger"] == "test"
    assert s["finished_at"] >= s["started_at"]
    assert pc.is_locked(db) is False
    assert pc.sentinel_text(db).startswith("SUCCEEDED at=")


def test_failure_and_crash_are_recorded_and_unlock(db: str) -> None:
    assert pc.start_guarded(db, trigger="t", background=False, runner=lambda db_path: 2)["status"] == "failed"
    assert pc.read_status(db)["exit_code"] == 2

    def boom(db_path: str) -> int:
        raise RuntimeError("step exploded")

    pc.start_guarded(db, trigger="t", background=False, runner=boom)
    s = pc.read_status(db)
    assert s["state"] == "failed" and "step exploded" in s["error"]
    assert pc.is_locked(db) is False


def test_concurrent_trigger_is_skipped_without_overlap(db: str) -> None:
    """While one run holds the lock, a second trigger is skipped and never runs."""
    gate = threading.Event()
    active = []
    max_active = []

    def slow(db_path: str) -> int:
        active.append(1)
        max_active.append(len(active))
        gate.wait(5)
        active.pop()
        return 0

    first = pc.start_guarded(db, trigger="first", background=True, runner=slow)
    assert first["status"] == "started"
    deadline = time.time() + 5
    while not active and time.time() < deadline:
        time.sleep(0.01)
    assert pc.read_status(db)["state"] == "running"

    second = pc.start_guarded(db, trigger="second", background=False, runner=slow)
    assert second == {"status": "skipped", "reason": "lock_held"}
    # The running record is not clobbered by the skipped attempt.
    s = pc.read_status(db)
    assert s["state"] == "running" and s["trigger"] == "first" and s["last_skip"]["reason"] == "lock_held"

    gate.set()
    deadline = time.time() + 5
    while pc.read_status(db).get("state") == "running" and time.time() < deadline:
        time.sleep(0.01)
    assert pc.read_status(db)["state"] == "succeeded"
    assert max(max_active) == 1
    assert pc.is_locked(db) is False


def test_status_persists_across_module_reload(db: str) -> None:
    import importlib

    pc.start_guarded(db, trigger="t", background=False, runner=lambda db_path: 0)
    fresh = importlib.reload(pc)
    assert fresh.read_status(db)["state"] == "succeeded"


def _client(monkeypatch: pytest.MonkeyPatch, db: str, key: str = "k" * 64) -> TestClient:
    monkeypatch.setenv("ALPHA_DB_PATH", db)
    monkeypatch.delenv("INTERNAL_READ_INSECURE", raising=False)
    monkeypatch.setenv("INTERNAL_READ_KEY", key)
    from app.internal_read_v1.app import app

    return TestClient(app)


def test_api_requires_key_and_reports_status(db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHA_PIPELINE_ENABLED", raising=False)
    with _client(monkeypatch, db) as client:
        assert client.post("/api/pipeline/run").status_code == 401
        assert client.get("/api/pipeline/status").status_code == 401
        h = {"X-Internal-Key": "k" * 64}
        res = client.post("/api/pipeline/run?trigger=cron", headers=h)
        assert res.status_code == 200 and res.json()["reason"] == "disabled"
        st = client.get("/api/pipeline/status", headers=h).json()
        assert st["would_run"] is False and st["blocked_reason"] == "disabled"
        assert st["locked"] is False and st["last_run"]["last_skip"]["trigger"] == "cron"
