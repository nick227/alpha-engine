"""/health reports the persistent pipeline status and why a run would be blocked."""
from __future__ import annotations

import json

import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")


def _health(monkeypatch: pytest.MonkeyPatch, db_path: str) -> dict:
    monkeypatch.setenv("ALPHA_DB_PATH", db_path)
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    monkeypatch.delenv("ALPHA_PIPELINE_STATUS_PATH", raising=False)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        res = client.get("/health")
    assert res.status_code == 200
    return res.json()


def test_no_run_recorded_and_disabled_is_explicit(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ALPHA_PIPELINE_ENABLED", raising=False)
    body = _health(monkeypatch, str(tmp_path / "alpha.db"))
    assert body["pipeline_last_status"] == "NO_RUN_RECORDED blocked=disabled"
    assert body["pipeline_gate"]["would_run"] is False
    assert body["pipeline_gate"]["blocked_reason"] == "disabled"


def test_storage_guard_is_reported(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALPHA_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("ALPHA_PIPELINE_MIN_FREE_MB", str(10**9))
    body = _health(monkeypatch, str(tmp_path / "alpha.db"))
    assert body["pipeline_last_status"] == "NO_RUN_RECORDED blocked=insufficient_storage"
    assert body["pipeline_gate"]["min_free_mb"] == 10**9


def test_recorded_run_is_reported_from_the_volume_only(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALPHA_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("ALPHA_PIPELINE_MIN_FREE_MB", "1")
    (tmp_path / "pipeline-status.json").write_text(
        json.dumps({"state": "succeeded", "finished_at": "2026-11-21T22:00:00+00:00", "exit_code": 0}), encoding="utf-8"
    )
    body = _health(monkeypatch, str(tmp_path / "alpha.db"))
    assert body["pipeline_last_status"] == "SUCCEEDED at=2026-11-21T22:00:00+00:00 exit_code=0"
    assert body["pipeline_gate"] == {**body["pipeline_gate"], "would_run": True, "blocked_reason": None}
