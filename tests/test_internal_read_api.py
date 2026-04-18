from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")


@pytest.fixture
def alpha_db_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPHA_DB_PATH", ":memory:")


@contextmanager
def _client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    insecure: bool,
    key: str | None,
) -> Iterator[TestClient]:
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    if insecure:
        monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    else:
        monkeypatch.delenv("INTERNAL_READ_INSECURE", raising=False)
    if key is not None:
        monkeypatch.setenv("INTERNAL_READ_KEY", key)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        yield client


def test_health_ok(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=True, key=None) as client:
        res = client.get("/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["db_path"] == ":memory:"


def test_protected_without_key_returns_503(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=False, key=None) as client:
        res = client.get("/ranking/top")
    assert res.status_code == 503
    assert "error" in res.json()


def test_protected_wrong_key_401(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=False, key="expected-secret") as client:
        res = client.get("/ranking/top", headers={"X-Internal-Key": "wrong"})
    assert res.status_code == 401


def test_protected_correct_key(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=False, key="expected-secret") as client:
        res = client.get("/ranking/top", headers={"X-Internal-Key": "expected-secret"})
    assert res.status_code == 200
    data = res.json()
    assert data["as_of"] is None
    assert "as_of_note" in data
    assert data["rankings"] == []


def test_insecure_bypasses_key(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=True, key=None) as client:
        res = client.get("/admission/changes")
    assert res.status_code == 200


def test_openapi_bypasses_auth_when_key_missing(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=False, key=None) as client:
        res = client.get("/openapi.json")
    assert res.status_code == 200


def test_ticker_performance_invalid_window(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=True, key=None) as client:
        res = client.get("/ticker/FOO/performance", params={"window": "5d"})
    assert res.status_code == 400


def test_ticker_why_not_found(alpha_db_memory: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch, insecure=True, key=None) as client:
        res = client.get("/ticker/UNKNOWN/why")
    assert res.status_code == 404
