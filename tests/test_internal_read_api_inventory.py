"""Milestone-1 guardrail tests for inventory-driven internal read API checks."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

pytest.importorskip("httpx")

sys.path.append(str(Path(__file__).resolve().parent))

from internal_read_inventory.endpoints import ENDPOINTS, ENDPOINTS_BY_ID, REGISTERED_GET_PATHS

_EXCLUDED_PATHS = {"/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"}


def _runtime_get_paths() -> set[str]:
    try:
        from app.internal_read_v1.app import app
    except ImportError as exc:
        pytest.skip(f"FastAPI app import unavailable in current environment: {exc}")

    # FastAPI >= 0.13x keeps included routers as nested nodes in app.routes; the OpenAPI
    # schema is the stable, public view of every routable path (none opt out of the schema).
    paths: set[str] = {
        path for path, ops in app.openapi().get("paths", {}).items()
        if "get" in ops and path not in _EXCLUDED_PATHS
    }
    return paths


def test_inventory_ids_are_unique() -> None:
    assert len(ENDPOINTS) == len(ENDPOINTS_BY_ID)


def test_route_inventory_matches_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPHA_DB_PATH", ":memory:")
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)

    runtime_paths = _runtime_get_paths()
    missing = runtime_paths - REGISTERED_GET_PATHS
    extra = REGISTERED_GET_PATHS - runtime_paths
    assert not missing and not extra, (
        "Route inventory drift detected. "
        f"missing_from_registry={sorted(missing)}; stale_registry_paths={sorted(extra)}"
    )

