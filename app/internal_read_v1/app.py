"""
FastAPI shell: thin adapter over DashboardService / explainability read models.

Bind 127.0.0.1 only. Set INTERNAL_READ_KEY; optional INTERNAL_READ_INSECURE=1 for local dev without key.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.ui.middle.dashboard_service import DashboardService, RankingView

_ENV_INSECURE = "INTERNAL_READ_INSECURE"
_ENV_KEY = "INTERNAL_READ_KEY"
_ENV_DB = "ALPHA_DB_PATH"


def _svc(request: Request) -> DashboardService:
    return request.app.state.service


def _ranking_row(r: RankingView) -> dict[str, Any]:
    return {
        "ticker": r.ticker,
        "score": r.score,
        "conviction": r.conviction,
        "regime": r.regime,
        "timestamp": r.timestamp,
        "attribution": r.attribution,
    }


def _parse_window(window: str | None) -> str | None:
    if window is None:
        return "30d"
    w = str(window).strip().lower()
    if w in ("30d", "60d", "90d"):
        return w
    if w.endswith("d") and w[:-1].isdigit():
        cand = f"{int(w[:-1])}d"
        if cand in ("30d", "60d", "90d"):
            return cand
    return None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    db = os.environ.get(_ENV_DB, "data/alpha.db")
    svc = DashboardService(db_path=db)
    app.state.service = svc
    try:
        yield
    finally:
        svc.close()


app = FastAPI(
    title="Alpha Engine Internal Read API",
    version="1",
    lifespan=_lifespan,
)


class _InternalKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in ("/health", "/docs", "/openapi.json", "/redoc"):
            return await call_next(request)
        key = os.environ.get(_ENV_KEY, "").strip()
        if not key:
            if os.environ.get(_ENV_INSECURE) == "1":
                return await call_next(request)
            return JSONResponse(
                status_code=503,
                content={"error": f"Set {_ENV_KEY} (or {_ENV_INSECURE}=1 for local dev only)"},
            )
        incoming = request.headers.get("x-internal-key") or request.headers.get("X-Internal-Key")
        if incoming != key:
            return JSONResponse(status_code=401, content={"error": "unauthorized"})
        return await call_next(request)


app.add_middleware(_InternalKeyMiddleware)


@app.exception_handler(HTTPException)
async def _http_exc(_request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})


@app.get("/health")
def health(request: Request) -> dict[str, Any]:
    svc = _svc(request)
    try:
        svc.store.conn.execute("SELECT 1")
        ok = True
    except Exception:
        ok = False
    return {
        "status": "ok" if ok else "degraded",
        "db_path": str(os.environ.get(_ENV_DB, "data/alpha.db")),
    }


@app.get("/ranking/top")
def ranking_top(
    request: Request,
    limit: int = 50,
    tenant_id: str = "default",
) -> dict[str, Any]:
    """v1: latest snapshot only; historical as_of is not supported yet."""
    lim = max(1, min(500, int(limit)))
    rows = _svc(request).get_target_rankings(tenant_id=tenant_id, limit=lim)
    return {
        "tenant_id": tenant_id,
        "as_of": None,
        "as_of_note": "v1 returns latest ranking_snapshots batch only; as_of query not implemented",
        "rankings": [_ranking_row(r) for r in rows],
    }


@app.get("/ranking/movers")
def ranking_movers(request: Request, limit: int = 50, tenant_id: str = "default") -> dict[str, Any]:
    n = max(1, min(200, int(limit)))
    return _svc(request).get_explain_ranking_movers(tenant_id=tenant_id, top_n=n)


@app.get("/ticker/{symbol}/why")
def ticker_why(
    request: Request,
    symbol: str,
    limit: int = 10,
    tenant_id: str = "default",
) -> dict[str, Any]:
    lim = max(1, min(100, int(limit)))
    panel = _svc(request).get_explain_ticker_panel(tenant_id=tenant_id, ticker=symbol)
    preds = panel.get("recent_predictions") or []
    panel = {**panel, "recent_predictions": preds[:lim]}
    cq = panel.get("candidate_queue")
    if cq is None and not preds:
        raise HTTPException(status_code=404, detail="ticker not found")
    return panel


@app.get("/ticker/{symbol}/performance")
def ticker_performance(
    request: Request,
    symbol: str,
    window: str | None = None,
    tenant_id: str = "default",
) -> dict[str, Any]:
    wkey = _parse_window(window)
    if wkey is None and window is not None:
        raise HTTPException(status_code=400, detail="invalid window; use 30d, 60d, or 90d")
    perf = _svc(request).get_explain_per_ticker_performance(tenant_id=tenant_id, ticker=symbol)
    windows = perf.get("windows") or {}
    if wkey:
        block = windows.get(wkey)
        if block is None:
            block = {"by_strategy": [], "best_strategy": None, "worst_strategy": None}
        return {
            "ticker": perf.get("ticker"),
            "tenant_id": tenant_id,
            "window": wkey,
            **block,
        }
    return perf


@app.get("/admission/changes")
def admission_changes(request: Request, hours: int = 24, tenant_id: str = "default") -> dict[str, Any]:
    h = max(1, min(168, int(hours)))
    return _svc(request).get_explain_what_changed(tenant_id=tenant_id, hours=h)
