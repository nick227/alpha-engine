"""
FastAPI shell: thin adapter over DashboardService / explainability read models.

Bind 127.0.0.1 only. Set INTERNAL_READ_KEY; optional INTERNAL_READ_INSECURE=1 for local dev without key.
"""

from __future__ import annotations

import os
from statistics import mean
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.internal_read_v1.api_routes import router as api_router
from app.core.pipeline_gates import (
    LIMITED_EDGE_SCORE_MULTIPLIER,
    build_ranking_consumer_experience,
    compute_pipeline_signals,
    infer_ranking_provenance,
    intelligence_confidence_tier,
    ranking_snapshot_age_hours,
    should_suppress_rankings,
    should_suppress_stale_legacy_ranking,
)
from app.internal_read_v1.intelligence_read import get_prediction_run_latest
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


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fragility_score(
    request: Request,
    *,
    tenant_id: str,
    ticker: str,
    max_snapshots: int = 5,
) -> float:
    rows = _svc(request).store.conn.execute(
        """
        SELECT score
        FROM ranking_snapshots
        WHERE tenant_id = ? AND UPPER(TRIM(ticker)) = UPPER(TRIM(?))
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (tenant_id, ticker, int(max_snapshots)),
    ).fetchall()
    scores = [float(r["score"]) for r in rows if r["score"] is not None]
    if len(scores) < 2:
        return 1.0
    deltas = [abs(scores[i] - scores[i + 1]) for i in range(len(scores) - 1)]
    avg_abs_delta = mean(deltas)
    return round(max(0.0, min(1.0, avg_abs_delta / 0.25)), 4)


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
        if request.method == "OPTIONS":
            return await call_next(request)
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


app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Internal-Key"],
)

app.add_middleware(_InternalKeyMiddleware)


@app.exception_handler(HTTPException)
async def _http_exc(_request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail
    body: dict[str, Any] = detail if isinstance(detail, dict) else {"error": str(detail)}
    return JSONResponse(status_code=exc.status_code, content=body)


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
    maxFragility: float | None = None,
    tenant_id: str = "default",
    allow_legacy: bool = Query(
        False,
        description="When true, skip bar-coverage and legacy-snapshot staleness suppression (gates must be configured via env)",
    ),
) -> dict[str, Any]:
    """v1: latest snapshot only; historical as_of is not supported yet."""
    lim = max(1, min(500, int(limit)))
    if maxFragility is not None and not (0.0 <= float(maxFragility) <= 1.0):
        raise HTTPException(status_code=400, detail="invalid maxFragility; use 0.0 to 1.0")
    conn = _svc(request).store.conn
    signals = compute_pipeline_signals(conn, tenant_id=tenant_id)
    rows = _svc(request).get_target_rankings(tenant_id=tenant_id, limit=lim)
    run = get_prediction_run_latest(conn, tenant_id=tenant_id, timeframe=None)
    ranked_under_degraded = bool(run and str(run.get("runStatus")) in {"DEGRADED", "FAILED"})
    run_quality = float(run.get("runQuality")) if run and run.get("runQuality") is not None else 1.0
    pre_tier = intelligence_confidence_tier(signals, rankings_suppressed=False)
    confidence_mult = LIMITED_EDGE_SCORE_MULTIPLIER if pre_tier == "limited" else 1.0
    rankings = []
    for r in rows:
        base = _ranking_row(r)
        fragility = _fragility_score(request, tenant_id=tenant_id, ticker=r.ticker)
        if maxFragility is not None and fragility > float(maxFragility):
            continue
        score01 = max(0.0, min(1.0, (float(r.score) + 1.0) / 2.0))
        conviction01 = max(0.0, min(1.0, float(r.conviction)))
        edge_score = round(
            max(0.0, min(1.0, (0.50 * score01) + (0.30 * conviction01) + (0.20 * (1.0 - fragility))))
            * run_quality
            * confidence_mult,
            4,
        )
        rankings.append(
            {
                **base,
                "edgeScore": edge_score,
                "fragilityScore": fragility,
            }
        )
    rankings_after_filters = len(rankings)
    filtered_out_entirely = len(rows) > 0 and rankings_after_filters == 0 and maxFragility is not None

    prov_tickers = [str(x.ticker).strip().upper() for x in rows[:lim]]
    ranking_provenance = infer_ranking_provenance(
        conn, tenant_id=tenant_id, ranking_tickers=prov_tickers
    )
    snap_age_hours = ranking_snapshot_age_hours(conn, tenant_id=tenant_id)

    suppress_bar = should_suppress_rankings(signals.bar_coverage_ratio) and not allow_legacy
    suppress_legacy = should_suppress_stale_legacy_ranking(ranking_provenance, snap_age_hours) and not allow_legacy

    suppression_reasons: list[str] = []
    if suppress_bar:
        suppression_reasons.append("bar_coverage")
    if suppress_legacy:
        suppression_reasons.append("legacy_stale")

    if suppress_bar or suppress_legacy:
        rankings = []

    rankings_suppressed = suppress_bar or suppress_legacy
    final_tier = intelligence_confidence_tier(signals, rankings_suppressed=rankings_suppressed)

    consumer_experience = build_ranking_consumer_experience(
        final_tier,
        rankings_count=len(rankings),
        suppression_reasons=suppression_reasons,
        filtered_out_entirely=filtered_out_entirely,
    )

    pipe = signals.as_public_dict()
    pipe["rankingsSuppressed"] = rankings_suppressed
    pipe["suppressionReasons"] = suppression_reasons
    pipe["latestRankingSnapshotAgeHours"] = snap_age_hours
    pipe["suppressRankingsGateConfigured"] = bool(
        str(os.environ.get("PIPELINE_SUPPRESS_RANKINGS_BELOW", "")).strip()
    )
    pipe["legacySnapshotStalenessConfigured"] = bool(
        str(os.environ.get("PIPELINE_SUPPRESS_LEGACY_SNAPSHOT_OLDER_THAN_HOURS", "")).strip()
    )
    return {
        "tenant_id": tenant_id,
        "as_of": None,
        "as_of_note": "v1 returns latest ranking_snapshots batch only; as_of query not implemented",
        "rankedUnderDegradedRun": ranked_under_degraded,
        "runStatus": run.get("runStatus") if run else None,
        "runQuality": run_quality if run else None,
        "maxFragility": float(maxFragility) if maxFragility is not None else None,
        "rankingProvenance": ranking_provenance,
        "intelligenceConfidenceTier": final_tier,
        "consumerExperience": consumer_experience,
        "pipelineSignals": pipe,
        "rankings": rankings,
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
