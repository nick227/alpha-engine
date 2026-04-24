"""
FastAPI shell: thin adapter over DashboardService / explainability read models.

Bind 127.0.0.1 only. Set INTERNAL_READ_KEY; optional INTERNAL_READ_INSECURE=1 for local dev without key.
"""

from __future__ import annotations

import os
import sqlite3
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
from app.engine.prediction_rank_sqlite import (
    _discovery_strategy_keys,
)
from app.internal_read_v1.intelligence_read import get_prediction_run_latest
from app.services.dashboard_service import DashboardService, RankingView

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


def _factor_percentile(value_01: float) -> int:
    return int(round(max(0.0, min(1.0, float(value_01))) * 100.0))


def _trend_from_delta(delta: float, *, eps: float = 0.01) -> str:
    if delta > eps:
        return "improving"
    if delta < -eps:
        return "deteriorating"
    return "stable"


def _load_previous_snapshot_state(
    conn: Any,
    *,
    tenant_id: str,
    current_ts: str | None,
) -> tuple[dict[str, float], dict[str, int]]:
    if not current_ts:
        return {}, {}
    prev_row = conn.execute(
        """
        SELECT MAX(timestamp) AS ts
        FROM ranking_snapshots
        WHERE tenant_id = ? AND timestamp < ?
        """,
        (tenant_id, current_ts),
    ).fetchone()
    prev_ts = str(prev_row["ts"]) if prev_row and prev_row["ts"] is not None else None
    if not prev_ts:
        return {}, {}
    prev_rows = conn.execute(
        """
        SELECT UPPER(TRIM(ticker)) AS ticker, score
        FROM ranking_snapshots
        WHERE tenant_id = ? AND timestamp = ?
        ORDER BY score DESC
        """,
        (tenant_id, prev_ts),
    ).fetchall()
    score_map: dict[str, float] = {}
    rank_map: dict[str, int] = {}
    for idx, row in enumerate(prev_rows, start=1):
        ticker = str(row["ticker"]).strip().upper()
        score_map[ticker] = float(row["score"] or 0.0)
        rank_map[ticker] = idx
    return score_map, rank_map


def _ranking_factors(
    *,
    ticker: str,
    score: float,
    conviction: float,
    fragility: float,
    edge_score: float,
    rank: int,
    peer_count: int,
    attribution: dict[str, Any],
    run_quality: float,
    ranked_under_degraded: bool,
    previous_score: float | None,
    previous_rank: int | None,
) -> dict[str, Any]:
    score01 = max(0.0, min(1.0, (float(score) + 1.0) / 2.0))
    conviction01 = max(0.0, min(1.0, float(conviction)))
    stability01 = max(0.0, min(1.0, 1.0 - float(fragility)))
    rank_percentile = _factor_percentile(1.0 if peer_count <= 1 else (peer_count - rank) / float(peer_count - 1))
    drivers: list[dict[str, Any]] = [
        {
            "key": "ranking_score",
            "label": "Ranking Score",
            "value": round(float(score), 4),
            "percentile": _factor_percentile(score01),
            "direction": "positive" if score >= 0.0 else "negative",
            "weight": 0.5,
        },
        {
            "key": "conviction",
            "label": "Conviction",
            "value": round(conviction01, 4),
            "percentile": _factor_percentile(conviction01),
            "direction": "positive",
            "weight": 0.3,
        },
        {
            "key": "stability",
            "label": "Ranking Stability",
            "value": round(stability01, 4),
            "percentile": _factor_percentile(stability01),
            "direction": "positive",
            "weight": 0.2,
        },
    ]
    attr_items: list[tuple[str, float]] = []
    for k, v in (attribution or {}).items():
        try:
            attr_items.append((str(k), float(v)))
        except (TypeError, ValueError):
            continue
    if attr_items:
        denom = sum(abs(v) for _, v in attr_items) or 1.0
        for key, val in sorted(attr_items, key=lambda x: abs(x[1]), reverse=True)[:2]:
            rel = max(0.0, min(1.0, abs(val) / denom))
            drivers.append(
                {
                    "key": f"attribution_{key}",
                    "label": f"Attribution: {key}",
                    "value": round(val, 4),
                    "percentile": _factor_percentile(rel),
                    "direction": "positive" if val >= 0.0 else "negative",
                    "weight": round(rel, 4),
                }
            )
    risks: list[dict[str, Any]] = []
    if fragility >= 0.5:
        risks.append(
            {
                "key": "fragile_rank",
                "label": "Fragile Rank",
                "value": round(fragility, 4),
                "percentile": _factor_percentile(fragility),
                "direction": "negative",
            }
        )
    if score < 0.0:
        risks.append(
            {
                "key": "negative_bias",
                "label": "Negative Directional Bias",
                "value": round(abs(score), 4),
                "percentile": _factor_percentile(min(1.0, abs(score))),
                "direction": "negative",
            }
        )
    if ranked_under_degraded or run_quality < 0.95:
        risks.append(
            {
                "key": "pipeline_quality",
                "label": "Pipeline Quality",
                "value": round(run_quality, 4),
                "percentile": _factor_percentile(run_quality),
                "direction": "negative" if run_quality < 0.9 else "neutral",
            }
        )
    score_delta = None if previous_score is None else round(float(score) - float(previous_score), 4)
    rank_delta = None if previous_rank is None else int(previous_rank - rank)
    score_trend = "new" if score_delta is None else _trend_from_delta(score_delta, eps=0.01)
    rank_trend = "new" if rank_delta is None else _trend_from_delta(float(rank_delta), eps=0.5)
    changes = [
        {
            "key": "score_change",
            "label": "Score vs Previous Snapshot",
            "value": score_delta,
            "trend": score_trend,
        },
        {
            "key": "rank_change",
            "label": "Rank vs Previous Snapshot",
            "value": rank_delta,
            "trend": rank_trend,
        },
    ]
    return {
        "ticker": ticker,
        "rank": rank,
        "peerCount": peer_count,
        "rankPercentile": rank_percentile,
        "edgeScorePercentile": _factor_percentile(edge_score),
        "drivers": drivers,
        "risks": risks,
        "changes": changes,
    }


def _load_rank_subdrivers(
    conn: Any,
    *,
    tenant_id: str,
    ticker: str,
    snapshot_ts: str | None,
) -> dict[str, Any]:
    if not snapshot_ts:
        return {}
    row = conn.execute(
        """
        SELECT id, strategy_id, confidence, rank_score, ranking_context_json, feature_snapshot_json
        FROM predictions
        WHERE tenant_id = ?
          AND UPPER(TRIM(ticker)) = UPPER(TRIM(?))
          AND rank_score IS NOT NULL
          AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (tenant_id, ticker, snapshot_ts),
    ).fetchone()
    if not row:
        return {}
    prediction_id = str(row["id"]) if row["id"] is not None else None
    strategy_id = str(row["strategy_id"] or "")
    feature_json = str(row["feature_snapshot_json"] or "{}")
    raw_keys = _discovery_strategy_keys(strategy_id, feature_json)
    keys: list[str] = []
    for raw in raw_keys:
        k = str(raw).replace("\x00", "").strip()
        if not k:
            continue
        if len(k) > 128:
            k = k[:128]
        if k not in keys:
            keys.append(k)
    if not keys:
        keys = [strategy_id[:128]] if strategy_id else ["unknown"]

    # Local, defensive lookups (avoid cross-module helper errors propagating to /ranking/top).
    accuracy, avg_return = 0.5, 0.0
    stability = 0.5
    live_score = 0.0
    try:
        for sid in keys:
            for hz in ("ALL", "5d", "20d", "1d"):
                perf = conn.execute(
                    """
                    SELECT accuracy, avg_return
                    FROM strategy_performance
                    WHERE tenant_id = ? AND strategy_id = ? AND horizon = ?
                    """,
                    (tenant_id, sid, hz),
                ).fetchone()
                if perf:
                    accuracy = float(perf["accuracy"])
                    avg_return = float(perf["avg_return"])
                    raise StopIteration
    except StopIteration:
        pass
    except sqlite3.Error:
        pass
    try:
        for sid in keys:
            st = conn.execute(
                """
                SELECT stability_score
                FROM strategy_stability
                WHERE tenant_id = ? AND strategy_id = ?
                """,
                (tenant_id, sid),
            ).fetchone()
            if st:
                stability = float(st["stability_score"])
                break
    except sqlite3.Error:
        pass
    try:
        # Backward-compatible for older schemas.
        conn.execute("SELECT live_score FROM strategies LIMIT 1").fetchone()
        for sid in keys:
            ls = conn.execute(
                """
                SELECT live_score
                FROM strategies
                WHERE tenant_id = ? AND id = ?
                """,
                (tenant_id, sid),
            ).fetchone()
            if ls and ls["live_score"] is not None:
                live_score = max(0.0, min(1.0, float(ls["live_score"])))
                break
    except sqlite3.Error:
        pass
    confidence = max(0.0, min(1.0, float(row["confidence"] or 0.0)))
    avg_return_norm = max(0.0, min(1.0, (max(-0.05, min(0.05, float(avg_return))) + 0.05) / 0.10))
    ranking_ctx: dict[str, Any] = {}
    try:
        import json

        parsed = json.loads(str(row["ranking_context_json"] or "{}"))
        ranking_ctx = parsed if isinstance(parsed, dict) else {}
    except Exception:
        ranking_ctx = {}
    temporal_multiplier = float(ranking_ctx.get("temporal_multiplier") or 1.0)
    score_breakdown = {
        "confidence": 0.35,
        "accuracy": 0.2,
        "avgReturn": 0.2,
        "liveScore": 0.15,
        "stability": 0.1,
    }
    sub_drivers = {
        "modelAccuracy": round(float(accuracy), 4),
        "avgReturn": round(float(avg_return), 6),
        "avgReturnNormalized": round(avg_return_norm, 4),
        "liveScore": round(float(live_score), 4),
        "stabilityScore": round(float(stability), 4),
        "temporalMultiplier": round(temporal_multiplier, 6),
        "regimeFit": round(max(0.0, min(1.0, temporal_multiplier / 1.2)), 4),
        "rankScoreRaw": round(float(row["rank_score"] or 0.0), 6),
        "componentValues01": {
            "confidence": round(confidence, 4),
            "accuracy": round(max(0.0, min(1.0, float(accuracy))), 4),
            "avgReturn": round(avg_return_norm, 4),
            "liveScore": round(max(0.0, min(1.0, float(live_score))), 4),
            "stability": round(max(0.0, min(1.0, float(stability))), 4),
        },
    }
    return {
        "predictionId": prediction_id,
        "scoreBreakdown": score_breakdown,
        "subDrivers": sub_drivers,
    }


def _percentile_rank_01(values: list[float], value: float) -> int:
    if not values:
        return 0
    total = len(values)
    if total <= 1:
        return 100
    le_count = sum(1 for v in values if v <= value)
    return int(round(((le_count - 1) / float(total - 1)) * 100.0))


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _contextualize_rank_subdrivers(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    keys = ("confidence", "accuracy", "avgReturn", "liveScore", "stability")
    series: dict[str, list[float]] = {k: [] for k in keys}
    for row in rows:
        comp = (((row.get("subDrivers") or {}).get("componentValues01")) or {})
        for k in keys:
            try:
                series[k].append(float(comp.get(k, 0.0)))
            except (TypeError, ValueError):
                series[k].append(0.0)
    medians = {k: _median(v) for k, v in series.items()}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = str(row.get("ticker", "")).strip().upper()
        comp = (((row.get("subDrivers") or {}).get("componentValues01")) or {})
        weights = (row.get("scoreBreakdown") or {})
        component_context: list[dict[str, Any]] = []
        for key in keys:
            val = float(comp.get(key, 0.0))
            w = float(weights.get(key, 0.0))
            component_context.append(
                {
                    "key": key,
                    "weight": round(w, 4),
                    "value01": round(val, 4),
                    "weightedContribution": round(val * w, 4),
                    "percentile": _percentile_rank_01(series[key], val),
                    "vsPeerMedian": round(val - medians[key], 4),
                }
            )
        component_context.sort(key=lambda c: c["weightedContribution"], reverse=True)
        top_components = component_context[:3]
        why_prioritized = [
            {
                "key": c["key"],
                "reason": "high_contribution" if c["weightedContribution"] >= 0.12 else "supporting_contribution",
                "weightedContribution": c["weightedContribution"],
                "percentile": c["percentile"],
            }
            for c in top_components
        ]
        out[ticker] = {
            "componentContext": component_context,
            "whyPrioritized": why_prioritized,
        }
    return out


def _load_recent_snapshot_rank_maps(
    conn: Any,
    *,
    tenant_id: str,
    max_snapshots: int = 5,
) -> list[dict[str, Any]]:
    ts_rows = conn.execute(
        """
        SELECT DISTINCT timestamp
        FROM ranking_snapshots
        WHERE tenant_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (tenant_id, int(max_snapshots)),
    ).fetchall()
    timestamps = [str(r["timestamp"]) for r in ts_rows if r["timestamp"] is not None]
    out: list[dict[str, Any]] = []
    for ts in timestamps:
        rows = conn.execute(
            """
            SELECT UPPER(TRIM(ticker)) AS ticker, score
            FROM ranking_snapshots
            WHERE tenant_id = ? AND timestamp = ?
            ORDER BY score DESC
            """,
            (tenant_id, ts),
        ).fetchall()
        rank_map: dict[str, int] = {}
        score_map: dict[str, float] = {}
        for idx, row in enumerate(rows, start=1):
            ticker = str(row["ticker"]).strip().upper()
            rank_map[ticker] = idx
            score_map[ticker] = float(row["score"] or 0.0)
        out.append({"timestamp": ts, "rankMap": rank_map, "scoreMap": score_map, "peerCount": len(rows)})
    return out


def _rank_context_for_ticker(
    *,
    ticker: str,
    rank: int,
    score: float,
    fragility: float,
    edge_score: float,
    current_peer_count: int,
    snapshot_windows: list[dict[str, Any]],
    spread_vs_next: float = 0.0,
    regime_fit: float | None = None,
    top_n: int = 10,
) -> dict[str, Any]:
    t = str(ticker).strip().upper()
    if not snapshot_windows:
        return {
            "basis": [],
            "timing": [],
            "risks": [],
            "status": "steady",
            "horizon": "swing",
            "fit": "neutral",
            "durability": "unknown",
            "freshness": "unknown",
            "spread": 0.0,
            "pressure": "unknown",
            "trigger": "none",
            "invalidators": [],
            "history": [],
            "scope": {"window": 0, "cutoff": top_n, "peers": "ranking_peer_set"},
        }
    latest = snapshot_windows[0]
    latest_scores = list((latest.get("scoreMap") or {}).values())
    peer_median = _median(latest_scores)
    vs_peer_median = round(float(score) - float(peer_median), 4)
    rank_series: list[int | None] = []
    score_series: list[float | None] = []
    topn_hits = 0
    for snap in snapshot_windows:
        rk = (snap.get("rankMap") or {}).get(t)
        sc = (snap.get("scoreMap") or {}).get(t)
        rank_series.append(int(rk) if rk is not None else None)
        score_series.append(float(sc) if sc is not None else None)
        if rk is not None and int(rk) <= int(top_n):
            topn_hits += 1
    prev_rank = rank_series[1] if len(rank_series) > 1 else None
    prev_score = score_series[1] if len(score_series) > 1 else None
    rank_delta = None if prev_rank is None else int(prev_rank - rank)
    score_delta = None if prev_score is None else round(float(score) - float(prev_score), 4)
    basis: list[str] = []
    if vs_peer_median >= 0.1:
        basis.append(f"Score is above current peer median by {vs_peer_median:+.2f}")
    elif vs_peer_median >= 0:
        basis.append(f"Score is modestly above current peer median ({vs_peer_median:+.2f})")
    else:
        basis.append(f"Score is below peer median ({vs_peer_median:+.2f}) but retained by composite ranking")
    if topn_hits >= max(2, len(snapshot_windows) - 1):
        basis.append(f"Persisted in top {top_n} for {topn_hits}/{len(snapshot_windows)} recent snapshots")
    elif topn_hits > 0:
        basis.append(f"Appeared in top {top_n} for {topn_hits}/{len(snapshot_windows)} recent snapshots")
    else:
        basis.append(f"No top {top_n} persistence in last {len(snapshot_windows)} snapshots")
    timing: list[str] = []
    if rank_delta is not None:
        if rank_delta > 0:
            timing.append(f"Rank improved by {rank_delta} since previous snapshot")
        elif rank_delta < 0:
            timing.append(f"Rank declined by {abs(rank_delta)} since previous snapshot")
        else:
            timing.append("Rank is unchanged since previous snapshot")
    if score_delta is not None:
        if score_delta > 0.01:
            timing.append(f"Score increased by {score_delta:+.2f} snapshot-over-snapshot")
        elif score_delta < -0.01:
            timing.append(f"Score decreased by {score_delta:+.2f} snapshot-over-snapshot")
        else:
            timing.append("Score is stable snapshot-over-snapshot")
    invalidators = [
        f"Rank falls below {min(current_peer_count, max(15, rank + 5))}",
        f"Fragility exceeds {max(0.5, round(fragility + 0.15, 2)):.2f}",
        f"Edge score drops below {max(0.35, round(edge_score - 0.15, 2)):.2f}",
    ]
    fit = "strong" if (regime_fit is not None and regime_fit >= 0.75) else "neutral"
    if regime_fit is not None and regime_fit < 0.5:
        fit = "weak"
    if topn_hits >= max(2, len(snapshot_windows) - 1) and fragility <= 0.35:
        durability = "high"
    elif topn_hits >= max(1, len(snapshot_windows) // 2):
        durability = "medium"
    else:
        durability = "low"
    if rank_delta is not None and score_delta is not None and rank_delta > 0 and score_delta > 0:
        status = "rising"
    elif rank_delta is not None and rank_delta < 0:
        status = "weakening"
    else:
        status = "steady"
    if fragility >= 0.6 or (rank_delta is not None and rank_delta < 0 and score_delta is not None and score_delta < 0):
        pressure = "high"
    elif fragility >= 0.4:
        pressure = "medium"
    else:
        pressure = "low"
    if fragility <= 0.3 and topn_hits >= max(2, len(snapshot_windows) - 1):
        horizon = "long"
    elif fragility <= 0.5:
        horizon = "swing"
    else:
        horizon = "near"
    if rank_delta is not None and rank_delta > 0:
        trigger = "rank_up"
    elif score_delta is not None and score_delta > 0.01:
        trigger = "score_up"
    elif rank_delta is not None and rank_delta < 0:
        trigger = "rank_down"
    else:
        trigger = "hold"
    risks = [
        f"Fragility is {fragility:.2f}",
        f"Pressure is {pressure}",
        f"Spread vs next is {spread_vs_next:+.2f}",
    ]
    history = [int(v) for v in rank_series if v is not None][:5]
    return {
        "basis": basis[:3],
        "timing": timing[:3],
        "risks": risks[:3],
        "status": status,
        "horizon": horizon,
        "fit": fit,
        "durability": durability,
        "freshness": "unknown",
        "spread": round(spread_vs_next, 4),
        "pressure": pressure,
        "trigger": trigger,
        "invalidators": invalidators,
        "history": history,
        "scope": {
            "window": len(snapshot_windows),
            "cutoff": int(top_n),
            "median": round(peer_median, 4),
            "edge": vs_peer_median,
            "peers": "ranking_peer_set",
        },
    }


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
    current_ts = str(rows[0].timestamp) if rows else None
    prev_score_map, prev_rank_map = _load_previous_snapshot_state(
        conn, tenant_id=tenant_id, current_ts=current_ts
    )
    run = get_prediction_run_latest(conn, tenant_id=tenant_id, timeframe=None)
    ranked_under_degraded = bool(run and str(run.get("runStatus")) in {"DEGRADED", "FAILED"})
    run_quality = float(run.get("runQuality")) if run and run.get("runQuality") is not None else 1.0
    pre_tier = intelligence_confidence_tier(signals, rankings_suppressed=False)
    confidence_mult = LIMITED_EDGE_SCORE_MULTIPLIER if pre_tier == "limited" else 1.0
    rankings = []
    peer_count = len(rows)
    snapshot_windows = _load_recent_snapshot_rank_maps(conn, tenant_id=tenant_id, max_snapshots=5)
    pre_rankings: list[dict[str, Any]] = []
    for r in rows:
        base = _ranking_row(r)
        rank_idx = len(pre_rankings) + 1
        ticker_key = str(r.ticker).strip().upper()
        rank_subdrivers = _load_rank_subdrivers(
            conn,
            tenant_id=tenant_id,
            ticker=str(r.ticker),
            snapshot_ts=current_ts,
        )
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
        pre_rankings.append(
            {
                "base": base,
                "ticker": str(r.ticker),
                "rank": rank_idx,
                "peerCount": peer_count,
                "edgeScore": edge_score,
                "fragilityScore": fragility,
                "rankSubdrivers": rank_subdrivers,
                "factorData": _ranking_factors(
                    ticker=str(r.ticker),
                    score=float(r.score),
                    conviction=float(r.conviction),
                    fragility=fragility,
                    edge_score=edge_score,
                    rank=rank_idx,
                    peer_count=peer_count,
                    attribution=dict(r.attribution or {}),
                    run_quality=run_quality,
                    ranked_under_degraded=ranked_under_degraded,
                    previous_score=prev_score_map.get(ticker_key),
                    previous_rank=prev_rank_map.get(ticker_key),
                ),
            }
        )
    context_map = _contextualize_rank_subdrivers(
        [
            {
                "ticker": item["ticker"],
                **(item["rankSubdrivers"] or {}),
            }
            for item in pre_rankings
        ]
    )
    for item in pre_rankings:
        ticker_key = str(item["ticker"]).strip().upper()
        ctx = context_map.get(ticker_key, {"componentContext": [], "whyPrioritized": []})
        subdrivers = dict((item["rankSubdrivers"] or {}).get("subDrivers") or {})
        subdrivers["context"] = {
            "components": ctx["componentContext"],
            "whyPrioritized": ctx["whyPrioritized"],
        }
        rankings.append(
            {
                **item["base"],
                "rank": item["rank"],
                "peerCount": item["peerCount"],
                "factorVersion": "ranking_factors_v1",
                "rankingKind": "relative_priority",
                "notActionable": True,
                "edgeScore": item["edgeScore"],
                "fragilityScore": item["fragilityScore"],
                "scoreBreakdown": (item["rankSubdrivers"] or {}).get("scoreBreakdown") or {},
                "predictionId": (item["rankSubdrivers"] or {}).get("predictionId"),
                "subDrivers": subdrivers,
                "selectionRationale": {
                    "prioritizedBy": ctx["whyPrioritized"],
                    "relativeOrderBasis": "rank_score_adjusted_then_edge_score",
                },
                "rankContext": _rank_context_for_ticker(
                    ticker=item["ticker"],
                    rank=item["rank"],
                    score=float(item["base"].get("score", 0.0)),
                    fragility=float(item["fragilityScore"]),
                    edge_score=float(item["edgeScore"]),
                    current_peer_count=int(item["peerCount"]),
                    snapshot_windows=snapshot_windows,
                    spread_vs_next=0.0,
                    regime_fit=float(subdrivers.get("regimeFit")) if subdrivers.get("regimeFit") is not None else None,
                    top_n=10,
                ),
                **item["factorData"],
            }
        )
    score_by_rank = {int(r["rank"]): float(r["score"]) for r in rankings if "rank" in r and "score" in r}
    for row in rankings:
        rk = int(row.get("rank", 0))
        cur_score = float(row.get("score", 0.0))
        next_score = score_by_rank.get(rk + 1, cur_score)
        spread = round(cur_score - float(next_score), 4)
        rc = dict(row.get("rankContext") or {})
        rc["spread"] = spread
        risks = list(rc.get("risks") or [])
        if risks:
            risks[-1] = f"Spread vs next is {spread:+.2f}"
        else:
            risks = [f"Spread vs next is {spread:+.2f}"]
        rc["risks"] = risks[:3]
        row["rankContext"] = rc
    rankings_after_filters = len(rankings)
    filtered_out_entirely = len(rows) > 0 and rankings_after_filters == 0 and maxFragility is not None

    prov_tickers = [str(x.ticker).strip().upper() for x in rows[:lim]]
    ranking_provenance = infer_ranking_provenance(
        conn, tenant_id=tenant_id, ranking_tickers=prov_tickers
    )
    snap_age_hours = ranking_snapshot_age_hours(conn, tenant_id=tenant_id)
    for row in rankings:
        rc = dict(row.get("rankContext") or {})
        if snap_age_hours is None:
            rc["freshness"] = "unknown"
        elif snap_age_hours < 6:
            rc["freshness"] = "fresh"
        elif snap_age_hours < 24:
            rc["freshness"] = "recent"
        else:
            rc["freshness"] = "stale"
        row["rankContext"] = rc

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
        "rankingKind": "relative_priority",
        "notActionable": True,
        "rankedUnderDegradedRun": ranked_under_degraded,
        "runStatus": run.get("runStatus") if run else None,
        "runQuality": run_quality if run else None,
        "factorVersion": "ranking_factors_v1",
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
