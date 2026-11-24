"""Public JSON routes under /api for chart and market reads."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request

from app.core.pipeline_gates import (
    compute_pipeline_signals,
    product_labels_for_tier,
    recommendation_quality,
)
from app.internal_read_v1.bars_chart import (
    build_candles_payload,
    build_company_payload,
    build_history_payload,
    build_quote_payload,
    build_stats_payload,
    normalize_ticker,
)
from app.internal_read_v1.chart_query_dep import ChartQueryParams, chart_range_interval
from app.internal_read_v1.recommendations import (
    get_recommendation_best,
    get_recommendation_for_ticker,
    get_recommendations_latest,
    get_recommendations_under_price,
    parse_best_preference,
    parse_mode,
)
from app.internal_read_v1.regime_read import build_regime_payload
from app.internal_read_v1.data_health_read import build_data_health_compact
from app.internal_read_v1.intelligence_read import (
    get_consensus_signals,
    get_engine_calendar,
    get_prediction_context,
    get_prediction_run_latest,
    get_regime_performance,
    get_strategy_performance,
    get_strategy_stability,
    get_system_heartbeat,
    get_ticker_accuracy,
    get_ticker_attribution,
    list_strategies_catalog,
)
from app.internal_read_v1.prediction_transparency import prediction_scoreboard, prediction_evidence
from app.internal_read_v1.pairwise_benchmark import compare_prediction_strategies
from app.internal_read_v1.research_trace import build_prediction_trace
from app.services.dashboard_service import DashboardService

router = APIRouter(prefix="/api", tags=["api"])


def _svc(request: Request) -> DashboardService:
    return request.app.state.service


@router.get("/tickers")
def api_tickers(
    request: Request,
    tenant_id: str = "default",
    q: str | None = None,
) -> dict[str, Any]:
    tickers = _svc(request).list_tickers(tenant_id=tenant_id)
    if q is not None and str(q).strip():
        needle = str(q).strip().lower()
        tickers = [t for t in tickers if needle in str(t).lower()]
    return {"tenant_id": tenant_id, "tickers": tickers}


@router.get("/quote/{ticker}")
def api_quote(request: Request, ticker: str, tenant_id: str = "default") -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    q = build_quote_payload(_svc(request).store.conn, tenant_id=tenant_id, ticker=sym)
    if q is None:
        raise HTTPException(status_code=404, detail="no price data for ticker")
    return q


@router.get("/history/{ticker}")
def api_history(
    request: Request,
    ticker: str,
    chart: Annotated[ChartQueryParams, Depends(chart_range_interval)],
    tenant_id: str = "default",
) -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    return build_history_payload(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        ticker=sym,
        range_key=chart.range_key,
        interval_key=chart.interval_key,
        now=datetime.now(),
    )


@router.get("/company/{ticker}")
def api_company(request: Request, ticker: str, tenant_id: str = "default") -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    return build_company_payload(_svc(request).store.conn, tenant_id=tenant_id, ticker=sym)


@router.get("/stats/{ticker}")
def api_stats(request: Request, ticker: str, tenant_id: str = "default") -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    out = build_stats_payload(_svc(request).store.conn, tenant_id=tenant_id, ticker=sym)
    if out is None:
        raise HTTPException(status_code=404, detail="no price data for ticker")
    return out


@router.get("/candles/{ticker}")
def api_candles(
    request: Request,
    ticker: str,
    chart: Annotated[ChartQueryParams, Depends(chart_range_interval)],
    tenant_id: str = "default",
) -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    return build_candles_payload(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        ticker=sym,
        range_key=chart.range_key,
        interval_key=chart.interval_key,
        now=datetime.now(),
    )


@router.get("/regime/{ticker}")
def api_regime(request: Request, ticker: str, tenant_id: str = "default") -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    out = build_regime_payload(_svc(request).store.conn, tenant_id=tenant_id, ticker=sym)
    if out is None:
        raise HTTPException(status_code=422, detail="insufficient_history")
    return out


@router.get("/regime/{ticker}/history")
def api_regime_history(request: Request, ticker: str, start: str, end: str) -> dict[str, Any]:
    """Point-in-time regime for each session in [start, end] from clean session bars (backtests).

    Same classification as /api/regime/{ticker}; each entry uses only bars up to its session.
    """
    from datetime import date as _date

    from app.internal_read_v1.regime_read import MIN_DAILY_BARS, regime_series
    from app.research_publication.live_universe import live_universe
    from app.research_publication.service import load_bars

    t = normalize_ticker(ticker)
    if t not in live_universe():
        raise HTTPException(status_code=404, detail="ticker not in live universe")
    try:
        s, e = _date.fromisoformat(start), _date.fromisoformat(end)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="start/end must be YYYY-MM-DD") from exc
    if s > e or (e - s).days > 366 * 6:
        raise HTTPException(status_code=422, detail="range must be ascending and at most 6 years")
    bars = load_bars(_svc(request).store.conn, [t], 1600 + MIN_DAILY_BARS, until=e.isoformat())[t]
    series = regime_series([b[0] for b in bars], [b[4] for b in bars])
    return {"ticker": t, "source": "alpha-engine price_bars (session bars)", "points": [p for p in series if p["asOf"] >= s.isoformat()]}


@router.get("/strategies/catalog")
def api_strategies_catalog(
    request: Request,
    tenant_id: str = "default",
    status: str | None = None,
    track: str | None = None,
    active_only: bool = True,
    limit: int = 100,
) -> dict[str, Any]:
    n = max(1, min(500, limit))
    return list_strategies_catalog(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        status=status,
        track=track,
        active_only=active_only,
        limit=n,
    )


@router.get("/strategies/{strategy_id}/stability")
def api_strategy_stability(request: Request, strategy_id: str, tenant_id: str = "default") -> dict[str, Any]:
    out = get_strategy_stability(_svc(request).store.conn, tenant_id=tenant_id, strategy_id=strategy_id)
    if out is None:
        raise HTTPException(status_code=404, detail="strategy stability not found")
    return out


@router.get("/strategies/{strategy_id}/performance")
def api_strategy_performance(request: Request, strategy_id: str, tenant_id: str = "default") -> dict[str, Any]:
    out = get_strategy_performance(_svc(request).store.conn, tenant_id=tenant_id, strategy_id=strategy_id)
    if out is None:
        raise HTTPException(status_code=404, detail="strategy not found")
    return out


@router.get("/performance/regime")
def api_performance_regime(request: Request, tenant_id: str = "default") -> dict[str, Any]:
    return get_regime_performance(_svc(request).store.conn, tenant_id=tenant_id)


@router.get("/experiments/leaderboard")
def api_experiments_leaderboard(
    request: Request,
    tenant_id: str = "default",
    horizon: str = "5d",
    limit: int = 50,
) -> dict[str, Any]:
    n = max(1, min(500, limit))
    hz = str(horizon).strip().lower()
    if hz not in {"5d", "20d"}:
        raise HTTPException(status_code=400, detail="invalid horizon; use 5d or 20d")
    rows = _svc(request).get_experiment_leaderboard(
        tenant_id=tenant_id,
        horizon=hz,
        limit=n,
    )
    return {
        "tenant_id": tenant_id,
        "horizon": hz,
        "rows": rows,
    }


@router.get("/experiments/trends")
def api_experiments_trends(
    request: Request,
    tenant_id: str = "default",
    horizon: str = "5d",
    class_key: str | None = None,
    experiment_key: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    n = max(1, min(1000, limit))
    hz = str(horizon).strip().lower()
    if hz not in {"5d", "20d"}:
        raise HTTPException(status_code=400, detail="invalid horizon; use 5d or 20d")
    rows = _svc(request).get_experiment_trends(
        tenant_id=tenant_id,
        horizon=hz,
        class_key=class_key,
        experiment_key=experiment_key,
        limit=n,
    )
    return {
        "tenant_id": tenant_id,
        "horizon": hz,
        "class_key": class_key,
        "experiment_key": experiment_key,
        "rows": rows,
    }


@router.get("/experiments/summary")
def api_experiments_summary(
    request: Request,
    tenant_id: str = "default",
    horizon: str = "5d",
    lookback_days: int = 14,
    limit: int = 200,
) -> dict[str, Any]:
    hz = str(horizon).strip().lower()
    if hz not in {"5d", "20d"}:
        raise HTTPException(status_code=400, detail="invalid horizon; use 5d or 20d")
    if lookback_days <= 0:
        raise HTTPException(status_code=400, detail="lookback_days must be > 0")
    n = max(1, min(1000, limit))
    lb = max(1, min(365, int(lookback_days)))
    return _svc(request).get_experiment_summary(
        tenant_id=tenant_id,
        horizon=hz,
        lookback_days=lb,
        limit=n,
    )


@router.get("/experiments/meta-ranker/latest")
def api_experiments_meta_ranker_latest(
    request: Request,
    tenant_id: str = "default",
    as_of_date: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    n = max(1, min(2000, int(limit)))
    return _svc(request).get_meta_ranker_latest(
        tenant_id=tenant_id,
        as_of_date=as_of_date,
        limit=n,
    )


@router.get("/experiments/meta-ranker/intents/latest")
def api_experiments_meta_ranker_intents_latest(
    request: Request,
    tenant_id: str = "default",
    as_of_date: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    n = max(1, min(2000, int(limit)))
    return _svc(request).get_meta_ranker_intents_latest(
        tenant_id=tenant_id,
        as_of_date=as_of_date,
        limit=n,
    )


@router.get("/experiments/meta-ranker/intents/replay")
def api_experiments_meta_ranker_intents_replay(
    request: Request,
    tenant_id: str = "default",
    as_of_date: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    n = max(1, min(2000, int(limit)))
    return _svc(request).get_meta_ranker_intent_replay(
        tenant_id=tenant_id,
        as_of_date=as_of_date,
        limit=n,
    )


@router.get("/experiments/meta-ranker/promotion-readiness")
def api_experiments_meta_ranker_promotion_readiness(
    request: Request,
    tenant_id: str = "default",
    experiment_key: str = "ml_meta_ranker_v1",
) -> dict[str, Any]:
    return _svc(request).get_meta_ranker_promotion_readiness(
        tenant_id=tenant_id,
        experiment_key=experiment_key,
    )


@router.get("/experiments/meta-ranker/alt-data/coverage")
def api_experiments_meta_ranker_alt_data_coverage(
    request: Request,
    tenant_id: str = "default",
    as_of_date: str | None = None,
    source: str | None = None,
    limit: int = 30,
) -> dict[str, Any]:
    n = max(1, min(365, int(limit)))
    return _svc(request).get_meta_ranker_alt_data_coverage(
        tenant_id=tenant_id,
        as_of_date=as_of_date,
        source=source,
        limit=n,
    )


@router.get("/experiments/meta-ranker/strategy-queue-share")
def api_experiments_meta_ranker_strategy_queue_share(
    request: Request,
    tenant_id: str = "default",
    as_of_date: str | None = None,
    status: str = "pending",
    limit: int = 50,
) -> dict[str, Any]:
    n = max(1, min(500, int(limit)))
    return _svc(request).get_meta_ranker_strategy_queue_share(
        tenant_id=tenant_id,
        as_of_date=as_of_date,
        status=status,
        limit=n,
    )


@router.get("/consensus/signals")
def api_consensus_signals(
    request: Request,
    tenant_id: str = "default",
    limit: int = 50,
    min_p_final: float | None = None,
    ticker: str | None = None,
) -> dict[str, Any]:
    n = max(1, min(500, limit))
    return get_consensus_signals(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        limit=n,
        min_p_final=min_p_final,
        ticker=ticker,
    )


@router.get("/ticker/{symbol}/attribution")
def api_ticker_attribution(
    request: Request, symbol: str, tenant_id: str = "default", limit: int = 20
) -> dict[str, Any]:
    n = max(1, min(200, limit))
    return get_ticker_attribution(_svc(request).store.conn, tenant_id=tenant_id, symbol=symbol, limit=n)


@router.get("/ticker/{symbol}/accuracy")
def api_ticker_accuracy(request: Request, symbol: str, tenant_id: str = "default") -> dict[str, Any]:
    return get_ticker_accuracy(_svc(request).store.conn, tenant_id=tenant_id, symbol=symbol)


@router.get("/system/heartbeat")
def api_system_heartbeat(request: Request, tenant_id: str = "default", limit: int = 200) -> dict[str, Any]:
    n = max(1, min(1000, limit))
    return get_system_heartbeat(_svc(request).store.conn, tenant_id=tenant_id, limit=n)


@router.get("/system/data-health")
def api_system_data_health(request: Request, tenant_id: str = "default") -> dict[str, Any]:
    """Single-glance warehouse + pipeline sentinel (for ops cards)."""
    return build_data_health_compact(_svc(request).store.conn, tenant_id=tenant_id)


@router.get("/predictions/runs/latest")
def api_prediction_runs_latest(
    request: Request, tenant_id: str = "default", timeframe: str | None = None
) -> dict[str, Any]:
    out = get_prediction_run_latest(_svc(request).store.conn, tenant_id=tenant_id, timeframe=timeframe)
    if out is None:
        raise HTTPException(status_code=404, detail="no prediction runs")
    return out


@router.get("/predictions/benchmarks/matched")
def api_matched_prediction_benchmark(
    request: Request, ml_strategy_id: str, rules_strategy_id: str,
    tenant_id: str = "default", horizon: str | None = None, limit: int = 10000
) -> dict[str, Any]:
    if not ml_strategy_id or not rules_strategy_id or ml_strategy_id == rules_strategy_id:
        raise HTTPException(status_code=422, detail="two different strategy IDs are required")
    return compare_prediction_strategies(
        _svc(request).store.conn, tenant_id=tenant_id,
        ml_strategy_id=ml_strategy_id, rules_strategy_id=rules_strategy_id,
        horizon=horizon, max_recent_rows=limit,
    )


@router.get("/predictions/scoreboard")
def api_prediction_scoreboard(request: Request, tenant_id: str = "default", ticker: str | None = None, horizon: str | None = None, limit: int = 5000) -> dict[str, Any]:
    return prediction_scoreboard(_svc(request).store.conn, tenant_id=tenant_id, ticker=ticker, horizon=horizon, limit=limit)


@router.get("/predictions/{prediction_id}/trace")
def api_prediction_trace(
    request: Request, prediction_id: str, tenant_id: str = "default"
) -> dict[str, Any]:
    """Versioned stored prediction evidence, for capture by a bot decision writer."""
    out = build_prediction_trace(
        _svc(request).store.conn, tenant_id=tenant_id, prediction_id=prediction_id
    )
    if out is None:
        raise HTTPException(status_code=404, detail="prediction not found")
    return out


@router.get("/predictions/{prediction_id}/evidence")
def api_prediction_evidence(request: Request, prediction_id: str, tenant_id: str = "default") -> dict[str, Any]:
    out = prediction_evidence(_svc(request).store.conn, tenant_id=tenant_id, prediction_id=prediction_id)
    if out is None:
        raise HTTPException(status_code=404, detail="prediction not found")
    return out


@router.get("/predictions/{prediction_id}/context")
def api_prediction_context(request: Request, prediction_id: str, tenant_id: str = "default") -> dict[str, Any]:
    out = get_prediction_context(_svc(request).store.conn, tenant_id=tenant_id, prediction_id=prediction_id)
    if out is None:
        raise HTTPException(status_code=404, detail="prediction not found")
    return out


@router.get("/engine/calendar")
def api_engine_calendar(
    request: Request,
    tenant_id: str = "default",
    month: str | None = None,
    limit: int = 50,
    distribution: str = "actual",
    min_days: int = 12,
) -> dict[str, Any]:
    dist = str(distribution).strip().lower()
    if dist not in {"actual", "uniform"}:
        raise HTTPException(status_code=400, detail="invalid distribution; use actual or uniform")
    current_month = datetime.utcnow().strftime("%Y-%m")
    requested_month = str(month).strip() if month is not None else current_month
    adjusted = requested_month != current_month
    out = get_engine_calendar(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        month=current_month,
        limit=limit,
        distribution=dist,
        min_days=min_days,
    )
    out["requestedMonth"] = requested_month
    out["servedMonth"] = current_month
    out["requestAdjustedToCurrentMonth"] = adjusted
    return out


def _recommendation_metadata(conn: Any, tenant_id: str) -> dict[str, Any]:
    quality = recommendation_quality(conn, tenant_id=tenant_id)
    tier = "full" if quality["actionable"] else "suppressed"
    return {
        "contractVersion": "2.0",
        "qualityStatus": quality,
        "suppressionReasons": quality["suppressionReasons"],
        "intelligenceConfidenceTier": tier,
        "pipelineSignals": quality["pipelineSignals"],
        "consumerExperience": {
            **product_labels_for_tier(tier),
            "message": "" if quality["actionable"] else "Research only; actionable recommendations unavailable.",
        },
    }


@router.get("/recommendations/latest")
def api_recommendations_latest(
    request: Request,
    limit: int = 10,
    mode: str = "balanced",
    preference: str = "absolute",
    tenant_id: str = "default",
) -> dict[str, Any]:
    try:
        mode_key = parse_mode(mode)
        pref_key = parse_best_preference(preference)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    n = max(1, min(100, limit))
    conn = _svc(request).store.conn
    rows = get_recommendations_latest(
        conn,
        tenant_id=tenant_id,
        mode=mode_key,
        preference=pref_key,
        limit=n,
    )
    return {
        "tenant_id": tenant_id, "mode": mode_key, "selectionPreference": pref_key,
        **_recommendation_metadata(conn, tenant_id), "recommendations": rows,
        "actionableCount": sum(r["actionable"] for r in rows),
    }


@router.get("/recommendations/best")
def api_recommendations_best(
    request: Request,
    mode: str = "balanced",
    preference: str = "absolute",
    tenant_id: str = "default",
) -> dict[str, Any]:
    try:
        mode_key = parse_mode(mode)
        pref_key = parse_best_preference(preference)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    conn = _svc(request).store.conn
    metadata = _recommendation_metadata(conn, tenant_id)
    if not metadata["qualityStatus"]["actionable"]:
        raise HTTPException(status_code=503, detail={
            "status": "limited_mode", "reason": "recommendations_suppressed",
            "retryHint": "Run pipeline", **metadata,
        })
    row = get_recommendation_best(
        conn,
        tenant_id=tenant_id,
        mode=mode_key,
        preference=pref_key,
    )
    if row is None:
        raise HTTPException(status_code=503, detail={
            **metadata, "status": "limited_mode", "reason": "no_eligible_recommendation",
            "suppressionReasons": ["NO_ELIGIBLE_RECOMMENDATION"],
            "qualityStatus": {**metadata["qualityStatus"], "status": "suppressed",
                              "actionable": False, "suppressionReasons": ["NO_ELIGIBLE_RECOMMENDATION"]},
            "intelligenceConfidenceTier": "suppressed",
            "consumerExperience": {**product_labels_for_tier("suppressed"),
                                   "message": "No eligible actionable recommendation."},
        })
    return {**metadata, **row}


@router.get("/recommendations/{ticker}")
def api_recommendations_ticker(
    request: Request,
    ticker: str,
    mode: str = "balanced",
    tenant_id: str = "default",
) -> dict[str, Any]:
    try:
        mode_key = parse_mode(mode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    sym = normalize_ticker(ticker)
    row = get_recommendation_for_ticker(
        _svc(request).store.conn, tenant_id=tenant_id, mode=mode_key, ticker=sym
    )
    if row is None:
        return {
            **_recommendation_metadata(_svc(request).store.conn, tenant_id),
            "found": False,
            "actionable": False,
            "ticker": sym,
            "tenant_id": tenant_id,
            "mode": mode_key,
            "message": "no recommendation for ticker",
        }
    return {
        **_recommendation_metadata(_svc(request).store.conn, tenant_id), "found": True, **row,
        "intelligenceConfidenceTier": "full" if row["actionable"] else "suppressed",
        "consumerExperience": {
            **product_labels_for_tier("full" if row["actionable"] else "suppressed"),
            "message": "" if row["actionable"] else "Research only; actionable recommendations unavailable.",
        },
    }


@router.get("/recommendations/under/{price_cap}")
def api_recommendations_under_price(
    request: Request,
    price_cap: float,
    limit: int = 10,
    mode: str = "balanced",
    preference: str = "long_only",
    tenant_id: str = "default",
) -> dict[str, Any]:
    if not math.isfinite(price_cap) or price_cap <= 0:
        raise HTTPException(status_code=400, detail="price_cap must be > 0")
    try:
        mode_key = parse_mode(mode)
        pref_key = parse_best_preference(preference)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    n = max(1, min(100, limit))
    rows = get_recommendations_under_price(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        mode=mode_key,
        price_cap=float(price_cap),
        preference=pref_key,
        limit=n,
    )
    return {
        "tenant_id": tenant_id,
        "mode": mode_key,
        "selectionPreference": pref_key,
        "priceCap": float(price_cap),
        **_recommendation_metadata(_svc(request).store.conn, tenant_id),
        "recommendations": rows,
        "actionableCount": sum(r["actionable"] for r in rows),
    }


@router.post("/pipeline/run")
def api_pipeline_run(request: Request, background: bool = True, trigger: str = "api") -> dict[str, Any]:
    """Trigger the daily pipeline: fail-closed preflight + single-writer flock on the data volume.

    Skips (and records why) when disabled, short on storage, or already running.
    """
    import os

    from app.jobs import pipeline_control

    db_file = os.environ.get("ALPHA_DB_PATH", "data/alpha.db")
    return pipeline_control.start_guarded(db_file, trigger=trigger[:40], background=background)


@router.get("/pipeline/status")
def api_pipeline_status(request: Request) -> dict[str, Any]:
    """Persistent last-run status, lock state, and whether a run would be allowed now."""
    import os

    from app.jobs import pipeline_control

    return pipeline_control.summary(os.environ.get("ALPHA_DB_PATH", "data/alpha.db"))


@router.get("/pipeline/scheduler")
def api_pipeline_scheduler(request: Request) -> dict[str, Any]:
    """Scheduler configuration and status."""
    import os

    from app.jobs.scheduler import get_scheduler

    db_file = os.environ.get("ALPHA_DB_PATH", "data/alpha.db")
    return get_scheduler(db_path=db_file).status()


# ── Research publication (P0: local-first research, Railway-published evidence) ──

def _db_file() -> str:
    import os

    return os.environ.get("ALPHA_DB_PATH", "data/alpha.db")


def _write_conn():
    import sqlite3

    return sqlite3.connect(_db_file(), timeout=30, isolation_level=None)


async def _verify_worker(request: Request) -> tuple[str, bytes]:
    from app.research_publication.contract import verify_request

    body = await request.body()
    path_qs = request.url.path + (f"?{request.url.query}" if request.url.query else "")
    ok, reason, key_id = verify_request(request.method, path_qs, {k.lower(): v for k, v in request.headers.items()}, body)
    if not ok:
        raise HTTPException(status_code=401, detail={"error": "unauthorized", "reason": reason})
    return str(key_id), body


@router.get("/sync/bars")
async def api_sync_bars(request: Request, tickers: str | None = None, sessions: int = 604) -> dict[str, Any]:
    """Full-window daily bars for the live universe (bar-sync/v1). Ed25519-signed workers only."""
    from app.research_publication.service import sync_payload

    await _verify_worker(request)
    if not 1 <= sessions <= 800:
        raise HTTPException(status_code=422, detail="sessions must be 1..800")
    wanted = [t.strip().upper() for t in tickers.split(",")] if tickers else None
    conn = _write_conn()
    try:
        return sync_payload(conn, wanted, sessions)
    finally:
        conn.close()


@router.post("/research/publications")
async def api_publish_research(request: Request):
    """Signed, immutable research publication. Railway re-derives every claim before storing."""
    from fastapi.responses import JSONResponse

    from app.research_publication.service import publish

    key_id, body = await _verify_worker(request)
    conn = _write_conn()
    try:
        status, out = publish(conn, body, key_id=key_id)
    finally:
        conn.close()
    return JSONResponse(status_code=status, content=out)


@router.get("/research/publications/{publication_id}")
def api_research_publication(request: Request, publication_id: str) -> dict[str, Any]:
    from app.research_publication.service import publication_record

    out = publication_record(_svc(request).store.conn, publication_id)
    if out is None:
        raise HTTPException(status_code=404, detail="publication not found")
    return out


@router.get("/research/latest")
def api_research_latest(
    request: Request, ticker: str, tenant_id: str = "default", mode: str = "live", session: str | None = None
) -> dict[str, Any]:
    """Bot locator: this ticker's prediction in the latest accepted publication, with run gates.
    mode=historical_replay (optionally &session=YYYY-MM-DD) selects labelled replay publications."""
    from app.research_publication.service import MODES, latest_for_ticker

    if tenant_id != "default" or mode not in MODES:
        raise HTTPException(status_code=422, detail="unsupported tenant or mode")
    return latest_for_ticker(_svc(request).store.conn, ticker, mode=mode, session=session)


@router.get("/market/daily-bars")
def api_market_daily_bars(request: Request, ticker: str, start: str, end: str) -> dict[str, Any]:
    """Clean daily session bars [date, open, high, low, close, volume] for a live-universe ticker
    over [start, end] (at most 6 years), with the bars-sync fingerprint. Session rows only — the
    same reader research uses (no intraday/mock rows, one canonical row per date). Prices are as
    stored: consumers must validate continuity (splits) themselves."""
    from datetime import date as _date

    from app.research_publication.contract import bars_fingerprint
    from app.research_publication.live_universe import live_universe
    from app.research_publication.service import load_bars

    t = str(ticker).strip().upper()
    if t not in live_universe():
        raise HTTPException(status_code=404, detail="ticker not in live universe")
    try:
        s, e = _date.fromisoformat(start), _date.fromisoformat(end)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="start/end must be YYYY-MM-DD") from exc
    if s > e or (e - s).days > 366 * 6:
        raise HTTPException(status_code=422, detail="range must be ascending and at most 6 years")
    bars = [b for b in load_bars(_svc(request).store.conn, [t], 1600, until=e.isoformat())[t] if b[0] >= s.isoformat()]
    return {"ticker": t, "timeframe": "1d", "start": start, "end": end, "bars": bars,
            "sha256": bars_fingerprint(bars), "source": "alpha-engine price_bars (session bars)"}


@router.get("/market/session-close")
def api_market_session_close(request: Request, ticker: str, session: str | None = None) -> dict[str, Any]:
    """Daily close of one session (latest by default) for a live-universe ticker.

    Used by TradeLoom's historical-replay simulator: fills are labelled replays of this
    session's close, never broker executions. Daily bars only (no intraday fallback).
    """
    from datetime import date as _date

    from app.research_publication.live_universe import live_universe

    t = str(ticker).strip().upper()
    if t not in live_universe():
        raise HTTPException(status_code=404, detail="ticker not in live universe")
    bound = "9999-12-31"
    if session:
        try:
            bound = _date.fromisoformat(session).isoformat()
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="session must be YYYY-MM-DD") from exc
    from app.research_publication.service import load_bars

    bars = load_bars(_svc(request).store.conn, [t], 21, until=bound)[t]
    if not bars:
        raise HTTPException(status_code=404, detail="no daily bar")
    last = bars[-1]
    prior = [b[5] for b in bars[:-1] if b[5] and b[5] > 0][-20:]
    return {
        "ticker": t, "session": last[0], "close": last[4], "volume": last[5] or None,
        # Average of the 20 sessions before this one (liquidity context for risk checks).
        "avgVolume20": round(sum(prior) / len(prior), 2) if prior else None,
        "timeframe": "1d", "source": "alpha-engine price_bars",
    }

