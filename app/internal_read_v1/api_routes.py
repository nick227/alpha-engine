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
def api_pipeline_run(request: Request, background: bool = True) -> dict[str, Any]:
    """Trigger daily processing pipeline with single-writer lock protection."""
    import threading
    from app.jobs.daily_pipeline_runner import run_daily_pipeline
    db_file = os.environ.get("ALPHA_DB_PATH", "data/alpha.db")
    lock_path = Path("pipeline.lock")
    if lock_path.exists():
        try:
            lock_content = lock_path.read_text(encoding="utf-8").strip()
        except Exception:
            lock_content = "active"
        return {
            "status": "skipped",
            "reason": "lock_collision",
            "message": f"Pipeline is already running (lock created: {lock_content}).",
        }

    if background:
        thread = threading.Thread(
            target=run_daily_pipeline,
            kwargs={"db_path": db_file},
            daemon=True,
            name="daily_pipeline_worker",
        )
        thread.start()
        return {
            "status": "started",
            "mode": "background",
            "db_path": db_file,
            "message": "Daily pipeline started asynchronously with run lock protection.",
        }
    else:
        rc = run_daily_pipeline(db_path=db_file)
        return {
            "status": "complete" if rc == 0 else "failed",
            "exit_code": rc,
            "db_path": db_file,
        }
