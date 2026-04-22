"""Public JSON routes under /api for chart and market reads."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request

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
    parse_best_preference,
    parse_mode,
)
from app.internal_read_v1.regime_read import build_regime_payload
from app.ui.middle.dashboard_service import DashboardService

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
    n = max(1, min(100, int(limit)))
    rows = get_recommendations_latest(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        mode=mode_key,
        preference=pref_key,
        limit=n,
    )
    return {"tenant_id": tenant_id, "mode": mode_key, "selectionPreference": pref_key, "recommendations": rows}


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
    row = get_recommendation_best(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        mode=mode_key,
        preference=pref_key,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="no recommendation available")
    return row


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
        raise HTTPException(status_code=404, detail="no recommendation for ticker")
    return row
