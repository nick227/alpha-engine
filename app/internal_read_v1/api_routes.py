"""Public JSON routes under /api for chart and market reads."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.internal_read_v1.bars_chart import (
    build_candles_payload,
    build_company_payload,
    build_history_payload,
    build_quote_payload,
    build_stats_payload,
    normalize_ticker,
    parse_interval_key,
    parse_range_key,
)
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
    rng: str | None = Query(None, alias="range"),
    interval: str | None = None,
    tenant_id: str = "default",
) -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    try:
        rk = parse_range_key(rng)
        ik = parse_interval_key(interval, rk)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return build_history_payload(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        ticker=sym,
        range_key=rk,
        interval_key=ik,
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
    rng: str | None = Query(None, alias="range"),
    interval: str | None = None,
    tenant_id: str = "default",
) -> dict[str, Any]:
    sym = normalize_ticker(ticker)
    try:
        rk = parse_range_key(rng)
        ik = parse_interval_key(interval, rk)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return build_candles_payload(
        _svc(request).store.conn,
        tenant_id=tenant_id,
        ticker=sym,
        range_key=rk,
        interval_key=ik,
        now=datetime.now(),
    )
