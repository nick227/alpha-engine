from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests


@dataclass(frozen=True)
class FundamentalsSnapshot:
    ticker: str
    as_of_date: str
    revenue_ttm: float | None
    shares_outstanding: float | None
    sector: str | None
    industry: str | None


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _get_json(url: str, params: dict[str, Any], timeout_s: int = 30) -> Any:
    r = requests.get(url, params=params, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def fetch_fmp_fundamentals(symbol: str, *, api_key: str) -> FundamentalsSnapshot:
    """
    Minimal fundamentals snapshot using FMP public endpoints.

    - sector/industry/sharesOutstanding: /api/v3/profile/{symbol}
    - revenue_ttm: sum last 4 quarterly revenues from /api/v3/income-statement/{symbol}?period=quarter
      (fallback: latest annual revenue if quarterly isn't available)
    """
    sym = str(symbol).upper().strip()
    params = {"apikey": api_key}

    profile_url = f"https://financialmodelingprep.com/api/v3/profile/{sym}"
    profile = _get_json(profile_url, params)
    sector = None
    industry = None
    shares = None
    if isinstance(profile, list) and profile:
        p0 = profile[0] if isinstance(profile[0], dict) else {}
        sector = p0.get("sector")
        industry = p0.get("industry")
        shares = p0.get("sharesOutstanding")

    revenue_ttm: float | None = None
    inc_q_url = f"https://financialmodelingprep.com/api/v3/income-statement/{sym}"
    inc_q = _get_json(inc_q_url, {**params, "period": "quarter", "limit": 8})
    if isinstance(inc_q, list) and inc_q:
        revs: list[float] = []
        for row in inc_q[:4]:
            if isinstance(row, dict) and row.get("revenue") is not None:
                try:
                    revs.append(float(row["revenue"]))
                except Exception:
                    continue
        if len(revs) == 4:
            revenue_ttm = float(sum(revs))

    if revenue_ttm is None:
        inc_a = _get_json(inc_q_url, {**params, "limit": 2})
        if isinstance(inc_a, list) and inc_a:
            row = inc_a[0] if isinstance(inc_a[0], dict) else {}
            if row.get("revenue") is not None:
                try:
                    revenue_ttm = float(row["revenue"])
                except Exception:
                    revenue_ttm = None

    return FundamentalsSnapshot(
        ticker=sym,
        as_of_date=date.today().isoformat(),
        revenue_ttm=revenue_ttm,
        shares_outstanding=(float(shares) if shares is not None else None),
        sector=(str(sector) if sector else None),
        industry=(str(industry) if industry else None),
    )


def fetch_fmp_fundamentals_batch(
    symbols: list[str],
    *,
    api_key: str | None = None,
    max_workers: int | None = None,
) -> list[FundamentalsSnapshot]:
    key = str(api_key or _env("FMP_API_KEY"))
    if not key:
        raise RuntimeError("FMP_API_KEY is required to fetch fundamentals")
    raw = _env("FMP_FETCH_CONCURRENCY", "6")
    try:
        n = int(raw) if max_workers is None else int(max_workers)
    except ValueError:
        n = 6
    workers = max(1, min(n, 32))
    if len(symbols) <= 1:
        return [fetch_fmp_fundamentals(symbols[0], api_key=key)] if symbols else []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(lambda s: fetch_fmp_fundamentals(s, api_key=key), symbols))

