#!/usr/bin/env python
"""
Download daily options/volatility metrics for an underlying symbol using Polygon
and append to a per-symbol CSV.

Primary path:
  - Polygon options chain snapshot (includes IV/greeks) when your plan is entitled.

Fallback path (no options snapshot entitlement):
  - Use Polygon reference contracts + daily aggregates to reconstruct IV and greeks
    from option prices (Black-Scholes) for a small near-ATM slice.

Output:
  data/raw_dumps/options/{symbol}_options_daily.csv

Schema:
  date,symbol,iv,iv_rank,gamma,put_call_ratio,oi,volume,iv_7d,iv_30d,iv_term_slope,iv_skew

Notes:
  - This is designed as a once-per-day job (Polygon's chain snapshot is real-time).
  - Dedup: skips if (date,symbol) already exists unless --force.
  - `iv_rank` is computed as (iv - min) / (max - min) * 100 over the last 252 iv values.
  - On tight Polygon RPM limits, use `--start/--end` on a small window first.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    from dotenv import load_dotenv

    load_dotenv(override=False)
except Exception:
    pass


DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "SPY", "QQQ", "IWM", "TSLA", "AMD", "META", "AMZN"]


def _env(name: str) -> str:
    v = os.getenv(name, "").strip()
    return v


def _safe_float(x: Any) -> float | None:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def _safe_int(x: Any) -> int | None:
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def _parse_iso_date(s: Any) -> _date | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).date()
    except Exception:
        return None


def _days_between(d0: _date, d1: _date) -> int:
    return int((d1 - d0).days)


def _get(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _deep_get(d: dict[str, Any], path: str) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        if part not in cur:
            return None
        cur = cur.get(part)
    return cur


@dataclass(frozen=True)
class ContractRow:
    ticker: str | None
    strike: float
    expiry: _date
    kind: str  # "call" or "put"
    iv: float | None
    gamma: float | None
    oi: int | None
    volume: int | None


def _polygon_get_json(url: str, *, params: dict[str, Any]) -> dict[str, Any]:
    backoff_s = 2.0
    for _ in range(0, 8):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            # respect tight free-tier RPM limits
            import time

            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except Exception:
                    time.sleep(backoff_s)
            else:
                time.sleep(backoff_s)
            backoff_s = min(90.0, backoff_s * 2.0)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        data = r.json()
        if not isinstance(data, dict):
            raise RuntimeError("unexpected response (not dict)")
        return data
    raise RuntimeError("HTTP 429: rate limited (exhausted retries)")


def _polygon_get_json_raw(url: str, *, params: dict[str, Any]) -> tuple[int, str, dict[str, Any] | None]:
    backoff_s = 2.0
    for _ in range(0, 8):
        r = requests.get(url, params=params, timeout=30)
        txt = r.text or ""
        try:
            js = r.json()
        except Exception:
            js = None
        if r.status_code == 429:
            import time

            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except Exception:
                    time.sleep(backoff_s)
            else:
                time.sleep(backoff_s)
            backoff_s = min(90.0, backoff_s * 2.0)
            continue
        if isinstance(js, dict):
            return int(r.status_code), txt, js
        return int(r.status_code), txt, None
    return 429, "", None


def _fetch_stock_price(symbol: str, *, api_base: str, api_key: str) -> float | None:
    # Fallback: stock snapshot endpoint (if the options snapshot doesn't include underlying price).
    # https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}
    url = f"{api_base}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}"
    data = _polygon_get_json(url, params={"apiKey": api_key})
    # best-effort: use last trade price, then day close, then prev day close.
    v = (
        _deep_get(data, "ticker.lastTrade.p")
        or _deep_get(data, "ticker.day.c")
        or _deep_get(data, "ticker.prevDay.c")
    )
    return _safe_float(v)


def _fetch_daily_close_and_volume(
    ticker: str,
    *,
    day: _date,
    api_base: str,
    api_key: str,
) -> tuple[float | None, int | None]:
    # https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
    url = f"{api_base}/v2/aggs/ticker/{ticker}/range/1/day/{day.isoformat()}/{day.isoformat()}"
    data = _polygon_get_json(url, params={"adjusted": "true", "sort": "asc", "limit": 1, "apiKey": api_key})
    res = data.get("results")
    if not isinstance(res, list) or not res:
        return None, None
    row = res[0] if isinstance(res[0], dict) else None
    if not row:
        return None, None
    close = _safe_float(row.get("c"))
    vol = _safe_int(row.get("v"))
    return close, vol


def _resolve_last_trading_day(
    symbol: str,
    *,
    day: _date,
    api_base: str,
    api_key: str,
    max_back: int = 7,
) -> _date | None:
    # Try the requested day, then walk back until we find a daily bar.
    for k in range(0, max_back + 1):
        d = day - timedelta(days=k)
        close, _ = _fetch_daily_close_and_volume(symbol.upper(), day=d, api_base=api_base, api_key=api_key)
        if close is not None and close > 0:
            return d
    return None


def _iter_chain_contracts(
    symbol: str,
    *,
    api_base: str,
    api_key: str,
    limit: int = 250,
) -> tuple[float | None, list[ContractRow]]:
    """
    Fetches the full chain snapshot (paginated) and returns (underlying_price, contracts).

    Polygon endpoint:
      GET {api_base}/v3/snapshot/options/{symbol}?apiKey=...
    """
    url = f"{api_base}/v3/snapshot/options/{symbol.upper()}"
    params: dict[str, Any] = {"apiKey": api_key, "limit": int(limit)}

    underlying: float | None = None
    rows: list[ContractRow] = []

    while True:
        data = _polygon_get_json(url, params=params)

        # Underlying price appears in different shapes depending on plan / endpoint version.
        for candidate in (
            _deep_get(data, "underlying_asset.price"),
            _deep_get(data, "underlying.price"),
            _deep_get(data, "underlying_asset.last_trade.p"),
            _deep_get(data, "underlying_asset.last_trade.price"),
            _deep_get(data, "underlying_asset.last.quote.midpoint"),
            _deep_get(data, "underlying_asset.last_quote.midpoint"),
        ):
            p = _safe_float(candidate)
            if p and p > 0:
                underlying = p
                break

        results = data.get("results")
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, dict):
                    continue

                # contract metadata
                details = item.get("details") if isinstance(item.get("details"), dict) else item
                strike = _safe_float(_get(details, "strike_price", "strike"))
                exp = _parse_iso_date(_get(details, "expiration_date", "expiration", "expiry"))
                kind = str(_get(details, "contract_type", "type", "call_put") or "").lower().strip()
                if kind in ("c", "call"):
                    kind = "call"
                if kind in ("p", "put"):
                    kind = "put"

                if strike is None or exp is None or kind not in ("call", "put"):
                    continue

                # greeks/iv
                iv = _safe_float(_get(item, "implied_volatility", "iv") or _deep_get(item, "greeks.iv"))
                gamma = _safe_float(_deep_get(item, "greeks.gamma") or _deep_get(item, "greeks.Gamma"))

                # oi/volume
                oi = _safe_int(_get(item, "open_interest", "openInterest") or _deep_get(item, "open_interest"))
                vol = _safe_int(
                    _get(item, "volume")
                    or _deep_get(item, "day.volume")
                    or _deep_get(item, "day.v")
                    or _deep_get(item, "last_trade.s")  # size as last resort
                )

                rows.append(
                    ContractRow(
                        ticker=str(_get(details, "ticker") or item.get("ticker") or "") or None,
                        strike=float(strike),
                        expiry=exp,
                        kind=kind,
                        iv=iv,
                        gamma=gamma,
                        oi=oi,
                        volume=vol,
                    )
                )

        next_url = data.get("next_url")
        if not next_url:
            break
        if not isinstance(next_url, str):
            break

        # next_url usually already includes apiKey for Polygon; but we keep passing apiKey anyway.
        url = next_url
        params = {"apiKey": api_key}

    return underlying, rows


def _fetch_reference_contracts(
    symbol: str,
    *,
    as_of: _date,
    expiration_gte: _date | None,
    expiration_lte: _date | None,
    strike_gte: float | None,
    strike_lte: float | None,
    api_base: str,
    api_key: str,
    limit: int,
    max_pages: int = 5,
) -> list[ContractRow]:
    """
    Fetch contracts from Polygon reference endpoint (no IV/greeks).

    GET /v3/reference/options/contracts?underlying_ticker=...&as_of=...
    """
    url = f"{api_base}/v3/reference/options/contracts"
    params: dict[str, Any] = {"underlying_ticker": symbol.upper(), "as_of": as_of.isoformat(), "limit": int(limit), "apiKey": api_key}
    if expiration_gte is not None:
        params["expiration_date.gte"] = expiration_gte.isoformat()
    if expiration_lte is not None:
        params["expiration_date.lte"] = expiration_lte.isoformat()
    if strike_gte is not None:
        params["strike_price.gte"] = float(strike_gte)
    if strike_lte is not None:
        params["strike_price.lte"] = float(strike_lte)
    out: list[ContractRow] = []

    pages = 0
    while True:
        try:
            data = _polygon_get_json(url, params=params)
        except RuntimeError as e:
            # If filter parameters are unsupported, retry once without them.
            msg = str(e)
            if "Invalid" in msg or "invalid" in msg or "parameter" in msg:
                params2 = {"underlying_ticker": symbol.upper(), "as_of": as_of.isoformat(), "limit": int(limit), "apiKey": api_key}
                data = _polygon_get_json(url, params=params2)
                params = params2
            else:
                raise
        results = data.get("results")
        if isinstance(results, list):
            for it in results:
                if not isinstance(it, dict):
                    continue
                strike = _safe_float(it.get("strike_price"))
                exp = _parse_iso_date(it.get("expiration_date"))
                kind = str(it.get("contract_type") or "").lower().strip()
                if kind not in ("call", "put") or strike is None or exp is None:
                    continue
                out.append(
                    ContractRow(
                        ticker=str(it.get("ticker") or "") or None,
                        strike=float(strike),
                        expiry=exp,
                        kind=kind,
                        iv=None,
                        gamma=None,
                        oi=None,
                        volume=None,
                    )
                )
        next_url = data.get("next_url")
        pages += 1
        if not next_url or not isinstance(next_url, str) or pages >= int(max_pages):
            break
        url = next_url
        params = {"apiKey": api_key}

    return out


def _norm_cdf(x: float) -> float:
    import math

    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    import math

    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)


def _bs_price(*, kind: str, s: float, k: float, t: float, r: float, sigma: float) -> float:
    import math

    if t <= 0 or sigma <= 0 or s <= 0 or k <= 0:
        return 0.0
    vsqrt = sigma * math.sqrt(t)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / vsqrt
    d2 = d1 - vsqrt
    if kind == "call":
        return s * _norm_cdf(d1) - k * math.exp(-r * t) * _norm_cdf(d2)
    return k * math.exp(-r * t) * _norm_cdf(-d2) - s * _norm_cdf(-d1)


def _bs_gamma(*, s: float, k: float, t: float, r: float, sigma: float) -> float | None:
    import math

    if t <= 0 or sigma <= 0 or s <= 0 or k <= 0:
        return None
    vsqrt = sigma * math.sqrt(t)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / vsqrt
    return _norm_pdf(d1) / (s * vsqrt)


def _implied_vol(*, kind: str, s: float, k: float, t: float, r: float, price: float) -> float | None:
    import math

    if price is None or price <= 0 or s <= 0 or k <= 0 or t <= 0:
        return None

    # Arbitrage bounds (no dividends)
    disc = k * math.exp(-r * t)
    if kind == "call":
        lo = max(0.0, s - disc)
        hi = s
    else:
        lo = max(0.0, disc - s)
        hi = disc
    if price < lo - 1e-6 or price > hi + 1e-6:
        return None

    # Bisection over sigma in [1e-4, 5.0]
    sig_lo = 1e-4
    sig_hi = 5.0
    p_lo = _bs_price(kind=kind, s=s, k=k, t=t, r=r, sigma=sig_lo)
    p_hi = _bs_price(kind=kind, s=s, k=k, t=t, r=r, sigma=sig_hi)
    if p_lo > price or p_hi < price:
        return None

    for _ in range(60):
        mid = 0.5 * (sig_lo + sig_hi)
        p_mid = _bs_price(kind=kind, s=s, k=k, t=t, r=r, sigma=mid)
        if abs(p_mid - price) < 1e-5:
            return mid
        if p_mid < price:
            sig_lo = mid
        else:
            sig_hi = mid
    return 0.5 * (sig_lo + sig_hi)


def _choose_expiry(as_of: _date, contracts: Iterable[ContractRow], *, min_dte: int, max_dte: int) -> _date | None:
    expiries: dict[_date, int] = {}
    for c in contracts:
        dte = _days_between(as_of, c.expiry)
        if dte < min_dte or dte > max_dte:
            continue
        expiries[c.expiry] = min(expiries.get(c.expiry, 10**9), dte)
    if not expiries:
        return None
    return sorted(expiries.items(), key=lambda kv: kv[1])[0][0]


def _choose_expiry_near(as_of: _date, contracts: Iterable[ContractRow], *, target_dte: int, tol: int) -> _date | None:
    best: tuple[int, _date] | None = None
    for c in contracts:
        dte = _days_between(as_of, c.expiry)
        if dte <= 0:
            continue
        dist = abs(dte - int(target_dte))
        if dist > int(tol):
            continue
        if best is None or dist < best[0]:
            best = (dist, c.expiry)
    return best[1] if best else None


def _agg_metrics(
    contracts: list[ContractRow],
    *,
    expiry: _date,
    spot: float,
    atm_band: float,
    skew_band: float,
) -> dict[str, Any] | None:
    lo_strike = spot * (1.0 - atm_band)
    hi_strike = spot * (1.0 + atm_band)
    sliced = [c for c in contracts if c.expiry == expiry and (lo_strike <= c.strike <= hi_strike)]
    if not sliced:
        return None

    put_oi = sum((c.oi or 0) for c in sliced if c.kind == "put")
    call_oi = sum((c.oi or 0) for c in sliced if c.kind == "call")
    total_oi = put_oi + call_oi
    total_vol = sum((c.volume or 0) for c in sliced)

    pcr = None
    if call_oi > 0:
        pcr = round(put_oi / call_oi, 6)

    iv_vals: list[tuple[float, float]] = []
    gamma_vals: list[tuple[float, float]] = []
    for c in sliced:
        w = float(c.oi or 0)
        if w <= 0:
            w = float(c.volume or 0)
        if w <= 0:
            w = 1.0
        if c.iv is not None:
            iv_vals.append((float(c.iv), w))
        if c.gamma is not None:
            gamma_vals.append((float(c.gamma), w))

    iv = _weighted_mean(iv_vals)
    gamma = _weighted_mean(gamma_vals)
    if iv is None:
        return None

    # Skew proxy: OTM put IV minus OTM call IV at symmetric bands.
    put_otm = spot * (1.0 - skew_band)
    call_otm = spot * (1.0 + skew_band)
    put_iv_vals: list[tuple[float, float]] = []
    call_iv_vals: list[tuple[float, float]] = []
    for c in contracts:
        if c.expiry != expiry or c.iv is None:
            continue
        w = float(c.oi or 0)
        if w <= 0:
            w = float(c.volume or 0)
        if w <= 0:
            w = 1.0
        if c.kind == "put" and c.strike <= put_otm:
            put_iv_vals.append((float(c.iv), w))
        if c.kind == "call" and c.strike >= call_otm:
            call_iv_vals.append((float(c.iv), w))
    put_iv = _weighted_mean(put_iv_vals)
    call_iv = _weighted_mean(call_iv_vals)
    iv_skew = None
    if put_iv is not None and call_iv is not None:
        iv_skew = round(float(put_iv) - float(call_iv), 6)

    return {
        "iv": round(float(iv), 6),
        "gamma": round(float(gamma), 10) if gamma is not None else None,
        "put_call_ratio": pcr,
        "oi": int(total_oi),
        "volume": int(total_vol),
        "iv_skew": iv_skew,
    }


def _agg_metrics_from_prices(
    symbol: str,
    *,
    ref_contracts: list[ContractRow],
    expiry: _date,
    spot: float,
    day: _date,
    api_base: str,
    api_key: str,
    rate: float,
    atm_band: float,
    skew_band: float,
    max_contracts_per_side: int,
) -> dict[str, Any] | None:
    # Select near-ATM strikes first (volume not known yet), then fetch prices.
    lo_strike = spot * (1.0 - atm_band)
    hi_strike = spot * (1.0 + atm_band)
    pool = [c for c in ref_contracts if c.expiry == expiry and (lo_strike <= c.strike <= hi_strike)]
    if not pool:
        return None

    # Cap to the closest strikes so we don't explode API calls.
    def _pick(kind: str) -> list[ContractRow]:
        xs = [c for c in pool if c.kind == kind]
        xs.sort(key=lambda c: abs(c.strike - spot))
        return xs[: int(max_contracts_per_side)]

    picked = _pick("call") + _pick("put")
    # Also fetch one OTM put/call for skew proxy (closest to +/-skew_band).
    puts_all = [c for c in ref_contracts if c.expiry == expiry and c.kind == "put"]
    calls_all = [c for c in ref_contracts if c.expiry == expiry and c.kind == "call"]
    put_thresh = spot * (1.0 - float(skew_band))
    call_thresh = spot * (1.0 + float(skew_band))
    otm_put = None
    otm_call = None
    if puts_all:
        puts_all.sort(key=lambda c: abs(c.strike - put_thresh))
        otm_put = next((c for c in puts_all if c.strike <= put_thresh), puts_all[0])
    if calls_all:
        calls_all.sort(key=lambda c: abs(c.strike - call_thresh))
        otm_call = next((c for c in calls_all if c.strike >= call_thresh), calls_all[0])
    for extra in (otm_put, otm_call):
        if extra is None:
            continue
        if extra not in picked:
            picked.append(extra)
    if not picked:
        return None

    t_years = max(1, _days_between(day, expiry)) / 365.0

    enriched: list[ContractRow] = []
    for c in picked:
        tick = c.ticker
        if not tick:
            continue

        px, vol = _fetch_daily_close_and_volume(str(tick), day=day, api_base=api_base, api_key=api_key)
        if px is None or px <= 0:
            continue
        iv = _implied_vol(kind=c.kind, s=spot, k=c.strike, t=t_years, r=float(rate), price=float(px))
        if iv is None:
            continue
        gamma = _bs_gamma(s=spot, k=c.strike, t=t_years, r=float(rate), sigma=float(iv))
        enriched.append(
            ContractRow(
                ticker=str(tick),
                strike=c.strike,
                expiry=c.expiry,
                kind=c.kind,
                iv=float(iv),
                gamma=float(gamma) if gamma is not None else None,
                oi=None,
                volume=vol,
            )
        )

    if not enriched:
        return None

    # Aggregate like _agg_metrics, but with volume weights.
    put_vol = sum((c.volume or 0) for c in enriched if c.kind == "put")
    call_vol = sum((c.volume or 0) for c in enriched if c.kind == "call")
    total_vol = put_vol + call_vol
    pcr = None
    if call_vol > 0:
        pcr = round(put_vol / call_vol, 6)

    iv_vals: list[tuple[float, float]] = []
    gamma_vals: list[tuple[float, float]] = []
    for c in enriched:
        w = float((c.volume or 0) + 1)
        if c.iv is not None:
            iv_vals.append((float(c.iv), w))
        if c.gamma is not None:
            gamma_vals.append((float(c.gamma), w))

    iv = _weighted_mean(iv_vals)
    gamma = _weighted_mean(gamma_vals)
    if iv is None:
        return None

    # Skew proxy: use the explicitly selected OTM pair if both IVs are present.
    iv_skew = None
    if otm_put is not None and otm_call is not None:
        put_iv = next((c.iv for c in enriched if c.kind == "put" and abs(c.strike - otm_put.strike) < 1e-9), None)
        call_iv = next((c.iv for c in enriched if c.kind == "call" and abs(c.strike - otm_call.strike) < 1e-9), None)
        if put_iv is not None and call_iv is not None:
            iv_skew = round(float(put_iv) - float(call_iv), 6)

    return {
        "iv": round(float(iv), 6),
        "gamma": round(float(gamma), 10) if gamma is not None else None,
        "put_call_ratio": pcr,
        "oi": None,
        "volume": int(total_vol),
        "iv_skew": iv_skew,
    }


def _weighted_mean(vals: list[tuple[float, float]]) -> float | None:
    # vals: [(value, weight)]
    num = 0.0
    den = 0.0
    for v, w in vals:
        if w <= 0:
            continue
        num += v * w
        den += w
    if den <= 0:
        return None
    return num / den


def _read_existing(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "date",
        "symbol",
        "iv",
        "iv_rank",
        "gamma",
        "put_call_ratio",
        "oi",
        "volume",
        "iv_7d",
        "iv_30d",
        "iv_term_slope",
        "iv_skew",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in cols})


def _compute_iv_rank(existing_iv: list[float], current_iv: float, *, lookback: int = 252) -> float | None:
    hist = [v for v in existing_iv if v is not None]
    if lookback > 0 and len(hist) > lookback:
        hist = hist[-lookback:]
    if not hist:
        return None
    lo = min(hist)
    hi = max(hist)
    if hi <= lo:
        return 0.0
    raw = (current_iv - lo) / (hi - lo) * 100.0
    raw = max(0.0, min(100.0, raw))
    return round(raw, 4)


def _extract_existing_iv(existing: list[dict[str, str]]) -> list[float]:
    out: list[float] = []
    for r in existing:
        v = _safe_float(r.get("iv"))
        if v is not None:
            out.append(v)
    return out


def run_one(
    symbol: str,
    *,
    as_of: _date,
    api_base: str,
    api_key: str,
    out_dir: Path,
    rate: float,
    min_dte: int,
    max_dte: int,
    atm_band: float,
    skew_band: float,
    limit: int,
    max_contracts_per_side: int,
    roll_non_trading: bool,
    force: bool,
) -> bool:
    out_path = out_dir / f"{symbol.upper()}_options_daily.csv"
    existing = _read_existing(out_path)
    date_str = as_of.isoformat()

    if not force:
        for r in existing:
            if str(r.get("date", "")).strip() == date_str and str(r.get("symbol", "")).strip().upper() == symbol.upper():
                print(f"  {symbol}: {date_str} exists -> skip")
                return True

    if roll_non_trading:
        trade_day = _resolve_last_trading_day(symbol, day=as_of, api_base=api_base, api_key=api_key)
        if trade_day is None:
            print(f"  {symbol}: no recent daily bar found near {as_of.isoformat()} -> skip")
            return False
    else:
        # Strict: only compute if the requested date is a trading day with an underlying bar.
        px, _ = _fetch_daily_close_and_volume(symbol.upper(), day=as_of, api_base=api_base, api_key=api_key)
        if px is None or px <= 0:
            return True  # skip silently in backfills
        trade_day = as_of

    underlying, contracts = None, []
    # Try snapshot first (best quality), but it may be blocked by plan entitlement.
    status, _, js = _polygon_get_json_raw(f"{api_base}/v3/snapshot/options/{symbol.upper()}", params={"apiKey": api_key, "limit": int(limit)})
    if status == 200 and isinstance(js, dict):
        try:
            underlying, contracts = _iter_chain_contracts(symbol, api_base=api_base, api_key=api_key, limit=int(limit))
        except Exception:
            underlying, contracts = None, []

    if not underlying or underlying <= 0:
        px, _ = _fetch_daily_close_and_volume(symbol.upper(), day=trade_day, api_base=api_base, api_key=api_key)
        underlying = px or _fetch_stock_price(symbol, api_base=api_base, api_key=api_key)

    if not underlying or underlying <= 0:
        print(f"  {symbol}: missing underlying price -> skip")
        return False

    # If snapshot contracts are missing (or un-entitled), fall back to reference contracts.
    if not contracts:
        exp_gte = trade_day + timedelta(days=int(min_dte))
        exp_lte = trade_day + timedelta(days=int(max_dte))
        strike_gte = float(underlying) * (1.0 - float(atm_band) * 1.5)
        strike_lte = float(underlying) * (1.0 + float(atm_band) * 1.5)
        contracts = _fetch_reference_contracts(
            symbol,
            as_of=trade_day,
            expiration_gte=exp_gte,
            expiration_lte=exp_lte,
            strike_gte=strike_gte,
            strike_lte=strike_lte,
            api_base=api_base,
            api_key=api_key,
            limit=max(200, int(limit)),
        )

    expiry = _choose_expiry(trade_day, contracts, min_dte=min_dte, max_dte=max_dte)
    if not expiry:
        print(f"  {symbol}: no expiry in {min_dte}..{max_dte} DTE -> skip")
        return False

    base = _agg_metrics(contracts, expiry=expiry, spot=float(underlying), atm_band=float(atm_band), skew_band=float(skew_band))
    if not base:
        # Snapshot path failed (no IV/greeks). Reconstruct from option prices.
        base = _agg_metrics_from_prices(
            symbol,
            ref_contracts=contracts,
            expiry=expiry,
            spot=float(underlying),
            day=trade_day,
            api_base=api_base,
            api_key=api_key,
            rate=float(rate),
            atm_band=float(atm_band),
            skew_band=float(skew_band),
            max_contracts_per_side=int(max_contracts_per_side),
        )
    if not base:
        print(f"  {symbol}: failed to compute iv from snapshot or prices -> skip")
        return False

    existing_iv = _extract_existing_iv(existing)
    iv_rank = _compute_iv_rank(existing_iv, float(base["iv"]))

    # Term structure: pick expiries near ~7D and ~30D (within tolerances), compute ATM IV for each.
    exp_7d = _choose_expiry_near(trade_day, contracts, target_dte=7, tol=5)
    exp_30d = _choose_expiry_near(trade_day, contracts, target_dte=30, tol=10)
    iv_7d = None
    iv_30d = None
    if exp_7d:
        if exp_7d == expiry:
            iv_7d = float(base["iv"])
        else:
            m7 = _agg_metrics(contracts, expiry=exp_7d, spot=float(underlying), atm_band=float(atm_band), skew_band=float(skew_band))
            if not m7:
                m7 = _agg_metrics_from_prices(
                    symbol,
                    ref_contracts=contracts,
                    expiry=exp_7d,
                    spot=float(underlying),
                    day=trade_day,
                    api_base=api_base,
                    api_key=api_key,
                    rate=float(rate),
                    atm_band=float(atm_band),
                    skew_band=float(skew_band),
                    max_contracts_per_side=int(max_contracts_per_side),
                )
            if m7 and m7.get("iv") is not None:
                iv_7d = float(m7["iv"])
    if exp_30d:
        if exp_30d == expiry:
            iv_30d = float(base["iv"])
        else:
            m30 = _agg_metrics(contracts, expiry=exp_30d, spot=float(underlying), atm_band=float(atm_band), skew_band=float(skew_band))
            if not m30:
                m30 = _agg_metrics_from_prices(
                    symbol,
                    ref_contracts=contracts,
                    expiry=exp_30d,
                    spot=float(underlying),
                    day=trade_day,
                    api_base=api_base,
                    api_key=api_key,
                    rate=float(rate),
                    atm_band=float(atm_band),
                    skew_band=float(skew_band),
                    max_contracts_per_side=int(max_contracts_per_side),
                )
            if m30 and m30.get("iv") is not None:
                iv_30d = float(m30["iv"])

    iv_term_slope = None
    if iv_7d is not None and iv_30d is not None:
        # Simple, stable definition: longer-dated IV minus short-dated IV.
        iv_term_slope = round(float(iv_30d) - float(iv_7d), 6)

    row = {
        "date": trade_day.isoformat(),
        "symbol": symbol.upper(),
        "iv": base["iv"],
        "iv_rank": iv_rank,
        "gamma": base.get("gamma"),
        "put_call_ratio": base.get("put_call_ratio"),
        "oi": base.get("oi"),
        "volume": base.get("volume"),
        "iv_7d": None if iv_7d is None else round(float(iv_7d), 6),
        "iv_30d": None if iv_30d is None else round(float(iv_30d), 6),
        "iv_term_slope": iv_term_slope,
        "iv_skew": base.get("iv_skew"),
    }

    # Replace existing row for that date if force, else append.
    kept: list[dict[str, Any]] = []
    for r in existing:
        if str(r.get("date", "")).strip() == trade_day.isoformat() and str(r.get("symbol", "")).strip().upper() == symbol.upper():
            continue
        kept.append(r)
    kept.append(row)
    kept.sort(key=lambda r: (str(r.get("date", "")), str(r.get("symbol", ""))))
    _write_rows(out_path, kept)
    print(f"  {symbol}: wrote {out_path} (date={trade_day.isoformat()}, expiry={expiry}, spot={underlying:.2f})")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="comma-separated symbols")
    ap.add_argument("--out", default="data/raw_dumps/options", help="output directory")
    ap.add_argument("--date", default=None, help="date to stamp (YYYY-MM-DD), default=today")
    ap.add_argument("--start", default=None, help="backfill start date (YYYY-MM-DD)")
    ap.add_argument("--end", default=None, help="backfill end date (YYYY-MM-DD)")
    ap.add_argument("--min-dte", type=int, default=7, help="min days-to-expiry (default: 7)")
    ap.add_argument("--max-dte", type=int, default=30, help="max days-to-expiry (default: 30)")
    ap.add_argument("--atm-band", type=float, default=0.05, help="ATM strike band (default: 0.05 = +/-5%)")
    ap.add_argument("--skew-band", type=float, default=0.05, help="skew band for OTM IV proxy (default: 0.05 = +/-5%)")
    ap.add_argument("--rate", type=float, default=0.05, help="risk-free rate for IV inversion (default: 0.05)")
    ap.add_argument("--limit", type=int, default=250, help="Polygon pagination limit (default: 250)")
    ap.add_argument("--max-contracts-per-side", type=int, default=8, help="cap IV reconstruction calls per side (default: 8)")
    ap.add_argument("--force", action="store_true", help="overwrite existing (date,symbol) row if present")
    args = ap.parse_args()

    api_key = _env("POLYGON_API_KEY")
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in environment")
        return 2

    api_base = _env("POLYGON_BASE_URL") or "https://api.polygon.io"

    def _parse_d(x: str | None) -> _date | None:
        if not x:
            return None
        try:
            return _date.fromisoformat(str(x))
        except Exception:
            return None

    as_of = _date.today()
    if args.date:
        d0 = _parse_d(str(args.date))
        if d0 is None:
            print("ERROR: --date must be YYYY-MM-DD")
            return 2
        as_of = d0

    start = _parse_d(str(args.start)) if args.start else None
    end = _parse_d(str(args.end)) if args.end else None
    if (args.start and start is None) or (args.end and end is None):
        print("ERROR: --start/--end must be YYYY-MM-DD")
        return 2
    if start and not end:
        end = as_of
    if end and not start:
        start = end
    if start and end and end < start:
        print("ERROR: --end must be >= --start")
        return 2

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    out_dir = Path(str(args.out))

    ok = 0
    bad = 0
    if start and end:
        print(f"Polygon options daily -> {out_dir}  (backfill {start.isoformat()}..{end.isoformat()})")
        d = start
        while d <= end:
            for sym in symbols:
                try:
                    if run_one(
                        sym,
                        as_of=d,
                        api_base=api_base,
                        api_key=api_key,
                        out_dir=out_dir,
                        rate=float(args.rate),
                        min_dte=int(args.min_dte),
                        max_dte=int(args.max_dte),
                        atm_band=float(args.atm_band),
                        skew_band=float(args.skew_band),
                        limit=int(args.limit),
                        max_contracts_per_side=int(args.max_contracts_per_side),
                        roll_non_trading=False,
                        force=bool(args.force),
                    ):
                        ok += 1
                    else:
                        bad += 1
                except Exception as e:
                    bad += 1
                    print(f"  {sym} {d.isoformat()}: ERROR - {e}")
            d = d + timedelta(days=1)
    else:
        print(f"Polygon options daily -> {out_dir}  (date={as_of.isoformat()})")
        for sym in symbols:
            try:
                if run_one(
                    sym,
                    as_of=as_of,
                    api_base=api_base,
                    api_key=api_key,
                    out_dir=out_dir,
                    rate=float(args.rate),
                    min_dte=int(args.min_dte),
                    max_dte=int(args.max_dte),
                    atm_band=float(args.atm_band),
                    skew_band=float(args.skew_band),
                    limit=int(args.limit),
                    max_contracts_per_side=int(args.max_contracts_per_side),
                    roll_non_trading=True,
                    force=bool(args.force),
                ):
                    ok += 1
                else:
                    bad += 1
            except Exception as e:
                bad += 1
                print(f"  {sym}: ERROR - {e}")

    print(f"Done: {ok} ok, {bad} failed")
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
