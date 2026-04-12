#!/usr/bin/env python
"""
Download a daily options/volatility snapshot for an underlying symbol using Polygon,
aggregate a small near-ATM slice, and append to a per-symbol CSV.

Output:
  data/raw_dumps/options/{symbol}_options_daily.csv

Schema:
  date,symbol,iv,iv_rank,gamma,put_call_ratio,oi,volume,iv_7d,iv_30d,iv_term_slope,iv_skew

Notes:
  - This is designed as a once-per-day job (Polygon's chain snapshot is real-time).
  - Dedup: skips if (date,symbol) already exists unless --force.
  - `iv_rank` is computed as (iv - min) / (max - min) * 100 over the last 252 iv values.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime
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
    strike: float
    expiry: _date
    kind: str  # "call" or "put"
    iv: float | None
    gamma: float | None
    oi: int | None
    volume: int | None


def _polygon_get_json(url: str, *, params: dict[str, Any]) -> dict[str, Any]:
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("unexpected response (not dict)")
    return data


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
    return round((current_iv - lo) / (hi - lo) * 100.0, 4)


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
    min_dte: int,
    max_dte: int,
    atm_band: float,
    skew_band: float,
    limit: int,
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

    underlying, contracts = _iter_chain_contracts(symbol, api_base=api_base, api_key=api_key, limit=int(limit))
    if not underlying or underlying <= 0:
        underlying = _fetch_stock_price(symbol, api_base=api_base, api_key=api_key)

    if not underlying or underlying <= 0:
        print(f"  {symbol}: missing underlying price -> skip")
        return False

    expiry = _choose_expiry(as_of, contracts, min_dte=min_dte, max_dte=max_dte)
    if not expiry:
        print(f"  {symbol}: no expiry in {min_dte}..{max_dte} DTE -> skip")
        return False

    base = _agg_metrics(contracts, expiry=expiry, spot=float(underlying), atm_band=float(atm_band), skew_band=float(skew_band))
    if not base:
        print(f"  {symbol}: iv missing in slice -> skip")
        return False

    existing_iv = _extract_existing_iv(existing)
    iv_rank = _compute_iv_rank(existing_iv, float(base["iv"]))

    # Term structure: pick expiries near ~7D and ~30D (within tolerances), compute ATM IV for each.
    exp_7d = _choose_expiry_near(as_of, contracts, target_dte=7, tol=5)
    exp_30d = _choose_expiry_near(as_of, contracts, target_dte=30, tol=10)
    iv_7d = None
    iv_30d = None
    if exp_7d:
        m7 = _agg_metrics(contracts, expiry=exp_7d, spot=float(underlying), atm_band=float(atm_band), skew_band=float(skew_band))
        if m7 and m7.get("iv") is not None:
            iv_7d = float(m7["iv"])
    if exp_30d:
        m30 = _agg_metrics(contracts, expiry=exp_30d, spot=float(underlying), atm_band=float(atm_band), skew_band=float(skew_band))
        if m30 and m30.get("iv") is not None:
            iv_30d = float(m30["iv"])

    iv_term_slope = None
    if iv_7d is not None and iv_30d is not None:
        # Simple, stable definition: longer-dated IV minus short-dated IV.
        iv_term_slope = round(float(iv_30d) - float(iv_7d), 6)

    row = {
        "date": date_str,
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
        if str(r.get("date", "")).strip() == date_str and str(r.get("symbol", "")).strip().upper() == symbol.upper():
            continue
        kept.append(r)
    kept.append(row)
    kept.sort(key=lambda r: (str(r.get("date", "")), str(r.get("symbol", ""))))
    _write_rows(out_path, kept)
    print(f"  {symbol}: wrote {out_path} (expiry={expiry}, spot={underlying:.2f})")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="comma-separated symbols")
    ap.add_argument("--out", default="data/raw_dumps/options", help="output directory")
    ap.add_argument("--date", default=None, help="date to stamp (YYYY-MM-DD), default=today")
    ap.add_argument("--min-dte", type=int, default=7, help="min days-to-expiry (default: 7)")
    ap.add_argument("--max-dte", type=int, default=30, help="max days-to-expiry (default: 30)")
    ap.add_argument("--atm-band", type=float, default=0.05, help="ATM strike band (default: 0.05 = +/-5%)")
    ap.add_argument("--skew-band", type=float, default=0.05, help="skew band for OTM IV proxy (default: 0.05 = +/-5%)")
    ap.add_argument("--limit", type=int, default=250, help="Polygon pagination limit (default: 250)")
    ap.add_argument("--force", action="store_true", help="overwrite existing (date,symbol) row if present")
    args = ap.parse_args()

    api_key = _env("POLYGON_API_KEY")
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in environment")
        return 2

    api_base = _env("POLYGON_BASE_URL") or "https://api.polygon.io"

    as_of = _date.today()
    if args.date:
        try:
            as_of = _date.fromisoformat(str(args.date))
        except Exception:
            print("ERROR: --date must be YYYY-MM-DD")
            return 2

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    out_dir = Path(str(args.out))

    ok = 0
    bad = 0
    print(f"Polygon options snapshot -> {out_dir}  (date={as_of.isoformat()})")
    for sym in symbols:
        try:
            if run_one(
                sym,
                as_of=as_of,
                api_base=api_base,
                api_key=api_key,
                out_dir=out_dir,
                min_dte=int(args.min_dte),
                max_dte=int(args.max_dte),
                atm_band=float(args.atm_band),
                skew_band=float(args.skew_band),
                limit=int(args.limit),
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
