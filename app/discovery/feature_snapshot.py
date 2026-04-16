from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from app.discovery.scoring import bucket_price
from app.discovery.types import FeatureRow


def _parse_date(s: str | date) -> date:
    if isinstance(s, date):
        return s
    return date.fromisoformat(str(s).strip())


def _safe_float(x) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if np.isnan(v):
            return None
        return v
    except Exception:
        return None


def _pct_change(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return (a / b) - 1.0


def _max_drawdown(closes: np.ndarray) -> float | None:
    if closes.size < 2:
        return None
    peak = np.maximum.accumulate(closes)
    dd = (closes / peak) - 1.0
    m = float(np.min(dd))
    return float(-m) if m < 0 else 0.0


def _percentile_rank(window: np.ndarray, value: float) -> float | None:
    if window.size == 0:
        return None
    return float(np.mean(window <= value))


def build_feature_snapshot(
    *,
    db_path: str | Path = "data/alpha.db",
    as_of: str | date,
    tenant_id: str = "default",
    timeframe: str = "1d",
    lookback_days: int = 420,
    symbols: list[str] | None = None,
) -> dict[str, FeatureRow]:
    """
    Build a point-in-time snapshot of discovery features for all symbols with a bar on `as_of`.
    """
    as_of_date = _parse_date(as_of)
    start = as_of_date - timedelta(days=int(lookback_days))
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    def _fetch_rows(symbol_subset: list[str] | None) -> list[sqlite3.Row]:
        if not symbol_subset:
            return conn.execute(
                """
                SELECT ticker, timestamp, close, volume
                FROM price_bars
                WHERE tenant_id = ? AND timeframe = ?
                  AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
                ORDER BY ticker ASC, timestamp ASC
                """,
                (tenant_id, timeframe, start.isoformat(), (as_of_date - timedelta(days=1)).isoformat()),
            ).fetchall()

        syms = [str(s).strip().upper() for s in symbol_subset if str(s).strip()]
        if not syms:
            return []
        placeholders = ",".join(["?"] * len(syms))
        return conn.execute(
            f"""
            SELECT ticker, timestamp, close, volume
            FROM price_bars
            WHERE tenant_id = ? AND timeframe = ?
              AND ticker IN ({placeholders})
              AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
            ORDER BY ticker ASC, timestamp ASC
            """,
            [tenant_id, timeframe, *syms, start.isoformat(), (as_of_date - timedelta(days=1)).isoformat()],
        ).fetchall()

    rows: list[sqlite3.Row] = []
    if symbols is None:
        rows = _fetch_rows(None)
    else:
        # SQLite variable limit is often 999; keep headroom for other params.
        chunk_size = 900
        normalized = [str(s).strip().upper() for s in symbols if str(s).strip()]
        for i in range(0, len(normalized), chunk_size):
            rows.extend(_fetch_rows(normalized[i : i + chunk_size]))

    if not rows:
        return {}

    df = pd.DataFrame([dict(r) for r in rows])
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["d"] = df["timestamp"].dt.date
    for col in ("close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Latest fundamentals <= as_of
    frows = conn.execute(
        """
        SELECT fs.*
        FROM fundamentals_snapshot fs
        JOIN (
          SELECT tenant_id, ticker, MAX(as_of_date) as mx
          FROM fundamentals_snapshot
          WHERE tenant_id = ? AND as_of_date <= ?
          GROUP BY tenant_id, ticker
        ) latest
          ON latest.tenant_id = fs.tenant_id AND latest.ticker = fs.ticker AND latest.mx = fs.as_of_date
        """,
        (tenant_id, as_of_date.isoformat()),
    ).fetchall()
    fdf = pd.DataFrame([dict(r) for r in frows]) if frows else pd.DataFrame()
    if not fdf.empty:
        fdf["ticker"] = fdf["ticker"].astype(str).str.upper()

    out: dict[str, FeatureRow] = {}

    for ticker, g in df.groupby("ticker", sort=False):
        sym = str(ticker).upper()
        g = g.sort_values("timestamp")

        # CRITICAL: Ensure no future data leakage
        max_data_date = g["timestamp"].max().date()
        if max_data_date >= as_of_date:
            raise ValueError(f"Feature leakage detected: data includes {max_data_date} >= prediction date {as_of_date}")

        # Use latest available data instead of exact date match
        # This ensures we get data even if as_of_date has limited coverage
        if g.empty:
            continue
        last = g.iloc[-1]
        close = _safe_float(last["close"])
        vol = _safe_float(last["volume"])
        dollar_volume = (close * vol) if (close is not None and vol is not None) else None

        closes = g["close"].to_numpy(dtype=float)
        volumes = g["volume"].to_numpy(dtype=float)
        dollar_volumes = g["close"].to_numpy(dtype=float) * g["volume"].to_numpy(dtype=float)

        # returns
        def close_shift(n: int) -> float | None:
            if closes.size <= n:
                return None
            return float(closes[-(n + 1)])

        ret_1d = _pct_change(close, close_shift(1))
        ret_5d = _pct_change(close, close_shift(5))
        ret_20d = _pct_change(close, close_shift(20))
        ret_63d = _pct_change(close, close_shift(63))
        ret_252d = _pct_change(close, close_shift(252))

        # windowed metrics
        w20 = closes[-21:] if closes.size >= 21 else closes
        r1 = pd.Series(closes).pct_change().to_numpy(dtype=float)
        r20 = r1[-20:] if r1.size >= 20 else r1
        volatility_20d = _safe_float(np.nanstd(r20, ddof=1) if r20.size >= 2 else None)

        w252 = closes[-252:] if closes.size >= 252 else closes
        max_dd_252 = _max_drawdown(w252.astype(float)) if w252.size >= 2 else None
        # CRITICAL: Don't use current close in percentile rank - use previous close
        prev_close = closes[-2] if closes.size >= 2 else close
        price_pct_252 = _percentile_rank(w252.astype(float), float(prev_close)) if prev_close is not None else None

        # liquidity
        dv20 = dollar_volumes[-20:] if dollar_volumes.size >= 20 else dollar_volumes
        avg_dv20 = _safe_float(float(np.nanmean(dv20)) if dv20.size >= 1 else None)

        v20 = volumes[-20:] if volumes.size >= 20 else volumes
        v_mean = float(np.nanmean(v20)) if v20.size >= 1 else float("nan")
        v_std = float(np.nanstd(v20, ddof=1)) if v20.size >= 2 else float("nan")
        volume_z = None
        if vol is not None and v_std and not np.isnan(v_std) and v_std > 0:
            volume_z = (float(vol) - v_mean) / v_std

        dv_mean = float(np.nanmean(dv20)) if dv20.size >= 1 else float("nan")
        dv_std = float(np.nanstd(dv20, ddof=1)) if dv20.size >= 2 else float("nan")
        dv_z = None
        if dollar_volume is not None and dv_std and not np.isnan(dv_std) and dv_std > 0:
            dv_z = (float(dollar_volume) - dv_mean) / dv_std

        # fundamentals join
        revenue_ttm = None
        revenue_growth = None
        shares_out = None
        shares_growth = None
        sector = None
        industry = None
        if not fdf.empty:
            m = fdf[fdf["ticker"] == sym]
            if not m.empty:
                r0 = m.iloc[0].to_dict()
                revenue_ttm = _safe_float(r0.get("revenue_ttm"))
                revenue_growth = _safe_float(r0.get("revenue_growth"))
                shares_out = _safe_float(r0.get("shares_outstanding"))
                shares_growth = _safe_float(r0.get("shares_growth"))
                sector = (str(r0.get("sector")) if r0.get("sector") else None)
                industry = (str(r0.get("industry")) if r0.get("industry") else None)

        out[sym] = FeatureRow(
            symbol=sym,
            as_of_date=as_of_date.isoformat(),
            close=close,
            volume=vol,
            dollar_volume=_safe_float(dollar_volume),
            avg_dollar_volume_20d=avg_dv20,
            return_1d=_safe_float(ret_1d),
            return_5d=_safe_float(ret_5d),
            return_20d=_safe_float(ret_20d),
            return_63d=_safe_float(ret_63d),
            return_252d=_safe_float(ret_252d),
            volatility_20d=volatility_20d,
            max_drawdown_252d=_safe_float(max_dd_252),
            price_percentile_252d=_safe_float(price_pct_252),
            volume_zscore_20d=_safe_float(volume_z),
            dollar_volume_zscore_20d=_safe_float(dv_z),
            revenue_ttm=revenue_ttm,
            revenue_growth=revenue_growth,
            shares_outstanding=shares_out,
            shares_growth=shares_growth,
            sector=sector,
            industry=industry,
            sector_return_63d=None,
            peer_relative_return_63d=None,
            price_bucket=bucket_price(close),
        )

    # sector-relative returns (cheap peer proxy)
    by_sector: dict[str, list[float]] = {}
    for fr in out.values():
        if fr.sector and fr.return_63d is not None:
            by_sector.setdefault(fr.sector, []).append(float(fr.return_63d))
    sector_mean = {k: float(np.mean(v)) for k, v in by_sector.items() if v}

    if sector_mean:
        for sym, fr in list(out.items()):
            sec = fr.sector
            if not sec or fr.return_63d is None or sec not in sector_mean:
                continue
            sr = sector_mean[sec]
            out[sym] = FeatureRow(**{**asdict(fr), "sector_return_63d": sr, "peer_relative_return_63d": float(fr.return_63d) - sr})

    return out
