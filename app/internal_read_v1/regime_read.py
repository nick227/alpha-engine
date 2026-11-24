"""Daily SMA regime read model for GET /api/regime/{ticker}."""

from __future__ import annotations

import math
import sqlite3
from typing import Any, Literal

from app.internal_read_v1.chart_symbols import normalize_ticker

RegimeLabel = Literal["risk_on", "risk_off"]
MIN_DAILY_BARS = 200
_SCORE_K = 6.25


def _rolling_sma(values: list[float], window: int) -> list[float]:
    out: list[float] = []
    run = 0.0
    for i, v in enumerate(values):
        run += v
        if i >= window:
            run -= values[i - window]
        if i >= window - 1:
            out.append(run / window)
        else:
            out.append(float("nan"))
    return out


def _classify(close: float, sma200: float) -> RegimeLabel:
    return "risk_on" if close >= sma200 else "risk_off"


def _score(close: float, sma200: float, regime: RegimeLabel) -> float:
    if sma200 <= 0:
        return 0.5
    bias = (close - sma200) / sma200
    raw = 0.5 + bias * _SCORE_K if regime == "risk_on" else 0.5 - bias * _SCORE_K
    return round(max(0.0, min(1.0, raw)), 2)


def _confirmed_bars(closes: list[float], sma200s: list[float]) -> tuple[RegimeLabel, int]:
    n = len(closes)
    current = _classify(closes[-1], sma200s[-1])
    cnt = 0
    # SMA(200) undefined before index 199
    for i in range(n - 1, 198, -1):
        if math.isnan(sma200s[i]):
            break
        if _classify(closes[i], sma200s[i]) != current:
            break
        cnt += 1
        if cnt >= 5:
            break
    return current, cnt


def regime_series(dates: list[str], closes: list[float]) -> list[dict[str, Any]]:
    """Regime as it was known at each session's close (only bars up to that session are used).

    The same classification as build_regime_payload, evaluated point-in-time for every session
    with at least MIN_DAILY_BARS of history: entry i equals build_regime_payload over bars[:i+1].
    """
    sma20s = _rolling_sma(closes, 20)
    sma200s = _rolling_sma(closes, 200)
    out: list[dict[str, Any]] = []
    for i in range(MIN_DAILY_BARS - 1, len(closes)):
        regime, confirmed = _confirmed_bars(closes[: i + 1], sma200s[: i + 1])
        out.append({
            "asOf": dates[i],
            "regime": regime,
            "score": _score(closes[i], sma200s[i], regime),
            "sma20": round(sma20s[i], 4),
            "sma200": round(sma200s[i], 4),
            "close": round(closes[i], 4),
            "confirmedBars": confirmed,
        })
    return out


def build_regime_payload(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
) -> dict[str, Any] | None:
    sym = normalize_ticker(ticker)
    rows = conn.execute(
        """
        SELECT timestamp, close
        FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
        ORDER BY timestamp ASC
        """,
        (tenant_id, sym),
    ).fetchall()
    if len(rows) < MIN_DAILY_BARS:
        return None

    dates: list[str] = []
    closes: list[float] = []
    for r in rows:
        ts = str(r["timestamp"])
        close = float(r["close"])
        dates.append(ts[:10] if len(ts) >= 10 else ts)
        closes.append(close)

    sma20s = _rolling_sma(closes, 20)
    sma200s = _rolling_sma(closes, 200)
    close = closes[-1]
    sma20 = sma20s[-1]
    sma200 = sma200s[-1]
    as_of = dates[-1]

    regime, confirmed = _confirmed_bars(closes, sma200s)
    score = _score(close, sma200, regime)

    return {
        "ticker": sym,
        "regime": regime,
        "score": score,
        "asOf": as_of,
        "sma20": round(sma20, 4),
        "sma200": round(sma200, 4),
        "close": round(close, 4),
        "confirmedBars": confirmed,
    }
