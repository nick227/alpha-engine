"""Built-in, structured, deterministic strategies (P0: one).

A strategy is a validated configuration, not code a user supplies. Evaluation is pure
Python with a fixed operation order so the worker and Railway's validator compute
bit-identical results from the same bars (no numpy/pandas version drift).

Evaluation honesty: parameters are fixed a priori (not fitted), so the holdout is
pseudo-out-of-sample — that period was observable when the rule was chosen. The
publication says so (genuineOutOfSample: false) instead of claiming more.
"""
from __future__ import annotations

from typing import Any

TREND_MOMENTUM_V1: dict[str, Any] = {
    "id": "trend_momentum",
    "version": 1,
    "dataset": "price_bars_1d",
    "signal": {"rule": "close_above_sma_and_positive_momentum", "smaSessions": 100, "momentumSessions": 20},
    "direction": "BUY",
    "horizonSessions": 5,
    "evaluation": {"windowSessions": 504, "holdoutSessions": 126, "embargoSessions": 5, "minHoldoutSignals": 30},
    "parametersFitted": False,
}
STRATEGIES = {f"{TREND_MOMENTUM_V1['id']}_v{TREND_MOMENTUM_V1['version']}": TREND_MOMENTUM_V1}


def input_sessions(cfg: dict[str, Any]) -> int:
    """Bars per ticker the strategy consumes: indicator warm-up + evaluation window."""
    return int(cfg["signal"]["smaSessions"]) + int(cfg["evaluation"]["windowSessions"])


def _r(x: float, nd: int = 6) -> float:
    return round(float(x), nd)


def _stats(signals: int, hits: int, up: int, total: int, start: str | None, end: str | None) -> dict[str, Any]:
    return {
        "start": start, "end": end,
        "signals": signals, "hits": hits,
        "precision": _r(hits / signals) if signals else None,
        "samples": total, "baselineUp": up,
        "baselineRate": _r(up / total) if total else None,
    }


def evaluate(bars_by_ticker: dict[str, list[list[Any]]], cfg: dict[str, Any] = TREND_MOMENTUM_V1) -> dict[str, Any]:
    """bars: [[date, open, high, low, close, volume], ...] ascending, exactly input_sessions(cfg) per ticker."""
    sma_n = int(cfg["signal"]["smaSessions"])
    mom_n = int(cfg["signal"]["momentumSessions"])
    h = int(cfg["horizonSessions"])
    ev = cfg["evaluation"]
    window, holdout, embargo = int(ev["windowSessions"]), int(ev["holdoutSessions"]), int(ev["embargoSessions"])
    need = input_sessions(cfg)
    hold_start = window - holdout - h       # holdout samples whose outcome is known
    cal_end = hold_start - embargo          # calibration ends an embargo before holdout

    acc = {"calibration": [0, 0, 0, 0, None, None], "holdout": [0, 0, 0, 0, None, None]}
    latest: dict[str, dict[str, Any]] = {}
    for ticker in sorted(bars_by_ticker):
        bars = bars_by_ticker[ticker]
        if len(bars) != need:
            raise ValueError(f"{ticker}: expected {need} bars, got {len(bars)}")
        closes = [float(b[4]) for b in bars]
        dates = [str(b[0]) for b in bars]
        for i in range(window):
            j = sma_n + i
            sma = sum(closes[j - sma_n + 1 : j + 1]) / sma_n
            mom = closes[j] / closes[j - mom_n] - 1.0
            sig = closes[j] > sma and mom > 0.0
            if i == window - 1:
                latest[ticker] = {"date": dates[j], "close": closes[j], "sma": sma, "momentum": mom, "signal": sig}
            if j + h >= need:
                continue
            up = closes[j + h] / closes[j] - 1.0 > 0.0
            part = "calibration" if i < cal_end else "holdout" if i >= hold_start else None
            if part is None:
                continue
            a = acc[part]
            a[2] += up
            a[3] += 1
            if sig:
                a[0] += 1
                a[1] += up
            a[4] = a[4] if a[4] and a[4] <= dates[j] else dates[j]
            a[5] = a[5] if a[5] and a[5] >= dates[j] else dates[j]

    cal = _stats(*acc["calibration"])
    hold = _stats(*acc["holdout"])
    edge = hold["precision"] is not None and hold["baselineRate"] is not None and hold["precision"] > hold["baselineRate"]
    sufficient = hold["signals"] >= int(ev["minHoldoutSignals"])
    evaluation = {
        "method": "calibration_then_embargoed_holdout",
        "parametersFitted": bool(cfg.get("parametersFitted")),
        "genuineOutOfSample": False,
        "note": "Rule parameters are fixed a priori; the holdout was observable when the rule was chosen (pseudo-out-of-sample).",
        "inSample": cal,
        "holdout": hold,
        "edgeOverBaseline": edge,
        "sufficientHoldout": sufficient,
    }
    confidence = hold["precision"]
    predictions, no_signal = [], []
    for ticker in sorted(latest):
        x = latest[ticker]
        if not x["signal"]:
            no_signal.append(ticker)
            continue
        predictions.append({
            "ticker": ticker,
            "observedAt": f"{x['date']}T00:00:00+00:00",
            "direction": cfg["direction"],
            "horizon": f"{h}d",
            "entryPrice": x["close"],
            "confidence": confidence,
            "features": {"close": _r(x["close"]), f"sma{sma_n}": _r(x["sma"]), f"momentum{mom_n}": _r(x["momentum"])},
        })
    return {"evaluation": evaluation, "predictions": predictions, "noSignal": no_signal}
