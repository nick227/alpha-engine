from __future__ import annotations

import json
import math
import os
from typing import Any

from app.db.repository import AlphaRepository

META_RANKER_MIN_LIQUIDITY = float(os.getenv("META_RANKER_MIN_LIQUIDITY", "1000000"))
META_RANKER_MIN_CONFIDENCE = float(os.getenv("META_RANKER_MIN_CONFIDENCE", "0.42"))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _load_json_dict(raw: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(raw or "{}"))
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _safe_float(v: Any, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


def _latest_price_series(conn: Any, *, tenant_id: str, symbol: str, n: int = 25) -> list[tuple[float, float]]:
    rows = conn.execute(
        """
        SELECT close, volume
        FROM price_bars
        WHERE tenant_id = ? AND timeframe = '1d' AND ticker = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (str(tenant_id), str(symbol), int(n)),
    ).fetchall()
    return [(float(r["close"]), float(r["volume"])) for r in rows if r["close"] is not None and r["volume"] is not None]


def _strategy_stats(conn: Any, *, tenant_id: str, strategy: str) -> tuple[float, float]:
    w5 = conn.execute(
        """
        SELECT accuracy
        FROM strategy_performance
        WHERE tenant_id = ? AND strategy_id = ? AND LOWER(horizon) = '5d'
        LIMIT 1
        """,
        (str(tenant_id), str(strategy)),
    ).fetchone()
    w20 = conn.execute(
        """
        SELECT accuracy
        FROM strategy_performance
        WHERE tenant_id = ? AND strategy_id = ? AND LOWER(horizon) = '20d'
        LIMIT 1
        """,
        (str(tenant_id), str(strategy)),
    ).fetchone()
    win = float(w5["accuracy"]) if w5 and w5["accuracy"] is not None else 0.5
    decay = win - (float(w20["accuracy"]) if w20 and w20["accuracy"] is not None else win)
    return win, decay


def _infer_regime(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "neutral"
    avg_mom = sum(float(r["momentum_20d"]) for r in rows) / len(rows)
    avg_vol = sum(float(r["volatility_20d"]) for r in rows) / len(rows)
    if avg_vol > 0.06:
        return "risk_off"
    if avg_mom > 0.02:
        return "risk_on"
    return "chop"


def build_meta_ranker_feature_rows(
    *,
    repo: AlphaRepository,
    as_of_date: str,
    tenant_id: str = "default",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    queue_rows = repo.list_prediction_queue(
        as_of_date=str(as_of_date),
        status="pending",
        limit=5000,
        tenant_id=str(tenant_id),
    )
    prepared: list[dict[str, Any]] = []
    dropped = {"illiquid": 0, "stale": 0, "weak_confidence": 0}

    for row in queue_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        md = _load_json_dict(row.get("metadata_json"))
        avg_score = _safe_float(md.get("avg_score", md.get("raw_score", 0.0)))
        days_seen = _safe_float(md.get("days_seen", 0.0))
        claim_count = _safe_float(md.get("claim_count", 1.0), 1.0)
        overlap_count = _safe_float(md.get("overlap_count", 1.0), 1.0)
        strategy = str(md.get("strategy") or md.get("primary_strategy") or "unknown")

        series = _latest_price_series(repo.conn, tenant_id=str(tenant_id), symbol=symbol, n=25)
        if not series:
            dropped["stale"] += 1
            continue
        closes = [x[0] for x in series]
        latest_close = closes[0]
        latest_volume = series[0][1]
        liquidity = latest_close * latest_volume
        if liquidity < META_RANKER_MIN_LIQUIDITY:
            dropped["illiquid"] += 1
            continue
        if avg_score < META_RANKER_MIN_CONFIDENCE:
            dropped["weak_confidence"] += 1
            continue

        c5 = closes[5] if len(closes) > 5 and closes[5] > 0 else closes[-1]
        c20 = closes[20] if len(closes) > 20 and closes[20] > 0 else closes[-1]
        momentum_5d = (latest_close / c5) - 1.0 if c5 > 0 else 0.0
        momentum_20d = (latest_close / c20) - 1.0 if c20 > 0 else 0.0

        rets = []
        for i in range(min(20, len(closes) - 1)):
            prev = closes[i + 1]
            cur = closes[i]
            if prev > 0:
                rets.append((cur / prev) - 1.0)
        volatility_20d = float(math.sqrt(sum(x * x for x in rets) / len(rets))) if rets else 0.0

        strategy_win, strategy_decay = _strategy_stats(repo.conn, tenant_id=str(tenant_id), strategy=strategy)
        prepared.append(
            {
                "as_of_date": str(row["as_of_date"]),
                "symbol": symbol,
                "source": str(row.get("source") or "discovery"),
                "metadata": md,
                "avg_score": avg_score,
                "days_seen": days_seen,
                "claim_count": claim_count,
                "overlap_count": overlap_count,
                "liquidity": liquidity,
                "momentum_5d": momentum_5d,
                "momentum_20d": momentum_20d,
                "volatility_20d": volatility_20d,
                "strategy": strategy,
                "strategy_win_rate": strategy_win,
                "strategy_decay": strategy_decay,
            }
        )

    if not prepared:
        return [], dropped

    # Queue-relative sector strength proxy: z-score of 20d momentum.
    mean_m20 = sum(float(r["momentum_20d"]) for r in prepared) / len(prepared)
    var_m20 = sum((float(r["momentum_20d"]) - mean_m20) ** 2 for r in prepared) / len(prepared)
    std_m20 = math.sqrt(var_m20) if var_m20 > 1e-12 else 1.0
    for r in prepared:
        r["sector_strength"] = (float(r["momentum_20d"]) - mean_m20) / std_m20

    regime = _infer_regime(prepared)
    for r in prepared:
        r["regime"] = regime
        base_score = (
            (0.60 * _clamp(float(r["avg_score"]), 0.0, 1.0))
            + (0.15 * _clamp(float(r["claim_count"]) / 5.0, 0.0, 1.0))
            + (0.15 * _clamp(float(r["overlap_count"]) / 5.0, 0.0, 1.0))
            + (0.10 * _clamp(float(r["days_seen"]) / 10.0, 0.0, 1.0))
        )
        r["base_score"] = _clamp(base_score, 0.0, 1.0)

    return prepared, dropped
