"""Read-model helpers for advanced intelligence API endpoints."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.internal_read_v1.chart_symbols import normalize_ticker


def _loads_list(raw: str | None) -> list[Any]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def list_strategies_catalog(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    status: str | None,
    track: str | None,
    active_only: bool,
    limit: int,
) -> dict[str, Any]:
    where = ["tenant_id = ?"]
    params: list[Any] = [tenant_id]
    if status:
        where.append("status = ?")
        params.append(status.strip().upper())
    if track:
        where.append("LOWER(track) = ?")
        params.append(track.strip().lower())
    if active_only:
        where.append("active = 1")
    params.append(int(limit))
    rows = conn.execute(
        f"""
        SELECT id, name, version, strategy_type, mode, track, status, active, is_champion,
               backtest_score, forward_score, live_score, stability_score, sample_size,
               created_at, activated_at, deactivated_at
        FROM strategies
        WHERE {" AND ".join(where)}
        ORDER BY is_champion DESC, status ASC, stability_score DESC, created_at DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    strategies = [
        {
            "id": str(r["id"]),
            "name": str(r["name"]),
            "version": str(r["version"]),
            "strategyType": str(r["strategy_type"]),
            "mode": str(r["mode"]),
            "track": str(r["track"]),
            "status": str(r["status"]),
            "active": bool(r["active"]),
            "isChampion": bool(r["is_champion"]),
            "backtestScore": float(r["backtest_score"]),
            "forwardScore": float(r["forward_score"]),
            "liveScore": float(r["live_score"]),
            "stabilityScore": float(r["stability_score"]),
            "sampleSize": int(r["sample_size"]),
            "createdAt": str(r["created_at"]),
            "activatedAt": str(r["activated_at"]) if r["activated_at"] is not None else None,
            "deactivatedAt": str(r["deactivated_at"]) if r["deactivated_at"] is not None else None,
        }
        for r in rows
    ]
    return {"tenant_id": tenant_id, "count": len(strategies), "strategies": strategies}


def get_strategy_stability(conn: sqlite3.Connection, *, tenant_id: str, strategy_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT s.id, s.name, s.version, s.track, s.status,
               ss.backtest_accuracy, ss.live_accuracy, ss.stability_score, ss.updated_at
        FROM strategy_stability ss
        JOIN strategies s ON s.tenant_id = ss.tenant_id AND s.id = ss.strategy_id
        WHERE ss.tenant_id = ? AND ss.strategy_id = ?
        LIMIT 1
        """,
        (tenant_id, strategy_id),
    ).fetchone()
    if not row:
        return None
    return {
        "strategyId": str(row["id"]),
        "name": str(row["name"]),
        "version": str(row["version"]),
        "track": str(row["track"]),
        "status": str(row["status"]),
        "backtestAccuracy": float(row["backtest_accuracy"]),
        "liveAccuracy": float(row["live_accuracy"]),
        "stabilityScore": float(row["stability_score"]),
        "updatedAt": str(row["updated_at"]),
    }


def get_regime_performance(conn: sqlite3.Connection, *, tenant_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT regime, prediction_count, accuracy, avg_return, updated_at
        FROM regime_performance
        WHERE tenant_id = ?
        ORDER BY updated_at DESC, regime ASC
        """,
        (tenant_id,),
    ).fetchall()
    return {
        "tenant_id": tenant_id,
        "regimes": [
            {
                "regime": str(r["regime"]),
                "predictionCount": int(r["prediction_count"]),
                "accuracy": float(r["accuracy"]),
                "avgReturn": float(r["avg_return"]),
                "updatedAt": str(r["updated_at"]),
            }
            for r in rows
        ],
    }


def get_consensus_signals(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    limit: int,
    min_p_final: float | None,
    ticker: str | None,
) -> dict[str, Any]:
    where = ["cs.tenant_id = ?"]
    params: list[Any] = [tenant_id]
    if min_p_final is not None:
        where.append("cs.p_final >= ?")
        params.append(float(min_p_final))
    if ticker:
        where.append("cs.ticker = ?")
        params.append(normalize_ticker(ticker))
    params.append(int(limit))
    rows = conn.execute(
        f"""
        SELECT cs.*
        FROM consensus_signals cs
        JOIN (
          SELECT tenant_id, ticker, MAX(created_at) AS max_created_at
          FROM consensus_signals
          WHERE tenant_id = ?
          GROUP BY tenant_id, ticker
        ) latest
          ON latest.tenant_id = cs.tenant_id
         AND latest.ticker = cs.ticker
         AND latest.max_created_at = cs.created_at
        WHERE {" AND ".join(where)}
        ORDER BY cs.p_final DESC, cs.created_at DESC
        LIMIT ?
        """,
        [tenant_id, *params],
    ).fetchall()
    signals = [
        {
            "ticker": str(r["ticker"]),
            "regime": str(r["regime"]),
            "sentimentStrategyId": str(r["sentiment_strategy_id"]) if r["sentiment_strategy_id"] is not None else None,
            "quantStrategyId": str(r["quant_strategy_id"]) if r["quant_strategy_id"] is not None else None,
            "sentimentScore": float(r["sentiment_score"]),
            "quantScore": float(r["quant_score"]),
            "ws": float(r["ws"]),
            "wq": float(r["wq"]),
            "agreementBonus": float(r["agreement_bonus"]),
            "pFinal": float(r["p_final"]),
            "stabilityScore": float(r["stability_score"]),
            "createdAt": str(r["created_at"]),
        }
        for r in rows
    ]
    return {"tenant_id": tenant_id, "count": len(signals), "signals": signals}


def get_ticker_attribution(
    conn: sqlite3.Connection, *, tenant_id: str, symbol: str, limit: int
) -> dict[str, Any]:
    ticker = normalize_ticker(symbol)
    rows = conn.execute(
        """
        SELECT id, category, materiality, direction, confidence,
               concept_tags_json, explanation_terms_json
        FROM scored_events
        WHERE tenant_id = ? AND primary_ticker = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (tenant_id, ticker, int(limit)),
    ).fetchall()
    items = [
        {
            "scoredEventId": str(r["id"]),
            "category": str(r["category"]),
            "materiality": float(r["materiality"]),
            "direction": str(r["direction"]),
            "confidence": float(r["confidence"]),
            "conceptTags": _loads_list(str(r["concept_tags_json"]) if r["concept_tags_json"] is not None else None),
            "explanationTerms": _loads_list(
                str(r["explanation_terms_json"]) if r["explanation_terms_json"] is not None else None
            ),
        }
        for r in rows
    ]
    return {"ticker": ticker, "tenant_id": tenant_id, "count": len(items), "attribution": items}


def get_ticker_accuracy(conn: sqlite3.Connection, *, tenant_id: str, symbol: str) -> dict[str, Any]:
    ticker = normalize_ticker(symbol)
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS sample_count,
          AVG(CASE WHEN po.direction_correct = 1 THEN 1.0 ELSE 0.0 END) AS hit_rate,
          AVG(po.residual_alpha) AS avg_residual_alpha
        FROM prediction_outcomes po
        JOIN predictions p
          ON p.tenant_id = po.tenant_id
         AND p.id = po.prediction_id
        WHERE po.tenant_id = ? AND p.ticker = ?
        """,
        (tenant_id, ticker),
    ).fetchone()
    count = int(row["sample_count"]) if row and row["sample_count"] is not None else 0
    return {
        "ticker": ticker,
        "tenant_id": tenant_id,
        "sampleCount": count,
        "hitRate": float(row["hit_rate"]) if row and row["hit_rate"] is not None else None,
        "avgResidualAlpha": float(row["avg_residual_alpha"]) if row and row["avg_residual_alpha"] is not None else None,
    }


def get_system_heartbeat(conn: sqlite3.Connection, *, tenant_id: str, limit: int) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT loop_type, status, notes, created_at
        FROM loop_heartbeats
        WHERE tenant_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (tenant_id, int(limit)),
    ).fetchall()
    latest: dict[str, dict[str, Any]] = {}
    for r in rows:
        lt = str(r["loop_type"])
        if lt not in latest:
            latest[lt] = {
                "loopType": lt,
                "status": str(r["status"]),
                "notes": str(r["notes"]) if r["notes"] is not None else None,
                "createdAt": str(r["created_at"]),
            }
    return {
        "tenant_id": tenant_id,
        "loops": list(latest.values()),
    }


def get_prediction_run_latest(conn: sqlite3.Connection, *, tenant_id: str, timeframe: str | None) -> dict[str, Any] | None:
    if timeframe:
        row = conn.execute(
            """
            SELECT id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime, created_at
            FROM prediction_runs
            WHERE tenant_id = ? AND timeframe = ?
            ORDER BY prediction_end DESC, created_at DESC
            LIMIT 1
            """,
            (tenant_id, str(timeframe)),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime, created_at
            FROM prediction_runs
            WHERE tenant_id = ?
            ORDER BY prediction_end DESC, created_at DESC
            LIMIT 1
            """,
            (tenant_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "id": str(row["id"]),
        "tenant_id": tenant_id,
        "timeframe": str(row["timeframe"]),
        "regime": str(row["regime"]) if row["regime"] is not None else None,
        "ingressStart": str(row["ingress_start"]),
        "ingressEnd": str(row["ingress_end"]),
        "predictionStart": str(row["prediction_start"]),
        "predictionEnd": str(row["prediction_end"]),
        "createdAt": str(row["created_at"]),
    }
