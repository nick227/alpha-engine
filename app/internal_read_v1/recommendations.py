"""House recommendation read model and builder for /api/recommendations/*."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Literal

from app.core.active_universe import get_active_universe_tickers
from app.internal_read_v1.chart_market import build_stats_payload
from app.internal_read_v1.chart_symbols import normalize_ticker

RecommendationMode = Literal["conservative", "balanced", "aggressive", "long_term"]
BestPreference = Literal["absolute", "long_only"]

_VALID_MODES: set[str] = {"conservative", "balanced", "aggressive", "long_term"}
_VALID_BEST_PREFERENCES: set[str] = {"absolute", "long_only"}
_MODE_WEIGHTS: dict[str, dict[str, float]] = {
    "conservative": {"ranking": 0.35, "consensus": 0.35, "momentum": 0.10, "admission": 0.20},
    "balanced": {"ranking": 0.30, "consensus": 0.40, "momentum": 0.20, "admission": 0.10},
    "aggressive": {"ranking": 0.20, "consensus": 0.45, "momentum": 0.30, "admission": 0.05},
    "long_term": {"ranking": 0.45, "consensus": 0.30, "momentum": 0.05, "admission": 0.20},
}
_MODE_HORIZON = {
    "conservative": "2-8 weeks",
    "balanced": "2-6 weeks",
    "aggressive": "1-4 weeks",
    "long_term": "90d+",
}


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS house_recommendations (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            mode TEXT NOT NULL,
            ticker TEXT NOT NULL,
            action TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            score REAL NOT NULL,
            risk TEXT NOT NULL,
            horizon TEXT NOT NULL,
            entry_min REAL,
            entry_max REAL,
            thesis_json TEXT NOT NULL DEFAULT '[]',
            avoid_if_json TEXT NOT NULL DEFAULT '[]',
            as_of TEXT NOT NULL,
            source_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (tenant_id, mode, ticker)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_house_recommendations_rank ON house_recommendations(tenant_id, mode, confidence DESC, score DESC)"
    )
    conn.commit()


def parse_mode(mode: str | None) -> RecommendationMode:
    m = str(mode or "balanced").strip().lower()
    if m not in _VALID_MODES:
        raise ValueError("invalid mode; use conservative, balanced, aggressive, or long_term")
    return m  # type: ignore[return-value]


def parse_best_preference(value: str | None) -> BestPreference:
    v = str(value or "absolute").strip().lower()
    if v not in _VALID_BEST_PREFERENCES:
        raise ValueError("invalid preference; use absolute or long_only")
    return v  # type: ignore[return-value]


def _latest_rankings(conn: sqlite3.Connection, *, tenant_id: str) -> dict[str, float]:
    row = conn.execute(
        "SELECT MAX(timestamp) AS ts FROM ranking_snapshots WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()
    if not row or row["ts"] is None:
        return {}
    ts = str(row["ts"])
    rows = conn.execute(
        """
        SELECT ticker, score
        FROM ranking_snapshots
        WHERE tenant_id = ? AND timestamp = ?
        """,
        (tenant_id, ts),
    ).fetchall()
    return {normalize_ticker(str(r["ticker"])): float(r["score"]) for r in rows}


def _latest_consensus(conn: sqlite3.Connection, *, tenant_id: str) -> dict[str, float]:
    rows = conn.execute(
        """
        SELECT ticker, p_final
        FROM consensus_signals
        WHERE tenant_id = ?
          AND created_at = (
            SELECT MAX(cs2.created_at)
            FROM consensus_signals cs2
            WHERE cs2.tenant_id = consensus_signals.tenant_id
              AND cs2.ticker = consensus_signals.ticker
          )
        """,
        (tenant_id,),
    ).fetchall()
    return {normalize_ticker(str(r["ticker"])): float(r["p_final"]) for r in rows}


def _candidate_status(conn: sqlite3.Connection, *, tenant_id: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT ticker, status FROM candidate_queue WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchall()
    return {normalize_ticker(str(r["ticker"])): str(r["status"]).strip().lower() for r in rows}


def _momentum_signal(day_change_pct: float | None) -> float:
    if day_change_pct is None:
        return 0.0
    # Clip into [-1, 1] using 5% daily move as full-scale.
    return max(-1.0, min(1.0, float(day_change_pct) / 5.0))


def _admission_signal(status: str | None) -> float:
    st = str(status or "").strip().lower()
    if st == "admitted":
        return 1.0
    if st == "shortlisted":
        return 0.5
    if st == "rejected":
        return -0.5
    return 0.0


def _risk_bucket(day_change_pct: float | None) -> str:
    v = abs(float(day_change_pct or 0.0))
    if v >= 3.0:
        return "High"
    if v >= 1.5:
        return "Moderate"
    return "Low"


def _action_from_score(score: float, *, mode: RecommendationMode) -> str:
    buy_thr = 0.30 if mode == "conservative" else 0.20
    sell_thr = -0.30 if mode == "conservative" else -0.20
    if score >= buy_thr:
        return "BUY"
    if score <= sell_thr:
        return "SELL"
    return "HOLD"


def _entry_zone(price: float, action: str) -> tuple[float, float]:
    p = float(price)
    if action == "BUY":
        return (round(p * 0.99, 4), round(p * 1.01, 4))
    if action == "SELL":
        return (round(p * 0.995, 4), round(p * 1.015, 4))
    return (round(p * 0.995, 4), round(p * 1.005, 4))


def _build_thesis(*, action: str, ranking: float, consensus: float, day_change_pct: float | None) -> list[str]:
    thesis: list[str] = []
    if action == "BUY":
        thesis.append("Composite model bias is bullish")
    elif action == "SELL":
        thesis.append("Composite model bias is bearish")
    else:
        thesis.append("Signals are mixed; conviction is muted")

    if ranking > 0:
        thesis.append("Ranking layer contributes positive conviction")
    elif ranking < 0:
        thesis.append("Ranking layer contributes negative conviction")

    if consensus > 0.15:
        thesis.append("Consensus track agreement supports upside")
    elif consensus < -0.15:
        thesis.append("Consensus track agreement supports downside")
    else:
        thesis.append("Consensus layer remains near neutral")

    if day_change_pct is not None:
        if day_change_pct > 0:
            thesis.append("Short-term momentum is positive")
        elif day_change_pct < 0:
            thesis.append("Short-term momentum is negative")
    return thesis[:3]


def _build_avoid_if(*, action: str, entry_min: float, entry_max: float) -> list[str]:
    if action == "BUY":
        return [f"Breaks below {round(entry_min * 0.975, 4)}", "Broad market shifts to risk-off"]
    if action == "SELL":
        return [f"Reclaims above {round(entry_max * 1.015, 4)}", "Broad market shifts to risk-on"]
    return ["Conviction remains below threshold", "Macro regime flips abruptly"]


def rebuild_house_recommendations(
    conn: sqlite3.Connection,
    *,
    tenant_id: str = "default",
    mode: RecommendationMode = "balanced",
    now: datetime | None = None,
) -> int:
    _ensure_table(conn)
    as_of = (now or datetime.now(timezone.utc)).isoformat()
    weights = _MODE_WEIGHTS[str(mode)]

    ranking_map = _latest_rankings(conn, tenant_id=tenant_id)
    consensus_map = _latest_consensus(conn, tenant_id=tenant_id)
    status_map = _candidate_status(conn, tenant_id=tenant_id)
    universe = get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn)

    conn.execute("DELETE FROM house_recommendations WHERE tenant_id = ? AND mode = ?", (tenant_id, mode))

    rows_in: list[tuple[Any, ...]] = []
    for t in universe:
        ticker = normalize_ticker(t)
        stats = build_stats_payload(conn, tenant_id=tenant_id, ticker=ticker)
        if not stats:
            continue
        price = float(stats["price"])
        rank_score = float(ranking_map.get(ticker, 0.0))
        consensus_score = float(consensus_map.get(ticker, 0.0))
        mom_score = _momentum_signal(stats.get("dayChangePct"))
        admission_score = _admission_signal(status_map.get(ticker))

        composite = (
            (weights["ranking"] * rank_score)
            + (weights["consensus"] * consensus_score)
            + (weights["momentum"] * mom_score)
            + (weights["admission"] * admission_score)
        )
        action = _action_from_score(composite, mode=mode)
        confidence = int(max(5, min(99, round(abs(composite) * 100.0))))
        risk = _risk_bucket(stats.get("dayChangePct"))
        horizon = _MODE_HORIZON[str(mode)]
        entry_min, entry_max = _entry_zone(price, action)
        thesis = _build_thesis(
            action=action,
            ranking=rank_score,
            consensus=consensus_score,
            day_change_pct=stats.get("dayChangePct"),
        )
        avoid_if = _build_avoid_if(action=action, entry_min=entry_min, entry_max=entry_max)

        source = {
            "ranking_score": round(rank_score, 6),
            "consensus_p_final": round(consensus_score, 6),
            "momentum_signal": round(mom_score, 6),
            "admission_signal": round(admission_score, 6),
            "weights": weights,
        }
        rows_in.append(
            (
                tenant_id,
                str(mode),
                ticker,
                action,
                confidence,
                round(composite, 6),
                risk,
                horizon,
                entry_min,
                entry_max,
                json.dumps(thesis),
                json.dumps(avoid_if),
                as_of,
                json.dumps(source),
            )
        )

    if rows_in:
        conn.executemany(
            """
            INSERT OR REPLACE INTO house_recommendations
              (tenant_id, mode, ticker, action, confidence, score, risk, horizon,
               entry_min, entry_max, thesis_json, avoid_if_json, as_of, source_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_in,
        )
    conn.commit()
    return len(rows_in)


def _row_to_payload(row: sqlite3.Row, *, mode: RecommendationMode) -> dict[str, Any]:
    thesis = json.loads(str(row["thesis_json"] or "[]"))
    avoid_if = json.loads(str(row["avoid_if_json"] or "[]"))
    return {
        "ticker": str(row["ticker"]),
        "action": str(row["action"]),
        "confidence": int(row["confidence"]),
        "score": round(float(row["score"]), 4),
        "risk": str(row["risk"]),
        "horizon": str(row["horizon"]),
        "entryZone": [float(row["entry_min"]), float(row["entry_max"])],
        "thesis": thesis if isinstance(thesis, list) else [],
        "avoidIf": avoid_if if isinstance(avoid_if, list) else [],
        "mode": str(mode),
        "asOf": str(row["as_of"]),
    }


def get_recommendations_latest(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    mode: RecommendationMode,
    preference: BestPreference = "absolute",
    limit: int = 10,
) -> list[dict[str, Any]]:
    rebuild_house_recommendations(conn, tenant_id=tenant_id, mode=mode)
    if preference == "long_only":
        rows = conn.execute(
            """
            SELECT *
            FROM house_recommendations
            WHERE tenant_id = ? AND mode = ? AND action = 'BUY'
            ORDER BY confidence DESC, score DESC
            LIMIT ?
            """,
            (tenant_id, mode, int(limit)),
        ).fetchall()
        # Fallback: if no BUY recommendations, return absolute set so list is never empty.
        if not rows:
            rows = conn.execute(
                """
                SELECT *
                FROM house_recommendations
                WHERE tenant_id = ? AND mode = ?
                ORDER BY confidence DESC, score DESC
                LIMIT ?
                """,
                (tenant_id, mode, int(limit)),
            ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM house_recommendations
            WHERE tenant_id = ? AND mode = ?
            ORDER BY confidence DESC, score DESC
            LIMIT ?
            """,
            (tenant_id, mode, int(limit)),
        ).fetchall()
    payload = [_row_to_payload(r, mode=mode) for r in rows]
    for p in payload:
        p["selectionPreference"] = preference
    return payload


def get_recommendation_best(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    mode: RecommendationMode,
    preference: BestPreference = "absolute",
) -> dict[str, Any] | None:
    rebuild_house_recommendations(conn, tenant_id=tenant_id, mode=mode)
    if preference == "long_only":
        row = conn.execute(
            """
            SELECT *
            FROM house_recommendations
            WHERE tenant_id = ? AND mode = ? AND action = 'BUY'
            ORDER BY confidence DESC, score DESC
            LIMIT 1
            """,
            (tenant_id, mode),
        ).fetchone()
        if row is None:
            # fallback to absolute best when no BUY exists
            row = conn.execute(
                """
                SELECT *
                FROM house_recommendations
                WHERE tenant_id = ? AND mode = ?
                ORDER BY confidence DESC, score DESC
                LIMIT 1
                """,
                (tenant_id, mode),
            ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT *
            FROM house_recommendations
            WHERE tenant_id = ? AND mode = ?
            ORDER BY confidence DESC, score DESC
            LIMIT 1
            """,
            (tenant_id, mode),
        ).fetchone()
    if not row:
        return None
    payload = _row_to_payload(row, mode=mode)
    payload["selectionPreference"] = preference
    return payload


def get_recommendation_for_ticker(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    mode: RecommendationMode,
    ticker: str,
) -> dict[str, Any] | None:
    rebuild_house_recommendations(conn, tenant_id=tenant_id, mode=mode)
    row = conn.execute(
        """
        SELECT *
        FROM house_recommendations
        WHERE tenant_id = ? AND mode = ? AND ticker = ?
        LIMIT 1
        """,
        (tenant_id, mode, normalize_ticker(ticker)),
    ).fetchone()
    if not row:
        return None
    return _row_to_payload(row, mode=mode)
