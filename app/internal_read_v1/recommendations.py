"""House recommendation read model and builder for /api/recommendations/*."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Literal

from app.core.active_universe import get_active_universe_tickers
from app.core.pipeline_gates import recommendation_quality
from app.internal_read_v1.chart_market import build_quote_payload, build_stats_payload, load_company_profile_json
from app.internal_read_v1.chart_symbols import normalize_ticker

RecommendationMode = Literal["conservative", "balanced", "aggressive", "long_term"]
BestPreference = Literal["absolute", "long_only"]

_VALID_MODES: set[str] = {"conservative", "balanced", "aggressive", "long_term"}
_VALID_BEST_PREFERENCES: set[str] = {"absolute", "long_only"}
_RECOMMENDATIONS_LOCK = threading.RLock()
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
        "SELECT timestamp AS ts FROM ranking_snapshots WHERE tenant_id = ? ORDER BY julianday(timestamp) DESC LIMIT 1",
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
    return {normalize_ticker(str(r["ticker"])): float(r["score"]) for r in rows if r["score"] is not None}


def _latest_consensus(conn: sqlite3.Connection, *, tenant_id: str) -> dict[str, float]:
    rows = conn.execute(
        """
        SELECT ticker, p_final
        FROM consensus_signals
        WHERE tenant_id = ?
          AND created_at = (
            SELECT cs2.created_at
            FROM consensus_signals cs2
            WHERE cs2.tenant_id = consensus_signals.tenant_id
              AND cs2.ticker = consensus_signals.ticker
            ORDER BY julianday(cs2.created_at) DESC LIMIT 1
          )
        """,
        (tenant_id,),
    ).fetchall()
    return {normalize_ticker(str(r["ticker"])): float(r["p_final"]) for r in rows if r["p_final"] is not None}


def _candidate_status(conn: sqlite3.Connection, *, tenant_id: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT ticker, status FROM candidate_queue WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchall()
    return {normalize_ticker(str(r["ticker"])): str(r["status"]).strip().lower() for r in rows}


def _latest_prediction_age_hours(conn: sqlite3.Connection, *, tenant_id: str, ticker: str) -> float | None:
    row = conn.execute(
        """
        SELECT MAX(timestamp) AS ts
        FROM predictions
        WHERE tenant_id = ? AND ticker = ?
        """,
        (tenant_id, ticker),
    ).fetchone()
    if not row or row["ts"] is None:
        return None
    try:
        ts = datetime.fromisoformat(str(row["ts"]).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return max(0.0, (now - ts.astimezone(timezone.utc)).total_seconds() / 3600.0)


def _recent_rank_repeat_count(conn: sqlite3.Connection, *, tenant_id: str, ticker: str, days: int) -> int:
    n_days = max(1, int(days))
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT date(timestamp)) AS n
        FROM ranking_snapshots
        WHERE tenant_id = ?
          AND ticker = ?
          AND timestamp >= datetime('now', ?)
        """,
        (tenant_id, ticker, f"-{n_days} day"),
    ).fetchone()
    return int(row["n"] or 0) if row else 0


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


def _action_from_score(score: float, *, mode: RecommendationMode) -> str:
    buy_thr = 0.30 if mode == "conservative" else 0.20
    sell_thr = -0.30 if mode == "conservative" else -0.20
    if score >= buy_thr:
        return "BUY"
    if score <= sell_thr:
        return "SELL"
    return "HOLD"


def _build_explanations(*, score: float, source: dict[str, Any]) -> list[str]:
    """Deterministic accounting of inputs; never an investment thesis."""
    return [
        f"Relative composite score: {score:.4f}; not a probability of success.",
        f"Ranking input: {source.get('ranking_score', 0):.4f}; "
        f"consensus input: {source.get('consensus_p_final', 0):.4f} (uncalibrated).",
        "Selection includes momentum, admission, company-profile proxies, freshness, "
        "repeat and sector-diversity adjustments.",
    ]


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _safe_positive(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def _semantic_signal(profile: dict[str, Any], stats: dict[str, Any]) -> float:
    signal = 0.0
    if str(profile.get("longName") or "").strip():
        signal += 0.18
    if str(profile.get("website") or "").strip():
        signal += 0.10
    if str(profile.get("sector") or "").strip():
        signal += 0.14
    if str(profile.get("industry") or "").strip():
        signal += 0.14
    if str(profile.get("country") or "").strip():
        signal += 0.06

    years_listed = _safe_positive(stats.get("yearsListed"))
    if years_listed is not None:
        signal += 0.16 * _clip01(years_listed / 20.0)

    market_cap = _safe_positive(stats.get("marketCap"))
    if market_cap is not None:
        # Saturates around 10B+ so mega-caps do not dominate.
        signal += 0.12 * _clip01(market_cap / 10_000_000_000.0)

    employees = _safe_positive(profile.get("fullTimeEmployees"))
    if employees is not None:
        signal += 0.10 * _clip01(employees / 10_000.0)

    return _clip01(signal)


def _undervaluation_signal(price: float, high_52: float) -> float:
    if high_52 <= 0:
        return 0.0
    return _clip01((high_52 - price) / high_52)


def _liquidity_efficiency_signal(avg_volume: float | None, market_cap: float | None) -> float:
    if avg_volume is None:
        return 0.0
    if market_cap is None or market_cap <= 0:
        # If cap is unavailable, still reward visible liquidity.
        return _clip01(avg_volume / 5_000_000.0)
    return _clip01((avg_volume / market_cap) * 200.0)


def _company_quality_signal(stats: dict[str, Any], profile: dict[str, Any]) -> float:
    price = float(stats["price"])
    high_52 = float(stats.get("high52") or price)
    avg_volume = _safe_positive(stats.get("avgVolume"))
    market_cap = _safe_positive(stats.get("marketCap"))
    semantic = _semantic_signal(profile, stats)
    undervaluation = _undervaluation_signal(price, high_52)
    liquidity = _liquidity_efficiency_signal(avg_volume, market_cap)
    return (0.55 * semantic) + (0.30 * undervaluation) + (0.15 * liquidity)


def _cheap_stock_quality_weight(price: float) -> float:
    if price <= 2.0:
        return 0.45
    if price <= 10.0:
        return 0.35
    if price <= 100.0:
        return 0.20
    return 0.10


def _dynamic_signal01(dynamic_composite: float) -> float:
    return _clip01((float(dynamic_composite) + 1.0) / 2.0)


def _semantic_presence_count(profile: dict[str, Any]) -> int:
    keys = ("longName", "sector", "industry", "website", "country")
    return sum(1 for k in keys if str(profile.get(k) or "").strip())


def _recommendation_band(price: float) -> str:
    if price <= 2.0:
        return "under_2"
    if price <= 10.0:
        return "under_10"
    if price <= 100.0:
        return "under_100"
    return "above_100"


def _evaluate_under_price_eligibility(
    *,
    price: float,
    action: str,
    preference: BestPreference,
    avg_volume: float | None,
    years_listed: float | None,
    semantic_count: int,
    dynamic_composite: float,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if preference == "long_only" and action != "BUY":
        reasons.append("not_long_only_buy")
    if price <= 0:
        reasons.append("invalid_price")
    if avg_volume is None or avg_volume < 300_000:
        reasons.append("min_volume_under_300k")
    if years_listed is None or years_listed < 1:
        reasons.append("min_years_listed_under_1")
    if semantic_count < 3:
        reasons.append("semantic_fields_under_3")

    if price <= 2.0:
        if avg_volume is None or avg_volume < 1_000_000:
            reasons.append("under2_min_volume_under_1m")
        if years_listed is None or years_listed < 2:
            reasons.append("under2_min_years_listed_under_2")
        if semantic_count < 4:
            reasons.append("under2_semantic_fields_under_4")
        if dynamic_composite <= 0:
            reasons.append("under2_dynamic_not_positive")

    return (len(reasons) == 0, reasons)


def _profile_composite_score(
    *,
    semantic_credibility: float,
    longevity: float,
    undervaluation: float,
    dynamic_signal: float,
) -> float:
    return _clip01(
        (0.35 * semantic_credibility)
        + (0.25 * longevity)
        + (0.20 * undervaluation)
        + (0.20 * dynamic_signal)
    )


def _score_under_price_candidate(
    *,
    price: float,
    dynamic_signal: float,
    semantic_credibility: float,
    undervaluation: float,
    liquidity_efficiency: float,
) -> float:
    if price <= 2.0:
        return _clip01(
            (0.30 * dynamic_signal)
            + (0.40 * semantic_credibility)
            + (0.10 * undervaluation)
            + (0.20 * liquidity_efficiency)
        )
    return _clip01(
        (0.40 * dynamic_signal)
        + (0.30 * semantic_credibility)
        + (0.15 * undervaluation)
        + (0.15 * liquidity_efficiency)
    )


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
    sector_cap = int(os.environ.get("ALPHA_REC_MAX_PER_SECTOR", "2"))
    repeat_window_days = int(os.environ.get("ALPHA_REC_REPEAT_WINDOW_DAYS", "3"))
    repeat_penalty_weight = float(os.environ.get("ALPHA_REC_REPEAT_PENALTY_WEIGHT", "0.08"))
    freshness_bonus_weight = float(os.environ.get("ALPHA_REC_FRESHNESS_BONUS_WEIGHT", "0.06"))
    sector_penalty_weight = float(os.environ.get("ALPHA_REC_SECTOR_PENALTY_WEIGHT", "0.08"))
    diversity_window = int(os.environ.get("ALPHA_REC_DIVERSITY_WINDOW", "10"))

    ranking_map = _latest_rankings(conn, tenant_id=tenant_id)
    consensus_map = _latest_consensus(conn, tenant_id=tenant_id)
    status_map = _candidate_status(conn, tenant_id=tenant_id)
    universe = get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn)

    # When live rankings exist, use the ranked tickers as the recommendation universe.
    # The ranking snapshot covers the prediction pipeline's broader discovery set, which
    # is wider than active_universe (static + admitted only). Building recommendations from
    # active_universe when rankings are available means most candidates have ranking_score=0.
    if ranking_map:
        universe = sorted(ranking_map.keys())

    quality = recommendation_quality(conn, tenant_id=tenant_id)
    conn.execute("DELETE FROM house_recommendations WHERE tenant_id = ? AND mode = ?", (tenant_id, mode))

    candidates: list[dict[str, Any]] = []
    for t in universe:
        ticker = normalize_ticker(t)
        stats = build_stats_payload(conn, tenant_id=tenant_id, ticker=ticker)
        if not stats:
            continue
        price = float(stats["price"])
        if not math.isfinite(price) or price <= 0:
            continue
        profile = load_company_profile_json(ticker)
        sector = str(profile.get("sector") or "unknown").strip().lower()
        rank_score = float(ranking_map.get(ticker, 0.0))
        consensus_score = float(consensus_map.get(ticker, 0.0))
        if not math.isfinite(rank_score) or not math.isfinite(consensus_score):
            continue
        mom_score = _momentum_signal(stats.get("dayChangePct"))
        admission_score = _admission_signal(status_map.get(ticker))

        dynamic_composite = (
            (weights["ranking"] * rank_score)
            + (weights["consensus"] * consensus_score)
            + (weights["momentum"] * mom_score)
            + (weights["admission"] * admission_score)
        )
        quality_score = _company_quality_signal(stats, profile)
        quality_centered = (quality_score * 2.0) - 1.0
        quality_weight = _cheap_stock_quality_weight(price)
        composite = ((1.0 - quality_weight) * dynamic_composite) + (quality_weight * quality_centered)
        repeat_count = _recent_rank_repeat_count(
            conn,
            tenant_id=tenant_id,
            ticker=ticker,
            days=repeat_window_days,
        )
        pred_age_hours = _latest_prediction_age_hours(conn, tenant_id=tenant_id, ticker=ticker)
        freshness_bonus = 0.0
        if pred_age_hours is not None:
            freshness_bonus = max(0.0, min(1.0, 1.0 - (pred_age_hours / 72.0)))
        repeat_penalty = min(1.0, repeat_count / float(max(1, repeat_window_days)))
        prelim = composite + (freshness_bonus_weight * freshness_bonus) - (repeat_penalty_weight * repeat_penalty)
        action = _action_from_score(composite, mode=mode)
        horizon = _MODE_HORIZON[str(mode)]
        # No validated entry, stop, or risk-estimation methodology is available.
        risk, entry_min, entry_max = "Unavailable", None, None
        thesis, avoid_if = [], []

        quote = build_quote_payload(conn, tenant_id=tenant_id, ticker=ticker)
        source = {
            "reference_price": price,
            "reference_price_as_of": quote.get("time") if quote else None,
            "ranking_score": round(rank_score, 6),
            "consensus_p_final": round(consensus_score, 6),
            "momentum_signal": round(mom_score, 6),
            "admission_signal": round(admission_score, 6),
            "dynamic_composite": round(dynamic_composite, 6),
            "company_quality_signal": round(quality_score, 6),
            "company_quality_weight": round(quality_weight, 6),
            "repeat_count_window": int(repeat_count),
            "prediction_age_hours": round(pred_age_hours, 4) if pred_age_hours is not None else None,
            "freshness_bonus": round(freshness_bonus, 6),
            "repeat_penalty": round(repeat_penalty, 6),
            "preliminary_score": round(prelim, 6),
            "weights": weights,
        }
        candidates.append(
            {
                "ticker": ticker,
                "sector": sector,
                "action": action,
                "risk": risk,
                "horizon": horizon,
                "entry_min": entry_min,
                "entry_max": entry_max,
                "thesis": thesis,
                "avoid_if": avoid_if,
                "source": source,
                "base_score": float(composite),
                "prelim_score": float(prelim),
            }
        )

    candidates.sort(key=lambda c: c["prelim_score"], reverse=True)
    sector_seen: dict[str, int] = {}
    rows_in: list[tuple[Any, ...]] = []
    for idx, c in enumerate(candidates):
        sec = str(c["sector"])
        seen = sector_seen.get(sec, 0)
        diversity_penalty = 0.0
        if idx < max(1, diversity_window) and sec != "unknown" and seen >= max(1, sector_cap):
            diversity_penalty = (seen - sector_cap + 1) * sector_penalty_weight
        final_score = c["prelim_score"] - diversity_penalty
        action = _action_from_score(final_score, mode=mode)
        sector_seen[sec] = seen + 1
        # Legacy NOT NULL storage column only; never published or used for ordering.
        confidence = 0
        symbol_quality = recommendation_quality(
            conn, tenant_id=tenant_id, ticker=c["ticker"], global_quality=quality,
        )
        c["source"]["quality"] = symbol_quality
        c["source"]["research_action"] = action
        if not symbol_quality["actionable"]:
            action = "WATCH"
        c["source"]["sector"] = sec
        c["source"]["sector_seen_before"] = int(seen)
        c["source"]["sector_diversity_penalty"] = round(diversity_penalty, 6)
        c["source"]["diversity_adjusted_score"] = round(final_score, 6)
        rows_in.append(
            (
                tenant_id,
                str(mode),
                c["ticker"],
                action,
                confidence,
                round(final_score, 6),
                c["risk"],
                c["horizon"],
                c["entry_min"],
                c["entry_max"],
                json.dumps(c["thesis"]),
                json.dumps(c["avoid_if"]),
                as_of,
                json.dumps(c["source"]),
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
    source_raw = json.loads(str(row["source_json"] or "{}"))
    source = source_raw if isinstance(source_raw, dict) else {}
    score = float(row["score"])
    quality = source.get("quality") or {
        "status": "suppressed", "actionable": False,
        "suppressionReasons": ["QUALITY_UNAVAILABLE"],
    }
    actionable = quality.get("actionable") is True
    return {
        "contractVersion": "2.0",
        "ticker": str(row["ticker"]),
        "action": str(row["action"]) if actionable else "WATCH",
        "actionable": actionable,
        "researchDirection": {"BUY": "bullish", "SELL": "bearish", "HOLD": "neutral"}.get(
            source.get("research_action"), "unknown"),
        "confidence": None,
        "successProbability": None,
        "confidenceMethodology": {
            "status": "unavailable", "method": "not_calibrated",
            "reason": "No empirical calibration evidence is attached to this recommendation.",
        },
        "heuristicConfidence": {
            "status": "available", "value": round(abs(score), 6),
            "method": "absolute_composite_score", "unit": "score", "isProbability": False,
        },
        "score": round(score, 4),
        "scoreMethodology": {"method": "weighted_composite_with_selection_adjustments",
                             "unit": "relative_score", "isProbability": False},
        "risk": None,
        "entryZone": None,
        "expectedReturn": None,
        "guidanceAvailability": {
            key: {"status": "unavailable", "reason": "NO_VALIDATED_METHODOLOGY"}
            for key in ("entryZone", "risk", "expectedReturn", "avoidIf")
        },
        "referencePrice": source.get("reference_price"),
        "referencePriceAsOf": source.get("reference_price_as_of"),
        "horizon": str(row["horizon"]),
        "horizonMethodology": "mode_policy_not_validated_forecast",
        "thesis": [],
        "explanations": _build_explanations(score=score, source=source),
        "explanationMethodology": "deterministic_score_components",
        "avoidIf": [],
        "mode": str(mode),
        "asOf": str(row["as_of"]),
        "asOfMethodology": "research_build_time_not_source_freshness",
        "qualityStatus": quality,
        "suppressionReasons": quality["suppressionReasons"],
        "selectionDiagnostics": {
            "freshnessBonus": source.get("freshness_bonus"),
            "repeatPenalty": source.get("repeat_penalty"),
            "recentRepeatCount": source.get("repeat_count_window"),
            "sector": source.get("sector"),
            "sectorSeenBefore": source.get("sector_seen_before"),
            "sectorDiversityPenalty": source.get("sector_diversity_penalty"),
            "preliminaryScore": source.get("preliminary_score"),
            "diversityAdjustedScore": source.get("diversity_adjusted_score"),
        },
    }


def get_recommendations_latest(
    conn: sqlite3.Connection, *, tenant_id: str, mode: RecommendationMode,
    preference: BestPreference = "absolute", limit: int = 10,
) -> list[dict[str, Any]]:
    with _RECOMMENDATIONS_LOCK:
        rebuild_house_recommendations(conn, tenant_id=tenant_id, mode=mode)
        rows = conn.execute(
            "SELECT * FROM house_recommendations WHERE tenant_id = ? AND mode = ? "
            "ORDER BY ABS(score) DESC, score DESC, ticker ASC", (tenant_id, mode),
        ).fetchall()
        payload = [_row_to_payload(r, mode=mode) for r in rows]
    if preference == "long_only":
        payload = [p for p in payload if p["researchDirection"] == "bullish"]
    for p in payload:
        p["selectionPreference"] = preference
    return payload[:limit]


def get_recommendation_best(
    conn: sqlite3.Connection, *, tenant_id: str, mode: RecommendationMode,
    preference: BestPreference = "absolute",
) -> dict[str, Any] | None:
    rows = get_recommendations_latest(
        conn, tenant_id=tenant_id, mode=mode, preference=preference, limit=10000,
    )
    return next((r for r in rows if r["actionable"]), None)


def get_recommendation_for_ticker(
    conn: sqlite3.Connection, *, tenant_id: str, mode: RecommendationMode, ticker: str,
) -> dict[str, Any] | None:
    with _RECOMMENDATIONS_LOCK:
        rebuild_house_recommendations(conn, tenant_id=tenant_id, mode=mode)
        row = conn.execute(
            "SELECT * FROM house_recommendations WHERE tenant_id = ? AND mode = ? AND ticker = ?",
            (tenant_id, mode, normalize_ticker(ticker)),
        ).fetchone()
        return _row_to_payload(row, mode=mode) if row else None


def get_recommendations_under_price(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    mode: RecommendationMode,
    price_cap: float,
    preference: BestPreference = "long_only",
    limit: int = 10,
) -> list[dict[str, Any]]:
    with _RECOMMENDATIONS_LOCK:
        rebuild_house_recommendations(conn, tenant_id=tenant_id, mode=mode)
        rows = conn.execute(
            """
            SELECT *
            FROM house_recommendations
            WHERE tenant_id = ? AND mode = ?
            ORDER BY ABS(score) DESC, score DESC
            """,
            (tenant_id, mode),
        ).fetchall()
    if not rows:
        return []

    passed: list[dict[str, Any]] = []
    for row in rows:
        base = _row_to_payload(row, mode=mode)
        ticker = str(row["ticker"])
        stats = build_stats_payload(conn, tenant_id=tenant_id, ticker=ticker)
        if stats is None:
            continue
        profile = load_company_profile_json(ticker)
        source = json.loads(str(row["source_json"] or "{}"))
        dynamic_composite = float(source.get("dynamic_composite") or row["score"] or 0.0)
        dynamic_signal = _dynamic_signal01(dynamic_composite)
        price = float(stats["price"])
        if not math.isfinite(price) or price <= 0 or price > price_cap:
            continue
        high_52 = float(stats.get("high52") or price)
        avg_volume = _safe_positive(stats.get("avgVolume"))
        market_cap = _safe_positive(stats.get("marketCap"))
        years_listed = _safe_positive(stats.get("yearsListed"))
        semantic_count = _semantic_presence_count(profile)
        semantic_credibility = _semantic_signal(profile, stats)
        undervaluation = _undervaluation_signal(price, high_52)
        liquidity_efficiency = _liquidity_efficiency_signal(avg_volume, market_cap)
        longevity = _clip01((years_listed or 0.0) / 20.0)
        composite_score = _score_under_price_candidate(
            price=price,
            dynamic_signal=dynamic_signal,
            semantic_credibility=semantic_credibility,
            undervaluation=undervaluation,
            liquidity_efficiency=liquidity_efficiency,
        )
        fifty_plus = _profile_composite_score(
            semantic_credibility=semantic_credibility,
            longevity=longevity,
            undervaluation=undervaluation,
            dynamic_signal=dynamic_signal,
        )
        eligible, reasons = _evaluate_under_price_eligibility(
            price=price,
            action=str(source.get("research_action", "WATCH")),
            preference=preference,
            avg_volume=avg_volume,
            years_listed=years_listed,
            semantic_count=semantic_count,
            dynamic_composite=dynamic_composite,
        )
        enriched = {
            **base,
            "selectionPreference": preference,
            "priceCap": float(price_cap),
            "band": _recommendation_band(price),
            "eligibilityPassed": eligible,
            "compositeScore": round(composite_score, 4),
            "semanticCredibility": round(semantic_credibility, 4),
            "liquidityEfficiency": round(liquidity_efficiency, 4),
            "undervaluation": None,
            "pricePositionScore": round(undervaluation, 4),
            "fiftyDollarPotential": None,
            "profileCompositeScore": round(fifty_plus, 4),
            "researchScoreMethodology": {
                "isProbability": False, "unit": "heuristic_score_0_to_1",
                "compositeScore": "weighted profile, price-position, liquidity and signal proxies",
                "semanticCredibility": "profile-field completeness proxy, not verified credibility",
                "liquidityEfficiency": "volume and market-cap proxy, not execution capacity",
                "pricePositionScore": "distance below historical high, not fair value",
                "profileCompositeScore": "profile, listing age, price-position and signal blend; no price target",
            },
            "disqualifiers": reasons,
            "_consensusPFinalSort": float(source.get("consensus_p_final", 0.0)),
        }
        if eligible:
            passed.append(enriched)
        # Ineligible candidates are not promoted to fill a requested result count.

    ranked = sorted(
        passed,
        key=lambda p: (
            float(p["compositeScore"]),
            abs(float(p["score"])),
            float(p["_consensusPFinalSort"]),
        ),
        reverse=True,
    )
    out = ranked[:int(limit)]
    for item in out:
        item.pop("_consensusPFinalSort", None)
    return out
