"""Shared upstream pipeline signals for read API gates and operational reports."""

from __future__ import annotations

import os
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from app.core.active_universe import get_active_universe_tickers

RankingProvenance = Literal["live_prediction", "legacy_snapshot", "fallback_consensus", "seeded", "none"]

# Pragmatic edge-score discount when tier is limited (recompute later; see internal read ranking handler).
LIMITED_EDGE_SCORE_MULTIPLIER = 0.85

InternalTier = Literal["full", "limited", "suppressed"]
DisplayTier = Literal["live_intelligence", "reduced_coverage", "refreshing_data"]

BAR_COVERAGE_SLA_RATIO = float(os.environ.get("PIPELINE_BAR_COVERAGE_SLA", "0.9"))
# Daily research policy: allows a normal weekend, fails closed on longer gaps.
# These limits cannot be disabled or loosened through environment settings.
FRESH_BAR_MAX_AGE_DAYS = 3
RECOMMENDATION_MAX_AGE_HOURS = 72
RECOMMENDATION_MIN_COVERAGE = 0.9


def _fresh_timestamp(value: Any, now: datetime, *, hours: int = 72) -> bool:
    try:
        age = (now - _parse_iso_utc(str(value))).total_seconds()
        return 0 <= age <= hours * 3600
    except (ValueError, TypeError, OverflowError):
        return False


def _parse_iso_utc(value: str) -> datetime:
    ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def fresh_bar_coverage(
    conn: sqlite3.Connection, *, tenant_id: str
) -> tuple[int, int, float]:
    """
    Returns (fresh_count, universe_expected, ratio in [0,1]).
    Fresh = latest 1d bar within FRESH_BAR_MAX_AGE_DAYS for each universe symbol.
    """
    now = datetime.now(timezone.utc)
    universe = sorted(get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn))
    expected = len(universe)
    if expected == 0:
        return 0, 0, 0.0

    try:
        rows = conn.execute(
            """
            SELECT ticker, MAX(julianday(timestamp)) AS jd, timestamp AS ts, close
            FROM price_bars
            WHERE tenant_id = ? AND timeframe = '1d'
            GROUP BY ticker
            """,
            (tenant_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0, expected, 0.0
    latest_by_ticker = {str(r["ticker"]).strip().upper(): r for r in rows}

    fresh = 0
    for t in universe:
        row = latest_by_ticker.get(t)
        if row is None:
            continue
        try:
            valid_price = math.isfinite(float(row["close"])) and float(row["close"]) > 0
        except (TypeError, ValueError):
            valid_price = False
        if valid_price and _fresh_timestamp(row["ts"], now):
            fresh += 1

    ratio = fresh / float(expected)
    return fresh, expected, round(ratio, 4)


@dataclass(frozen=True, slots=True)
class PipelineSignals:
    bar_coverage_ratio: float
    fresh_bar_count: int
    universe_expected: int
    predictions_total: int
    predictions_last_7d: int
    service_mode: Literal["limited", "normal"]
    prediction_pipeline_active: bool

    def as_public_dict(self) -> dict[str, Any]:
        return {
            "barCoverageRatio": self.bar_coverage_ratio,
            "freshBarCount": self.fresh_bar_count,
            "universeExpected": self.universe_expected,
            "serviceMode": self.service_mode,
            "predictionsTotal": self.predictions_total,
            "predictionsLast7d": self.predictions_last_7d,
            "predictionPipelineActive": self.prediction_pipeline_active,
        }


def compute_pipeline_signals(conn: sqlite3.Connection, *, tenant_id: str) -> PipelineSignals:
    fresh_n, expected, ratio = fresh_bar_coverage(conn, tenant_id=tenant_id)
    pred_total = pred_7d = pred_fresh = 0
    try:
        pt = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        pred_total = int(pt["n"] or 0)
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=7)).isoformat()
        p7 = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ? AND julianday(timestamp) BETWEEN julianday(?) AND julianday(?)",
            (tenant_id, cutoff, now.isoformat()),
        ).fetchone()
        pred_7d = int(p7["n"] or 0)
        pred_fresh = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE tenant_id = ? "
            "AND julianday(timestamp) BETWEEN julianday(?) AND julianday(?)",
            (tenant_id, (now - timedelta(hours=RECOMMENDATION_MAX_AGE_HOURS)).isoformat(), now.isoformat()),
        ).fetchone()[0]
    except sqlite3.OperationalError:
        pass

    active = pred_fresh > 0
    limited = ratio < max(RECOMMENDATION_MIN_COVERAGE, BAR_COVERAGE_SLA_RATIO) or not active
    return PipelineSignals(
        bar_coverage_ratio=ratio,
        fresh_bar_count=fresh_n,
        universe_expected=expected,
        predictions_total=pred_total,
        predictions_last_7d=pred_7d,
        service_mode="limited" if limited else "normal",
        prediction_pipeline_active=active,
    )


def intelligence_confidence_tier(
    signals: PipelineSignals, *, rankings_suppressed: bool = False
) -> Literal["full", "limited", "suppressed"]:
    if rankings_suppressed:
        return "suppressed"
    if signals.service_mode == "limited" or not signals.prediction_pipeline_active:
        return "limited"
    return "full"


def should_suppress_rankings(bar_coverage_ratio: float) -> bool:
    raw = os.environ.get("PIPELINE_SUPPRESS_RANKINGS_BELOW", "").strip()
    if not raw:
        return False
    try:
        return bar_coverage_ratio < float(raw)
    except ValueError:
        return False


def should_block_best_pick_without_predictions(predictions_total: int) -> bool:
    return predictions_total == 0


def latest_ranking_snapshot_timestamp(
    conn: sqlite3.Connection, *, tenant_id: str
) -> datetime | None:
    try:
        row = conn.execute(
            "SELECT MAX(timestamp) AS ts FROM ranking_snapshots WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        if row and row["ts"] is not None:
            return _parse_iso_utc(str(row["ts"]))
    except sqlite3.OperationalError:
        pass
    return None


def ranking_snapshot_age_hours(conn: sqlite3.Connection, *, tenant_id: str) -> float | None:
    ts = latest_ranking_snapshot_timestamp(conn, tenant_id=tenant_id)
    if ts is None:
        return None
    delta = datetime.now(timezone.utc) - ts
    return round(max(0.0, delta.total_seconds() / 3600.0), 4)


def should_suppress_stale_legacy_ranking(
    provenance: RankingProvenance,
    snapshot_age_hours: float | None,
) -> bool:
    raw = os.environ.get("PIPELINE_SUPPRESS_LEGACY_SNAPSHOT_OLDER_THAN_HOURS", "").strip()
    if not raw:
        return False
    try:
        max_hours = float(raw)
    except ValueError:
        return False
    if provenance not in ("legacy_snapshot", "seeded"):
        return False
    if snapshot_age_hours is None:
        return False
    return snapshot_age_hours > max_hours


def product_labels_for_tier(internal_tier: InternalTier) -> dict[str, str]:
    """User-facing labels (avoid exposing raw pipeline tier strings in UI)."""
    mapping: dict[InternalTier, tuple[DisplayTier, str]] = {
        "full": ("live_intelligence", "Live Intelligence"),
        "limited": ("reduced_coverage", "Reduced Coverage"),
        "suppressed": ("refreshing_data", "Refreshing Data"),
    }
    display_id, title = mapping[internal_tier]
    return {"displayTier": display_id, "title": title}


def build_ranking_consumer_experience(
    internal_tier: InternalTier,
    *,
    rankings_count: int,
    suppression_reasons: list[str],
    filtered_out_entirely: bool,
) -> dict[str, Any]:
    labels = product_labels_for_tier(internal_tier)
    reason: str | None = None
    if "bar_coverage" in suppression_reasons:
        reason = "market_data_coverage"
    elif "legacy_stale" in suppression_reasons:
        reason = "legacy_snapshot_stale"

    if rankings_count > 0:
        message = ""
    elif filtered_out_entirely:
        message = "No rankings match the current filters."
    elif suppression_reasons or internal_tier in ("limited", "suppressed"):
        message = (
            "Refreshing market intelligence. Rankings temporarily unavailable."
        )
    else:
        message = "No rankings are available yet."

    return {
        **labels,
        "message": message,
        "rankingsUnavailable": rankings_count == 0,
        "reason": reason,
    }


def infer_ranking_provenance(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ranking_tickers: list[str],
) -> RankingProvenance:
    """Best-effort provenance from warehouse state (no historical lineage column yet)."""
    if not ranking_tickers:
        return "none"

    pred_total = 0
    pred_ranked = 0
    try:
        pt = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        pred_total = int(pt["n"] or 0)
        pr = conn.execute(
            """
            SELECT COUNT(*) AS n FROM predictions
            WHERE tenant_id = ? AND rank_score IS NOT NULL
            """,
            (tenant_id,),
        ).fetchone()
        pred_ranked = int(pr["n"] or 0)
    except sqlite3.OperationalError:
        pass

    sentinels = {"AAPL", "SPY", "QQQ"}
    upper = {str(t).strip().upper() for t in ranking_tickers}
    only_sentinels = upper.issubset(sentinels) and len(upper) <= 3

    if pred_total == 0:
        if only_sentinels and len(upper) >= 1:
            return "seeded"
        return "legacy_snapshot"
    if pred_ranked > 0:
        return "live_prediction"
    return "fallback_consensus"


def recommendation_quality(
    conn: sqlite3.Connection, *, tenant_id: str, ticker: str | None = None,
    now: datetime | None = None, global_quality: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Publication gate, independent of optional research-ranking display filters.

    No probability or execution approval is implied by passing this gate.
    Unknown health, invalid timestamps and missing per-symbol inputs fail closed.
    """
    from app.internal_read_v1.intelligence_read import get_prediction_run_latest

    now = now or datetime.now(timezone.utc)
    reasons: list[str] = []
    run_id = None
    if global_quality is None:
        signals = compute_pipeline_signals(conn, tenant_id=tenant_id)
        if signals.universe_expected == 0:
            reasons.append("UNIVERSE_UNAVAILABLE")
        if signals.bar_coverage_ratio < RECOMMENDATION_MIN_COVERAGE:
            reasons.append("INSUFFICIENT_FRESH_PRICE_COVERAGE")
        if not signals.prediction_pipeline_active:
            reasons.append("PREDICTIONS_STALE_OR_MISSING")
        try:
            run = get_prediction_run_latest(conn, tenant_id=tenant_id, timeframe="1d")
        except (sqlite3.Error, ValueError, TypeError, OverflowError):
            run = None
        if not run:
            reasons.append("PIPELINE_HEALTH_UNAVAILABLE")
        else:
            run_id = run.get("id")
            # predictionEnd can be a future forecast horizon, not completion time.
            if not _fresh_timestamp(run.get("createdAt"), now):
                reasons.append("PIPELINE_RUN_STALE_OR_INVALID")
            if run.get("runStatus") != "HEALTHY":
                reasons.append("PIPELINE_NOT_HEALTHY")
                reasons.extend(str(r) for r in (run.get("degradedReasons") or []))
        # A derived health score is insufficient evidence of job completion.
        # The queue runner records actual success/failure separately from forecast horizons.
        try:
            job = conn.execute(
                "SELECT * FROM prediction_jobs WHERE tenant_id = ? AND job_type = 'run_queue' "
                "ORDER BY julianday(started_at) DESC, id DESC LIMIT 1", (tenant_id,),
            ).fetchone()
            if job is None:
                reasons.append("PREDICTION_JOB_UNAVAILABLE")
            elif job["status"] != "success":
                reasons.append("PREDICTION_JOB_NOT_SUCCESSFUL")
            elif not _fresh_timestamp(job["completed_at"], now):
                reasons.append("PREDICTION_JOB_STALE_OR_INVALID")
            elif run:
                created = _parse_iso_utc(run["createdAt"])
                if not (_parse_iso_utc(job["started_at"]) <= created <= _parse_iso_utc(job["completed_at"])):
                    reasons.append("PREDICTION_JOB_RUN_MISMATCH")
                elif job["run_id"] and job["run_id"] != run_id:
                    reasons.append("PREDICTION_JOB_RUN_MISMATCH")
        except (sqlite3.Error, KeyError, IndexError, TypeError, ValueError, OverflowError):
            reasons.append("PREDICTION_JOB_UNAVAILABLE")
        pipeline = signals.as_public_dict()
    else:
        reasons.extend(global_quality["suppressionReasons"])
        pipeline = global_quality["pipelineSignals"]
        run_id = global_quality.get("runId")

    if ticker is not None:
        from app.internal_read_v1.chart_market import build_quote_payload
        try:
            quote = build_quote_payload(conn, tenant_id=tenant_id, ticker=ticker, now=now)
            if not quote or not _fresh_timestamp(quote.get("time"), now):
                reasons.append("REFERENCE_QUOTE_STALE_OR_MISSING")
            elif not math.isfinite(float(quote["price"])) or float(quote["price"]) <= 0:
                reasons.append("REFERENCE_QUOTE_INVALID")
        except (sqlite3.Error, ValueError, TypeError, OverflowError):
            reasons.append("REFERENCE_QUOTE_UNAVAILABLE")
        checks = (
            ("price_bars", "timestamp", "PRICE", "AND timeframe = '1d'"),
            ("predictions", "timestamp", "PREDICTION", ""),
            ("ranking_snapshots", "timestamp", "RANKING", ""),
            ("consensus_signals", "created_at", "CONSENSUS", ""),
        )
        for table, timestamp, label, extra in checks:
            try:
                row = conn.execute(
                    f"SELECT * FROM {table} WHERE tenant_id = ? AND ticker = ? "
                    f"{extra} ORDER BY julianday({timestamp}) DESC LIMIT 1",
                    (tenant_id, ticker.upper()),
                ).fetchone()
                if row is None or not _fresh_timestamp(row[timestamp], now):
                    reasons.append(f"SYMBOL_{label}_STALE_OR_MISSING")
                    continue
                if label == "PRICE":
                    price = row["close"]
                    if price is None or not math.isfinite(float(price)) or float(price) <= 0:
                        reasons.append("SYMBOL_PRICE_INVALID")
                if label in ("CONSENSUS", "RANKING"):
                    value = row["p_final" if label == "CONSENSUS" else "score"]
                    if value is None or not math.isfinite(float(value)):
                        reasons.append(f"SYMBOL_{label}_INVALID")
                if label == "PREDICTION" and (not run_id or row["run_id"] != run_id):
                    reasons.append("SYMBOL_PREDICTION_RUN_MISMATCH")
            except (sqlite3.Error, KeyError, IndexError, ValueError, TypeError):
                reasons.append(f"SYMBOL_{label}_UNAVAILABLE")
    reasons = list(dict.fromkeys(reasons))
    return {
        "status": "suppressed" if reasons else "healthy",
        "actionable": not reasons,
        "suppressionReasons": reasons,
        "evaluatedAt": now.isoformat(),
        "maxAgeHours": RECOMMENDATION_MAX_AGE_HOURS,
        "minimumPriceCoverage": RECOMMENDATION_MIN_COVERAGE,
        "runId": run_id,
        "pipelineSignals": pipeline,
    }
