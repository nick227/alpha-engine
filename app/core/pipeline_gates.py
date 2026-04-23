"""Shared upstream pipeline signals for read API gates and operational reports."""

from __future__ import annotations

import os
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
FRESH_BAR_MAX_AGE_DAYS = int(os.environ.get("PIPELINE_FRESH_BAR_DAYS", "7"))


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
    cutoff = now - timedelta(days=FRESH_BAR_MAX_AGE_DAYS)
    universe = sorted(get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn))
    expected = len(universe)
    if expected == 0:
        return 0, 0, 1.0

    try:
        rows = conn.execute(
            """
            SELECT ticker, MAX(timestamp) AS ts
            FROM price_bars
            WHERE tenant_id = ? AND timeframe = '1d'
            GROUP BY ticker
            """,
            (tenant_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0, expected, 0.0
    latest_by_ticker = {str(r["ticker"]).strip().upper(): r["ts"] for r in rows}

    fresh = 0
    for t in universe:
        ts_raw = latest_by_ticker.get(t)
        if ts_raw is None:
            continue
        if _parse_iso_utc(str(ts_raw)) >= cutoff:
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
    pred_total = pred_7d = 0
    try:
        pt = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        pred_total = int(pt["n"] or 0)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        p7 = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?",
            (tenant_id, cutoff),
        ).fetchone()
        pred_7d = int(p7["n"] or 0)
    except sqlite3.OperationalError:
        pass

    limited = ratio < BAR_COVERAGE_SLA_RATIO
    active = pred_total > 0 or pred_7d > 0
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
    if os.environ.get("PIPELINE_BLOCK_BEST_WITHOUT_PREDICTIONS", "").strip() not in ("1", "true", "yes"):
        return False
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
