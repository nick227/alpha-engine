"""Central configuration for Data Health + API Smoke lanes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FreshnessThresholds:
    max_run_age_hours: int = 30
    max_heartbeat_age_minutes: int = 180
    max_allowed_empty_critical_surfaces: int = 0


SENTINEL_SYMBOLS: tuple[str, ...] = ("AAPL", "SPY", "QQQ")

CRITICAL_SURFACES: tuple[str, ...] = (
    "recommendations_latest",
    "recommendations_best",
    "ranking_top",
    "quote",
    "stats",
    "system_heartbeat",
    "predictions_runs_latest",
)

DEFAULT_THRESHOLDS = FreshnessThresholds()

# Latest 1d bar must be within this window to count as "fresh" for ingest coverage reconciliation.
FRESH_BAR_MAX_AGE_DAYS = 7

# Operational policy: warn when fresh-bar coverage (expected universe) falls below this ratio.
# Downstream ranking/recommendation quality should not be trusted as "production-grade" below SLA.
BAR_COVERAGE_SLA_RATIO = 0.9

# Warn if recommendations/latest exposes fewer unique tickers than this (breadth guardrail).
MIN_RECOMMENDATION_UNIQUE_TICKERS_WARNING = 5

