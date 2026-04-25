"""Endpoint registry for inventory-driven Data Health + API Smoke tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from internal_read_inventory.scenarios import ScenarioName

Lane = Literal["data_health", "api_smoke", "both"]
EmptyPolicy = Literal["allowed", "warn", "fail"]
Criticality = Literal["critical", "high", "normal"]


@dataclass(frozen=True, slots=True)
class EndpointSpec:
    id: str
    method: Literal["GET"]
    path_template: str
    lane: Lane
    scenario: ScenarioName
    empty_policy: EmptyPolicy
    criticality: Criticality


ENDPOINTS: tuple[EndpointSpec, ...] = (
    EndpointSpec("health", "GET", "/health", "both", "minimal_empty", "allowed", "normal"),
    EndpointSpec("ranking_top", "GET", "/ranking/top", "both", "market_baseline", "warn", "critical"),
    EndpointSpec("ranking_movers", "GET", "/ranking/movers", "api_smoke", "market_baseline", "allowed", "normal"),
    EndpointSpec("ticker_why", "GET", "/ticker/{symbol}/why", "api_smoke", "market_baseline", "allowed", "normal"),
    EndpointSpec(
        "ticker_performance",
        "GET",
        "/ticker/{symbol}/performance",
        "api_smoke",
        "market_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "admission_changes",
        "GET",
        "/admission/changes",
        "api_smoke",
        "market_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec("api_tickers", "GET", "/api/tickers", "api_smoke", "market_baseline", "allowed", "normal"),
    EndpointSpec("api_quote", "GET", "/api/quote/{ticker}", "both", "market_baseline", "warn", "critical"),
    EndpointSpec("api_history", "GET", "/api/history/{ticker}", "api_smoke", "market_baseline", "allowed", "normal"),
    EndpointSpec("api_candles", "GET", "/api/candles/{ticker}", "api_smoke", "market_baseline", "allowed", "normal"),
    EndpointSpec("api_company", "GET", "/api/company/{ticker}", "api_smoke", "market_baseline", "allowed", "high"),
    EndpointSpec("api_stats", "GET", "/api/stats/{ticker}", "both", "market_baseline", "warn", "critical"),
    EndpointSpec("api_regime", "GET", "/api/regime/{ticker}", "both", "market_baseline", "warn", "high"),
    EndpointSpec(
        "api_recommendations_latest",
        "GET",
        "/api/recommendations/latest",
        "both",
        "market_baseline",
        "fail",
        "critical",
    ),
    EndpointSpec(
        "api_recommendations_best",
        "GET",
        "/api/recommendations/best",
        "both",
        "market_baseline",
        "fail",
        "critical",
    ),
    EndpointSpec(
        "api_recommendations_ticker",
        "GET",
        "/api/recommendations/{ticker}",
        "both",
        "market_baseline",
        "warn",
        "high",
    ),
    EndpointSpec(
        "api_recommendations_under_price",
        "GET",
        "/api/recommendations/under/{price_cap}",
        "both",
        "market_baseline",
        "warn",
        "high",
    ),
    EndpointSpec(
        "api_strategies_catalog",
        "GET",
        "/api/strategies/catalog",
        "both",
        "intelligence_baseline",
        "warn",
        "high",
    ),
    EndpointSpec(
        "api_strategy_stability",
        "GET",
        "/api/strategies/{strategy_id}/stability",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_strategy_performance",
        "GET",
        "/api/strategies/{strategy_id}/performance",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_experiments_leaderboard",
        "GET",
        "/api/experiments/leaderboard",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_experiments_trends",
        "GET",
        "/api/experiments/trends",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_experiments_summary",
        "GET",
        "/api/experiments/summary",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_meta_ranker_latest",
        "GET",
        "/api/experiments/meta-ranker/latest",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_meta_ranker_intents_latest",
        "GET",
        "/api/experiments/meta-ranker/intents/latest",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_meta_ranker_intents_replay",
        "GET",
        "/api/experiments/meta-ranker/intents/replay",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_meta_ranker_promotion_readiness",
        "GET",
        "/api/experiments/meta-ranker/promotion-readiness",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_meta_ranker_alt_data_coverage",
        "GET",
        "/api/experiments/meta-ranker/alt-data/coverage",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_meta_ranker_strategy_queue_share",
        "GET",
        "/api/experiments/meta-ranker/strategy-queue-share",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_performance_regime",
        "GET",
        "/api/performance/regime",
        "both",
        "intelligence_baseline",
        "warn",
        "high",
    ),
    EndpointSpec(
        "api_consensus_signals",
        "GET",
        "/api/consensus/signals",
        "both",
        "intelligence_baseline",
        "warn",
        "high",
    ),
    EndpointSpec(
        "api_ticker_attribution",
        "GET",
        "/api/ticker/{symbol}/attribution",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_ticker_accuracy",
        "GET",
        "/api/ticker/{symbol}/accuracy",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_system_heartbeat",
        "GET",
        "/api/system/heartbeat",
        "both",
        "intelligence_baseline",
        "warn",
        "critical",
    ),
    EndpointSpec(
        "api_predictions_runs_latest",
        "GET",
        "/api/predictions/runs/latest",
        "both",
        "intelligence_baseline",
        "warn",
        "critical",
    ),
    EndpointSpec(
        "api_predictions_context",
        "GET",
        "/api/predictions/{prediction_id}/context",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_engine_calendar",
        "GET",
        "/api/engine/calendar",
        "api_smoke",
        "intelligence_baseline",
        "allowed",
        "normal",
    ),
    EndpointSpec(
        "api_system_data_health",
        "GET",
        "/api/system/data-health",
        "both",
        "intelligence_baseline",
        "warn",
        "high",
    ),
)


ENDPOINTS_BY_ID: dict[str, EndpointSpec] = {ep.id: ep for ep in ENDPOINTS}
REGISTERED_GET_PATHS: set[str] = {ep.path_template for ep in ENDPOINTS}

