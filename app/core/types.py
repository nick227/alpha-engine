from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal

Direction = Literal["positive", "negative", "neutral", "mixed"]
PredictionDirection = Literal["up", "down", "flat"]


@dataclass(slots=True)
class RawEvent:
    id: str
    timestamp: datetime
    source: str
    text: str
    tickers: List[str]
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScoredEvent:
    id: str
    raw_event_id: str
    primary_ticker: str
    category: str
    materiality: float
    direction: Direction
    confidence: float
    company_relevance: float
    concept_tags: List[str]
    explanation_terms: List[str]
    scorer_version: str = "v2"
    taxonomy_version: str = "v1"


@dataclass(slots=True)
class MRAOutcome:
    id: str
    scored_event_id: str
    return_1m: float
    return_5m: float
    return_15m: float
    return_1h: float
    volume_ratio: float
    vwap_distance: float
    range_expansion: float
    continuation_slope: float
    pullback_depth: float
    mra_score: float
    market_context: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyConfig:
    id: str
    name: str
    version: str
    strategy_type: str
    mode: str
    config: Dict[str, Any]
    active: bool = True


@dataclass(slots=True)
class Prediction:
    id: str
    strategy_id: str
    scored_event_id: str
    ticker: str
    timestamp: datetime
    prediction: PredictionDirection
    confidence: float
    horizon: str
    entry_price: float
    mode: str
    feature_snapshot: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PredictionOutcome:
    id: str
    prediction_id: str
    exit_price: float
    return_pct: float
    direction_correct: bool
    max_runup: float
    max_drawdown: float
    evaluated_at: datetime
    exit_reason: str = "horizon"