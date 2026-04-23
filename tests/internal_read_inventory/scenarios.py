"""Seed scenario names for inventory-driven verification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ScenarioName = Literal[
    "market_baseline",
    "intelligence_baseline",
    "minimal_empty",
    "stale_job",
]


@dataclass(frozen=True, slots=True)
class Scenario:
    name: ScenarioName
    description: str


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        name="market_baseline",
        description="Seed market bars/profiles/rankings so read endpoints should be populated.",
    ),
    Scenario(
        name="intelligence_baseline",
        description="Seed strategy, prediction, and heartbeat intelligence outputs.",
    ),
    Scenario(
        name="minimal_empty",
        description="Healthy DB with sparse rows to validate expected empty semantics.",
    ),
    Scenario(
        name="stale_job",
        description="Seed stale heartbeat/run windows to validate freshness degradation.",
    ),
)

