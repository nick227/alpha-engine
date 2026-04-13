"""
Factor specification models and YAML loader.

Supports:
  - Rich FactorSpec with all transform types, categories, and per-factor flags
  - FactorMeta  — global quality controls
  - FactorGroup — per-family importance weights
  - FactorConfig — assembled config with horizon filtering and group weight lookup
  - Auto-expansion: shorthand `expand` blocks generate families of factors
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import BaseModel, field_validator, model_validator

# All supported transform identifiers
Transform = Literal[
    "return", "level", "diff",
    "volatility", "zscore", "percentile",
    "atr_ratio", "range_expansion",
    "rsi", "stochastic", "ma_distance", "drawdown",
    "trend_slope", "volume_surge", "dollar_volume_trend",
    "relative_return", "beta",
    # Sentiment/positioning transforms
    "volume_zscore", "dollar_volume_zscore",
    "gap_open", "gap_follow_through",
    "level_diff",
    # Intraday confirmation transforms
    "candle_body", "intraday_trend",
]

Source = Literal["price", "fred", "price_relative"]

# Maps horizon string → calendar days (used for window guard fallback)
HORIZON_DAYS: dict[str, float] = {
    "1h":  1 / 24,
    "4h":  4 / 24,
    "1d":  1.0,
    "7d":  7.0,
    "30d": 30.0,
}


class FactorSpec(BaseModel):
    name: str
    source: Source
    transform: Transform
    window: int

    # price / price_relative
    symbol: Optional[str] = None
    benchmark: Optional[str] = None  # price_relative only

    # fred
    series: Optional[str] = None

    # Metadata
    category: str = "other"
    enabled: bool = True
    lag: int = 0                  # calendar days subtracted from as_of (publication lag)
    publication_lag_days: int = 0 # deprecated alias — prefer lag
    winsorize: bool = True        # set False to skip winsorization for this factor

    @field_validator("symbol", "benchmark", mode="before")
    @classmethod
    def _strip_quotes(cls, v: object) -> object:
        return v.strip('"') if isinstance(v, str) else v

    @field_validator("source", mode="before")
    @classmethod
    def _normalize_source(cls, v: object) -> object:
        # Accept "stock" as an alias for "price"
        if v == "stock":
            return "price"
        return v

    @model_validator(mode="after")
    def _unify_lag(self) -> "FactorSpec":
        # Prefer `lag`; fall back to `publication_lag_days` for compat
        if self.lag == 0 and self.publication_lag_days > 0:
            self.lag = self.publication_lag_days
        return self

    def effective_lag(self) -> int:
        return self.lag

    def resolve_symbol(self, ticker: str) -> Optional[str]:
        """Substitute {ticker} placeholder with the actual stock symbol."""
        if self.symbol is None:
            return None
        return self.symbol.replace("{ticker}", ticker)

    def passes_horizon_guard(self, horizon_days: float) -> bool:
        """
        Automatic window-guard fallback (used when horizon_sets not defined).
        Returns True when window <= horizon_days * 6.
        """
        return self.window <= horizon_days * 6


# ── Supporting config types ──────────────────────────────────────────────────

@dataclass
class FactorMeta:
    min_coverage: float = 0.8
    max_corr: float = 0.92
    min_weight: float = 0.01
    allow_negative: bool = True
    stability_window: int = 30


@dataclass
class FactorGroup:
    weight: float = 1.0


@dataclass
class FactorConfig:
    meta: FactorMeta
    groups: dict[str, FactorGroup]
    horizon_sets: dict[str, list[str]]   # {"1d": ["stock_r1", ...]}
    factors: list[FactorSpec]            # all enabled factors after expansion

    def get_eligible_specs(self, horizon: str, horizon_days: float) -> list[FactorSpec]:
        """
        Return specs eligible for (horizon, horizon_days).

        If a horizon_sets entry exists for `horizon`, use it as a whitelist.
        Otherwise fall back to the automatic window guard.
        Only enabled factors are returned.
        """
        enabled = [s for s in self.factors if s.enabled]

        if horizon in self.horizon_sets:
            allowed = set(self.horizon_sets[horizon])
            return [s for s in enabled if s.name in allowed]

        return [s for s in enabled if s.passes_horizon_guard(horizon_days)]

    def group_weight(self, category: str) -> float:
        """Return the group weight for a category (1.0 default)."""
        return self.groups.get(category, FactorGroup()).weight

    def factor_group_weights(self) -> dict[str, float]:
        """Return {factor_name: group_weight} for all enabled factors."""
        return {s.name: self.group_weight(s.category) for s in self.factors if s.enabled}

    def by_name(self, name: str) -> Optional[FactorSpec]:
        return next((s for s in self.factors if s.name == name), None)


# ── YAML loading + expansion ─────────────────────────────────────────────────

def load_factor_config(path: str = "config/factors.yaml") -> FactorConfig:
    """Load, expand, and validate the full factor configuration."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"factors.yaml not found at {path}")

    raw: dict[str, Any] = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

    # Meta
    meta_raw = raw.get("meta", {})
    meta = FactorMeta(
        min_coverage=float(meta_raw.get("min_coverage", 0.8)),
        max_corr=float(meta_raw.get("max_corr", 0.92)),
        min_weight=float(meta_raw.get("min_weight", 0.01)),
        allow_negative=bool(meta_raw.get("allow_negative", True)),
        stability_window=int(meta_raw.get("stability_window", 30)),
    )

    # Groups
    groups: dict[str, FactorGroup] = {}
    for name, attrs in (raw.get("groups") or {}).items():
        groups[name] = FactorGroup(weight=float((attrs or {}).get("weight", 1.0)))

    # Horizon sets
    horizon_sets: dict[str, list[str]] = {}
    for h, names in (raw.get("horizon_sets") or {}).items():
        horizon_sets[str(h)] = list(names or [])

    # Explicit factors
    factor_rows: list[dict] = list(raw.get("factors") or [])

    # Auto-expansion
    for block in (raw.get("expand") or []):
        template: dict = dict(block.get("template", {}))
        for entry in (block.get("entries") or []):
            merged = {**template, **entry}
            factor_rows.append(merged)

    # Parse and deduplicate (last definition wins)
    seen: dict[str, FactorSpec] = {}
    for row in factor_rows:
        try:
            spec = FactorSpec(**row)
            seen[spec.name] = spec
        except Exception as e:
            print(f"[factors.yaml] Skipping invalid factor {row.get('name', '?')}: {e}")

    return FactorConfig(
        meta=meta,
        groups=groups,
        horizon_sets=horizon_sets,
        factors=list(seen.values()),
    )


def load_factor_specs(path: str = "config/factors.yaml") -> list[FactorSpec]:
    """Convenience wrapper returning just the factor list (backward compat)."""
    return load_factor_config(path).factors
