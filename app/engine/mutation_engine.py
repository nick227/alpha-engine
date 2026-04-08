from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import uuid4

from app.core.types import StrategyConfig


class MutationEngine:
    """Creates child strategy variants from a parent config."""

    def __init__(self, mutation_steps: dict[str, list[Any]] | None = None) -> None:
        self.mutation_steps = mutation_steps or {
            "confidence_threshold": [-0.05, 0.05],
            "hold_minutes": [-5, 5],
            "consensus_bonus": [-0.02, 0.02],
        }

    def mutate(self, parent: dict, max_children: int = 10) -> list[dict]:
        children: list[dict] = []
        parent_name = parent.get("name", "strategy")
        parent_version = parent.get("version", "0")

        for key, deltas in self.mutation_steps.items():
            if key not in parent:
                continue

            for delta in deltas:
                child = deepcopy(parent)
                current = child[key]
                if isinstance(current, (int, float)):
                    child[key] = round(current + delta, 4)
                else:
                    continue

                child["parent_id"] = parent.get("id")
                child["status"] = "CANDIDATE"
                child["version"] = f"{parent_version}.{len(children) + 1}"
                child["name"] = f"{parent_name}_mut_{key}_{len(children) + 1}"
                children.append(child)

                if len(children) >= max_children:
                    return children
        return children


def mutate_strategy_config(parent: StrategyConfig, max_children: int = 10) -> list[StrategyConfig]:
    """
    Creates mutated StrategyConfig variants by perturbing numeric fields inside `config`.

    This is intentionally simple: it supports the current StrategyBase strategies and is meant
    to power a first working optimizer loop (not a perfect evolutionary search).
    """
    base = deepcopy(parent.config)
    children: list[StrategyConfig] = []

    def add(patch: dict[str, Any], suffix: str) -> None:
        nonlocal children
        if len(children) >= max_children:
            return
        cfg = deepcopy(base)
        cfg.update(patch)

        if parent.strategy_type == "text_mra":
            tw = float(cfg.get("text_weight", 0.6))
            tw = max(0.0, min(1.0, tw))
            cfg["text_weight"] = tw
            cfg["mra_weight"] = max(0.0, min(1.0, 1.0 - tw))

        children.append(
            StrategyConfig(
                id=str(uuid4()),
                name=f"{parent.name}_mut_{suffix}",
                version=f"{parent.version}.m{len(children)+1}",
                strategy_type=parent.strategy_type,
                mode=parent.mode,
                active=False,
                config=cfg,
            )
        )

    st = parent.strategy_type
    if st == "text_mra":
        add({"min_materiality": max(0.0, float(base.get("min_materiality", 0.4)) - 0.05)}, "mat_dn")
        add({"min_materiality": min(1.0, float(base.get("min_materiality", 0.4)) + 0.05)}, "mat_up")
        add({"min_mra_score": max(0.0, float(base.get("min_mra_score", 0.15)) - 0.05)}, "mra_dn")
        add({"min_mra_score": min(1.0, float(base.get("min_mra_score", 0.15)) + 0.05)}, "mra_up")
        add({"text_weight": float(base.get("text_weight", 0.6)) - 0.1}, "tw_dn")
        add({"text_weight": float(base.get("text_weight", 0.6)) + 0.1}, "tw_up")
    elif st == "baseline_momentum":
        add({"min_short_trend": max(0.0001, float(base.get("min_short_trend", 0.004)) - 0.001)}, "trend_dn")
        add({"min_short_trend": float(base.get("min_short_trend", 0.004)) + 0.001}, "trend_up")
    elif st == "technical_vwap_reclaim":
        add({"min_volume_ratio": max(1.0, float(base.get("min_volume_ratio", 1.5)) - 0.2)}, "vr_dn")
        add({"min_volume_ratio": float(base.get("min_volume_ratio", 1.5)) + 0.2}, "vr_up")
    elif st == "technical_rsi_reversion":
        oversold = float(base.get("oversold", 30.0))
        overbought = float(base.get("overbought", 70.0))
        add({"oversold": max(10.0, oversold - 5.0)}, "os_dn")
        add({"oversold": min(overbought - 5.0, oversold + 5.0)}, "os_up")
        add({"overbought": max(oversold + 5.0, overbought - 5.0)}, "ob_dn")
        add({"overbought": min(95.0, overbought + 5.0)}, "ob_up")
    elif st == "technical_bollinger_reversion":
        add({"zscore_threshold": max(0.5, float(base.get("zscore_threshold", 2.0)) - 0.3)}, "z_dn")
        add({"zscore_threshold": float(base.get("zscore_threshold", 2.0)) + 0.3}, "z_up")

    return children[:max_children]

