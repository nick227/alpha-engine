"""
Threshold-based enqueue from a discovery run summary (multi-strategy throughput).

Used by discovery_cli nightly and queue_discovery_predictions so promotion is
rule-driven (score / confidence bars + optional per-strategy caps) instead of a
fixed short allowlist only.
"""

from __future__ import annotations

import json
import os
from typing import Any

from app.discovery.strategies.registry import STRATEGIES, THRESHOLDS

# Signal density: cap total supplemental rows per as-of date (env override).
DEFAULT_TARGET_SIGNALS_PER_DAY = int(os.getenv("ALPHA_TARGET_SIGNALS_PER_DAY", "120"))
DEFAULT_PER_STRATEGY_CAP = int(os.getenv("ALPHA_PER_STRATEGY_CAP", "22"))
# Confidence = same 0..1 scale as cross-sectional score in DiscoveryCandidate.
DEFAULT_MIN_CONFIDENCE = float(os.getenv("ALPHA_MIN_DISCOVERY_CONFIDENCE", "0.42"))

# Optional comma-separated strategies to skip (e.g. "narrative_lag").
def _inactive_strategies() -> set[str]:
    raw = os.getenv("ALPHA_INACTIVE_STRATEGIES", "")
    return {s.strip() for s in raw.split(",") if s.strip()}


def _default_horizon_for(strategy: str) -> int:
    if strategy == "balance_sheet_survivor":
        return 5
    if strategy == "silent_compounder":
        return 20
    if strategy == "sniper_coil":
        return 5
    return 15


def build_threshold_queue_rows(
    *,
    disc_summary: dict[str, Any],
    as_of_str: str,
    target_signals: int | None = None,
    per_strategy_cap: int | None = None,
    min_confidence: float | None = None,
    promoted_overrides: dict[str, dict[str, Any]] | None = None,
    exclude_symbols: set[str] | None = None,
    source_pipeline: str = "threshold_queue",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Build prediction_queue row dicts from run_discovery summary.

    Returns (rows, stats_by_strategy_counts).
    """
    target = int(target_signals or DEFAULT_TARGET_SIGNALS_PER_DAY)
    per_cap = int(per_strategy_cap or DEFAULT_PER_STRATEGY_CAP)
    min_conf = float(min_confidence if min_confidence is not None else DEFAULT_MIN_CONFIDENCE)
    promoted = promoted_overrides or {}
    inactive = _inactive_strategies()
    excluded = exclude_symbols or set()

    strategies_block = disc_summary.get("strategies") or {}
    scored: list[tuple[float, str, str, dict[str, Any]]] = []

    for strategy_name in STRATEGIES:
        if strategy_name in inactive:
            continue
        st_summary = strategies_block.get(strategy_name)
        if not isinstance(st_summary, dict):
            continue
        tops = st_summary.get("top") or []
        if not isinstance(tops, list):
            continue
        min_bar = max(float(THRESHOLDS.get(strategy_name, 0.5)), min_conf)
        cfg = promoted.get(strategy_name) if isinstance(promoted.get(strategy_name), dict) else {}
        min_close = float(cfg.get("min_close", 5.0))
        max_close = float(cfg.get("max_close", 1e9))
        direction = str(cfg.get("direction", "UP"))
        horizon_days = int(cfg.get("horizon_days", _default_horizon_for(strategy_name)))
        priority_base = int(cfg.get("priority_base", 12))

        n = 0
        for c in tops[:per_cap]:
            if not isinstance(c, dict):
                continue
            raw = float(c.get("score") or 0.0)
            if raw < min_bar:
                continue
            sym = str(c.get("symbol") or "").strip().upper()
            if not sym or sym in excluded:
                continue
            meta = c.get("metadata") or {}
            if not isinstance(meta, dict):
                meta = {}
            close = float(meta.get("close") or 0.0)
            if close < min_close or close >= max_close:
                continue

            metadata = {
                "strategy": strategy_name,
                "direction": direction,
                "avg_score": raw,
                "horizon_days": horizon_days,
                "close": close,
                "raw_score": raw,
                "source_pipeline": source_pipeline,
                "as_of_date": as_of_str,
            }
            priority = priority_base + int((1.0 - raw) * 10)
            scored.append(
                (
                    raw,
                    strategy_name,
                    sym,
                    {
                        "symbol": sym,
                        "source": f"discovery_{strategy_name}",
                        "priority": priority,
                        "status": "pending",
                        "metadata_json": json.dumps(metadata, sort_keys=True),
                    },
                )
            )
            n += 1

    scored.sort(key=lambda t: -t[0])
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    by_strategy: dict[str, int] = {}
    for _score, strat, sym, row in scored:
        if sym in seen:
            continue
        if len(out) >= target:
            break
        seen.add(sym)
        out.append(row)
        by_strategy[strat] = by_strategy.get(strat, 0) + 1

    return out, by_strategy
