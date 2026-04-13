from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WatchlistRow:
    symbol: str
    overlap_count: int
    days_seen: int
    avg_score: float
    strategies: list[str]
    playbook_id: str
    prediction_plan: dict[str, Any]


def _asof(s: str | date) -> str:
    if isinstance(s, date):
        return s.isoformat()
    return date.fromisoformat(str(s).strip()).isoformat()


def select_high_conviction(
    *,
    db_path: str | Path,
    tenant_id: str,
    as_of_date: str | date,
    window_days: int = 5,
    min_overlap: int = 2,
    min_days_seen: int = 3,
    min_avg_score: float = 0.85,
    top_k: int = 20,
) -> list[WatchlistRow]:
    """
    Select Tier-1 candidates: overlap >= 2 AND days_seen >= N over trailing window.

    avg_score is computed across strategy rows for the as_of_date.
    days_seen counts distinct as_of_date occurrences for the symbol over the trailing window (across all strategies).
    """
    asof = _asof(as_of_date)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # 1) overlap + avg_score on the day
    day = conn.execute(
        """
        SELECT
          symbol,
          COUNT(DISTINCT strategy_type) as overlap_count,
          AVG(score) as avg_score,
          group_concat(DISTINCT strategy_type) as strategies_csv
        FROM discovery_candidates
        WHERE tenant_id = ? AND as_of_date = ?
        GROUP BY symbol
        HAVING COUNT(DISTINCT strategy_type) >= ?
           AND AVG(score) >= ?
        """,
        (str(tenant_id), asof, int(min_overlap), float(min_avg_score)),
    ).fetchall()

    if not day:
        return []

    by_symbol: dict[str, dict[str, Any]] = {}
    for r in day:
        by_symbol[str(r["symbol"])] = {
            "symbol": str(r["symbol"]),
            "overlap_count": int(r["overlap_count"]),
            "avg_score": float(r["avg_score"]),
            "strategies": [s for s in str(r["strategies_csv"] or "").split(",") if s],
        }

    # 2) persistence over trailing window (distinct days)
    symbols = list(by_symbol.keys())
    placeholders = ",".join(["?"] * len(symbols))
    pers = conn.execute(
        f"""
        SELECT symbol, COUNT(DISTINCT as_of_date) as days_seen
        FROM discovery_candidates
        WHERE tenant_id = ?
          AND as_of_date <= ?
          AND as_of_date >= date(?, '-' || (? - 1) || ' day')
          AND symbol IN ({placeholders})
        GROUP BY symbol
        """,
        [str(tenant_id), asof, asof, int(window_days), *symbols],
    ).fetchall()

    days_seen_map = {str(r["symbol"]): int(r["days_seen"]) for r in pers}

    out: list[WatchlistRow] = []
    for sym, payload in by_symbol.items():
        ds = int(days_seen_map.get(sym, 0))
        if ds < int(min_days_seen):
            continue
        playbook_id, prediction_plan = assign_playbook(list(payload.get("strategies") or []))
        out.append(
            WatchlistRow(
                symbol=sym,
                overlap_count=int(payload["overlap_count"]),
                days_seen=ds,
                avg_score=float(payload["avg_score"]),
                strategies=list(payload["strategies"]),
                playbook_id=str(playbook_id),
                prediction_plan=dict(prediction_plan),
            )
        )

    out.sort(key=lambda r: (r.overlap_count, r.days_seen, r.avg_score), reverse=True)
    return out[: int(top_k)]

def assign_playbook(discovery_strategies: list[str]) -> tuple[str, dict[str, Any]]:
    """
    Map discovery situation -> playbook and recommended prediction plan.

    This is intentionally rule-based and transparent.
    """
    s = {str(x).strip().lower() for x in (discovery_strategies or []) if str(x).strip()}

    def plan(*, horizons: list[str], prediction_strategy_types: list[str]) -> dict[str, Any]:
        return {
            "horizons": list(horizons),
            "prediction_strategy_types": list(prediction_strategy_types),
        }

    # Priority order matters when a symbol matches multiple discovery scenarios.
    if {"realness_repricer", "balance_sheet_survivor"}.issubset(s):
        return (
            "distressed_repricer",
            plan(
                horizons=["1d", "7d", "30d"],
                prediction_strategy_types=[
                    "technical_rsi_reversion",
                    "technical_bollinger_reversion",
                    "technical_vwap_reclaim",
                    "text_mra",
                ],
            ),
        )

    if "ownership_vacuum" in s:
        return (
            "early_accumulation_breakout",
            plan(
                horizons=["1d", "7d"],
                prediction_strategy_types=[
                    "technical_vol_expansion_continuation",
                    "technical_range_breakout_continuation",
                    "technical_vwap_reclaim",
                ],
            ),
        )

    if "silent_compounder" in s:
        return (
            "silent_compounder_trend_adoption",
            plan(
                horizons=["30d"],
                prediction_strategy_types=[
                    "baseline_momentum",
                    "cross_asset_relative_strength",
                    "ml_factor",
                ],
            ),
        )

    if "narrative_lag" in s:
        return (
            "narrative_lag_catchup",
            plan(
                horizons=["7d", "30d"],
                prediction_strategy_types=[
                    "baseline_momentum",
                    "technical_range_breakout_continuation",
                    "cross_asset_relative_strength",
                    "text_mra",
                ],
            ),
        )

    return ("unclassified", plan(horizons=["7d"], prediction_strategy_types=["baseline_momentum"]))


def watchlist_to_repo_rows(rows: list[WatchlistRow]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "symbol": r.symbol,
                "overlap_count": int(r.overlap_count),
                "days_seen": int(r.days_seen),
                "avg_score": float(r.avg_score),
                "playbook_id": str(r.playbook_id),
                "prediction_plan_json": json.dumps(dict(r.prediction_plan), separators=(",", ":"), sort_keys=True),
                "strategies_json": json.dumps(list(r.strategies), separators=(",", ":"), sort_keys=True),
            }
        )
    return out


def watchlist_to_queue_rows(rows: list[WatchlistRow], *, source: str = "discovery") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        priority = int(r.overlap_count) * 10 + int(r.days_seen)
        out.append(
            {
                "symbol": r.symbol,
                "source": source,
                "priority": priority,
                "status": "pending",
                "metadata_json": json.dumps(
                    {
                        "playbook_id": str(r.playbook_id),
                        "prediction_plan": dict(r.prediction_plan),
                        "overlap_count": int(r.overlap_count),
                        "days_seen": int(r.days_seen),
                        "avg_score": float(r.avg_score),
                        "strategies": list(r.strategies),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
        )
    return out
