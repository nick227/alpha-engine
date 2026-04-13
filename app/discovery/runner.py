from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from app.db.repository import AlphaRepository
from app.core.target_stocks import get_target_stocks, get_target_stocks_registry
from app.discovery.feature_snapshot import build_feature_snapshot
from app.discovery.strategies import STRATEGIES, score_candidates, to_repo_rows


def _parse_date(s: str | date) -> str:
    if isinstance(s, date):
        return s.isoformat()
    return date.fromisoformat(str(s).strip()).isoformat()


def run_discovery(
    *,
    db_path: str | Path = "data/alpha.db",
    tenant_id: str = "default",
    as_of: str | date,
    top_n: int = 50,
    min_avg_dollar_volume_20d: float | None = None,
    timeframe: str = "1d",
    use_target_universe: bool = False,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run all discovery strategies and persist top candidates per strategy.

    Returns a JSON-serializable summary:
      { "as_of_date": "...", "strategies": {strategy: {"top": [...], "top_lt5": [...]}} }
    """
    as_of_date = _parse_date(as_of)
    universe_version = None
    universe_symbols: list[str] | None = symbols
    if use_target_universe and symbols is None:
        universe_symbols = get_target_stocks(asof=date.fromisoformat(as_of_date))
        universe_version = get_target_stocks_registry().target_universe_version

    repo = AlphaRepository(db_path=db_path)
    try:
        features = build_feature_snapshot(
            db_path=db_path,
            as_of=as_of_date,
            tenant_id=tenant_id,
            timeframe=timeframe,
            symbols=universe_symbols,
        )

        if min_avg_dollar_volume_20d is not None:
            features = {
                s: fr
                for s, fr in features.items()
                if fr.avg_dollar_volume_20d is not None and fr.avg_dollar_volume_20d >= float(min_avg_dollar_volume_20d)
            }

        summary: dict[str, Any] = {
            "as_of_date": as_of_date,
            "tenant_id": tenant_id,
            "universe_version": universe_version,
            "universe_size": len(universe_symbols) if universe_symbols is not None else None,
            "feature_rows": len(features),
            "strategies": {},
        }
        for strat in STRATEGIES.keys():
            cands = score_candidates(features, strategy_type=strat)
            top = cands[: int(top_n)]
            top_lt5 = [c for c in cands if (c.metadata.get("close") is not None and float(c.metadata["close"]) < 5.0)][
                : int(top_n)
            ]

            repo_rows = to_repo_rows(top)
            repo.upsert_discovery_candidates(as_of_date=as_of_date, candidates=repo_rows, tenant_id=tenant_id)

            summary["strategies"][strat] = {
                "top": [asdict(c) for c in top],
                "top_lt5": [asdict(c) for c in top_lt5],
            }
        return summary
    finally:
        repo.close()


def format_summary_json(summary: dict[str, Any]) -> str:
    return json.dumps(summary, indent=2, sort_keys=True)
