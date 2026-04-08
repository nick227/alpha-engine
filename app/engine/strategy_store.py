from __future__ import annotations

import json
from typing import Any

from app.core.repository import Repository
from app.core.types import StrategyConfig


def load_active_strategy_configs_from_db(repo: Repository, tenant_id: str = "default") -> list[StrategyConfig]:
    rows = repo.conn.execute(
        """
        SELECT id, name, version, strategy_type, mode, active, config_json
        FROM strategies
        WHERE tenant_id = ? AND active = 1
        ORDER BY strategy_type, name, version
        """,
        (tenant_id,),
    ).fetchall()

    out: list[StrategyConfig] = []
    for row in rows:
        try:
            cfg = json.loads(str(row["config_json"] or "{}"))
        except Exception:
            cfg = {}
        out.append(
            StrategyConfig(
                id=str(row["id"]),
                name=str(row["name"]),
                version=str(row["version"]),
                strategy_type=str(row["strategy_type"]),
                mode=str(row["mode"]),
                active=bool(int(row["active"])),
                config=dict(cfg) if isinstance(cfg, dict) else {},
            )
        )
    return out


def bootstrap_strategies_from_experiments(repo: Repository, tenant_id: str = "default") -> int:
    """
    Seeds the `strategies` table from `experiments/strategies/*.json` if empty.
    Returns count inserted (best-effort).
    """
    existing = repo.conn.execute(
        "SELECT COUNT(*) as c FROM strategies WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()
    if existing and int(existing["c"]) > 0:
        return 0

    from app.engine.runner import load_strategy_configs

    configs = load_strategy_configs()
    for cfg in configs:
        repo.persist_strategy(cfg, tenant_id=tenant_id)
    return len(configs)

