from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.core.repository import Repository
from app.core.types import StrategyConfig
from app.engine.runner import _strategy_track


@dataclass(frozen=True)
class ChampionPick:
    config: StrategyConfig
    track: str
    stability_score: float
    prediction_count: int
    accuracy: float
    avg_return: float
    avg_residual_alpha: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.config.id,
            "name": self.config.name,
            "version": self.config.version,
            "strategy_type": self.config.strategy_type,
            "mode": self.config.mode,
            "track": self.track,
            "stability_score": self.stability_score,
            "prediction_count": self.prediction_count,
            "accuracy": self.accuracy,
            "avg_return": self.avg_return,
            "avg_residual_alpha": self.avg_residual_alpha,
        }


def _safe_json_dict(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def select_champions(
    repo: Repository,
    *,
    tenant_id: str = "default",
    min_predictions: int = 5,
) -> dict[str, ChampionPick]:
    """
    Select one champion per track ("sentiment", "quant") from active strategies.

    Ranking (desc):
      1) stability_score
      2) avg_return
      3) accuracy
      4) prediction_count

    If no strategy meets `min_predictions` for a track, fall back to best available.
    """
    rows = repo.conn.execute(
        """
        SELECT
          s.id,
          s.name,
          s.version,
          s.strategy_type,
          s.mode,
          s.active,
          s.config_json,
          COALESCE(sp.prediction_count, 0) as prediction_count,
          COALESCE(sp.accuracy, 0.0) as accuracy,
          COALESCE(sp.avg_return, 0.0) as avg_return,
          COALESCE(sp.avg_residual_alpha, 0.0) as avg_residual_alpha,
          COALESCE(ss.stability_score, 0.5) as stability_score
        FROM strategies s
        LEFT JOIN strategy_performance sp
          ON sp.tenant_id = s.tenant_id
         AND sp.strategy_id = s.id
         AND sp.horizon = 'ALL'
        LEFT JOIN strategy_stability ss
          ON ss.tenant_id = s.tenant_id
         AND ss.strategy_id = s.id
        WHERE s.tenant_id = ?
          AND s.active = 1
          AND LOWER(s.strategy_type) <> 'consensus'
        """,
        (tenant_id,),
    ).fetchall()

    candidates: list[ChampionPick] = []
    for r in rows:
        cfg = StrategyConfig(
            id=str(r["id"]),
            name=str(r["name"]),
            version=str(r["version"]),
            strategy_type=str(r["strategy_type"]),
            mode=str(r["mode"]),
            active=bool(int(r["active"])),
            config=_safe_json_dict(str(r["config_json"])),
        )
        track = _strategy_track(cfg.strategy_type)
        if track not in ("sentiment", "quant"):
            continue

        candidates.append(
            ChampionPick(
                config=cfg,
                track=track,
                stability_score=float(r["stability_score"]),
                prediction_count=int(r["prediction_count"]),
                accuracy=float(r["accuracy"]),
                avg_return=float(r["avg_return"]),
                avg_residual_alpha=float(r["avg_residual_alpha"]),
            )
        )

    picks: dict[str, ChampionPick] = {}
    for track in ("sentiment", "quant"):
        pool = [c for c in candidates if c.track == track]
        if not pool:
            continue

        eligible = [c for c in pool if c.prediction_count >= int(min_predictions)]
        ranked_pool = eligible if eligible else pool
        picks[track] = max(
            ranked_pool,
            key=lambda c: (c.stability_score, c.avg_return, c.accuracy, c.prediction_count),
        )

    return picks


def persist_champion_snapshot(
    repo: Repository,
    champions: dict[str, ChampionPick],
    *,
    key: str = "champions:last",
    tenant_id: str = "default",
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "captured_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "sentiment": champions.get("sentiment").to_dict() if champions.get("sentiment") else None,
        "quant": champions.get("quant").to_dict() if champions.get("quant") else None,
    }
    repo.set_kv(key, json.dumps(payload, sort_keys=True), tenant_id=tenant_id)
    return payload

