from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.core.repository import Repository
from app.core.types import StrategyConfig
from app.engine.champion_selector import persist_champion_snapshot, select_champions


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception:
        return {}


@dataclass(frozen=True)
class ActiveChampion:
    track: str
    strategy_id: str
    updated_at: str
    reason: str | None = None
    meta: dict[str, Any] | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "track": self.track,
                "strategy_id": self.strategy_id,
                "updated_at": self.updated_at,
                "reason": self.reason,
                "meta": self.meta or {},
            },
            sort_keys=True,
        )


def set_active_champion(
    repo: Repository,
    *,
    track: str,
    strategy_id: str,
    tenant_id: str = "default",
    reason: str | None = None,
    meta: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> ActiveChampion:
    now = now or datetime.now(timezone.utc)
    champ = ActiveChampion(
        track=str(track),
        strategy_id=str(strategy_id),
        updated_at=_isoz(now),
        reason=reason,
        meta=dict(meta or {}),
    )
    repo.set_kv(f"champions:active:{track}", champ.to_json(), tenant_id=tenant_id)
    return champ


def get_active_champion_id(repo: Repository, *, track: str, tenant_id: str = "default") -> str | None:
    raw = repo.get_kv(f"champions:active:{track}", tenant_id=tenant_id)
    payload = _safe_json(raw)
    sid = str(payload.get("strategy_id") or "").strip()
    return sid or None


def _load_strategy_config(repo: Repository, *, strategy_id: str, tenant_id: str = "default") -> StrategyConfig | None:
    row = repo.conn.execute(
        """
        SELECT id, name, version, strategy_type, mode, active, config_json
        FROM strategies
        WHERE tenant_id = ? AND id = ?
        """,
        (tenant_id, str(strategy_id)),
    ).fetchone()
    if row is None:
        return None
    try:
        cfg = json.loads(str(row["config_json"] or "{}"))
    except Exception:
        cfg = {}
    return StrategyConfig(
        id=str(row["id"]),
        name=str(row["name"]),
        version=str(row["version"]),
        strategy_type=str(row["strategy_type"]),
        mode=str(row["mode"]),
        active=bool(int(row["active"])),
        config=dict(cfg) if isinstance(cfg, dict) else {},
    )


def load_active_champion_configs(
    repo: Repository,
    *,
    tenant_id: str = "default",
) -> list[StrategyConfig]:
    """
    Fast path for LiveLoop:
    - read champion ids from `system_kv` (no ranking query),
    - load those StrategyConfigs from `strategies`,
    - ignore missing/inactive ones.
    """
    out: list[StrategyConfig] = []
    for track in ("sentiment", "quant"):
        sid = get_active_champion_id(repo, track=track, tenant_id=tenant_id)
        if not sid:
            continue
        cfg = _load_strategy_config(repo, strategy_id=sid, tenant_id=tenant_id)
        if cfg is None or not cfg.active:
            continue
        out.append(cfg)
    return out


def refresh_active_champions_from_ranked(
    repo: Repository,
    *,
    tenant_id: str = "default",
    min_predictions: int = 5,
    now: datetime | None = None,
) -> dict[str, Any]:
    """
    Compute ranked champions (from performance/stability), persist:
      - `champions:last` snapshot for UI/debug,
      - `champions:active:{track}` for LiveLoop fast-path.

    Note: this intentionally only considers active strategies, so setting them as active champions is safe.
    """
    now = now or datetime.now(timezone.utc)
    champs = select_champions(repo, tenant_id=tenant_id, min_predictions=min_predictions)
    snap = persist_champion_snapshot(repo, champs, tenant_id=tenant_id, now=now)
    for track, pick in champs.items():
        set_active_champion(
            repo,
            track=track,
            strategy_id=pick.config.id,
            tenant_id=tenant_id,
            reason="ranked_refresh",
            meta={
                "stability_score": pick.stability_score,
                "avg_return": pick.avg_return,
                "accuracy": pick.accuracy,
                "prediction_count": pick.prediction_count,
            },
            now=now,
        )
    return snap

