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
    scored_total_at_switch: int | None = None
    reason: str | None = None
    meta: dict[str, Any] | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "track": self.track,
                "strategy_id": self.strategy_id,
                "updated_at": self.updated_at,
                "scored_total_at_switch": self.scored_total_at_switch,
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
    scored_total_at_switch: int | None = None,
    reason: str | None = None,
    meta: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> ActiveChampion:
    now = now or datetime.now(timezone.utc)
    champ = ActiveChampion(
        track=str(track),
        strategy_id=str(strategy_id),
        updated_at=_isoz(now),
        scored_total_at_switch=int(scored_total_at_switch) if scored_total_at_switch is not None else None,
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


def _get_active_champion_payload(repo: Repository, *, track: str, tenant_id: str = "default") -> dict[str, Any]:
    raw = repo.get_kv(f"champions:active:{track}", tenant_id=tenant_id)
    return _safe_json(raw)


def _total_scored_outcomes(repo: Repository, *, tenant_id: str = "default") -> int:
    row = repo.conn.execute(
        "SELECT COUNT(*) as c FROM prediction_outcomes WHERE tenant_id = ? AND exit_reason = 'horizon'",
        (tenant_id,),
    ).fetchone()
    return int(row["c"]) if row is not None else 0


def _candidate_metrics(repo: Repository, *, strategy_id: str, tenant_id: str = "default") -> dict[str, Any] | None:
    row = repo.conn.execute(
        """
        SELECT
          s.id as id,
          s.strategy_type as strategy_type,
          COALESCE(sp.prediction_count, 0) as prediction_count,
          COALESCE(sp.accuracy, 0.0) as accuracy,
          COALESCE(sp.avg_return, 0.0) as avg_return,
          COALESCE(ss.stability_score, 0.5) as stability_score
        FROM strategies s
        LEFT JOIN strategy_performance sp
          ON sp.tenant_id = s.tenant_id
         AND sp.strategy_id = s.id
         AND sp.horizon = 'ALL'
        LEFT JOIN strategy_stability ss
          ON ss.tenant_id = s.tenant_id
         AND ss.strategy_id = s.id
        WHERE s.tenant_id = ? AND s.id = ? AND s.active = 1
        """,
        (tenant_id, str(strategy_id)),
    ).fetchone()
    if row is None:
        return None
    return {
        "strategy_id": str(row["id"]),
        "strategy_type": str(row["strategy_type"]),
        "prediction_count": int(row["prediction_count"]),
        "accuracy": float(row["accuracy"]),
        "avg_return": float(row["avg_return"]),
        "stability_score": float(row["stability_score"]),
    }


def _should_switch(
    *,
    incumbent: dict[str, Any] | None,
    challenger: dict[str, Any],
    min_delta_stability: float = 0.02,
    min_delta_avg_return: float = 0.001,
) -> bool:
    if incumbent is None:
        return True

    inc_stab = float(incumbent.get("stability_score", 0.5))
    ch_stab = float(challenger.get("stability_score", 0.5))
    inc_ret = float(incumbent.get("avg_return", 0.0))
    ch_ret = float(challenger.get("avg_return", 0.0))

    # Primary: require a meaningful stability improvement.
    if ch_stab >= inc_stab + float(min_delta_stability):
        return True

    # If stability is comparable, allow a meaningful return improvement.
    if abs(ch_stab - inc_stab) < float(min_delta_stability) and ch_ret >= inc_ret + float(min_delta_avg_return):
        return True

    return False


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
    fear_regime: bool = False,
) -> dict[str, Any]:
    """
    Compute ranked champions (from performance/stability), persist:
      - `champions:last` snapshot for UI/debug,
      - `champions:active:{track}` for LiveLoop fast-path.

    Note: this intentionally only considers active strategies, so setting them as active champions is safe.

    Args:
        fear_regime: When True (VIX > VIX3M), mean-reversion and ML strategies receive
                     a ranking bonus in the quant pool. Pass from RegimeContext.fear_regime.
    """
    now = now or datetime.now(timezone.utc)
    champs = select_champions(repo, tenant_id=tenant_id, min_predictions=min_predictions,
                               fear_regime=fear_regime)
    snap = persist_champion_snapshot(repo, champs, tenant_id=tenant_id, now=now)

    total_scored = _total_scored_outcomes(repo, tenant_id=tenant_id)
    min_scored_between_switches = 50

    for track, pick in champs.items():
        incumbent_payload = _get_active_champion_payload(repo, track=track, tenant_id=tenant_id)
        incumbent_id = str(incumbent_payload.get("strategy_id") or "").strip() or None
        incumbent_scored_at_switch = incumbent_payload.get("scored_total_at_switch")
        try:
            incumbent_scored_at_switch_i = int(incumbent_scored_at_switch) if incumbent_scored_at_switch is not None else None
        except Exception:
            incumbent_scored_at_switch_i = None

        # Default if we don't know: treat as "just switched".
        if incumbent_scored_at_switch_i is None:
            incumbent_scored_at_switch_i = total_scored

        challenger_id = pick.config.id
        challenger_metrics = {
            "strategy_id": challenger_id,
            "stability_score": pick.stability_score,
            "avg_return": pick.avg_return,
            "accuracy": pick.accuracy,
            "prediction_count": pick.prediction_count,
        }

        if incumbent_id == challenger_id:
            # Update metadata without resetting cooldown counter.
            set_active_champion(
                repo,
                track=track,
                strategy_id=challenger_id,
                tenant_id=tenant_id,
                scored_total_at_switch=incumbent_scored_at_switch_i,
                reason="ranked_refresh_same",
                meta=challenger_metrics,
                now=now,
            )
            continue

        # Cooldown: avoid flapping.
        if incumbent_id is not None and (total_scored - incumbent_scored_at_switch_i) < min_scored_between_switches:
            continue

        incumbent_metrics = _candidate_metrics(repo, strategy_id=incumbent_id, tenant_id=tenant_id) if incumbent_id else None
        if not _should_switch(incumbent=incumbent_metrics, challenger=challenger_metrics):
            continue

        set_active_champion(
            repo,
            track=track,
            strategy_id=challenger_id,
            tenant_id=tenant_id,
            scored_total_at_switch=total_scored,
            reason="ranked_refresh_switched",
            meta=challenger_metrics,
            now=now,
        )
    return snap
