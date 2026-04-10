from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


def _isoz(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_isoz(value: str | None) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _sha1_16(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def _clamp01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return float(x)


@dataclass(frozen=True, slots=True)
class StrategyTrustResult:
    tenant_id: str
    strategy_id: str
    horizon: str
    trust_score: float
    trust_conservative: float
    trust_exploratory: float
    sample_size: int
    effective_sample_size: float
    calibration_score: float
    stability_score: float
    recency_score: float
    brier: float | None
    mean_confidence: float | None
    realized_accuracy: float | None
    mean_return: float | None
    std_return: float | None
    mean_drawdown: float | None
    std_drawdown: float | None
    evidence_start_at: str | None
    evidence_end_at: str | None
    computed_at: str
    params_json: str
    components_json: str


class TrustEngine:
    """
    Trust metric for prediction reliability.

    Informational only: does not modify confidence or execution behavior.
    Evidence-driven: computed from predictions + prediction_outcomes history.
    """

    VERSION = "trust_v1"

    def __init__(
        self,
        *,
        half_life_days: float = 30.0,
        n0: float = 30.0,
        sigma0_return: float = 0.03,
        sigma0_drawdown: float = 0.05,
    ) -> None:
        self.half_life_days = float(half_life_days)
        self.n0 = float(n0)
        self.sigma0_return = float(sigma0_return)
        self.sigma0_drawdown = float(sigma0_drawdown)

    @staticmethod
    def ensure_strategy_trust_schema(conn: sqlite3.Connection) -> None:
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(strategy_trust)").fetchall()}
        if not cols:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS strategy_trust (
                  id TEXT PRIMARY KEY,
                  tenant_id TEXT NOT NULL,
                  strategy_id TEXT NOT NULL,
                  horizon TEXT NOT NULL,
                  trust_score REAL NOT NULL,
                  trust_conservative REAL NOT NULL,
                  trust_exploratory REAL NOT NULL,
                  sample_size INTEGER NOT NULL,
                  effective_sample_size REAL NOT NULL,
                  calibration_score REAL NOT NULL,
                  stability_score REAL NOT NULL,
                  recency_score REAL NOT NULL,
                  brier REAL,
                  mean_confidence REAL,
                  realized_accuracy REAL,
                  mean_return REAL,
                  std_return REAL,
                  mean_drawdown REAL,
                  std_drawdown REAL,
                  evidence_start_at TEXT,
                  evidence_end_at TEXT,
                  computed_at TEXT NOT NULL,
                  params_json TEXT NOT NULL,
                  components_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(tenant_id, strategy_id, horizon)
                );
                CREATE INDEX IF NOT EXISTS idx_strategy_trust_lookup
                  ON strategy_trust(tenant_id, strategy_id, horizon, updated_at);
                """
            )
            return

        # Additive upgrade for older DBs.
        for col, ddl in (
            ("trust_conservative", "ALTER TABLE strategy_trust ADD COLUMN trust_conservative REAL;"),
            ("trust_exploratory", "ALTER TABLE strategy_trust ADD COLUMN trust_exploratory REAL;"),
        ):
            if col not in cols:
                conn.execute(ddl)

    def compute_strategy_trust(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        strategy_id: str,
        horizon: str,
        as_of: datetime | None = None,
    ) -> StrategyTrustResult:
        as_of_dt = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)

        out_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(prediction_outcomes)").fetchall()}
        ret_sql = "o.return_pct as return_pct"
        if "return_pct" not in out_cols and "actual_return" in out_cols:
            ret_sql = "o.actual_return as return_pct"
        if "return_pct" not in out_cols and "actual_return" not in out_cols:
            ret_sql = "NULL as return_pct"

        dd_sql = "o.max_drawdown as max_drawdown" if "max_drawdown" in out_cols else "NULL as max_drawdown"

        rows = conn.execute(
            f"""
            SELECT
              p.confidence as confidence,
              o.evaluated_at as evaluated_at,
              o.direction_correct as direction_correct,
              {ret_sql},
              {dd_sql}
            FROM predictions p
            JOIN prediction_outcomes o
              ON o.tenant_id = p.tenant_id AND o.prediction_id = p.id
            WHERE p.tenant_id = ?
              AND p.strategy_id = ?
              AND p.horizon = ?
            ORDER BY o.evaluated_at ASC
            """,
            (str(tenant_id), str(strategy_id), str(horizon)),
        ).fetchall()

        obs: list[tuple[float, int, float | None, float | None, datetime]] = []
        for r in rows:
            ev = _parse_isoz(r["evaluated_at"])
            if ev is None or ev > as_of_dt:
                continue
            try:
                c = float(r["confidence"])
                y = 1 if int(r["direction_correct"]) != 0 else 0
            except Exception:
                continue
            ret = None
            dd = None
            try:
                if r["return_pct"] is not None:
                    ret = float(r["return_pct"])
            except Exception:
                ret = None
            try:
                if r["max_drawdown"] is not None:
                    dd = float(r["max_drawdown"])
            except Exception:
                dd = None
            obs.append((c, y, ret, dd, ev))

        n = len(obs)
        params = {
            "version": self.VERSION,
            "half_life_days": self.half_life_days,
            "n0": self.n0,
            "sigma0_return": self.sigma0_return,
            "sigma0_drawdown": self.sigma0_drawdown,
            "as_of": _isoz(as_of_dt),
        }

        if n == 0:
            computed_at = _isoz(as_of_dt)
            components = {"sample_score": 0.0, "calibration_score": 0.0, "stability_score": 0.0, "recency_score": 0.0}
            return StrategyTrustResult(
                tenant_id=str(tenant_id),
                strategy_id=str(strategy_id),
                horizon=str(horizon),
                trust_score=0.0,
                trust_conservative=0.0,
                trust_exploratory=0.0,
                sample_size=0,
                effective_sample_size=0.0,
                calibration_score=0.0,
                stability_score=0.0,
                recency_score=0.0,
                brier=None,
                mean_confidence=None,
                realized_accuracy=None,
                mean_return=None,
                std_return=None,
                mean_drawdown=None,
                std_drawdown=None,
                evidence_start_at=None,
                evidence_end_at=None,
                computed_at=computed_at,
                params_json=json.dumps(params, sort_keys=True, separators=(",", ":")),
                components_json=json.dumps(components, sort_keys=True, separators=(",", ":")),
            )

        ln2 = math.log(2.0)
        denom = max(self.half_life_days, 1e-9)
        weights = []
        for *_, ev in obs:
            age_days = max(0.0, (as_of_dt - ev).total_seconds() / 86400.0)
            weights.append(float(math.exp(-ln2 * age_days / denom)))

        wsum = float(sum(weights))
        if wsum <= 0.0:
            weights = [1.0 for _ in weights]
            wsum = float(n)

        n_eff = float(wsum)
        sample_score = 1.0 - math.exp(-n_eff / max(self.n0, 1e-9))

        mean_c = sum(w * c for (c, *_), w in zip(obs, weights)) / wsum
        acc = sum(w * float(y) for (_, y, *_), w in zip(obs, weights)) / wsum

        brier = sum(w * (c - float(y)) ** 2 for (c, y, *_), w in zip(obs, weights)) / wsum
        calibration_score = _clamp01(1.0 - float(brier))

        def wmean(vals: list[tuple[float, float]]) -> float | None:
            if not vals:
                return None
            sw = sum(w for _, w in vals)
            if sw <= 0:
                return None
            return sum(v * w for v, w in vals) / sw

        def wstd(vals: list[tuple[float, float]]) -> float | None:
            if not vals:
                return None
            if len(vals) < 2:
                return 0.0
            mu = wmean(vals)
            if mu is None:
                return None
            sw = sum(w for _, w in vals)
            if sw <= 0:
                return None
            var = sum(w * (v - mu) ** 2 for v, w in vals) / sw
            return float(math.sqrt(max(0.0, var)))

        returns = [(float(ret), w) for (_, _, ret, _, _), w in zip(obs, weights) if ret is not None]
        dds = [(float(dd), w) for (_, _, _, dd, _), w in zip(obs, weights) if dd is not None]
        mean_ret = wmean(returns)
        std_ret = wstd(returns)
        mean_dd = wmean(dds)
        std_dd = wstd(dds)

        vol_penalty = 0.0
        if std_ret is not None:
            vol_penalty += float(std_ret) / max(self.sigma0_return, 1e-9)
        if std_dd is not None:
            vol_penalty += float(std_dd) / max(self.sigma0_drawdown, 1e-9)
        stability_score = _clamp01(math.exp(-vol_penalty))

        recency_score = _clamp01(float(wsum) / float(n))

        core = (0.45 * calibration_score) + (0.35 * stability_score) + (0.20 * recency_score)
        trust_conservative = _clamp01(sample_score * core)

        # Exploratory: additive blend so new strategies aren't crushed to ~0.
        trust_exploratory = _clamp01(
            (0.25 * sample_score)
            + (0.35 * calibration_score)
            + (0.25 * stability_score)
            + (0.15 * recency_score)
        )

        # Backward-compatible "primary" trust (conservative).
        trust_score = trust_conservative

        ev_start = min(ev for *_, ev in obs)
        ev_end = max(ev for *_, ev in obs)
        computed_at = _isoz(ev_end)  # deterministic for a fixed evidence set

        components = {
            "sample_score": float(sample_score),
            "calibration_score": float(calibration_score),
            "stability_score": float(stability_score),
            "recency_score": float(recency_score),
        }

        return StrategyTrustResult(
            tenant_id=str(tenant_id),
            strategy_id=str(strategy_id),
            horizon=str(horizon),
            trust_score=float(trust_score),
            trust_conservative=float(trust_conservative),
            trust_exploratory=float(trust_exploratory),
            sample_size=int(n),
            effective_sample_size=float(n_eff),
            calibration_score=float(calibration_score),
            stability_score=float(stability_score),
            recency_score=float(recency_score),
            brier=float(brier),
            mean_confidence=float(mean_c),
            realized_accuracy=float(acc),
            mean_return=float(mean_ret) if mean_ret is not None else None,
            std_return=float(std_ret) if std_ret is not None else None,
            mean_drawdown=float(mean_dd) if mean_dd is not None else None,
            std_drawdown=float(std_dd) if std_dd is not None else None,
            evidence_start_at=_isoz(ev_start),
            evidence_end_at=_isoz(ev_end),
            computed_at=computed_at,
            params_json=json.dumps(params, sort_keys=True, separators=(",", ":")),
            components_json=json.dumps(components, sort_keys=True, separators=(",", ":")),
        )

    def upsert_strategy_trust(self, conn: sqlite3.Connection, r: StrategyTrustResult) -> None:
        self.ensure_strategy_trust_schema(conn)
        # Deterministic recomputation: updated_at is tied to the evidence end time.
        now = str(r.computed_at)
        trust_id = f"st_{_sha1_16(f'{r.tenant_id}|{r.strategy_id}|{r.horizon}|{self.VERSION}')}"
        conn.execute(
            """
            INSERT OR REPLACE INTO strategy_trust
              (id, tenant_id, strategy_id, horizon, trust_score, trust_conservative, trust_exploratory,
               sample_size, effective_sample_size,
               calibration_score, stability_score, recency_score,
               brier, mean_confidence, realized_accuracy,
               mean_return, std_return, mean_drawdown, std_drawdown,
               evidence_start_at, evidence_end_at, computed_at,
               params_json, components_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trust_id,
                str(r.tenant_id),
                str(r.strategy_id),
                str(r.horizon),
                float(r.trust_score),
                float(r.trust_conservative),
                float(r.trust_exploratory),
                int(r.sample_size),
                float(r.effective_sample_size),
                float(r.calibration_score),
                float(r.stability_score),
                float(r.recency_score),
                float(r.brier) if r.brier is not None else None,
                float(r.mean_confidence) if r.mean_confidence is not None else None,
                float(r.realized_accuracy) if r.realized_accuracy is not None else None,
                float(r.mean_return) if r.mean_return is not None else None,
                float(r.std_return) if r.std_return is not None else None,
                float(r.mean_drawdown) if r.mean_drawdown is not None else None,
                float(r.std_drawdown) if r.std_drawdown is not None else None,
                r.evidence_start_at,
                r.evidence_end_at,
                r.computed_at,
                r.params_json,
                r.components_json,
                now,
            ),
        )

    def compute_and_persist_strategy_trust(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        strategy_horizons: Iterable[tuple[str, str]],
        as_of: datetime | None = None,
    ) -> dict[tuple[str, str], StrategyTrustResult]:
        out: dict[tuple[str, str], StrategyTrustResult] = {}
        for strategy_id, horizon in sorted({(str(s), str(h)) for s, h in strategy_horizons}):
            r = self.compute_strategy_trust(
                conn, tenant_id=str(tenant_id), strategy_id=str(strategy_id), horizon=str(horizon), as_of=as_of
            )
            self.upsert_strategy_trust(conn, r)
            out[(strategy_id, horizon)] = r
        return out

    @staticmethod
    def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
        try:
            cols = {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            return column in cols
        except Exception:
            return False

    def apply_trust_to_signals(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        trust_by_strategy_horizon: Mapping[tuple[str, str], StrategyTrustResult],
    ) -> int:
        if not (
            self._has_column(conn, "signals", "trust_score")
            or self._has_column(conn, "signals", "trust_conservative")
            or self._has_column(conn, "signals", "trust_exploratory")
        ):
            return 0
        has_horizon = self._has_column(conn, "signals", "horizon")
        has_trust_json = self._has_column(conn, "signals", "trust_json")
        has_updated = self._has_column(conn, "signals", "trust_updated_at")
        has_cons = self._has_column(conn, "signals", "trust_conservative")
        has_exp = self._has_column(conn, "signals", "trust_exploratory")

        updated = 0
        for (strategy_id, horizon), tr in trust_by_strategy_horizon.items():
            set_cols: list[str] = []
            params: list[Any] = []
            if self._has_column(conn, "signals", "trust_score"):
                set_cols.append("trust_score = ?")
                params.append(float(tr.trust_score))
            if has_cons:
                set_cols.append("trust_conservative = ?")
                params.append(float(tr.trust_conservative))
            if has_exp:
                set_cols.append("trust_exploratory = ?")
                params.append(float(tr.trust_exploratory))

            if not set_cols:
                continue
            if has_trust_json:
                set_cols.append("trust_json = ?")
                params.append(
                    json.dumps(
                        {
                            "components": json.loads(tr.components_json),
                            "trust_conservative": float(tr.trust_conservative),
                            "trust_exploratory": float(tr.trust_exploratory),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
            if has_updated:
                set_cols.append("trust_updated_at = ?")
                params.append(str(tr.computed_at))

            where = ["tenant_id = ?", "strategy_id = ?"]
            params.extend([str(tenant_id), str(strategy_id)])
            if has_horizon:
                where.append("horizon = ?")
                params.append(str(horizon))

            cur = conn.execute(
                f"UPDATE signals SET {', '.join(set_cols)} WHERE {' AND '.join(where)}",
                tuple(params),
            )
            try:
                updated += int(cur.rowcount or 0)
            except Exception:
                pass
        return updated

    def apply_trust_to_consensus(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        trust_by_strategy_horizon: Mapping[tuple[str, str], StrategyTrustResult],
    ) -> int:
        if not (
            self._has_column(conn, "consensus_signals", "trust_score")
            or self._has_column(conn, "consensus_signals", "trust_conservative")
            or self._has_column(conn, "consensus_signals", "trust_exploratory")
        ):
            return 0
        if not self._has_column(conn, "consensus_signals", "weights_json"):
            return 0

        has_trust_json = self._has_column(conn, "consensus_signals", "trust_json")
        has_updated = self._has_column(conn, "consensus_signals", "trust_updated_at")
        has_horizon = self._has_column(conn, "consensus_signals", "horizon")
        has_cons = self._has_column(conn, "consensus_signals", "trust_conservative")
        has_exp = self._has_column(conn, "consensus_signals", "trust_exploratory")

        rows = conn.execute(
            """
            SELECT id, horizon, weights_json, created_at
            FROM consensus_signals
            WHERE tenant_id = ?
            ORDER BY created_at DESC
            """,
            (str(tenant_id),),
        ).fetchall()

        updated = 0
        for r in rows:
            try:
                weights = json.loads(str(r["weights_json"] or "{}"))
                if not isinstance(weights, dict) or not weights:
                    continue
            except Exception:
                continue

            h = str(r["horizon"] or "1d") if has_horizon else "1d"
            num_cons = 0.0
            num_exp = 0.0
            den = 0.0
            used: dict[str, float] = {}
            for sid, w in weights.items():
                try:
                    wv = float(w)
                except Exception:
                    continue
                tr = trust_by_strategy_horizon.get((str(sid), str(h)))
                if tr is None:
                    continue
                num_cons += wv * float(tr.trust_conservative)
                num_exp += wv * float(tr.trust_exploratory)
                den += wv
                used[str(sid)] = float(tr.trust_exploratory)

            if den <= 0.0:
                continue
            trust_cons = _clamp01(num_cons / den)
            trust_exp = _clamp01(num_exp / den)
            trust = trust_cons

            set_cols: list[str] = []
            params: list[Any] = []
            if self._has_column(conn, "consensus_signals", "trust_score"):
                set_cols.append("trust_score = ?")
                params.append(float(trust))
            if has_cons:
                set_cols.append("trust_conservative = ?")
                params.append(float(trust_cons))
            if has_exp:
                set_cols.append("trust_exploratory = ?")
                params.append(float(trust_exp))
            if not set_cols:
                continue
            if has_trust_json:
                set_cols.append("trust_json = ?")
                params.append(
                    json.dumps(
                        {"by_strategy_exploratory": used, "trust_conservative": trust_cons, "trust_exploratory": trust_exp},
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
            if has_updated:
                set_cols.append("trust_updated_at = ?")
                params.append(str(r["created_at"]))

            params.extend([str(tenant_id), str(r["id"])])
            cur = conn.execute(
                f"UPDATE consensus_signals SET {', '.join(set_cols)} WHERE tenant_id = ? AND id = ?",
                tuple(params),
            )
            try:
                updated += int(cur.rowcount or 0)
            except Exception:
                pass
        return updated
