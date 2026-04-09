from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
from uuid import uuid4

from dateutil.parser import isoparse

from app.core.repository import Repository
from app.engine.performance_engine import compute_stability, summarize_regime_performance
from app.engine.replay_worker import (
    MetricsUpdater,
    OutcomeWriter,
    PredictionRecord,
    PredictionRepository,
    PriceRepository,
)
from app.engine.continuous_learning import ContinuousLearner, Signal, SignalOutcome


def _isoz(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_ts(ts: str) -> datetime:
    parsed = isoparse(ts)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _horizon_to_minutes(horizon: str) -> int:
    h = str(horizon).strip().lower()
    if h.endswith("m"):
        try:
            return int(h[:-1])
        except ValueError:
            return 15
    if h.endswith("h"):
        try:
            return int(float(h[:-1]) * 60)
        except ValueError:
            return 60
    if h.endswith("d"):
        try:
            days = int(h[:-1])
        except ValueError:
            days = 1
        return max(1, days) * 24 * 60
    return 15


def _strategy_track(strategy_type: str) -> str:
    st = str(strategy_type).lower()
    if st.startswith("text_") or st.startswith("sentiment"):
        return "sentiment"
    if st.startswith("technical_") or st.startswith("baseline_") or st.startswith("quant"):
        return "quant"
    if st == "consensus":
        return "consensus"
    return "unknown"


def _extract_regime(feature_snapshot_json: str) -> str | None:
    # Backwards-compat fallback for older predictions without `predictions.regime`.
    try:
        snap = json.loads(feature_snapshot_json or "{}")
    except Exception:
        return None

    if isinstance(snap, dict):
        if "regime" in snap and snap["regime"]:
            return str(snap["regime"])
        rs = snap.get("regime_snapshot")
        if isinstance(rs, dict):
            vr = rs.get("volatility_regime")
            if vr:
                return str(vr)
    return None


class SQLitePredictionRepository(PredictionRepository):
    def __init__(self, repo: Repository, tenant_id: str = "default") -> None:
        self.repo = repo
        self.tenant_id = tenant_id

    def list_unscored_predictions(self, now: datetime) -> Iterable[PredictionRecord]:
        rows = self.repo.conn.execute(
            """
            SELECT
              p.id,
              p.strategy_id,
              p.ticker,
              p.mode,
              p.horizon,
              p.timestamp,
              p.entry_price,
              p.prediction,
              p.feature_snapshot_json,
              p.regime,
              s.strategy_type
            FROM predictions p
            JOIN strategies s
              ON s.id = p.strategy_id
             AND s.tenant_id = p.tenant_id
            LEFT JOIN prediction_outcomes o
              ON o.prediction_id = p.id
             AND o.tenant_id = p.tenant_id
            WHERE p.tenant_id = ?
              AND o.id IS NULL
            ORDER BY p.timestamp ASC
            """,
            (self.tenant_id,),
        ).fetchall()

        for row in rows:
            created_at = _parse_ts(str(row["timestamp"]))
            horizon_minutes = _horizon_to_minutes(str(row["horizon"]))
            strategy_type = str(row["strategy_type"])
            track = _strategy_track(strategy_type)
            regime = str(row["regime"]) if row["regime"] is not None and str(row["regime"]) else _extract_regime(str(row["feature_snapshot_json"]))
            yield PredictionRecord(
                id=str(row["id"]),
                strategy_id=str(row["strategy_id"]),
                ticker=str(row["ticker"]),
                track=track,
                mode=str(row["mode"]),
                horizon_minutes=horizon_minutes,
                created_at=created_at,
                entry_price=float(row["entry_price"]),
                direction=str(row["prediction"]),
                regime=regime,
                market_return=None,
            )

    def mark_scored(self, prediction_id: str, outcome_id: str) -> None:
        self.repo.execute(
            "UPDATE predictions SET scored_outcome_id = ?, scored_at = ? WHERE tenant_id = ? AND id = ?",
            (outcome_id, _isoz(datetime.now(timezone.utc)), self.tenant_id, prediction_id),
        )


class SQLitePriceRepository(PriceRepository):
    def __init__(self, repo: Repository, tenant_id: str = "default") -> None:
        self.repo = repo
        self.tenant_id = tenant_id

    def get_exit_price_at_or_after(self, ticker: str, at: datetime) -> float | None:
        ts = _isoz(at)
        row = self.repo.conn.execute(
            """
            SELECT close
            FROM price_bars
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe IN ('1m','1h','1d')
              AND timestamp >= ?
            ORDER BY
              timestamp ASC,
              CASE timeframe WHEN '1m' THEN 0 WHEN '1h' THEN 1 WHEN '1d' THEN 2 ELSE 3 END ASC
            LIMIT 1
            """,
            (self.tenant_id, ticker, ts),
        ).fetchone()
        if row is None:
            return None
        return float(row["close"])


class SQLiteOutcomeWriter(OutcomeWriter):
    def __init__(self, repo: Repository, tenant_id: str = "default") -> None:
        self.repo = repo
        self.tenant_id = tenant_id

    def write_outcome(self, payload: dict) -> str:
        outcome_id = str(uuid4())
        return_pct = float(payload.get("return_pct", 0.0))
        max_runup = float(payload.get("max_runup", max(return_pct, 0.0)))
        max_drawdown = float(payload.get("max_drawdown", min(return_pct, 0.0)))
        evaluated_at = str(payload.get("evaluated_at") or _isoz(datetime.now(timezone.utc)))

        self.repo.execute(
            """
            INSERT OR REPLACE INTO prediction_outcomes
              (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outcome_id,
                self.tenant_id,
                str(payload["prediction_id"]),
                float(payload.get("exit_price", 0.0)),
                float(payload.get("return_pct", 0.0)),
                1 if bool(payload.get("direction_correct")) else 0,
                max_runup,
                max_drawdown,
                evaluated_at,
                str(payload.get("exit_reason") or "horizon"),
                float(payload.get("residual_alpha", 0.0)),
            ),
        )
        return outcome_id


@dataclass
class _PerfRow:
    strategy_id: str
    return_pct: float
    residual_alpha: float
    direction_correct: bool
    mode: str
    regime: str | None
    evaluated_at: str


class SQLiteMetricsUpdater(MetricsUpdater):
    def __init__(self, repo: Repository, tenant_id: str = "default") -> None:
        self.repo = repo
        self.tenant_id = tenant_id

    def _strategy_outcome_rows(self, strategy_id: str) -> list[_PerfRow]:
        rows = self.repo.conn.execute(
            """
            SELECT
              p.strategy_id as strategy_id,
              p.mode as mode,
              p.regime as regime,
              o.return_pct as return_pct,
              o.residual_alpha as residual_alpha,
              o.direction_correct as direction_correct,
              o.evaluated_at as evaluated_at
            FROM predictions p
            JOIN prediction_outcomes o
              ON o.prediction_id = p.id
             AND o.tenant_id = p.tenant_id
            WHERE p.tenant_id = ?
              AND p.strategy_id = ?
              AND o.exit_reason = 'horizon'
            ORDER BY o.evaluated_at ASC
            """,
            (self.tenant_id, strategy_id),
        ).fetchall()

        out: list[_PerfRow] = []
        for row in rows:
            regime = str(row["regime"]) if row["regime"] is not None and str(row["regime"]) else None
            out.append(
                _PerfRow(
                    strategy_id=str(row["strategy_id"]),
                    return_pct=float(row["return_pct"]),
                    residual_alpha=float(row["residual_alpha"]),
                    direction_correct=bool(int(row["direction_correct"])),
                    mode=str(row["mode"]),
                    regime=regime,
                    evaluated_at=str(row["evaluated_at"]),
                )
            )
        return out

    def update_strategy_performance(self, strategy_id: str) -> None:
        rows = self._strategy_outcome_rows(strategy_id)
        if not rows:
            return

        correctness = [1.0 if r.direction_correct else 0.0 for r in rows]
        returns = [r.return_pct for r in rows]
        residuals = [r.residual_alpha for r in rows]
        prediction_count = len(rows)
        accuracy = sum(correctness) / prediction_count if prediction_count else 0.0
        avg_return = sum(returns) / prediction_count if prediction_count else 0.0
        avg_residual = sum(residuals) / prediction_count if prediction_count else 0.0

        # For now we store one rollup per strategy_id for horizon="ALL".
        self.repo.execute(
            """
            INSERT OR REPLACE INTO strategy_performance
              (id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"perf_{strategy_id}_ALL",
                self.tenant_id,
                strategy_id,
                "ALL",
                int(prediction_count),
                float(round(accuracy, 6)),
                float(round(avg_return, 6)),
                float(round(avg_residual, 6)),
                _isoz(datetime.now(timezone.utc)),
            ),
        )

    def update_regime_performance(self, regime: str | None) -> None:
        if not regime:
            return

        # Build regime summaries from all outcomes (simple and robust).
        rows = self.repo.conn.execute(
            """
            SELECT
              p.regime as regime,
              p.feature_snapshot_json as feature_snapshot_json,
              o.return_pct as return_pct,
              o.direction_correct as direction_correct
            FROM predictions p
            JOIN prediction_outcomes o
              ON o.prediction_id = p.id
             AND o.tenant_id = p.tenant_id
            WHERE p.tenant_id = ?
              AND o.exit_reason = 'horizon'
            """,
            (self.tenant_id,),
        ).fetchall()

        outcomes: list[dict] = []
        for row in rows:
            reg = (str(row["regime"]) if row["regime"] is not None and str(row["regime"]) else None) or _extract_regime(str(row["feature_snapshot_json"])) or "UNKNOWN"
            outcomes.append(
                {
                    "regime": reg,
                    "return_pct": float(row["return_pct"]),
                    "direction_correct": bool(int(row["direction_correct"])),
                }
            )

        summaries = summarize_regime_performance(outcomes)
        payload = summaries.get(regime) or summaries.get("UNKNOWN")
        if not payload:
            return

        self.repo.execute(
            """
            INSERT OR REPLACE INTO regime_performance
              (id, tenant_id, regime, prediction_count, accuracy, avg_return, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"reg_{regime}",
                self.tenant_id,
                str(payload["regime"]),
                int(payload["prediction_count"]),
                float(payload["accuracy"]),
                float(payload["avg_return"]),
                _isoz(datetime.now(timezone.utc)),
            ),
        )

    def update_stability(self, strategy_id: str) -> None:
        rows = self._strategy_outcome_rows(strategy_id)
        if not rows:
            return

        backtest = [r for r in rows if str(r.mode).lower() == "backtest"]
        liveish = [r for r in rows if str(r.mode).lower() in {"paper", "live"}]

        def acc(group: list[_PerfRow]) -> float:
            if not group:
                return 0.0
            return sum(1.0 if r.direction_correct else 0.0 for r in group) / len(group)

        backtest_accuracy = acc(backtest)
        live_accuracy = acc(liveish)
        stability_score = compute_stability(backtest_accuracy=backtest_accuracy, live_accuracy=live_accuracy)

        self.repo.execute(
            """
            INSERT OR REPLACE INTO strategy_stability
              (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"stab_{strategy_id}",
                self.tenant_id,
                strategy_id,
                float(round(backtest_accuracy, 6)),
                float(round(live_accuracy, 6)),
                float(stability_score),
                _isoz(datetime.now(timezone.utc)),
            ),
        )

    def refresh_weight_engine_inputs(self) -> None:
        # Materialize per-strategy weight-engine inputs via ContinuousLearner, persisted to `strategy_weights`.
        # This keeps the UI and the routing weights grounded in the same computed artifacts.
        rows = self.repo.conn.execute(
            """
            SELECT
              p.id as prediction_id,
              p.strategy_id as strategy_id,
              p.ticker as ticker,
              p.timestamp as timestamp,
              p.prediction as direction,
              p.confidence as confidence,
              p.regime as regime,
              p.feature_snapshot_json as feature_snapshot_json,
              o.return_pct as return_pct
            FROM predictions p
            JOIN prediction_outcomes o
              ON o.prediction_id = p.id
             AND o.tenant_id = p.tenant_id
            WHERE p.tenant_id = ?
              AND o.exit_reason = 'horizon'
            ORDER BY o.evaluated_at DESC
            LIMIT 5000
            """,
            (self.tenant_id,),
        ).fetchall()

        learner = ContinuousLearner()

        def dir_i(d: str) -> int:
            dl = str(d).strip().lower()
            if dl in {"up", "long", "buy", "1", "+1"}:
                return 1
            if dl in {"down", "short", "sell", "-1"}:
                return -1
            return 0

        for r in rows:
            pred_id = str(r["prediction_id"])
            strategy_id = str(r["strategy_id"])
            ticker = str(r["ticker"])
            ts = str(r["timestamp"])
            regime = str(r["regime"]) if r["regime"] is not None and str(r["regime"]) else _extract_regime(str(r["feature_snapshot_json"])) or "UNKNOWN"
            conf = float(r["confidence"])
            ret = float(r["return_pct"])

            signal = Signal(
                id=pred_id,
                strategy_id=strategy_id,
                ticker=ticker,
                direction=dir_i(str(r["direction"])),
                confidence=conf,
                timestamp=ts,
                regime=regime,
            )
            outcome = SignalOutcome(signal_id=pred_id, actual_return_pct=ret)
            learner.ingest_pairing(signal, outcome)

        performances = learner.evaluate_all()
        now = datetime.now(timezone.utc)
        for sid, perf in performances.items():
            try:
                self.repo.upsert_strategy_weight(
                    strategy_id=str(sid),
                    win_rate=float(perf.win_rate),
                    alpha=float(perf.alpha),
                    stability=float(perf.stability),
                    confidence_weight=float(perf.confidence_weight),
                    regime_strength_json=json.dumps(dict(perf.regime_strength or {}), sort_keys=True),
                    tenant_id=self.tenant_id,
                    updated_at=now,
                )
            except Exception:
                continue
