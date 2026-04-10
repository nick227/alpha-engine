from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from app.core.mra import compute_mra
from app.core.scoring import score_event
from app.core.types import RawEvent, StrategyConfig
from app.engine.outcome_resolver import OutcomeWrite, resolve_outcome
from app.engine.prediction_writer import PredictionWrite, upsert_prediction
from app.engine.strategy_factory import build_strategy_instance
from app.read_models.consensus_writer import ConsensusWrite, upsert_consensus
from app.read_models.signal_writer import SignalWrite, upsert_signal


def _isoz(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *, tenant_id: str, idempotency_key: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}:{idempotency_key}".encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"

def _idem_key(*, ticker: str, strategy_id: str, horizon: str, timestamp: datetime) -> str:
    return _sha256_hex(f"{ticker}|{strategy_id}|{horizon}|{_isoz(timestamp)}")


def _split_context(ctx: dict | None) -> tuple[dict, dict]:
    """
    Split (features, outcomes) to prevent look-ahead bias.

    outcome keys are peeled off:
      - future_return_*
      - max_runup / max_drawdown
    """
    if not isinstance(ctx, dict):
        return {}, {}
    outcomes: dict[str, Any] = {}
    features: dict[str, Any] = {}
    for k, v in ctx.items():
        if isinstance(k, str) and (k.startswith("future_return_") or k in {"max_runup", "max_drawdown"}):
            outcomes[k] = v
        else:
            features[k] = v
    return features, outcomes


def predicted_return_deterministic(
    *,
    ticker: str,
    strategy_id: str,
    horizon: str,
    timestamp: datetime,
    direction: str,
) -> float:
    """
    Deterministic placeholder predicted_return.

    Contract: stable across reruns.
      predicted_return = f(hash(ticker + strategy_id + horizon + timestamp), horizon) * sign(direction)
    """
    h = str(horizon).strip().lower()
    if h not in {"1d", "7d", "30d"}:
        return 0.0
    d = str(direction).strip().lower()
    if d == "flat":
        return 0.0
    s = 1.0 if d == "up" else (-1.0 if d == "down" else 0.0)
    if s == 0.0:
        return 0.0

    # Small, horizon-scaled ranges.
    mag_max = {"1d": 0.02, "7d": 0.05, "30d": 0.10}[h]

    # Stable uniform [0,1).
    key = f"{ticker}|{strategy_id}|{h}|{_isoz(timestamp)}"
    u = int(_sha256_hex(key)[:16], 16) / float(16**16)
    mag = float(u) * float(mag_max)
    return float(s) * float(mag)


def ensure_raw_scored_mra_schema(conn: sqlite3.Connection) -> None:
    # raw_events
    raw_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(raw_events)").fetchall()}
    if not raw_cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              source TEXT NOT NULL,
              text TEXT NOT NULL,
              tickers_json TEXT NOT NULL,
              metadata_json TEXT NOT NULL,
              ingested_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_events_idem
              ON raw_events(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_raw_events_ts
              ON raw_events(tenant_id, timestamp);
            """
        )
    else:
        for col, ddl in (
            ("run_id", "ALTER TABLE raw_events ADD COLUMN run_id TEXT;"),
            ("idempotency_key", "ALTER TABLE raw_events ADD COLUMN idempotency_key TEXT;"),
            ("ingested_at", "ALTER TABLE raw_events ADD COLUMN ingested_at TEXT;"),
        ):
            if col not in raw_cols:
                conn.execute(ddl)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_events_idempotency_key ON raw_events(tenant_id, idempotency_key);")

    # scored_events
    se_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(scored_events)").fetchall()}
    if not se_cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS scored_events (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              raw_event_id TEXT NOT NULL,
              primary_ticker TEXT NOT NULL,
              category TEXT NOT NULL,
              materiality REAL NOT NULL,
              direction TEXT NOT NULL,
              confidence REAL NOT NULL,
              company_relevance REAL NOT NULL,
              concept_tags_json TEXT NOT NULL,
              explanation_terms_json TEXT NOT NULL,
              scorer_version TEXT NOT NULL,
              taxonomy_version TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_scored_events_idem
              ON scored_events(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_scored_events_raw
              ON scored_events(tenant_id, raw_event_id);
            """
        )
    else:
        for col, ddl in (
            ("run_id", "ALTER TABLE scored_events ADD COLUMN run_id TEXT;"),
            ("idempotency_key", "ALTER TABLE scored_events ADD COLUMN idempotency_key TEXT;"),
        ):
            if col not in se_cols:
                conn.execute(ddl)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_scored_events_idempotency_key ON scored_events(tenant_id, idempotency_key);"
        )

    # mra_outcomes
    mra_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(mra_outcomes)").fetchall()}
    if not mra_cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS mra_outcomes (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              scored_event_id TEXT NOT NULL,
              return_1m REAL NOT NULL,
              return_5m REAL NOT NULL,
              return_15m REAL NOT NULL,
              return_1h REAL NOT NULL,
              volume_ratio REAL NOT NULL,
              vwap_distance REAL NOT NULL,
              range_expansion REAL NOT NULL,
              continuation_slope REAL NOT NULL,
              pullback_depth REAL NOT NULL,
              mra_score REAL NOT NULL,
              market_context_json TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mra_outcomes_idem
              ON mra_outcomes(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_mra_outcomes_scored
              ON mra_outcomes(tenant_id, scored_event_id);
            """
        )
    else:
        for col, ddl in (
            ("run_id", "ALTER TABLE mra_outcomes ADD COLUMN run_id TEXT;"),
            ("idempotency_key", "ALTER TABLE mra_outcomes ADD COLUMN idempotency_key TEXT;"),
        ):
            if col not in mra_cols:
                conn.execute(ddl)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mra_outcomes_idempotency_key ON mra_outcomes(tenant_id, idempotency_key);")


def ensure_loop_heartbeats_schema(conn: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(loop_heartbeats)").fetchall()}
    if not cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS loop_heartbeats (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              loop_type TEXT NOT NULL,
              status TEXT NOT NULL,
              notes TEXT,
              created_at TEXT NOT NULL,
              timestamp TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_loop_heartbeats_idem
              ON loop_heartbeats(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_loop_heartbeats_ts
              ON loop_heartbeats(tenant_id, loop_type, created_at);
            """
        )
        return

    for col, ddl in (
        ("run_id", "ALTER TABLE loop_heartbeats ADD COLUMN run_id TEXT;"),
        ("idempotency_key", "ALTER TABLE loop_heartbeats ADD COLUMN idempotency_key TEXT;"),
        ("timestamp", "ALTER TABLE loop_heartbeats ADD COLUMN timestamp TEXT;"),
    ):
        if col not in cols:
            conn.execute(ddl)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_loop_heartbeats_idempotency_key ON loop_heartbeats(tenant_id, idempotency_key);"
    )


def upsert_loop_heartbeat(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    run_id: str,
    loop_type: str,
    status: str,
    notes: str | None = None,
    created_at: datetime | None = None,
) -> str:
    ensure_loop_heartbeats_schema(conn)
    created_at = (created_at or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    # Idempotent per (run_id,status) to avoid duplicates on rerun/resume.
    idem = _sha256_hex(f"{run_id}|{status}")
    hb_id = _stable_id("hb", tenant_id=str(tenant_id), idempotency_key=idem)
    conn.execute(
        """
        INSERT OR REPLACE INTO loop_heartbeats
          (id, tenant_id, run_id, idempotency_key, loop_type, status, notes, created_at, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (hb_id, str(tenant_id), str(run_id), idem, str(loop_type), str(status), notes, _isoz(created_at), _isoz(created_at)),
    )
    return hb_id


class ReplayEngine:
    """
    Backfill replay engine that writes only:
      raw_events, scored_events, mra_outcomes,
      predictions, prediction_outcomes,
      signals, consensus_signals (placeholder),
      loop_heartbeats.
    """

    def __init__(self, *, db_path: str | Path, strategy_dir: str | Path = "experiments/strategies") -> None:
        self.db_path = str(db_path)
        self.strategy_dir = str(strategy_dir)
        self._strategies: list[tuple[StrategyConfig, Any]] | None = None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=OFF;")
        conn.execute("PRAGMA busy_timeout=3000;")
        return conn

    def _load_strategy_configs(self) -> list[StrategyConfig]:
        """
        Load StrategyConfig JSON files without importing `app.engine.runner` (which may
        pull optional dependencies in some environments).
        """
        base = Path(self.strategy_dir)
        if not base.is_absolute():
            base = Path(__file__).resolve().parents[2] / base
        if not base.exists():
            return []

        out: list[StrategyConfig] = []
        for path in sorted(base.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if "strategy_type" not in payload:
                continue
            if "id" not in payload or "name" not in payload or "version" not in payload:
                continue
            if "mode" not in payload or "config" not in payload:
                continue
            try:
                out.append(
                    StrategyConfig(
                        id=str(payload["id"]),
                        name=str(payload["name"]),
                        version=str(payload["version"]),
                        strategy_type=str(payload["strategy_type"]),
                        mode=str(payload["mode"]),
                        config=dict(payload.get("config") or {}),
                        active=bool(payload.get("active", True)),
                    )
                )
            except Exception:
                continue
        return out

    def _ensure_strategies(self) -> list[tuple[StrategyConfig, Any]]:
        if self._strategies is not None:
            return self._strategies

        base = [c for c in self._load_strategy_configs() if bool(getattr(c, "active", True))]
        desired_horizons = ["1d", "7d", "30d"]
        expanded: list[StrategyConfig] = []
        for cfg in base:
            if str(cfg.strategy_type).strip().lower() == "consensus":
                continue
            for h in desired_horizons:
                expanded.append(
                    StrategyConfig(
                        id=f"{cfg.id}-{h}",
                        name=f"{cfg.name} ({h})",
                        version=str(cfg.version),
                        strategy_type=str(cfg.strategy_type),
                        mode=str(cfg.mode),
                        config={**dict(cfg.config or {}), "horizon": h},
                        active=bool(cfg.active),
                    )
                )

        strategies: list[tuple[StrategyConfig, Any]] = []
        for cfg in expanded:
            instance = build_strategy_instance(cfg)
            if instance is not None:
                strategies.append((cfg, instance))

        self._strategies = strategies
        return strategies

    def replay_batch(
        self,
        *,
        raw_events: list[RawEvent],
        price_contexts: dict[str, dict],
        tenant_id: str,
        run_id: str,
    ) -> dict[str, int]:
        """Replay events strictly chronologically and write seed-parity tables."""
        if not raw_events:
            return {"events": 0, "predictions": 0, "outcomes": 0, "signals": 0, "consensus": 0}

        strategies = self._ensure_strategies()

        events_written = 0
        preds_written = 0
        outs_written = 0
        sig_written = 0
        con_written = 0

        with self._connect() as conn:
            ensure_raw_scored_mra_schema(conn)

            try:
                upsert_loop_heartbeat(
                    conn,
                    tenant_id=str(tenant_id),
                    run_id=str(run_id),
                    loop_type="replay",
                    status="start",
                    notes=f"batch_size={len(raw_events)}",
                )

                with conn:
                    for raw in raw_events:
                        ctx = price_contexts.get(raw.id) or {}
                        features_ctx, outcomes_ctx = _split_context(ctx)

                        ticker_for_idem = raw.tickers[0] if raw.tickers else "UNKNOWN"
                        raw_idem = _idem_key(ticker=str(ticker_for_idem), strategy_id="raw_event", horizon="event", timestamp=raw.timestamp)
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO raw_events
                              (id, tenant_id, run_id, idempotency_key, timestamp, source, text, tickers_json, metadata_json, ingested_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                str(raw.id),
                                str(tenant_id),
                                str(run_id),
                                raw_idem,
                                _isoz(raw.timestamp),
                                str(raw.source),
                                str(raw.text),
                                json.dumps(list(raw.tickers or []), sort_keys=True),
                                json.dumps(dict(raw.metadata or {}), sort_keys=True),
                                _isoz(datetime.now(timezone.utc)),
                            ),
                        )
                        events_written += 1

                        scored = score_event(raw)
                        scored_idem = _idem_key(ticker=str(scored.primary_ticker), strategy_id="scored_event", horizon="event", timestamp=raw.timestamp)
                        scored.id = _stable_id("se", tenant_id=str(tenant_id), idempotency_key=scored_idem)
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO scored_events
                              (id, tenant_id, run_id, idempotency_key, raw_event_id, primary_ticker, category, materiality,
                               direction, confidence, company_relevance, concept_tags_json, explanation_terms_json,
                               scorer_version, taxonomy_version)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                str(scored.id),
                                str(tenant_id),
                                str(run_id),
                                scored_idem,
                                str(scored.raw_event_id),
                                str(scored.primary_ticker),
                                str(scored.category),
                                float(scored.materiality),
                                str(scored.direction),
                                float(scored.confidence),
                                float(scored.company_relevance),
                                json.dumps(list(scored.concept_tags or []), sort_keys=True),
                                json.dumps(list(scored.explanation_terms or []), sort_keys=True),
                                str(scored.scorer_version),
                                str(scored.taxonomy_version),
                            ),
                        )

                        mra = compute_mra(scored, features_ctx)
                        mra_idem = _idem_key(ticker=str(scored.primary_ticker), strategy_id="mra_outcome", horizon="event", timestamp=raw.timestamp)
                        mra.id = _stable_id("mra", tenant_id=str(tenant_id), idempotency_key=mra_idem)
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO mra_outcomes
                              (id, tenant_id, run_id, idempotency_key, scored_event_id,
                               return_1m, return_5m, return_15m, return_1h,
                               volume_ratio, vwap_distance, range_expansion, continuation_slope, pullback_depth,
                               mra_score, market_context_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                str(mra.id),
                                str(tenant_id),
                                str(run_id),
                                mra_idem,
                                str(mra.scored_event_id),
                                float(mra.return_1m),
                                float(mra.return_5m),
                                float(mra.return_15m),
                                float(mra.return_1h),
                                float(mra.volume_ratio),
                                float(mra.vwap_distance),
                                float(mra.range_expansion),
                                float(mra.continuation_slope),
                                float(mra.pullback_depth),
                                float(mra.mra_score),
                                json.dumps(dict(getattr(mra, "market_context", {}) or {}), sort_keys=True),
                            ),
                        )

                        entry_price = float(features_ctx.get("entry_price", 0.0) or 0.0)
                        if entry_price <= 0:
                            continue

                        consensus_buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
                        emitted_by_horizon: dict[str, bool] = {"1d": False, "7d": False, "30d": False}

                        for cfg, strat in strategies:
                            pred = strat.maybe_predict(scored, mra, features_ctx, raw.timestamp)
                            if pred is None:
                                continue

                            direction = str(getattr(pred, "prediction", "flat")).strip().lower()
                            if direction not in {"up", "down", "flat"}:
                                direction = "flat"

                            confidence = float(getattr(pred, "confidence", 0.0) or 0.0)
                            horizon = str(getattr(pred, "horizon", cfg.config.get("horizon", "1d"))).strip().lower()
                            if horizon not in {"1d", "7d", "30d"}:
                                continue
                            emitted_by_horizon[horizon] = True

                            ticker = str(getattr(pred, "ticker", scored.primary_ticker))
                            predicted_return = predicted_return_deterministic(
                                ticker=str(ticker),
                                strategy_id=str(cfg.id),
                                horizon=str(horizon),
                                timestamp=raw.timestamp,
                                direction=str(direction),
                            )

                            feature_time = raw.timestamp.astimezone(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)
                            if not (raw.timestamp < feature_time):
                                raise RuntimeError('lookahead_violation: prediction_time !< feature_time')

                            feature_snapshot = {
                                "feature_time": _isoz(feature_time),
                                "entry_price": entry_price,
                                "strategy_type": str(cfg.strategy_type),
                                "strategy_version": str(cfg.version),
                            }

                            pred_id = upsert_prediction(
                                conn,
                                PredictionWrite(
                                    tenant_id=str(tenant_id),
                                    run_id=str(run_id),
                                    strategy_id=str(cfg.id),
                                    scored_event_id=str(scored.id),
                                    ticker=ticker,
                                    timestamp=raw.timestamp,
                                    horizon=horizon,
                                    direction=direction,
                                    confidence=confidence,
                                    entry_price=entry_price,
                                    predicted_return=predicted_return,
                                    feature_snapshot=feature_snapshot,
                                    mode="backfill",
                                ),
                            )
                            preds_written += 1

                            upsert_signal(
                                conn,
                                SignalWrite(
                                    tenant_id=str(tenant_id),
                                    run_id=str(run_id),
                                    prediction_id=str(pred_id),
                                    ticker=ticker,
                                    strategy_id=str(cfg.id),
                                    horizon=horizon,
                                    timestamp=raw.timestamp,
                                    direction=direction,
                                    confidence=confidence,
                                    predicted_return=predicted_return,
                                ),
                            )
                            sig_written += 1

                            days = {"1d": 1, "7d": 7, "30d": 30}[horizon]
                            if (raw.timestamp + timedelta(days=int(days))) <= datetime.now(timezone.utc):
                                actual_return_hint = outcomes_ctx.get(f"future_return_{horizon}")
                                if actual_return_hint is not None:
                                    outcome_time = raw.timestamp.astimezone(timezone.utc).replace(microsecond=0) + timedelta(days=int(days))
                                    if not (feature_time < outcome_time):
                                        raise RuntimeError('lookahead_violation: feature_time !< outcome_time')
                                    resolve_outcome(
                                        conn,
                                        OutcomeWrite(
                                            tenant_id=str(tenant_id),
                                            run_id=str(run_id),
                                            ticker=str(ticker),
                                            strategy_id=str(cfg.id),
                                            prediction_id=str(pred_id),
                                            prediction_time=raw.timestamp,
                                            horizon=horizon,
                                            direction=direction,
                                            entry_price=entry_price,
                                            actual_return_hint=float(actual_return_hint),
                                            exit_price_hint=None,
                                            evaluated_at=None,
                                            exit_reason="horizon",
                                        ),
                                    )
                                    outs_written += 1

                            bucket_key = (ticker, horizon, _isoz(raw.timestamp))
                            consensus_buckets.setdefault(bucket_key, []).append({
                                "strategy_id": str(cfg.id),
                                "predicted_return": float(predicted_return),
                                "confidence": float(confidence),
                                "direction": direction,
                            })

                        # Ensure multi-horizon rows exist.
                        for h in ("1d", "7d", "30d"):
                            if emitted_by_horizon.get(h):
                                continue
                            ticker = str(scored.primary_ticker)
                            placeholder_strategy_id = f"placeholder-backfill-{h}"
                            feature_time = raw.timestamp.astimezone(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)
                            pred_id = upsert_prediction(
                                conn,
                                PredictionWrite(
                                    tenant_id=str(tenant_id),
                                    run_id=str(run_id),
                                    strategy_id=str(placeholder_strategy_id),
                                    scored_event_id=str(scored.id),
                                    ticker=ticker,
                                    timestamp=raw.timestamp,
                                    horizon=h,
                                    direction="flat",
                                    confidence=0.0,
                                    entry_price=entry_price,
                                    predicted_return=predicted_return_deterministic(
                                        ticker=str(ticker),
                                        strategy_id=str(placeholder_strategy_id),
                                        horizon=str(h),
                                        timestamp=raw.timestamp,
                                        direction="flat",
                                    ),
                                    feature_snapshot={"feature_time": _isoz(feature_time), "entry_price": entry_price, "strategy_type": "placeholder", "strategy_version": "v0"},
                                    mode="backfill",
                                ),
                            )
                            preds_written += 1
                            upsert_signal(
                                conn,
                                SignalWrite(
                                    tenant_id=str(tenant_id),
                                    run_id=str(run_id),
                                    prediction_id=str(pred_id),
                                    ticker=ticker,
                                    strategy_id=str(placeholder_strategy_id),
                                    horizon=h,
                                    timestamp=raw.timestamp,
                                    direction="flat",
                                    confidence=0.0,
                                    predicted_return=0.0,
                                ),
                            )
                            sig_written += 1
                            bucket_key = (ticker, h, _isoz(raw.timestamp))
                            consensus_buckets.setdefault(bucket_key, []).append({
                                "strategy_id": str(placeholder_strategy_id),
                                "predicted_return": 0.0,
                                "confidence": 0.0,
                                "direction": "flat",
                            })

                        for (ticker, horizon, ts), parts in consensus_buckets.items():
                            score = sum(float(p.get("predicted_return", 0.0)) for p in parts) / max(1, len(parts))
                            conf = sum(float(p.get("confidence", 0.0)) for p in parts) / max(1, len(parts))
                            weights = {str(p["strategy_id"]): (1.0 / len(parts)) for p in parts if p.get("strategy_id")}
                            upsert_consensus(
                                conn,
                                ConsensusWrite(
                                    tenant_id=str(tenant_id),
                                    run_id=str(run_id),
                                    ticker=str(ticker),
                                    horizon=str(horizon),
                                    timestamp=datetime.fromisoformat(ts.replace("Z", "+00:00")),
                                    score=float(score),
                                    confidence=float(conf),
                                    regime=str(features_ctx.get("regime")) if features_ctx.get("regime") is not None else None,
                                    strategies=list(parts),
                                    weights=weights,
                                ),
                            )
                            con_written += 1

                upsert_loop_heartbeat(
                    conn,
                    tenant_id=str(tenant_id),
                    run_id=str(run_id),
                    loop_type="replay",
                    status="end",
                    notes=json.dumps({"events": events_written, "predictions": preds_written, "outcomes": outs_written, "signals": sig_written, "consensus": con_written}, sort_keys=True),
                )

            except Exception as e:
                upsert_loop_heartbeat(
                    conn,
                    tenant_id=str(tenant_id),
                    run_id=str(run_id),
                    loop_type="replay",
                    status="error",
                    notes=str(e),
                )
                raise

        return {"events": int(events_written), "predictions": int(preds_written), "outcomes": int(outs_written), "signals": int(sig_written), "consensus": int(con_written)}
