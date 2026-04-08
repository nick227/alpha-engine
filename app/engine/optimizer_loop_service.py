from __future__ import annotations

from datetime import datetime, timezone, timedelta

import json
import pandas as pd

from app.core.repository import Repository
from app.core.types import RawEvent, StrategyConfig
from app.engine.genetic_optimizer_service import GeneticOptimizerService
from app.engine.strategy_store import bootstrap_strategies_from_experiments, load_active_strategy_configs_from_db
from app.engine.reaper_engine import should_reap
from dateutil.parser import isoparse
from app.engine.runner import _strategy_track

class OptimizerLoopService:
    """Optimizer mutation loop scaffold."""

    def __init__(self, cache_size: int = 4) -> None:
        self._cache_size = int(cache_size)
        # cache_key -> (raw_events_version, bars_version, (train_pre, fwd_pre))
        self._window_cache: dict[tuple, tuple] = {}
        self._window_cache_lru: list[tuple] = []

    def _cache_get(self, key: tuple):
        if key not in self._window_cache:
            return None
        # refresh LRU
        try:
            self._window_cache_lru.remove(key)
        except ValueError:
            pass
        self._window_cache_lru.append(key)
        return self._window_cache[key]

    def _cache_put(self, key: tuple, value: tuple) -> None:
        if key in self._window_cache:
            self._window_cache[key] = value
            try:
                self._window_cache_lru.remove(key)
            except ValueError:
                pass
            self._window_cache_lru.append(key)
            return

        self._window_cache[key] = value
        self._window_cache_lru.append(key)
        while len(self._window_cache_lru) > self._cache_size:
            old = self._window_cache_lru.pop(0)
            self._window_cache.pop(old, None)

    def run_once(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        repo = Repository("data/alpha.db")
        bootstrap_strategies_from_experiments(repo)

        def track_for_strategy_id(strategy_id: str) -> str:
            row = repo.conn.execute(
                "SELECT strategy_type FROM strategies WHERE tenant_id='default' AND id=?",
                (strategy_id,),
            ).fetchone()
            if row is None:
                return "unknown"
            return _strategy_track(str(row["strategy_type"]))

        # Load recent raw events (need at least a handful to slice).
        event_rows = repo.conn.execute(
            """
            SELECT id, timestamp, source, text, tickers_json, metadata_json
            FROM raw_events
            WHERE tenant_id = 'default'
            ORDER BY timestamp ASC
            LIMIT 200
            """
        ).fetchall()

        raw_events: list[RawEvent] = []
        for r in event_rows:
            try:
                tickers = json.loads(str(r["tickers_json"] or "[]"))
            except Exception:
                tickers = []
            try:
                metadata = json.loads(str(r["metadata_json"] or "{}"))
            except Exception:
                metadata = {}
            raw_events.append(
                RawEvent(
                    id=str(r["id"]),
                    timestamp=isoparse(str(r["timestamp"])),
                    source=str(r["source"]),
                    text=str(r["text"]),
                    tickers=list(tickers) if isinstance(tickers, list) else [],
                    metadata=dict(metadata) if isinstance(metadata, dict) else {},
                )
            )

        if len(raw_events) < 5:
            repo.add_heartbeat("optimizer", "ok", "insufficient raw events")
            repo.close()
            return {"mode": "optimizer", "ran_at": now.isoformat(), "status": "ok", "notes": "insufficient events"}

        # Bars from SQLite (bounded to the event window to avoid loading the entire bar history).
        # Add buffers so forward-horizon features (e.g. 15m/60m) have exit bars available.
        min_dt = (min(evt.timestamp for evt in raw_events).astimezone(timezone.utc) - timedelta(minutes=30)).replace(microsecond=0)
        max_dt = (max(evt.timestamp for evt in raw_events).astimezone(timezone.utc) + timedelta(minutes=180)).replace(microsecond=0)
        min_ts = min_dt.isoformat().replace("+00:00", "Z")
        max_ts = max_dt.isoformat().replace("+00:00", "Z")
        tickers = sorted({(evt.tickers[0] if evt.tickers else "") for evt in raw_events} - {""})
        if not tickers:
            repo.add_heartbeat("optimizer", "ok", "no tickers in events")
            repo.close()
            return {"mode": "optimizer", "ran_at": now.isoformat(), "status": "ok", "notes": "no tickers"}

        placeholders = ",".join(["?"] * len(tickers))
        bar_rows = repo.conn.execute(
            f"""
            SELECT ticker, timestamp, open, high, low, close, volume
            FROM price_bars
            WHERE tenant_id = 'default'
              AND ticker IN ({placeholders})
              AND timestamp >= ? AND timestamp <= ?
            ORDER BY ticker, timestamp ASC
            """,
            tuple(tickers) + (min_ts, max_ts),
        ).fetchall()
        if not bar_rows:
            repo.add_heartbeat("optimizer", "ok", "no price_bars")
            repo.close()
            return {"mode": "optimizer", "ran_at": now.isoformat(), "status": "ok", "notes": "no bars"}

        bars = pd.DataFrame([dict(br) for br in bar_rows])
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)

        # Ensure strategy_state exists for actives.
        active = load_active_strategy_configs_from_db(repo)
        for cfg in active:
            track = _strategy_track(cfg.strategy_type)
            repo.upsert_strategy_state(strategy_id=cfg.id, track=track, status="ACTIVE", parent_id=None, version=cfg.version, notes=cfg.name)

        # Choose a parent in round-robin.
        parent_idx = int(repo.get_kv("optimizer:parent_idx") or "0")
        parents = [cfg for cfg in active if cfg.strategy_type != "consensus"]
        if not parents:
            repo.add_heartbeat("optimizer", "ok", "no active parents")
            repo.close()
            return {"mode": "optimizer", "ran_at": now.isoformat(), "status": "ok", "notes": "no parents"}

        parent = parents[parent_idx % len(parents)]
        repo.set_kv("optimizer:parent_idx", str((parent_idx + 1) % len(parents)))

        service = GeneticOptimizerService(repo)
        candidates = service.propose_candidates(parent, max_children=6)

        # Precompute train/forward windows, cached across ticks keyed by (ticker set, time window).
        # Invalidation: version stamps from raw_events / price_bars.
        raw_events_version = repo.conn.execute("SELECT COALESCE(MAX(ingested_at), '') as v FROM raw_events WHERE tenant_id='default'").fetchone()["v"]
        bars_version = repo.conn.execute("SELECT COALESCE(MAX(timestamp), '') as v FROM price_bars WHERE tenant_id='default'").fetchone()["v"]
        cache_key = (tuple(tickers), min_ts, max_ts, 0.3)

        cached = self._cache_get(cache_key)
        if cached is None or cached[0] != str(raw_events_version) or cached[1] != str(bars_version):
            train_pre, fwd_pre = service.precompute_windows(raw_events=raw_events, bars=bars, forward_ratio=0.3)
            self._cache_put(cache_key, (str(raw_events_version), str(bars_version), (train_pre, fwd_pre)))
        else:
            train_pre, fwd_pre = cached[2]
        parent_train = service.evaluate_strategy_on_window(strategy=parent, window=train_pre)
        parent_fwd = service.evaluate_strategy_on_window(strategy=parent, window=fwd_pre)

        promoted = 0
        evaluated = 0
        for cand in candidates:
            evaluated += 1
            service.persist_candidate(parent=parent, candidate=cand, status="CANDIDATE")

            cand_train = service.evaluate_strategy_on_window(strategy=cand, window=train_pre)
            cand_fwd = service.evaluate_strategy_on_window(strategy=cand, window=fwd_pre)
            passed, gate_logs = service.gate_decision(
                parent=parent,
                candidate=cand,
                parent_train=parent_train,
                parent_forward=parent_fwd,
                candidate_train=cand_train,
                candidate_forward=cand_fwd,
                min_stability_required=0.6,
                min_sample_size=5,
            )
            service.record_gate_result(parent=parent, candidate=cand, passed=passed, gate_logs=gate_logs)

            if passed:
                # Promote candidate: mark active in strategies + state; archive parent.
                repo.set_strategy_active(cand.id, True)
                repo.set_strategy_active(parent.id, False)
                cand_track = _strategy_track(cand.strategy_type)
                parent_track = _strategy_track(parent.strategy_type)
                repo.upsert_strategy_state(strategy_id=cand.id, track=cand_track, status="ACTIVE", parent_id=parent.id, version=cand.version, notes=cand.name)
                repo.upsert_strategy_state(strategy_id=parent.id, track=parent_track, status="ARCHIVED", parent_id=None, version=parent.version, notes=parent.name)
                repo.add_promotion_event(strategy_id=cand.id, parent_id=parent.id, track=cand_track, action="promoted", reason="forward_gate_pass", gate_logs=gate_logs)
                promoted += 1
                break

        # Rollback guardrail (minimal): reap active strategies that degrade vs parent.
        active_rows = repo.conn.execute(
            "SELECT strategy_id, parent_id, consecutive_bad_windows FROM strategy_state WHERE tenant_id='default' AND status='ACTIVE' AND parent_id IS NOT NULL"
        ).fetchall()
        rolled_back = 0
        for r in active_rows:
            strategy_id = str(r["strategy_id"])
            parent_id = str(r["parent_id"])

            s_perf = repo.conn.execute(
                "SELECT avg_return, prediction_count FROM strategy_performance WHERE tenant_id='default' AND strategy_id=? AND horizon='ALL'",
                (strategy_id,),
            ).fetchone()
            p_perf = repo.conn.execute(
                "SELECT avg_return, prediction_count FROM strategy_performance WHERE tenant_id='default' AND strategy_id=? AND horizon='ALL'",
                (parent_id,),
            ).fetchone()
            if s_perf is None or p_perf is None:
                continue

            s_avg = float(s_perf["avg_return"])
            p_avg = float(p_perf["avg_return"])
            under = ((s_avg - p_avg) / abs(p_avg) * 100.0) if p_avg != 0 else (s_avg - p_avg) * 100.0

            stability_row = repo.conn.execute(
                "SELECT stability_score FROM strategy_stability WHERE tenant_id='default' AND strategy_id=?",
                (strategy_id,),
            ).fetchone()
            stability = float(stability_row["stability_score"]) if stability_row is not None else 1.0

            consecutive = int(r["consecutive_bad_windows"])
            if stability < 0.6:
                consecutive += 1
            else:
                consecutive = 0

            repo.upsert_strategy_state(
                strategy_id=strategy_id,
                track=track_for_strategy_id(strategy_id),
                status="ACTIVE",
                parent_id=parent_id,
                consecutive_bad_windows=consecutive,
            )

            reap, reason = should_reap(
                {
                    "stability_score": stability,
                    "consecutive_bad_windows": consecutive,
                    "parent_underperformance_pct": under,
                }
            )
            if not reap:
                continue

            repo.set_strategy_active(strategy_id, False)
            repo.set_strategy_active(parent_id, True)
            repo.upsert_strategy_state(strategy_id=strategy_id, track=track_for_strategy_id(strategy_id), status="ROLLED_BACK", parent_id=parent_id)
            repo.upsert_strategy_state(strategy_id=parent_id, track=track_for_strategy_id(parent_id), status="ACTIVE", parent_id=None)
            repo.add_promotion_event(strategy_id=strategy_id, parent_id=parent_id, track=track_for_strategy_id(strategy_id), action="rolled_back", reason=reason, gate_logs={"stability": stability, "underperformance_pct": under})
            rolled_back += 1

        repo.add_heartbeat("optimizer", "ok", f"evaluated={evaluated} promoted={promoted} rolled_back={rolled_back}")
        repo.close()
        return {
            "mode": "optimizer",
            "ran_at": now.isoformat(),
            "status": "ok",
            "parent": parent.id,
            "evaluated": evaluated,
            "promoted": promoted,
            "rolled_back": rolled_back,
        }
