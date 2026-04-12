from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path

import pandas as pd
from dateutil.parser import isoparse

from app.core.repository import Repository
from app.core.types import RawEvent
from app.core.price_context import build_price_context_for_event
from app.core.bars import BarsCache, bar_window_for_events, build_bars_provider
from app.core.target_stocks import get_target_stocks, get_target_stocks_registry
from app.engine.champion_state import load_active_champion_configs, refresh_active_champions_from_ranked
from app.engine.runner import run_pipeline
from app.engine.strategy_store import bootstrap_strategies_from_experiments, load_active_strategy_configs_from_db

class LiveLoopService:
    """Live prediction loop scaffold."""

    def __init__(self, db_path: str = "data/alpha.db", tenant_id: str = "default") -> None:
        self.db_path = str(db_path)
        self.tenant_id = str(tenant_id)

    def _bars_cache(self) -> BarsCache | None:
        name = (str(os.getenv("HISTORICAL_BARS_PROVIDER", "") or "").strip().lower() or None)
        if not name:
            return None
        try:
            provider = build_bars_provider(name)
        except Exception:
            return None
        return BarsCache(db_path=self.db_path, provider=provider, tenant_id=self.tenant_id)

    def run_once(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        repo = Repository(self.db_path)
        bootstrap_strategies_from_experiments(repo)

        # Target Stocks: canonical universe (fails fast if empty).
        reg = get_target_stocks_registry()
        targets = get_target_stocks(asof=now)
        targets_set = set(targets)

        # Ingest new raw events from a simple inbox file.
        inbox = Path("data/live/raw_events.jsonl")
        if inbox.exists():
            lines = inbox.read_text(encoding="utf-8").splitlines()
            with repo.transaction():
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        payload = json.loads(line)
                        raw = RawEvent(
                            id=str(payload["id"]),
                            timestamp=isoparse(str(payload["timestamp"])),
                            source=str(payload.get("source", "live")),
                            text=str(payload.get("text", "")),
                            tickers=list(payload.get("tickers") or []),
                            metadata=dict(payload.get("metadata") or {}),
                        )
                        repo.persist_raw_event(raw)
                    except Exception:
                        continue

            # Best-effort archive to avoid re-reading the whole file every tick.
            if lines:
                archive_dir = Path("data/live/archive")
                archive_dir.mkdir(parents=True, exist_ok=True)
                stamp = now.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z").replace(":", "").replace("-", "")
                try:
                    inbox.replace(archive_dir / f"raw_events_{stamp}.jsonl")
                except Exception:
                    # If archiving fails, leave the inbox intact (idempotent inserts still protect correctness).
                    pass

        # Pull NEW + due DEFERRED events from the queue. This prevents cursor stall when bars are missing.
        now_iso = now.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        rows = repo.conn.execute(
            """
            SELECT
              q.raw_event_id,
              r.timestamp,
              r.source,
              r.text,
              r.tickers_json,
              r.metadata_json
            FROM raw_event_queue q
            JOIN raw_events r
              ON r.tenant_id = q.tenant_id
             AND r.id = q.raw_event_id
            WHERE q.tenant_id = 'default'
              AND (
                q.status = 'NEW'
                OR (q.status = 'DEFERRED' AND (q.next_retry_at IS NULL OR q.next_retry_at <= ?))
              )
            ORDER BY q.status DESC, COALESCE(q.next_retry_at, '') ASC
            LIMIT 50
            """,
            (now_iso,),
        ).fetchall()

        if not rows:
            repo.add_heartbeat("live", "ok", "queue empty")
            repo.close()
            return {"mode": "live", "ran_at": now.isoformat(), "status": "ok", "processed_events": 0}

        raw_events: list[RawEvent] = []
        for r in rows:
            try:
                tickers = json.loads(str(r["tickers_json"] or "[]"))
            except Exception:
                tickers = []
            try:
                metadata = json.loads(str(r["metadata_json"] or "{}"))
            except Exception:
                metadata = {}
            raw_id = str(r["raw_event_id"])
            raw_events.append(
                RawEvent(
                    id=raw_id,
                    timestamp=isoparse(str(r["timestamp"])),
                    source=str(r["source"]),
                    text=str(r["text"]),
                    tickers=list(tickers) if isinstance(tickers, list) else [],
                    metadata=dict(metadata) if isinstance(metadata, dict) else {},
                )
            )

        if not raw_events:
            repo.add_heartbeat("live", "ok", "queue items missing from raw_events")
            repo.close()
            return {"mode": "live", "ran_at": now.isoformat(), "status": "ok", "processed_events": 0}

        # Target Stocks filtering: events enrich the universe; they do not define it.
        filtered: list[RawEvent] = []
        ignored_ids: list[str] = []
        for evt in raw_events:
            t = (evt.tickers[0] if evt.tickers else "")
            ticker = str(t).strip().upper() if t else ""
            if ticker and ticker not in targets_set:
                ignored_ids.append(evt.id)
                continue
            # Normalize ticker casing if present.
            if ticker:
                evt.tickers = [ticker]
            filtered.append(evt)
        raw_events = filtered

        if ignored_ids:
            processed_at = now.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            with repo.transaction():
                for rid in ignored_ids:
                    repo.set_raw_event_queue_status(rid, "PROCESSED", processed_at=processed_at, next_retry_at=None, last_error="not_target_stock")

        if not raw_events:
            repo.add_heartbeat("live", "ok", f"ignored {len(ignored_ids)} non-target events")
            repo.close()
            return {"mode": "live", "ran_at": now.isoformat(), "status": "ok", "processed_events": 0, "ignored_events": len(ignored_ids)}

        bars_cache = self._bars_cache()
        can_ensure = True
        if bars_cache is None:
            # Offline-safe mode: read from existing cached bars without fetching.
            bars_cache = BarsCache(db_path=self.db_path, provider=build_bars_provider("mock"), tenant_id=self.tenant_id)
            can_ensure = False

        # Determine the live window and ensure bars for the whole universe (deterministic).
        window = bar_window_for_events(event_times=[evt.timestamp for evt in raw_events] + [now])
        if can_ensure:
            bars_cache.ensure_policy(tickers=targets, start=window.start, end=window.end, now=now)

        seg_1m_start = max(window.start, now - timedelta(days=5))
        seg_1h_start = max(window.start, now - timedelta(days=90))
        seg_1d_end = min(window.end, now - timedelta(days=90))

        coverage_1m = bars_cache.coverage_pct(tickers=targets, timeframe="1m", start=seg_1m_start, end=window.end) if seg_1m_start < window.end else 0.0
        coverage_1h = bars_cache.coverage_pct(tickers=targets, timeframe="1h", start=seg_1h_start, end=window.end) if seg_1h_start < window.end else 0.0
        coverage_1d = bars_cache.coverage_pct(tickers=targets, timeframe="1d", start=window.start, end=seg_1d_end) if window.start < seg_1d_end else 0.0

        # Build per-event price_context from cached multi-timeframe bars.
        price_contexts: dict[str, dict] = {}
        from app.core.price_context import default_benchmark_tickers

        tickers_needed = sorted({(evt.tickers[0] if evt.tickers else "") for evt in raw_events} - {""})
        tickers_fetch = sorted(set(tickers_needed).union(set(default_benchmark_tickers())))
        if tickers_needed:
            bars_by_tf = {
                "1m": bars_cache.fetch_bars_df(timeframe="1m", tickers=tickers_fetch, start=window.start, end=window.end),
                "1h": bars_cache.fetch_bars_df(timeframe="1h", tickers=tickers_fetch, start=window.start, end=window.end),
                "1d": bars_cache.fetch_bars_df(timeframe="1d", tickers=tickers_fetch, start=window.start, end=window.end),
            }
            from app.core.price_context import build_price_contexts_from_bars_multi

            price_contexts = build_price_contexts_from_bars_multi(
                raw_events=raw_events,
                bars_by_timeframe=bars_by_tf,
                benchmark_tickers=default_benchmark_tickers(),
            )

        strategies = load_active_champion_configs(repo, tenant_id=self.tenant_id)
        if not strategies:
            refresh_active_champions_from_ranked(repo, tenant_id=self.tenant_id, min_predictions=5, now=now)
            strategies = load_active_champion_configs(repo, tenant_id=self.tenant_id)

        if not strategies:
            strategies = load_active_strategy_configs_from_db(repo, tenant_id=self.tenant_id)
        processable_events = [evt for evt in raw_events if evt.id in price_contexts]
        if not processable_events:
            # Defer queue items to retry later, but do not block newer events.
            retry_at = (now + timedelta(minutes=5)).astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            for evt in raw_events:
                repo.set_raw_event_queue_status(evt.id, "DEFERRED", next_retry_at=retry_at, last_error="missing_bars")
            repo.add_heartbeat("live", "ok", f"deferred {len(raw_events)} events (missing bars)")
            repo.close()
            return {
                "mode": "live",
                "ran_at": now.isoformat(),
                "status": "ok",
                "processed_events": 0,
                "deferred_events": len(raw_events),
            }

        result = run_pipeline(
            processable_events,
            price_contexts,
            persist=True,
            db_path=self.db_path,
            strategy_configs=strategies,
            mode_override="live",
            evaluate_outcomes=False,
        )

        processed_at = now.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        processed_ids = {e.id for e in processable_events}
        retry_at = (now + timedelta(minutes=5)).astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        with repo.transaction():
            for evt in raw_events:
                if evt.id in processed_ids:
                    repo.set_raw_event_queue_status(evt.id, "PROCESSED", processed_at=processed_at, next_retry_at=None, last_error=None)
                else:
                    repo.set_raw_event_queue_status(evt.id, "DEFERRED", next_retry_at=retry_at, last_error="missing_bars")

        deferred = len(raw_events) - len(processable_events)
        targets_processed = len({(e.tickers[0] if e.tickers else "") for e in processable_events} - {""})
        repo.add_heartbeat(
            "live",
            "ok",
            f"processed {len(processable_events)} deferred {deferred} targets {targets_processed}/{len(targets)} "
            f"coverage_1m={coverage_1m:.1f}% coverage_1h={coverage_1h:.1f}% coverage_1d={coverage_1d:.1f}% "
            f"universe={reg.target_universe_version[:10]}",
        )
        repo.close()
        return {
            "mode": "live",
            "ran_at": now.isoformat(),
            "status": "ok",
            "processed_events": len(processable_events),
            "deferred_events": deferred,
            "predictions": len(result.get("prediction_rows", [])),
            "ignored_events": len(ignored_ids),
            "targets_total": len(targets),
            "targets_processed": targets_processed,
            "bars_coverage_pct": {"1m": coverage_1m, "1h": coverage_1h, "1d": coverage_1d},
            "target_universe_version": reg.target_universe_version,
        }
