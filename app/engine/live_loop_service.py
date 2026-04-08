from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path

import pandas as pd
from dateutil.parser import isoparse

from app.core.repository import Repository
from app.core.types import RawEvent
from app.core.price_context import build_price_context_for_event
from app.engine.runner import run_pipeline
from app.engine.strategy_store import bootstrap_strategies_from_experiments, load_active_strategy_configs_from_db

class LiveLoopService:
    """Live prediction loop scaffold."""

    def run_once(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        repo = Repository("data/alpha.db")
        bootstrap_strategies_from_experiments(repo)

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

        # Build per-event price_context from bars stored in SQLite.
        price_contexts: dict[str, dict] = {}
        tickers = sorted({(evt.tickers[0] if evt.tickers else "") for evt in raw_events} - {""})
        if tickers:
            placeholders = ",".join(["?"] * len(tickers))
            bars_all = repo.query_df(
                f"""
                SELECT ticker, timestamp, open, high, low, close, volume
                FROM price_bars
                WHERE tenant_id = 'default' AND ticker IN ({placeholders})
                ORDER BY ticker, timestamp ASC
                """,
                params=tuple(tickers),
            )
            if not bars_all.empty:
                bars_all["timestamp"] = pd.to_datetime(bars_all["timestamp"], utc=True)
                by_ticker = {t: df for t, df in bars_all.groupby("ticker")}

                for evt in raw_events:
                    ticker = evt.tickers[0] if evt.tickers else None
                    if not ticker or ticker not in by_ticker:
                        continue
                    ctx = build_price_context_for_event(ticker_bars=by_ticker[ticker], event_ts=evt.timestamp)
                    if ctx:
                        price_contexts[evt.id] = ctx

        strategies = load_active_strategy_configs_from_db(repo)
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
            db_path="data/alpha.db",
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
        repo.add_heartbeat("live", "ok", f"processed {len(processable_events)} events, deferred {deferred}")
        repo.close()
        return {
            "mode": "live",
            "ran_at": now.isoformat(),
            "status": "ok",
            "processed_events": len(processable_events),
            "deferred_events": deferred,
            "predictions": len(result.get("prediction_rows", [])),
        }
