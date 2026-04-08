from __future__ import annotations

from datetime import datetime, timezone
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

        last_ingested = repo.get_kv("live:last_ingested_at") or ""
        rows = repo.conn.execute(
            """
            SELECT id, timestamp, source, text, tickers_json, metadata_json, COALESCE(ingested_at, '') as ingested_at
            FROM raw_events
            WHERE tenant_id = 'default' AND COALESCE(ingested_at, '') > ?
            ORDER BY ingested_at ASC
            LIMIT 50
            """,
            (last_ingested,),
        ).fetchall()

        raw_events: list[RawEvent] = []
        ingested_by_id: dict[str, str] = {}
        max_ingested = last_ingested
        for r in rows:
            ingested_at = str(r["ingested_at"] or "")
            if ingested_at > max_ingested:
                max_ingested = ingested_at
            try:
                tickers = json.loads(str(r["tickers_json"] or "[]"))
            except Exception:
                tickers = []
            try:
                metadata = json.loads(str(r["metadata_json"] or "{}"))
            except Exception:
                metadata = {}
            raw_id = str(r["id"])
            ingested_by_id[raw_id] = ingested_at
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
            repo.add_heartbeat("live", "ok", "no new raw events")
            repo.close()
            return {"mode": "live", "ran_at": now.isoformat(), "status": "ok", "processed_events": 0}

        # Build per-event price_context from bars stored in SQLite.
        price_contexts: dict[str, dict] = {}
        processed_max_ingested = last_ingested
        tickers = sorted({(evt.tickers[0] if evt.tickers else "") for evt in raw_events} - {""})
        if tickers:
            placeholders = ",".join(["?"] * len(tickers))
            bar_rows = repo.conn.execute(
                f"""
                SELECT ticker, timestamp, open, high, low, close, volume
                FROM price_bars
                WHERE tenant_id = 'default' AND ticker IN ({placeholders})
                ORDER BY ticker, timestamp ASC
                """,
                tuple(tickers),
            ).fetchall()
            if bar_rows:
                bars_all = pd.DataFrame([dict(br) for br in bar_rows])
                bars_all["timestamp"] = pd.to_datetime(bars_all["timestamp"], utc=True)
                by_ticker = {t: df for t, df in bars_all.groupby("ticker")}

                for evt in raw_events:
                    ticker = evt.tickers[0] if evt.tickers else None
                    if not ticker or ticker not in by_ticker:
                        continue
                    ctx = build_price_context_for_event(ticker_bars=by_ticker[ticker], event_ts=evt.timestamp)
                    if ctx:
                        price_contexts[evt.id] = ctx
                        ia = ingested_by_id.get(evt.id, "")
                        if ia and ia > processed_max_ingested:
                            processed_max_ingested = ia

        strategies = load_active_strategy_configs_from_db(repo)
        processable_events = [evt for evt in raw_events if evt.id in price_contexts]
        if not processable_events:
            repo.add_heartbeat("live", "ok", f"deferred {len(raw_events)} events (missing bars)")
            repo.close()
            return {"mode": "live", "ran_at": now.isoformat(), "status": "ok", "processed_events": 0, "deferred_events": len(raw_events)}

        result = run_pipeline(
            processable_events,
            price_contexts,
            persist=True,
            db_path="data/alpha.db",
            strategy_configs=strategies,
            mode_override="live",
            evaluate_outcomes=False,
        )

        # Cursor by ingested_at for only the events we actually processed.
        if processed_max_ingested and processed_max_ingested > last_ingested:
            repo.set_kv("live:last_ingested_at", processed_max_ingested)

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
