from __future__ import annotations
import asyncio
import hashlib
import os
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from app.ingest.key_manager import KeyManager
from app.ingest.fetch_context import FetchContext
from app.ingest.extractor import Extractor
from app.ingest.validator import validate_sources_yaml, validate_events
from app.ingest.registry import resolve_adapter
from app.ingest.event_model import Event
from app.ingest.dedupe import Deduper
from app.ingest.event_store import EventStore
from app.ingest.router import EventRouter
from app.core.types import RawEvent
from app.core.repository import Repository
from app.core.time_utils import to_utc_datetime, normalize_timestamp
from app.core.bars import BarsCache, bar_window_for_events, build_bars_provider
from app.core.price_context import build_price_contexts_from_bars_multi
from app.core.target_stocks import get_target_stocks, get_target_stocks_registry
from app.engine.runner import run_pipeline
from app.engine.continuous_learning import ContinuousLearner
from app.engine.strategy_registry import StrategyRegistry
from app.engine.promotion_engine import PromotionEngine
from app.engine.genetic_optimizer import GeneticOptimizer

BACKFILL_TENANT_ID = "backfill"
DEFAULT_HORIZONS_MINUTES = (1, 5, 15, 60, 240, 1440)


@dataclass
class ReplaySummary:
    replayed: int = 0
    deferred: int = 0
    deferred_reasons: Counter[str] = field(default_factory=Counter)

    def merge(self, other: "ReplaySummary") -> None:
        self.replayed += int(other.replayed or 0)
        self.deferred += int(other.deferred or 0)
        self.deferred_reasons.update(other.deferred_reasons or {})


def _provider_group(spec) -> str:
    adapter = str(getattr(spec, "adapter", "") or "")
    if "alpaca" in adapter:
        return "alpaca"
    if "reddit" in adapter:
        return "reddit"
    if "fred" in adapter:
        return "fred"
    if "yahoo" in adapter:
        return "yahoo"
    return str(getattr(spec, "id", "unknown") or "unknown")


@dataclass(frozen=True)
class SliceFetchResult:
    source_id: str
    events: list[Event]
    ok: bool
    error: str | None = None


async def _fetch_slice(
    spec,
    start_date: datetime,
    end_date: datetime,
    key_manager: KeyManager,
    extractor: Extractor,
    *,
    cache_handle: dict[str, Any] | None = None,
) -> SliceFetchResult:
    # Optional shortcut: allow sources.yaml to opt into the canonical universe.
    try:
        if isinstance(getattr(spec, "symbols", None), str) and str(getattr(spec, "symbols", "")).strip().upper() == "TARGET_STOCKS":
            targets = get_target_stocks(asof=start_date)
            if hasattr(spec, "model_copy"):
                spec = spec.model_copy(update={"symbols": targets})
            elif hasattr(spec, "copy"):
                spec = spec.copy(update={"symbols": targets})
    except Exception:
        pass

    adapter = resolve_adapter(spec.adapter)
    if not adapter:
        return SliceFetchResult(source_id=str(spec.id), events=[], ok=False, error="adapter_not_found")

    from app.ingest.async_runner import get_limiter
    provider = _provider_group(spec)
    limiter = get_limiter(provider)
    
    ctx = FetchContext(
        provider=provider,
        key_manager=key_manager,
        rate_limiter=limiter,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        cache_handle=cache_handle,
        run_metadata={"mode": "backfill", "source_id": spec.id},
    )

    try:
        t0 = time.perf_counter()
        raw_rows = await adapter.fetch_raw(spec, ctx)
        dt_fetch = time.perf_counter() - t0
        
        events = extractor.normalize_many(raw_rows, spec)
        valid_events = validate_events(events)
        
        # Enforce slice bounds even if the adapter ignores date filtering.
        start_utc = to_utc_datetime(start_date)
        end_utc = to_utc_datetime(end_date)
        bounded: list[Event] = []
        for e in valid_events:
            try:
                ts = to_utc_datetime(e.timestamp)
            except Exception:
                continue
            if start_utc <= ts < end_utc:
                e.timestamp = normalize_timestamp(ts)
                bounded.append(e)
        
        dt_total = time.perf_counter() - t0
        print(f"[{datetime.now(timezone.utc).isoformat()}] {spec.id}: {dt_total:.2f}s (fetch: {dt_fetch:.2f}s), {len(bounded)} events")
        return SliceFetchResult(source_id=str(spec.id), events=bounded, ok=True)
    except Exception as e:
        print(f"Backfill Error [{spec.id}]: {e}")
        return SliceFetchResult(source_id=str(spec.id), events=[], ok=False, error=str(e))

class BackfillRunner:
    def __init__(self, db_path: str = "data/alpha.db", *, bars_provider: str | None = None):
        self.store = EventStore(db_path)
        self.router = EventRouter()
        self.key_manager = KeyManager()
        self.extractor = Extractor()
        self._fetch_cache: dict[str, Any] = {}
        self.bars_provider_name = bars_provider or os.getenv("HISTORICAL_BARS_PROVIDER", "").strip() or None
        
        # Self-learning components
        self.learner = ContinuousLearner()
        self.registry = StrategyRegistry()
        self.promotion_engine = PromotionEngine(self.registry)
        self._rng = random.Random()
        self.genetic_optimizer = GeneticOptimizer(self.registry, rng=self._rng)

    def _init_determinism(self, *, seed_material: str) -> None:
        digest = hashlib.sha1(seed_material.encode("utf-8")).hexdigest()
        seed = int(digest[:16], 16)
        self._rng.seed(seed)

    def _bars_cache(self) -> BarsCache | None:
        if not self.bars_provider_name:
            # Try a sane default order.
            candidates = ["alpaca", "polygon", "yfinance"]
            if str(os.getenv("ALLOW_MOCK_BARS", "false")).lower() == "true":
                candidates.append("mock")

            for candidate in candidates:
                try:
                    provider = build_bars_provider(candidate)
                    self.bars_provider_name = candidate
                    return BarsCache(db_path=str(self.store.db_path), provider=provider, tenant_id=BACKFILL_TENANT_ID)
                except Exception:
                    continue
            return None
        try:
            if str(self.bars_provider_name).lower() == "mock":
                if str(os.getenv("ALLOW_MOCK_BARS", "false")).lower() != "true":
                    raise RuntimeError("Mock bars provider requested but ALLOW_MOCK_BARS is false. Replay rejected for economic safety.")

            provider = build_bars_provider(self.bars_provider_name)
        except Exception as e:
            print(f"[BackfillRunner] Bars provider '{self.bars_provider_name}' unavailable: {e}")
            return None
        return BarsCache(db_path=str(self.store.db_path), provider=provider, tenant_id=BACKFILL_TENANT_ID)

    async def backfill_range(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        batch_size_days: int = 1,
        replay: bool = True,
        skip_completed: bool = True,
        fail_fast: bool = True,
        max_zero_insert_slices: int = 2,
    ) -> None:
        """
        Fetches historical events for [start_time, end_time) and optionally replays only the unseen subset.
        """
        start_time = to_utc_datetime(start_time).replace(microsecond=0)
        end_time = to_utc_datetime(end_time).replace(microsecond=0)
        if start_time >= end_time:
            raise ValueError("backfill_range requires start_time < end_time")

        repo = Repository(self.store.db_path)
        total_inserted = 0
        zero_insert_streak = 0
        try:
            specs = validate_sources_yaml()

            # Optimization: if a custom bundle has no rows anywhere in the requested range,
            # skip it for the whole run to avoid per-slice overhead.
            effective_specs = []
            for spec in specs:
                if not getattr(spec, "enabled", True):
                    continue
                if str(getattr(spec, "adapter", "")).strip().lower() == "custom_bundle":
                    try:
                        adapter = resolve_adapter(spec.adapter)
                        if adapter is None:
                            continue
                        ctx = FetchContext(
                            provider=_provider_group(spec),
                            key_manager=self.key_manager,
                            rate_limiter=None,
                            start_date=start_time.isoformat(),
                            end_date=end_time.isoformat(),
                            cache_handle=self._fetch_cache,
                            run_metadata={"mode": "backfill", "source_id": spec.id, "prefilter": True},
                        )
                        raw_rows = await adapter.fetch_raw(spec, ctx)
                        if not raw_rows:
                            continue
                    except Exception:
                        # If prefilter fails, keep the source enabled (safer than skipping).
                        pass
                effective_specs.append(spec)
            specs = effective_specs

            # Fetch slices.
            current_start = start_time
            while current_start < end_time:
                current_end = min(current_start + timedelta(days=batch_size_days), end_time)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching slice: {current_start.date()} to {current_end.date()}")
                
                t_slice_start = time.perf_counter()
                slice_start_ts = normalize_timestamp(current_start)
                slice_end_ts = normalize_timestamp(current_end)
                tasks = []
                for spec in specs:
                    if not spec.enabled:
                        continue
                    if skip_completed:
                        sid = str(spec.id)
                        if self.store.is_slice_completed(source_id=sid, start_ts=slice_start_ts, end_ts=slice_end_ts):
                            continue
                        # If events for this source already exist in the slice, treat as satisfied and skip refetch.
                        try:
                            existing_for_source = self.store.count_events_for_source_in_range(
                                source_id=sid,
                                start_ts=slice_start_ts,
                                end_ts=slice_end_ts,
                                end_inclusive=False,
                            )
                        except Exception:
                            existing_for_source = 0
                        if existing_for_source > 0:
                            self.store.record_slice_marker(
                                source_id=sid,
                                start_ts=slice_start_ts,
                                end_ts=slice_end_ts,
                                ok=True,
                                fetched_count=0,
                                last_error="skipped_existing_events",
                            )
                            continue
                    tasks.append(
                        _fetch_slice(
                            spec,
                            current_start,
                            current_end,
                            self.key_manager,
                            self.extractor,
                            cache_handle=self._fetch_cache,
                        )
                    )

                if not tasks:
                    # Fully satisfied slice (no sources needed). Avoid any network fetches.
                    print(
                        f"[{datetime.now(timezone.utc).isoformat()}] Slice already completed by markers; skipping fetch/store."
                    )
                    current_start = current_end
                    continue

                results = await asyncio.gather(*tasks, return_exceptions=True)
                dt_fetch = time.perf_counter() - t_slice_start
                
                t_store_start = time.perf_counter()
                all_events = []
                for r in results:
                    if isinstance(r, Exception):
                        continue
                    if isinstance(r, SliceFetchResult):
                        all_events.extend(r.events)

                deduper = Deduper()
                unique_events, _ = deduper.process(all_events)

                existing_in_slice = self.store.count_events_in_range(start_ts=slice_start_ts, end_ts=slice_end_ts)
                inserted = self.store.save_batch(unique_events)
                db_skipped = max(0, len(unique_events) - inserted)
                dt_store = time.perf_counter() - t_store_start

                # Record per-source slice markers (avoid refetching on reruns).
                for r in results:
                    if isinstance(r, SliceFetchResult):
                        self.store.record_slice_marker(
                            source_id=r.source_id,
                            start_ts=slice_start_ts,
                            end_ts=slice_end_ts,
                            ok=bool(r.ok),
                            fetched_count=len(r.events),
                            last_error=r.error,
                        )

                if fail_fast:
                    if len(all_events) > 0 and len(unique_events) == 0:
                        raise RuntimeError(
                            f"Backfill slice {slice_start_ts}..{slice_end_ts} fetched {len(all_events)} events but deduped to 0. "
                            "Likely ID collision or timestamp normalization bug."
                        )

                    # Treat 0 inserts as suspicious only if this slice window was empty before.
                    # (Reruns should be allowed to no-op when data already exists.)
                    if len(unique_events) > 0 and inserted == 0 and existing_in_slice == 0:
                        zero_insert_streak += 1
                        if zero_insert_streak >= max_zero_insert_slices:
                            raise RuntimeError(
                                f"Backfill produced {len(unique_events)} unique events but inserted 0 new rows for "
                                f"{zero_insert_streak} consecutive slices. Likely dedupe/ID/DB constraint issue."
                            )
                    else:
                        zero_insert_streak = 0
                
                total_inserted += inserted
                dt_slice = time.perf_counter() - t_slice_start
                
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] Slice complete: "
                    f"{inserted} inserted ({db_skipped} skipped: already present in DB) "
                    f"({len(all_events)} fetched, {len(unique_events)} unique, {existing_in_slice} preexisting in window) "
                    f"({dt_slice:.2f}s total: {dt_fetch:.2f}s fetch, {dt_store:.2f}s store)"
                )
                print(f"Running total events inserted: {total_inserted}")
                
                current_start = current_end

            # Track fetched window.
            prev_start = repo.get_kv("backfill_history_start", tenant_id=BACKFILL_TENANT_ID)
            prev_end = repo.get_kv("backfill_history_end", tenant_id=BACKFILL_TENANT_ID)
            hist_start = min([start_time, to_utc_datetime(prev_start)] if prev_start else [start_time])
            hist_end = max([end_time, to_utc_datetime(prev_end)] if prev_end else [end_time])
            repo.set_kv("backfill_history_start", normalize_timestamp(hist_start), tenant_id=BACKFILL_TENANT_ID)
            repo.set_kv("backfill_history_end", normalize_timestamp(hist_end), tenant_id=BACKFILL_TENANT_ID)

            if replay:
                t_replay_start = time.perf_counter()
                replay_summary = await self.replay_unseen(start_time=start_time, end_time=end_time, repo=repo)
                dt_replay = time.perf_counter() - t_replay_start
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] Replay phase complete in {dt_replay:.2f}s "
                    f"({replay_summary.replayed} replayed, {replay_summary.deferred} deferred)"
                )
                if replay_summary.deferred_reasons:
                    top = replay_summary.deferred_reasons.most_common(6)
                    reasons_str = ", ".join([f"{k}={v}" for k, v in top])
                    print(f"Deferred reasons (top {len(top)}): {reasons_str}")
        finally:
            repo.close()

    async def run_backfill(self, days: int | None = None, batch_size_days: int = 1):
        """
        Orchestrates the backfill process.
        """
        repo = Repository(self.store.db_path)
        specs = validate_sources_yaml()
        
        if days is None:
            # Determine max backfill days from enabled sources
            possible_days = [s.backfill_days for s in specs if s.enabled and s.backfill_days]
            days = max(possible_days) if possible_days else 90

        print(f"Starting {days}-day backfill...")
        
        end_time = datetime.now(timezone.utc).replace(microsecond=0)
        start_time = (end_time - timedelta(days=days)).replace(microsecond=0)
        
        await self.backfill_range(start_time=start_time, end_time=end_time, batch_size_days=batch_size_days, replay=True)
        print("Backfill run complete.")

    async def replay_unseen(self, *, start_time: datetime, end_time: datetime, repo: Repository) -> ReplaySummary:
        """
        Replay only ranges that have not been replayed before.
        Supports both forward-append and backward-expansion.
        """
        start_time = to_utc_datetime(start_time).replace(microsecond=0)
        end_time = to_utc_datetime(end_time).replace(microsecond=0)

        rmin_ts = repo.get_kv("backfill_replayed_min_ts", tenant_id=BACKFILL_TENANT_ID)
        rmin_id = repo.get_kv("backfill_replayed_min_id", tenant_id=BACKFILL_TENANT_ID)
        rmax_ts = repo.get_kv("backfill_replayed_max_ts", tenant_id=BACKFILL_TENANT_ID)
        rmax_id = repo.get_kv("backfill_replayed_max_id", tenant_id=BACKFILL_TENANT_ID)

        # First replay: do the whole requested range.
        if not rmin_ts or not rmax_ts:
            return await self.replay_range(start_time=start_time, end_time=end_time, repo=repo)

        # Overlap-safe: replay only the unseen tails on either side.
        rmin = to_utc_datetime(rmin_ts)
        rmax = to_utc_datetime(rmax_ts)

        summary = ReplaySummary()

        # Unseen older tail.
        if start_time < rmin:
            summary.merge(await self.replay_range(start_time=start_time, end_time=min(end_time, rmin), repo=repo))

        # Unseen newer tail.
        if end_time > rmax:
            summary.merge(
                await self.replay_range(
                    start_time=max(start_time, rmax),
                    end_time=end_time,
                    repo=repo,
                    start_exclusive=True,
                    cursor_id=rmax_id,
                )
            )

        return summary

    async def replay_range(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        repo: Repository,
        start_exclusive: bool = False,
        cursor_id: str | None = None,
    ) -> ReplaySummary:
        """
        Replays stored events in [start_time, end_time) chronologically through the engine.
        """
        start_time = to_utc_datetime(start_time).replace(microsecond=0)
        end_time = to_utc_datetime(end_time).replace(microsecond=0)
        if start_time >= end_time:
            return ReplaySummary()

        seed_material = f"{BACKFILL_TENANT_ID}:{normalize_timestamp(start_time)}:{normalize_timestamp(end_time)}"
        self._init_determinism(seed_material=seed_material)

        start_ts = normalize_timestamp(start_time)
        end_ts = normalize_timestamp(end_time)
        events = self.store.get_events_chronological(start_ts=start_ts, end_ts=end_ts)
        if start_exclusive:
            filtered = []
            for e in events:
                if str(e.timestamp) > str(start_ts):
                    filtered.append(e)
                elif str(e.timestamp) == str(start_ts) and cursor_id and str(e.id) > str(cursor_id):
                    filtered.append(e)
            events = filtered

        print(f"[{datetime.now(timezone.utc).isoformat()}] Replaying {len(events)} events in range {start_ts} .. {end_ts} ...")

        # Target Stocks: canonical universe version for this replay run.
        try:
            reg = get_target_stocks_registry()
            repo.set_kv("backfill:target_universe_version", reg.target_universe_version, tenant_id=BACKFILL_TENANT_ID)
            repo.set_kv(
                "backfill:target_universe_loaded_at",
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                tenant_id=BACKFILL_TENANT_ID,
            )
        except Exception:
            reg = None

        slice_days = 1
        generation_counter = 1
        try:
            stored_gen = repo.get_kv("backfill_generation_counter", tenant_id=BACKFILL_TENANT_ID)
            if stored_gen:
                generation_counter = int(stored_gen)
        except Exception:
            generation_counter = 1
        
        processed_min: RawEvent | None = None
        processed_max: RawEvent | None = None
        observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        total_replayed = 0
        total_deferred = 0
        deferred_reasons: Counter[str] = Counter()

        now_ref = datetime.now(timezone.utc).replace(microsecond=0)
        slice_start = start_time
        slice_idx = 0
        while slice_start < end_time:
            slice_end = min(slice_start + timedelta(days=slice_days), end_time)
            slice_idx += 1
            t_chunk_start = time.perf_counter()

            # Load slice events and enforce [slice_start, slice_end) bounds (EventStore uses BETWEEN).
            slice_events = self.store.get_events_chronological(
                start_ts=normalize_timestamp(slice_start),
                end_ts=normalize_timestamp(slice_end),
                start_inclusive=True,
            )
            bounded = []
            for e in slice_events:
                try:
                    ts = to_utc_datetime(e.timestamp)
                except Exception:
                    continue
                if not (slice_start <= ts < slice_end):
                    continue
                bounded.append(e)

            # Apply start_exclusive/cursor_id only to the very first slice.
            if start_exclusive and slice_start == start_time:
                filtered = []
                for e in bounded:
                    if str(e.timestamp) > str(start_ts):
                        filtered.append(e)
                    elif str(e.timestamp) == str(start_ts) and cursor_id and str(e.id) > str(cursor_id):
                        filtered.append(e)
                bounded = filtered

            # Target Stocks filtering: events may enrich, but do not define tickers.
            raw_events: list[RawEvent] = []
            for e in bounded:
                evt_ts = to_utc_datetime(e.timestamp)
                ticker = str(e.ticker).strip().upper() if e.ticker else None
                if ticker:
                    try:
                        allowed = set(get_target_stocks(asof=evt_ts))
                    except Exception:
                        allowed = set()
                    if ticker not in allowed:
                        continue
                    tickers = [ticker]
                else:
                    tickers = []

                raw_events.append(
                    RawEvent(
                        id=e.id or "unknown",
                        timestamp=evt_ts,
                        source=e.source_id,
                        text=e.text or "",
                        tickers=tickers,
                        tenant_id=BACKFILL_TENANT_ID,
                    )
                )

            if not raw_events:
                slice_start = slice_end
                continue

            bars_cache = self._bars_cache()
            price_contexts: dict[str, dict] = {}
            if bars_cache is None:
                raise RuntimeError(
                    "No historical bars provider available. Set HISTORICAL_BARS_PROVIDER=alpaca|polygon|yfinance and required API keys."
                )

            try:
                t_bars_start = time.perf_counter()
                window = bar_window_for_events(event_times=[re.timestamp for re in raw_events])

                # Ensure coverage for the whole Target Stocks universe (deterministic).
                targets = get_target_stocks(asof=slice_end) if reg is not None else sorted({t for re in raw_events for t in (re.tickers or []) if t})
                bars_cache.ensure_policy(tickers=targets, start=window.start, end=window.end, now=now_ref)

                # Fetch only the tickers we need to build contexts for.
                tickers = sorted({t for re in raw_events for t in (re.tickers or []) if t})
                bars_by_tf = {
                    "1m": bars_cache.fetch_bars_df(timeframe="1m", tickers=tickers, start=window.start, end=window.end),
                    "1h": bars_cache.fetch_bars_df(timeframe="1h", tickers=tickers, start=window.start, end=window.end),
                    "1d": bars_cache.fetch_bars_df(timeframe="1d", tickers=tickers, start=window.start, end=window.end),
                }
                dt_bars = time.perf_counter() - t_bars_start
                
                price_contexts = build_price_contexts_from_bars_multi(
                    raw_events=raw_events,
                    bars_by_timeframe=bars_by_tf,
                    horizons_minutes=DEFAULT_HORIZONS_MINUTES,
                )
                
                bars_tickers: set[str] = set()
                try:
                    for df in bars_by_tf.values():
                        if df is None or df.empty or "ticker" not in df.columns:
                            continue
                        bars_tickers.update({str(x) for x in df["ticker"].astype(str).unique().tolist()})
                except Exception:
                    bars_tickers = set()
                
                if tickers:
                    rows_total = 0
                    for df in bars_by_tf.values():
                        try:
                            rows_total += int(len(df))
                        except Exception:
                            continue
                    print(f"[{datetime.now(timezone.utc).isoformat()}] Bars: {len(tickers)} tickers {self.bars_provider_name} {dt_bars:.2f}s rows={rows_total}")

            except Exception as e:
                # Provider failure: record and skip the chunk (do not train on missing outcomes).
                provider_reason = f"provider_fail:{type(e).__name__}"
                rows = []
                for evt in raw_events:
                    ticker = evt.tickers[0] if evt.tickers else None
                    rows.append(
                        (
                            BACKFILL_TENANT_ID,
                            str(evt.id),
                            str(ticker) if ticker else None,
                            normalize_timestamp(evt.timestamp),
                            provider_reason,
                            observed_at,
                        )
                    )
                with repo.transaction():
                    repo.persist_missing_price_context_events(rows)
                
                total_deferred += len(raw_events)
                deferred_reasons[provider_reason] += len(raw_events)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Provider failure for chunk ({type(e).__name__}); recorded {len(rows)} missing contexts.")
                slice_start = slice_end
                continue

            # Only process events with price contexts; missing bars should not poison learning.
            processable = [evt for evt in raw_events if evt.id in price_contexts and price_contexts.get(evt.id)]
            
            if not processable:
                rows = []
                for evt in raw_events:
                    ticker = evt.tickers[0] if evt.tickers else None
                    reason = "no_ticker" if not ticker else ("no_bars" if ticker not in bars_tickers else "gap")
                    deferred_reasons[reason] += 1
                    rows.append(
                        (
                            BACKFILL_TENANT_ID,
                            str(evt.id),
                            str(ticker) if ticker else None,
                            normalize_timestamp(evt.timestamp),
                            reason,
                            observed_at,
                        )
                    )
                with repo.transaction():
                    repo.persist_missing_price_context_events(rows)
                
                total_deferred += len(raw_events)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Deferred chunk of {len(raw_events)} events (missing bars).")
                continue

            if len(processable) != len(raw_events):
                missing = [evt for evt in raw_events if evt.id not in price_contexts or not price_contexts.get(evt.id)]
                rows = []
                for evt in missing:
                    ticker = evt.tickers[0] if evt.tickers else None
                    reason = "no_ticker" if not ticker else ("no_bars" if ticker not in bars_tickers else "gap")
                    deferred_reasons[reason] += 1
                    rows.append(
                        (
                            BACKFILL_TENANT_ID,
                            str(evt.id),
                            str(ticker) if ticker else None,
                            normalize_timestamp(evt.timestamp),
                            reason,
                            observed_at,
                        )
                    )
                with repo.transaction():
                    repo.persist_missing_price_context_events(rows)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Processing {len(processable)}/{len(raw_events)} events (missing bars for {len(raw_events) - len(processable)}).")

            # Track processed bounds for replay window markers.
            if processed_min is None or processable[0].timestamp < processed_min.timestamp:
                processed_min = processable[0]
            if processed_max is None or processable[-1].timestamp > processed_max.timestamp:
                processed_max = processable[-1]
            
            # Execute pipeline with learner components
            t_pipeline_start = time.perf_counter()
            run_pipeline(
                raw_events=processable,
                price_contexts=price_contexts,
                persist=True,
                db_path=self.store.db_path,
                mode_override="backfill",
                learner=self.learner,
                registry=self.registry,
                promotion_engine=self.promotion_engine,
                genetic_optimizer=self.genetic_optimizer,
                generation_counter=generation_counter
            )
            dt_pipeline = time.perf_counter() - t_pipeline_start
            
            total_replayed += len(processable)
            total_deferred += (len(raw_events) - len(processable))
            
            # Persist replay cursor + generation marker for resumability.
            last_ts = normalize_timestamp(processable[-1].timestamp)
            last_id = str(processable[-1].id or "")
            repo.set_kv("backfill_replay_cursor_ts", last_ts, tenant_id=BACKFILL_TENANT_ID)
            repo.set_kv("backfill_replay_cursor_id", last_id, tenant_id=BACKFILL_TENANT_ID)
            repo.set_kv("backfill_generation_counter", str(generation_counter), tenant_id=BACKFILL_TENANT_ID)

            # Increment generation every 10 chunks (placeholder policy).
            if slice_idx % 10 == 0:
                generation_counter += 1
            
            dt_chunk = time.perf_counter() - t_chunk_start
            print(f"[{datetime.now(timezone.utc).isoformat()}] Slice {slice_idx}: {len(processable)} replayed in {dt_chunk:.2f}s (pipeline: {dt_pipeline:.2f}s)")
            print(f"Running total: {total_replayed} replayed, {total_deferred} deferred")

            slice_start = slice_end
            
        # Update replayed min/max bounds (processed-only).
        if processed_min is not None and processed_max is not None:
            new_min = processed_min
            new_max = processed_max
            prev_min_ts = repo.get_kv("backfill_replayed_min_ts", tenant_id=BACKFILL_TENANT_ID)
            prev_max_ts = repo.get_kv("backfill_replayed_max_ts", tenant_id=BACKFILL_TENANT_ID)
            if not prev_min_ts or str(new_min.timestamp) < str(prev_min_ts):
                repo.set_kv("backfill_replayed_min_ts", str(new_min.timestamp), tenant_id=BACKFILL_TENANT_ID)
                repo.set_kv("backfill_replayed_min_id", str(new_min.id or ""), tenant_id=BACKFILL_TENANT_ID)
            if not prev_max_ts or str(new_max.timestamp) > str(prev_max_ts):
                repo.set_kv("backfill_replayed_max_ts", str(new_max.timestamp), tenant_id=BACKFILL_TENANT_ID)
                repo.set_kv("backfill_replayed_max_id", str(new_max.id or ""), tenant_id=BACKFILL_TENANT_ID)

        print(f"[{datetime.now(timezone.utc).isoformat()}] Replay complete: {total_replayed} replayed, {total_deferred} deferred.")
        return ReplaySummary(replayed=total_replayed, deferred=total_deferred, deferred_reasons=deferred_reasons)

if __name__ == "__main__":
    runner = BackfillRunner()
    asyncio.run(runner.run_backfill(days=90))
