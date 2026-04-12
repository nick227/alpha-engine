from __future__ import annotations
import asyncio
import hashlib
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.ingest.key_manager import KeyManager
from app.ingest.extractor import Extractor
from app.ingest.validator import validate_sources_yaml, validate_events_with_reasons
from app.ingest.registry import resolve_adapter
from app.ingest.event_model import Event
from app.ingest.dedupe import Deduper
from app.ingest.event_store import EventStore
from app.ingest.router import EventRouter
from app.core.types import RawEvent
from app.core.repository import Repository
from app.core.time_utils import to_utc_datetime, normalize_timestamp
from app.core.bars import BarsCache, bar_window_for_events, build_bars_provider, FallbackBarsProvider
from app.core.price_context import build_price_contexts_from_bars_multi, default_benchmark_tickers
from app.core.target_stocks import get_target_stocks, get_target_stocks_registry
from app.core.macro.config import load_macro_series_specs
from app.core.macro.yfinance_series import fetch_and_build_macro_features
from app.core.company_profiles.yfinance_profiles import ensure_yfinance_company_profiles
from app.ingest.replay_engine import ReplayEngine
from app.ingest.runner_core import provider_for_adapter
from app.ingest.runner_core import build_ctx
from app.ingest.runner_core import safe_adapter_fetch
from app.ingest.adapters.dump_adapter import DumpAdapter

BACKFILL_TENANT_ID = "backfill"
DEFAULT_HORIZONS_MINUTES = (1, 5, 15, 60, 240, 1440, 10080, 43200)

# Dump-first guard: API adapters are only called for data within this many
# days of *now*.  Historical windows are served exclusively by dump adapters.
# Override via env: API_RECENT_WINDOW_DAYS=7
_API_RECENT_WINDOW_DAYS: int = int(os.getenv("API_RECENT_WINDOW_DAYS", "3"))
_TICKER_TOKEN_RE = re.compile(r"(?:(?<=\$)|\b)([A-Z]{1,5}(?:\.[A-Z])?)(?=\b)")
_PAREN_TICKER_RE = re.compile(r"\(([A-Z]{1,5}(?:\.[A-Z])?)\)")
_COMMON_FALSE_TICKERS = {
    "A",
    "I",
    "AN",
    "AND",
    "ARE",
    "AS",
    "AT",
    "BE",
    "BY",
    "CEO",
    "CFO",
    "COO",
    "CPI",
    "BUY",
    "CALL",
    "DAY",
    "DAYS",
    "EPS",
    "ETF",
    "EU",
    "FED",
    "FOR",
    "GDP",
    "IN",
    "IPO",
    "IS",
    "IT",
    "ITS",
    "HOLD",
    "LLC",
    "NYSE",
    "NASDAQ",
    "OF",
    "ON",
    "OR",
    "SEC",
    "SELL",
    "THE",
    "TO",
    "UK",
    "US",
    "USA",
    "WSB",
    "YOY",
    "YTD",
    "PUT",
}


def _event_primary_type(tags: list[str] | None) -> str | None:
    if not tags:
        return None
    known = {
        "news",
        "market",
        "macro",
        "social",
        "crowd",
        "market_structure",
        "volatility_event",
        "positioning",
        "regime",
        "sentiment_index",
        "intermarket_regime",
        "momentum",
        "bundle",
    }
    for t in tags:
        s = str(t or "").strip().lower()
        if s in known:
            return s
    return None


def _unique_preserve_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _parse_ticker_candidates(value: str | None) -> list[str]:
    """
    Parse possible tickers from a stored ticker field.

    Handles cases like:
      - "NVDA"
      - "NVDA, AAPL"
      - "['NVDA', 'AAPL']" (common when adapters return lists)
      - "Apple (AAPL)"
    """
    if value is None:
        return []
    s = str(value).strip().upper()
    if not s:
        return []

    out: list[str] = []

    # Parenthetical tickers often appear as "Company (TICKER)".
    out.extend([m.group(1) for m in _PAREN_TICKER_RE.finditer(s)])

    # General tokens (also catches "$NVDA").
    out.extend([m.group(1) for m in _TICKER_TOKEN_RE.finditer(s)])

    cleaned: list[str] = []
    for t in out:
        tt = str(t).strip().upper()
        if not tt or tt in _COMMON_FALSE_TICKERS:
            continue
        cleaned.append(tt)
    return _unique_preserve_order(cleaned)


def _infer_tickers_from_text(
    text: str | None,
    *,
    allowed: set[str],
    company_name_map: dict[str, str] | None = None,
) -> list[str]:
    if not text or not str(text).strip():
        return []
    s = str(text)
    candidates = _parse_ticker_candidates(s)
    if candidates:
        in_allowed = [t for t in candidates if t in allowed]
        if in_allowed:
            return _unique_preserve_order(in_allowed)

    # Company name fallback (best-effort): match shortName/longName against text.
    if company_name_map:
        s_low = s.lower()
        hits: list[str] = []
        for name_low, ticker in company_name_map.items():
            try:
                if name_low and name_low in s_low:
                    hits.append(ticker)
            except Exception:
                continue
        hits = [t for t in hits if t in allowed]
        if hits:
            return _unique_preserve_order(hits)

    return []


def _load_company_name_map_for_allowed(
    allowed: set[str],
    *,
    profiles_dir: str | Path = Path("data") / "company_profiles",
) -> dict[str, str]:
    """
    Build a {lower(company_name): TICKER} mapping from yfinance profiles (best-effort).
    """
    out: dict[str, str] = {}
    try:
        base = Path(profiles_dir)
        if not base.exists():
            return out
        for t in allowed:
            p = base / f"{str(t).strip().upper()}.json"
            if not p.exists():
                continue
            try:
                payload = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            for k in ("shortName", "longName"):
                v = payload.get(k)
                if not v:
                    continue
                name = str(v).strip()
                if len(name) < 4:
                    continue
                out[name.lower()] = str(t).strip().upper()
    except Exception:
        return out
    return out


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _spec_hash_for(store: EventStore, spec: Any) -> str:
    """
    Stable hash of the source spec that affects ingestion output.

    Excludes fields that should not change idempotency behavior (enabled/poll/backfill_days).
    """
    sid = str(getattr(spec, "id", "unknown") or "unknown")
    try:
        payload = spec.model_dump()
    except Exception:
        payload = dict(getattr(spec, "__dict__", {}) or {})
        payload.setdefault("id", sid)
    for k in ("enabled", "poll", "backfill_days", "priority"):
        payload.pop(k, None)
    return store.stable_spec_hash(payload)


@dataclass
class ReplaySummary:
    replayed: int = 0
    deferred: int = 0
    deferred_reasons: Counter[str] = field(default_factory=Counter)
    tradeable_seen: int = 0
    no_ticker_tradeable: int = 0

    def merge(self, other: "ReplaySummary") -> None:
        self.replayed += int(other.replayed or 0)
        self.deferred += int(other.deferred or 0)
        self.deferred_reasons.update(other.deferred_reasons or {})
        self.tradeable_seen += int(getattr(other, "tradeable_seen", 0) or 0)
        self.no_ticker_tradeable += int(getattr(other, "no_ticker_tradeable", 0) or 0)


def _provider_for_spec(spec) -> str:
    return provider_for_adapter(
        str(getattr(spec, "adapter", "") or ""),
        source_id=str(getattr(spec, "id", "unknown") or "unknown"),
    )


def _running_ttl_s_for_provider(provider: str) -> int:
    """
    Provider-specific TTL for "running" ingest windows.

    Env overrides (examples):
      - INGEST_RUNNING_TTL_S=1800
      - INGEST_RUNNING_TTL_S_ALPACA=3600
      - INGEST_RUNNING_TTL_S_REDDIT=900
    """
    base = int(float(os.getenv("INGEST_RUNNING_TTL_S", "1800") or "1800"))
    key = f"INGEST_RUNNING_TTL_S_{str(provider).strip().upper()}"
    try:
        override = os.getenv(key)
        if override is None or str(override).strip() == "":
            return base
        return int(float(str(override).strip()))
    except Exception:
        return base


def _derive_ingest_status(
    *,
    raw_rows_count: int,
    emitted_count: int,
    ok: bool,
    error: str | None,
) -> tuple[bool, str | None, str | None]:
    """
    Derive (ok_override, status_override, last_error_override) for a window.

    - If we got a non-empty provider response but emitted 0 events, treat as schema/mapping drift.
    """
    if bool(ok) and int(raw_rows_count) > 0 and int(emitted_count) == 0:
        return False, "failed_schema", "zero_emission_nonempty_response"
    return bool(ok), None, (str(error) if error else None)


@dataclass(frozen=True)
class SliceFetchResult:
    source_id: str
    events: list[Event]
    ok: bool
    error: str | None = None
    provider: str | None = None
    request_hash: str | None = None
    request_cache_hit: bool = False
    response_fingerprint: str | None = None
    raw_rows_count: int = 0
    normalized_count: int = 0
    valid_count: int = 0
    fetch_time_s: float | None = None
    total_time_s: float | None = None
    dropped_empty_text: int = 0
    dropped_bad_timestamp: int = 0
    dropped_invalid_shape: int = 0
    dropped_out_of_bounds: int = 0


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
        return SliceFetchResult(
            source_id=str(spec.id),
            events=[],
            ok=False,
            error="adapter_not_found",
            provider=_provider_for_spec(spec),
        )

    # ------------------------------------------------------------------
    # DUMP-FIRST GUARD
    # API adapters are only queried for data within the recent window.
    # Historical slices (end_date <= today - _API_RECENT_WINDOW_DAYS) are
    # served exclusively by dump adapters, which handle availability via
    # their own has_data() check inside fetch_raw().
    # ------------------------------------------------------------------
    if not isinstance(adapter, DumpAdapter):
        _api_cutoff = datetime.now(timezone.utc) - timedelta(days=_API_RECENT_WINDOW_DAYS)
        if end_date <= _api_cutoff:
            print(
                f"[ingest] mode=backfill source={spec.id} adapter={getattr(spec, 'adapter', '')} "
                f"skipped (historical api guard end={end_date.date()} cutoff={_api_cutoff.date()})"
            )
            return SliceFetchResult(
                source_id=str(spec.id),
                events=[],
                ok=True,
                provider=_provider_for_spec(spec),
                raw_rows_count=0,
                normalized_count=0,
                valid_count=0,
            )

    ctx = build_ctx(
        adapter_name=str(getattr(spec, "adapter", "") or ""),
        source_id=str(getattr(spec, "id", "unknown") or "unknown"),
        key_manager=key_manager,
        cache_handle=cache_handle,
        mode="backfill",
        start_date=start_date,
        end_date=end_date,
    )
    provider = str(getattr(ctx, "provider", "") or _provider_for_spec(spec))

    try:
        t0 = time.perf_counter()

        # Request-level cache (in-run): prevents repeated API calls for identical requests.
        # This enables overlapping windows/slices to reuse the same raw payload without network calls.
        request_cache: dict[str, list[dict[str, Any]]] | None = None
        if isinstance(cache_handle, dict):
            rc = cache_handle.get("ingest_request_cache")
            if not isinstance(rc, dict):
                rc = {}
                cache_handle["ingest_request_cache"] = rc
            request_cache = rc  # type: ignore[assignment]

        fetch_payload = None
        try:
            fetch_payload = spec.fetch.model_dump() if getattr(spec, "fetch", None) is not None else None
        except Exception:
            fetch_payload = None

        req_key_payload = {
            "source_id": str(spec.id),
            "adapter": str(getattr(spec, "adapter", "")),
            "endpoint": str(getattr(spec, "endpoint", "") or ""),
            "fetch": fetch_payload,
            "symbols": getattr(spec, "symbols", None),
            "options": getattr(spec, "options", None),
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        }
        request_hash = hashlib.sha256(
            json.dumps(req_key_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()[:16]

        cache_hit = False
        if request_cache is not None and request_hash in request_cache:
            raw_rows = request_cache[request_hash]
            cache_hit = True
        else:
            raw_rows = await safe_adapter_fetch(adapter, spec, ctx, timeout_s=30.0, retries=0)
            if request_cache is not None:
                request_cache[request_hash] = raw_rows
        dt_fetch = time.perf_counter() - t0

        raw_rows_list = raw_rows if isinstance(raw_rows, list) else []
        try:
            response_fingerprint = hashlib.sha256(
                json.dumps(raw_rows_list, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
            ).hexdigest()[:16]
        except Exception:
            response_fingerprint = None

        events = extractor.normalize_many(raw_rows_list, spec)
        normalized_count = len(events)
        valid_events, dropped_counts = validate_events_with_reasons(events)
        valid_count = len(valid_events)
        dropped_empty_text = int(dropped_counts.get("empty_text", 0))
        dropped_bad_timestamp = int(dropped_counts.get("bad_timestamp", 0))
        dropped_invalid_shape = int(dropped_counts.get("invalid_shape", 0))
        
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
        dropped_out_of_bounds = max(0, len(valid_events) - len(bounded))
        
        dt_total = time.perf_counter() - t0
        print(
            f"[ingest] mode=backfill source={spec.id} adapter={spec.adapter} raw={len(raw_rows_list)} "
            f"bounded={len(bounded)} fetch_ms={dt_fetch*1000.0:.2f} total_ms={dt_total*1000.0:.2f}"
        )
        return SliceFetchResult(
            source_id=str(spec.id),
            events=bounded,
            ok=True,
            provider=str(provider),
            request_hash=str(request_hash),
            request_cache_hit=bool(cache_hit),
            response_fingerprint=response_fingerprint,
            raw_rows_count=len(raw_rows_list),
            normalized_count=int(normalized_count),
            valid_count=int(valid_count),
            fetch_time_s=float(dt_fetch),
            total_time_s=float(dt_total),
            dropped_empty_text=int(dropped_empty_text),
            dropped_bad_timestamp=int(dropped_bad_timestamp),
            dropped_invalid_shape=int(dropped_invalid_shape),
            dropped_out_of_bounds=int(dropped_out_of_bounds),
        )
    except Exception as e:
        print(f"[ingest] mode=backfill source={spec.id} adapter={getattr(spec, 'adapter', '')} error={type(e).__name__}:{e}")
        return SliceFetchResult(source_id=str(spec.id), events=[], ok=False, error=str(e), provider=_provider_for_spec(spec))

class BackfillRunner:
    def __init__(self, db_path: str = "data/alpha.db", *, bars_provider: str | None = None):
        self.store = EventStore(db_path)
        self.router = EventRouter()
        self.key_manager = KeyManager()
        self.extractor = Extractor()
        self._fetch_cache: dict[str, Any] = {}
        self.bars_provider_name = bars_provider or os.getenv("HISTORICAL_BARS_PROVIDER", "").strip() or None
        self.replay_engine = ReplayEngine(db_path=str(self.store.db_path))

    def _macro_snapshot_for_slice(self, *, asof: datetime) -> dict[str, float]:
        """
        Build global macro snapshot features for the slice (best-effort).
        """
        if str(os.getenv("ENABLE_MACRO_SNAPSHOT", "true")).lower() == "false":
            return {}
        specs = load_macro_series_specs()
        yf_specs = [(s.name, s.symbol) for s in specs if str(s.provider).lower() == "yfinance"]
        if not yf_specs:
            return {}
        try:
            snap = fetch_and_build_macro_features(specs=yf_specs, asof=asof, lookback_days=120)
            return dict(snap.features or {})
        except Exception:
            return {}

    def _bars_cache(self) -> BarsCache | None:
        if not self.bars_provider_name:
            # Try a sane default order.
            candidates = ["alpaca", "polygon", "yfinance"]
            if str(os.getenv("ALLOW_MOCK_BARS", "false")).lower() == "true":
                candidates.append("mock")

            for candidate in candidates:
                try:
                    primary = build_bars_provider(candidate)
                    fallbacks: list[Any] = [primary]
                    if str(candidate).lower() != "yfinance":
                        try:
                            fallbacks.append(build_bars_provider("yfinance"))
                        except Exception:
                            pass
                    provider = FallbackBarsProvider(fallbacks) if len(fallbacks) > 1 else primary
                    self.bars_provider_name = candidate
                    return BarsCache(db_path=str(self.store.db_path), provider=provider, tenant_id=BACKFILL_TENANT_ID)
                except Exception:
                    continue
            return None
        try:
            if str(self.bars_provider_name).lower() == "mock":
                if str(os.getenv("ALLOW_MOCK_BARS", "false")).lower() != "true":
                    raise RuntimeError("Mock bars provider requested but ALLOW_MOCK_BARS is false. Replay rejected for economic safety.")

            primary = build_bars_provider(self.bars_provider_name)
            fallbacks2: list[Any] = [primary]
            if str(self.bars_provider_name).lower() != "yfinance":
                try:
                    fallbacks2.append(build_bars_provider("yfinance"))
                except Exception:
                    pass
            provider = FallbackBarsProvider(fallbacks2) if len(fallbacks2) > 1 else primary
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
        force_replay: bool = False,
        skip_completed: bool = True,
        fail_fast: bool = True,
        max_zero_insert_slices: int = 2,
        force_refetch_source: str | None = None,
    ) -> None:
        """
        Fetches historical events for [start_time, end_time) and optionally replays only the unseen subset.
        """
        start_time = to_utc_datetime(start_time).replace(microsecond=0)
        end_time = to_utc_datetime(end_time).replace(microsecond=0)
        if start_time >= end_time:
            raise ValueError("backfill_range requires start_time < end_time")

        # One-time per-run company profile hydration (best-effort).
        # Uses the canonical target stocks universe and writes to data/company_profiles/*.json.
        try:
            tickers = get_target_stocks(asof=start_time)
            await ensure_yfinance_company_profiles(tickers, cache_handle=self._fetch_cache)
        except Exception:
            pass

        repo = Repository(self.store.db_path)
        total_inserted = 0
        zero_insert_streak = 0
        try:
            specs = validate_sources_yaml()
            specs = [s for s in specs if getattr(s, "enabled", True)]
            try:
                specs = sorted(specs, key=lambda s: (int(getattr(s, "priority", 9999)), str(getattr(s, "id", ""))))
            except Exception:
                pass

            # Fetch slices.
            current_start = start_time
            while current_start < end_time:
                current_end = min(current_start + timedelta(days=batch_size_days), end_time)
                print(f"[ingest] mode=backfill slice={current_start.date()}..{current_end.date()}")
                
                t_slice_start = time.perf_counter()
                slice_start_ts = normalize_timestamp(current_start)
                slice_end_ts = normalize_timestamp(current_end)
                tasks = []
                for spec in specs:
                    if not spec.enabled:
                        continue
                    sid = str(spec.id)
                    force = bool(force_refetch_source and str(force_refetch_source) == sid)
                    if skip_completed and not force:
                        spec_hash = _spec_hash_for(self.store, spec)
                        if self.store.is_ingest_window_completed(
                            source_id=sid,
                            start_ts=slice_start_ts,
                            end_ts=slice_end_ts,
                            spec_hash=spec_hash,
                        ):
                            continue
                        if self.store.is_slice_completed(source_id=sid, start_ts=slice_start_ts, end_ts=slice_end_ts):
                            try:
                                self.store.record_ingest_run(
                                    source_id=sid,
                                    start_ts=slice_start_ts,
                                    end_ts=slice_end_ts,
                                    spec_hash=spec_hash,
                                    provider=_provider_for_spec(spec),
                                    ok=True,
                                    fetched_count=0,
                                    emitted_count=0,
                                    last_error="skipped_slice_marker",
                                )
                            except Exception:
                                pass
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
                            try:
                                self.store.record_ingest_run(
                                    source_id=sid,
                                    start_ts=slice_start_ts,
                                    end_ts=slice_end_ts,
                                    spec_hash=spec_hash,
                                    provider=_provider_for_spec(spec),
                                    ok=True,
                                    fetched_count=0,
                                    emitted_count=0,
                                    last_error="skipped_existing_events",
                                )
                            except Exception:
                                pass
                            continue

                        # In-progress lock: if another worker is already fetching this window, skip.
                        try:
                            provider = _provider_for_spec(spec)
                            if not self.store.begin_ingest_window(
                                source_id=sid,
                                start_ts=slice_start_ts,
                                end_ts=slice_end_ts,
                                spec_hash=spec_hash,
                                provider=provider,
                                running_ttl_s=_running_ttl_s_for_provider(provider),
                            ):
                                continue
                        except Exception:
                            # If locking fails, proceed (safer than skipping ingestion entirely).
                            pass
                    else:
                        # Force-refetch path: ignore ingest_runs/slice markers/existing-events checks, but still lock.
                        spec_hash = _spec_hash_for(self.store, spec)
                        try:
                            provider = _provider_for_spec(spec)
                            if not self.store.begin_ingest_window(
                                source_id=sid,
                                start_ts=slice_start_ts,
                                end_ts=slice_end_ts,
                                spec_hash=spec_hash,
                                provider=provider,
                                running_ttl_s=_running_ttl_s_for_provider(provider),
                            ):
                                continue
                        except Exception:
                            pass
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
                unique_events, in_run_dups = deduper.process(all_events)

                existing_in_slice = self.store.count_events_in_range(start_ts=slice_start_ts, end_ts=slice_end_ts)
                inserted = self.store.save_batch(unique_events)
                db_skipped = max(0, len(unique_events) - inserted)
                dt_store = time.perf_counter() - t_store_start

                # Coverage metrics: how dense/usable is this slice?
                try:
                    active_sources = {
                        str(r.source_id)
                        for r in results
                        if isinstance(r, SliceFetchResult) and bool(r.ok) and int(len(r.events or [])) > 0
                    }
                except Exception:
                    active_sources = set()
                try:
                    allowed = set(get_target_stocks(asof=current_end))
                except Exception:
                    allowed = set()
                tickers_seen: set[str] = set()
                for e in unique_events:
                    for t in _parse_ticker_candidates(getattr(e, "ticker", None)):
                        if not allowed or t in allowed:
                            tickers_seen.add(t)
                print(
                    f"[coverage] slice={current_start.date()}..{current_end.date()} "
                    f"sources_active={len(active_sources)} unique={len(unique_events)} inserted={inserted} "
                    f"tickers={len(tickers_seen)} existing={existing_in_slice} db_skipped={db_skipped}"
                )

                # Per-source duplicate counts (after dedupe).
                pre_counts: dict[str, int] = {}
                for r in results:
                    if isinstance(r, SliceFetchResult):
                        pre_counts[str(r.source_id)] = pre_counts.get(str(r.source_id), 0) + len(r.events or [])
                post_counts: dict[str, int] = {}
                for e in unique_events:
                    post_counts[str(e.source_id)] = post_counts.get(str(e.source_id), 0) + 1

                # Record per-source slice markers (avoid refetching on reruns).
                for r in results:
                    if isinstance(r, SliceFetchResult):
                        spec_obj = next((s for s in specs if str(getattr(s, "id", "")) == str(r.source_id)), None)
                        spec_hash = _spec_hash_for(self.store, spec_obj) if spec_obj is not None else self.store.stable_spec_hash({"id": str(r.source_id)})
                        dropped_duplicate = max(0, int(pre_counts.get(str(r.source_id), 0)) - int(post_counts.get(str(r.source_id), 0)))
                        warnings: list[str] = []
                        # Provider drift detection: fingerprint changed AND drop rate spikes.
                        try:
                            if r.response_fingerprint:
                                # Compare with most recent completed window for same source/spec_hash.
                                import sqlite3

                                with sqlite3.connect(self.store.db_path) as conn:
                                    conn.row_factory = sqlite3.Row
                                    prev = conn.execute(
                                        """
                                        SELECT s.response_fingerprint, s.normalized_count, s.dropped_empty_text, s.dropped_bad_timestamp,
                                               s.dropped_invalid_shape, s.dropped_out_of_bounds, s.dropped_duplicate
                                        FROM ingest_run_stats s
                                        JOIN ingest_runs r
                                          ON r.source_id = s.source_id AND r.start_ts = s.start_ts AND r.end_ts = s.end_ts AND r.spec_hash = s.spec_hash
                                        WHERE s.source_id = ? AND s.spec_hash = ? AND r.status = 'complete'
                                        ORDER BY s.end_ts DESC
                                        LIMIT 1
                                        """,
                                        (str(r.source_id), str(spec_hash)),
                                    ).fetchone()
                                if prev is not None:
                                    prev_fp = str(prev["response_fingerprint"] or "")
                                    prev_norm = int(prev["normalized_count"] or 0)
                                    prev_drops = (
                                        int(prev["dropped_empty_text"] or 0)
                                        + int(prev["dropped_bad_timestamp"] or 0)
                                        + int(prev["dropped_invalid_shape"] or 0)
                                        + int(prev["dropped_out_of_bounds"] or 0)
                                        + int(prev["dropped_duplicate"] or 0)
                                    )
                                    cur_norm = int(r.normalized_count or 0)
                                    cur_drops = int(r.dropped_empty_text or 0) + int(r.dropped_bad_timestamp or 0) + int(r.dropped_invalid_shape or 0) + int(r.dropped_out_of_bounds or 0) + int(dropped_duplicate)
                                    prev_rate = (prev_drops / prev_norm) if prev_norm > 0 else 0.0
                                    cur_rate = (cur_drops / cur_norm) if cur_norm > 0 else 0.0
                                    if prev_fp and prev_fp != str(r.response_fingerprint) and cur_rate >= 0.75 and (cur_rate - prev_rate) >= 0.25:
                                        warnings.append("provider_schema_changed")
                        except Exception:
                            pass
                        try:
                            self.store.record_ingest_run_stats(
                                source_id=str(r.source_id),
                                start_ts=slice_start_ts,
                                end_ts=slice_end_ts,
                                spec_hash=spec_hash,
                                request_hash=r.request_hash,
                                request_cache_hit=bool(r.request_cache_hit),
                                response_fingerprint=r.response_fingerprint,
                                fetch_time_s=r.fetch_time_s,
                                total_time_s=r.total_time_s,
                                raw_rows_count=int(r.raw_rows_count or 0),
                                normalized_count=int(r.normalized_count or 0),
                                valid_count=int(r.valid_count or 0),
                                bounded_count=len(r.events or []),
                                dropped_empty_text=int(r.dropped_empty_text or 0),
                                dropped_bad_timestamp=int(r.dropped_bad_timestamp or 0),
                                dropped_invalid_shape=int(r.dropped_invalid_shape or 0),
                                dropped_out_of_bounds=int(r.dropped_out_of_bounds or 0),
                                dropped_duplicate=int(dropped_duplicate),
                                warnings=warnings,
                            )
                        except Exception:
                            pass
                        try:
                            emitted = int(post_counts.get(str(r.source_id), 0))
                            status_override = None
                            ok_override, status_override, err_override = _derive_ingest_status(
                                raw_rows_count=int(r.raw_rows_count or 0),
                                emitted_count=int(emitted),
                                ok=bool(r.ok),
                                error=r.error,
                            )
                            # Data freshness markers from bounded event timestamps.
                            oldest_ts = None
                            newest_ts = None
                            if r.events:
                                try:
                                    times = [str(e.timestamp) for e in r.events if getattr(e, "timestamp", None)]
                                    if times:
                                        oldest_ts = min(times)
                                        newest_ts = max(times)
                                except Exception:
                                    oldest_ts = None
                                    newest_ts = None
                            self.store.record_ingest_run(
                                source_id=str(r.source_id),
                                start_ts=slice_start_ts,
                                end_ts=slice_end_ts,
                                spec_hash=spec_hash,
                                provider=str(r.provider or (_provider_for_spec(spec_obj) if spec_obj is not None else "")),
                                ok=bool(ok_override),
                                fetched_count=int(r.raw_rows_count or 0),
                                emitted_count=int(emitted),
                                oldest_event_ts=oldest_ts,
                                newest_event_ts=newest_ts,
                                status_override=status_override,
                                last_error=(err_override if status_override is None else "zero_emission_nonempty_response"),
                            )
                        except Exception:
                            pass
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

            # Horizon marker: this source set has been backfilled through end_time for the current spec_hashes.
            try:
                end_ts = normalize_timestamp(end_time)
                for spec in specs:
                    if not getattr(spec, "enabled", True):
                        continue
                    sid = str(getattr(spec, "id", "") or "")
                    if not sid:
                        continue
                    spec_hash = _spec_hash_for(self.store, spec)
                    self.store.set_backfilled_until(source_id=sid, spec_hash=spec_hash, backfilled_until_ts=end_ts)
            except Exception:
                pass

            if replay:
                t_replay_start = time.perf_counter()
                if force_replay:
                    replay_summary = await self.replay_range(start_time=start_time, end_time=end_time, repo=repo)
                else:
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

                # Hard quality gates to prevent "fake learning" runs.
                # These gates intentionally fail fast when the backfill is too sparse to train on.
                if fail_fast:
                    start_ts = normalize_timestamp(start_time)
                    end_ts = normalize_timestamp(end_time)
                    days = max(1.0, (to_utc_datetime(end_time) - to_utc_datetime(start_time)).total_seconds() / 86400.0)

                    total_events = self.store.count_events_in_half_open_range(start_ts=start_ts, end_ts=end_ts)
                    events_per_day = float(total_events) / float(days)
                    sources_active = self.store.count_active_sources_in_half_open_range(start_ts=start_ts, end_ts=end_ts)

                    tradeable = int(replay_summary.tradeable_seen or 0)
                    no_ticker = int(replay_summary.no_ticker_tradeable or 0)
                    no_ticker_pct = (float(no_ticker) / float(max(1, tradeable))) * 100.0

                    try:
                        row = repo.conn.execute(
                            """
                            SELECT COUNT(1) AS n
                            FROM predictions
                            WHERE tenant_id = ?
                              AND timestamp >= ?
                              AND timestamp < ?
                            """,
                            (BACKFILL_TENANT_ID, _isoz(to_utc_datetime(start_time)), _isoz(to_utc_datetime(end_time))),
                        ).fetchone()
                        if not row:
                            predictions_n = 0
                        else:
                            try:
                                predictions_n = int(row["n"])  # sqlite3.Row
                            except Exception:
                                predictions_n = int(row[0] or 0)  # tuple
                    except Exception:
                        predictions_n = 0

                    failures: list[str] = []
                    # Lowered thresholds for dump-first data (limited historical data)
                    if events_per_day < 15.0:
                        failures.append(f"events/day={events_per_day:.1f} (< 15)")
                    if sources_active < 1:
                        failures.append(f"sources_active={sources_active} (< 1)")
                    if no_ticker_pct > 30.0:
                        failures.append(f"no_ticker={no_ticker_pct:.1f}% (> 30%) (tradeable_seen={tradeable})")
                    if predictions_n < 5:
                        failures.append(f"predictions={predictions_n} (< 5)")

                    if failures:
                        raise RuntimeError(
                            "Backfill quality gates failed: " + "; ".join(failures) + f" (range={start_ts}..{end_ts})"
                        )
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

        # Cursor-based resumability: if a previous run set a replay cursor within the requested range,
        # resume from that point even if replayed_min/max markers are stale or from a different window.
        cursor_ts = repo.get_kv("backfill_replay_cursor_ts", tenant_id=BACKFILL_TENANT_ID)
        cursor_id = repo.get_kv("backfill_replay_cursor_id", tenant_id=BACKFILL_TENANT_ID)

        # First replay: do the whole requested range.
        if not rmin_ts or not rmax_ts:
            return await self.replay_range(start_time=start_time, end_time=end_time, repo=repo)

        # Overlap-safe: replay only the unseen tails on either side.
        rmin = to_utc_datetime(rmin_ts)
        rmax = to_utc_datetime(rmax_ts)

        # If the stored replayed bounds claim we're already beyond this window, but the cursor suggests
        # we haven't advanced through the requested range yet, prefer the cursor.
        try:
            if cursor_ts:
                cdt = to_utc_datetime(cursor_ts)
                if start_time <= cdt < end_time and rmax >= end_time:
                    return await self.replay_range(
                        start_time=max(start_time, cdt),
                        end_time=end_time,
                        repo=repo,
                        start_exclusive=True,
                        cursor_id=cursor_id,
                    )
        except Exception:
            pass

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

        # Stable run_id for this replay window (reruns should upsert, not duplicate rows).
        run_id = hashlib.sha1(
            f"{BACKFILL_TENANT_ID}|{normalize_timestamp(start_time)}|{normalize_timestamp(end_time)}".encode("utf-8")
        ).hexdigest()[:16]

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
        
        processed_min: RawEvent | None = None
        processed_max: RawEvent | None = None
        observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        total_replayed = 0
        total_deferred = 0
        deferred_reasons: Counter[str] = Counter()
        tradeable_seen = 0
        no_ticker_tradeable = 0

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
            macro_snapshot: dict[str, float] = self._macro_snapshot_for_slice(asof=slice_end)
            try:
                allowed = set(get_target_stocks(asof=slice_end))
            except Exception:
                allowed = set()
            company_name_map: dict[str, str] | None = None
            try:
                cache_key = ("company_name_map", tuple(sorted(allowed)))
                cached = self._fetch_cache.get(cache_key)
                if isinstance(cached, dict):
                    company_name_map = cached  # type: ignore[assignment]
                else:
                    company_name_map = _load_company_name_map_for_allowed(allowed)
                    self._fetch_cache[cache_key] = company_name_map
            except Exception:
                company_name_map = None

            skipped_nontradeable = 0
            for e in bounded:
                evt_ts = to_utc_datetime(e.timestamp)
                tickers: list[str] = []
                evt_type = _event_primary_type(getattr(e, "tags", None))
                is_tradeable = bool(evt_type in {"news", "social", "market", "volatility_event", "positioning"})
                if is_tradeable:
                    tradeable_seen += 1

                # Normalize any pre-extracted ticker field first (handles list-ish strings).
                if e.ticker:
                    candidates = _parse_ticker_candidates(str(e.ticker))
                    tickers = [t for t in candidates if (not allowed or t in allowed)]

                # Text-based inference fallback (WSB style "$NVDA", "Apple (AAPL)", etc.).
                if not tickers:
                    tickers = _infer_tickers_from_text(
                        e.text,
                        allowed=allowed,
                        company_name_map=company_name_map,
                    )

                # If still untickered, drop clearly non-tradeable source types to avoid drowning replay.
                if not tickers:
                    if is_tradeable:
                        no_ticker_tradeable += 1
                    if evt_type and evt_type not in {"news", "social", "market", "volatility_event", "positioning"}:
                        skipped_nontradeable += 1
                        continue

                raw_events.append(
                    RawEvent(
                        id=e.id or "unknown",
                        timestamp=evt_ts,
                        source=e.source_id,
                        text=e.text or "",
                        tickers=tickers,
                        tenant_id=BACKFILL_TENANT_ID,
                        metadata=dict(e.numeric_features or {}),
                    )
                )

            if not raw_events:
                slice_start = slice_end
                continue
            if skipped_nontradeable:
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] Skipped {skipped_nontradeable} non-tradeable events (no ticker)."
                )

            bars_cache = self._bars_cache()
            price_contexts: dict[str, dict] = {}
            if bars_cache is None:
                provider_reason = "no_bars_provider"
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
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] No historical bars provider available; recorded {len(rows)} missing contexts. "
                    "Set HISTORICAL_BARS_PROVIDER=alpaca|polygon|yfinance and required API keys "
                    "(or set ALLOW_MOCK_BARS=true and HISTORICAL_BARS_PROVIDER=mock for local runs)."
                )
                slice_start = slice_end
                continue

            try:
                t_bars_start = time.perf_counter()
                # Lookahead must cover the largest horizon (30d) so outcomes can be resolved without lookahead bias.
                window = bar_window_for_events(
                    event_times=[re.timestamp for re in raw_events],
                    lookback=timedelta(days=5),
                    lookahead=timedelta(days=35),
                )

                # Ensure coverage.
                #
                # Default behavior: only fetch bars for tickers actually referenced by events in this slice.
                # The legacy behavior ("whole Target Stocks universe") is very expensive and can easily
                # trip provider rate limits during backfills; keep it behind an env flag.
                tickers = sorted({t for re in raw_events for t in (re.tickers or []) if t})
                ensure_universe = str(os.getenv("BACKFILL_ENSURE_TARGET_UNIVERSE_BARS", "false")).lower() == "true"
                if ensure_universe and reg is not None:
                    targets = get_target_stocks(asof=slice_end)
                else:
                    targets = tickers
                # Ensure benchmark bars exist so cross-asset strategies can compute relative returns.
                try:
                    targets = sorted(set(targets).union(set(default_benchmark_tickers())))
                except Exception:
                    pass

                # Important: for historical replays, policy windows should be relative to the slice time,
                # not wall-clock "now", otherwise old data only fetches 1d bars and contexts will be missing.
                #
                # Also: never request bars beyond wall-clock now (providers may reject future ranges).
                bars_end = min(window.end, now_ref)
                if window.start >= bars_end:
                    raise RuntimeError("bars_window_empty")

                bars_cache.ensure_policy(tickers=targets, start=window.start, end=bars_end, now=slice_end)

                # Fetch only the tickers we need to build contexts for.
                tickers_fetch = sorted(set(tickers).union(set(default_benchmark_tickers()))) if tickers else list(default_benchmark_tickers())
                bars_by_tf = {
                    "1m": bars_cache.fetch_bars_df(timeframe="1m", tickers=tickers_fetch, start=window.start, end=bars_end),
                    "1h": bars_cache.fetch_bars_df(timeframe="1h", tickers=tickers_fetch, start=window.start, end=bars_end),
                    "1d": bars_cache.fetch_bars_df(timeframe="1d", tickers=tickers_fetch, start=window.start, end=bars_end),
                }
                dt_bars = time.perf_counter() - t_bars_start
                
                price_contexts = build_price_contexts_from_bars_multi(
                    raw_events=raw_events,
                    bars_by_timeframe=bars_by_tf,
                    horizons_minutes=DEFAULT_HORIZONS_MINUTES,
                    benchmark_tickers=default_benchmark_tickers(),
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
            processable: list[RawEvent] = []
            for evt in raw_events:
                if not evt.tickers:
                    # Context-only events are useful for enriching other contexts; don't run them through the engine.
                    continue
                if evt.id in price_contexts and price_contexts.get(evt.id):
                    processable.append(evt)
            
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
                slice_start = slice_end
                continue

            # Enrich all tickers' price contexts with global macro snapshot for this slice.
            if macro_snapshot:
                for evt in processable:
                    try:
                        ctx = price_contexts.get(evt.id)
                        if isinstance(ctx, dict):
                            ctx.setdefault("macro", {}).update(macro_snapshot)
                    except Exception:
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
            
            # Replay: write raw/scored/mra + predictions/outcomes + read models (signals/consensus).
            t_pipeline_start = time.perf_counter()
            self.replay_engine.replay_batch(
                raw_events=processable,
                price_contexts=price_contexts,
                tenant_id=BACKFILL_TENANT_ID,
                run_id=run_id,
            )
            dt_pipeline = time.perf_counter() - t_pipeline_start
            
            total_replayed += len(processable)
            total_deferred += (len(raw_events) - len(processable))
            
            # Persist replay cursor + generation marker for resumability.
            last_ts = normalize_timestamp(processable[-1].timestamp)
            last_id = str(processable[-1].id or "")
            repo.set_kv("backfill_replay_cursor_ts", last_ts, tenant_id=BACKFILL_TENANT_ID)
            repo.set_kv("backfill_replay_cursor_id", last_id, tenant_id=BACKFILL_TENANT_ID)
            
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

        return ReplaySummary(
            replayed=total_replayed,
            deferred=total_deferred,
            deferred_reasons=deferred_reasons,
            tradeable_seen=tradeable_seen,
            no_ticker_tradeable=no_ticker_tradeable,
        )

if __name__ == "__main__":
    runner = BackfillRunner()
    asyncio.run(runner.run_backfill(days=730))  # 2 years
