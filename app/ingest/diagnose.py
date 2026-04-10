from __future__ import annotations

import asyncio
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.core.time_utils import to_utc_datetime
from app.ingest.extractor import Extractor
from app.ingest.fetch_context import FetchContext
from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter
from app.ingest.registry import resolve_adapter
from app.ingest.runner_core import provider_for_adapter
from app.ingest.validator import validate_sources_yaml


def _provider_for_adapter(adapter_name: str, *, source_id: str) -> str:
    return provider_for_adapter(adapter_name, source_id=source_id)


def _timestamp_ok(ts: Any) -> bool:
    if ts is None:
        return False
    try:
        return to_utc_datetime(ts).year != 1970
    except Exception:
        return False


@dataclass(frozen=True)
class DiagnoseResult:
    source_id: str
    adapter: str
    rows: int
    timestamp_found: bool
    numeric_features: int
    numeric_feature_keys: list[str]
    error: str | None = None


def _err(kind: str, e: Exception) -> str:
    msg = str(e).strip().replace("\n", " ")
    if len(msg) > 160:
        msg = msg[:157] + "..."
    return f"{kind}:{type(e).__name__}:{msg}" if msg else f"{kind}:{type(e).__name__}"


async def _diagnose_one(
    spec,
    *,
    key_manager: KeyManager,
    extractor: Extractor,
    limiter: RateLimiter,
    cache_handle: dict[str, Any],
    timeout_s: float,
) -> DiagnoseResult:
    adapter_name = str(getattr(spec, "adapter", "") or "")
    source_id = str(getattr(spec, "id", "") or "")

    adapter = resolve_adapter(adapter_name)
    if adapter is None:
        return DiagnoseResult(
            source_id=source_id,
            adapter=adapter_name,
            rows=0,
            timestamp_found=False,
            numeric_features=0,
            numeric_feature_keys=[],
            error="missing_adapter",
        )

    ctx = FetchContext(
        provider=_provider_for_adapter(adapter_name, source_id=source_id),
        key_manager=key_manager,
        rate_limiter=limiter,
        cache_handle=cache_handle,
        # Some adapters expect datetime-like behavior (strftime/replace); keep this as a datetime.
        run_timestamp=datetime.now(timezone.utc),
        run_metadata={"mode": "diagnose"},
    )

    try:
        raw_rows = await asyncio.wait_for(adapter.fetch_raw(spec, ctx), timeout=float(timeout_s))
    except asyncio.TimeoutError as e:
        return DiagnoseResult(
            source_id=source_id,
            adapter=adapter_name,
            rows=0,
            timestamp_found=False,
            numeric_features=0,
            numeric_feature_keys=[],
            error=_err("timeout", e),
        )
    except Exception as e:
        return DiagnoseResult(
            source_id=source_id,
            adapter=adapter_name,
            rows=0,
            timestamp_found=False,
            numeric_features=0,
            numeric_feature_keys=[],
            error=_err("fetch_error", e),
        )

    rows = raw_rows if isinstance(raw_rows, list) else []
    row_count = len(rows)
    if row_count == 0 or not isinstance(rows[0], dict):
        return DiagnoseResult(
            source_id=source_id,
            adapter=adapter_name,
            rows=row_count,
            timestamp_found=False,
            numeric_features=0,
            numeric_feature_keys=[],
            error=None,
        )

    try:
        event = extractor.normalize(rows[0], spec)
    except Exception as e:
        return DiagnoseResult(
            source_id=source_id,
            adapter=adapter_name,
            rows=row_count,
            timestamp_found=False,
            numeric_features=0,
            numeric_feature_keys=[],
            error=_err("normalize_error", e),
        )

    features = getattr(event, "numeric_features", None)
    feature_dict = features if isinstance(features, dict) else {}
    keys = sorted([str(k) for k in feature_dict.keys()])

    return DiagnoseResult(
        source_id=source_id,
        adapter=adapter_name,
        rows=row_count,
        timestamp_found=_timestamp_ok(getattr(event, "timestamp", None)),
        numeric_features=len(feature_dict),
        numeric_feature_keys=keys,
        error=None,
    )


def main() -> int:
    allow_network = str(os.getenv("ALPHA_DIAGNOSE_ALLOW_NETWORK", "")).strip().lower() in {"1", "true", "yes"}
    timeout_s = float(str(os.getenv("ALPHA_DIAGNOSE_TIMEOUT_S", "2.0")).strip() or "2.0")
    warnings.filterwarnings("ignore", category=FutureWarning)
    if not allow_network:
        # Best-effort guardrail: block shared HTTP fetcher usage.
        def _no_network(*_args, **_kwargs):
            raise RuntimeError("Network disabled for diagnose. Set ALPHA_DIAGNOSE_ALLOW_NETWORK=1 to override.")

        import app.ingest.fetchers as fetchers

        fetchers.urlopen = _no_network  # type: ignore[assignment]

    specs = validate_sources_yaml("config/sources.yaml")
    enabled = [s for s in specs if getattr(s, "enabled", True)]

    key_manager = KeyManager()
    extractor = Extractor()
    cache_handle: dict[str, Any] = {}

    limiters: dict[str, RateLimiter] = {}
    for s in enabled:
        provider = _provider_for_adapter(getattr(s, "adapter", ""), source_id=getattr(s, "id", "unknown"))
        if provider not in limiters:
            limiter = RateLimiter(provider)
            # Diagnose is a best-effort offline check; don't spend seconds sleeping on rate limits.
            limiter.interval = 0.0
            limiters[provider] = limiter

    async def _run() -> list[DiagnoseResult]:
        tasks = []
        for s in enabled:
            provider = _provider_for_adapter(getattr(s, "adapter", ""), source_id=getattr(s, "id", "unknown"))
            tasks.append(
                asyncio.create_task(
                    _diagnose_one(
                        s,
                        key_manager=key_manager,
                        extractor=extractor,
                        limiter=limiters[provider],
                        cache_handle=cache_handle,
                        timeout_s=timeout_s,
                    )
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: list[DiagnoseResult] = []
        for r in results:
            if isinstance(r, Exception):
                out.append(
                    DiagnoseResult(
                        source_id="UNKNOWN",
                        adapter="UNKNOWN",
                        rows=0,
                        timestamp_found=False,
                        numeric_features=0,
                        numeric_feature_keys=[],
                        error=f"runner_error:{type(r).__name__}",
                    )
                )
            else:
                out.append(r)
        return out

    results = asyncio.run(_run())

    print("source_id\tadapter\trows\ttimestamp_found\tnumeric_features\tnumeric_feature_keys\terror")
    for r in sorted(results, key=lambda x: (x.error is not None, x.source_id, x.adapter)):
        keys = ",".join(r.numeric_feature_keys)
        print(
            f"{r.source_id}\t{r.adapter}\t{r.rows}\t{int(r.timestamp_found)}\t{r.numeric_features}\t{keys}\t{r.error or ''}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
