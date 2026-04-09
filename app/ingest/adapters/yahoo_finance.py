from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class YahooFinanceAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        """
        Backfill-only: fetch and persist yfinance company profiles once per ticker.

        This adapter intentionally returns no ingest Events; it exists to populate a local
        profile cache for tickers during historical runs.
        """
        await ctx.rate_limiter.throttle()

        mode = str((ctx.run_metadata or {}).get("mode") or "").strip().lower()
        if mode != "backfill":
            return []

        symbols = spec.symbols if isinstance(spec.symbols, list) else []
        tickers = [str(s).strip().upper() for s in symbols if str(s).strip()]
        if not tickers:
            return []

        cache = ctx.cache_handle if isinstance(ctx.cache_handle, dict) else None
        attempted: set[str] | None = None
        if cache is not None:
            attempted = cache.get("company_profiles_attempted")
            if not isinstance(attempted, set):
                attempted = set()
                cache["company_profiles_attempted"] = attempted

        out_dir = Path("data") / "company_profiles"
        out_dir.mkdir(parents=True, exist_ok=True)

        async def _fetch_info(ticker: str) -> dict[str, Any] | None:
            try:
                import yfinance as yf  # type: ignore
            except Exception as e:
                raise ImportError("yfinance is not installed; cannot fetch company profiles.") from e

            def _do() -> dict[str, Any]:
                info = yf.Ticker(ticker).info
                return info if isinstance(info, dict) else {}

            info = await asyncio.to_thread(_do)
            if not info:
                return None

            # Only keep the fields we explicitly care about (stable contract).
            picked: dict[str, Any] = {
                "shortName": info.get("shortName"),
                "longName": info.get("longName"),
                "website": info.get("website"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "city": info.get("city"),
                "country": info.get("country"),
                "marketCap": info.get("marketCap"),
                "sharesOutstanding": info.get("sharesOutstanding"),
                "beta": info.get("beta"),
                "52WeekChange": info.get("52WeekChange"),
                "grossMargins": info.get("grossMargins"),
                "operatingMargins": info.get("operatingMargins"),
                "profitMargins": info.get("profitMargins"),
            }
            return picked

        def _profile_path(ticker: str) -> Path:
            safe_name = "".join([c if (c.isalnum() or c in {"-", "_", "."}) else "_" for c in ticker])
            return out_dir / f"{safe_name}.json"

        to_fetch: list[str] = []
        for ticker in tickers:
            if attempted is not None and ticker in attempted:
                continue
            if _profile_path(ticker).exists():
                if attempted is not None:
                    attempted.add(ticker)
                continue
            to_fetch.append(ticker)

        sem = asyncio.Semaphore(4)

        async def _fetch_and_write(ticker: str) -> None:
            async with sem:
                try:
                    profile = await _fetch_info(ticker)
                except Exception:
                    if attempted is not None:
                        attempted.add(ticker)
                    return

            if profile is None:
                if attempted is not None:
                    attempted.add(ticker)
                return

            payload = {
                "ticker": ticker,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "provider": "yfinance",
                **profile,
            }
            _profile_path(ticker).write_text(
                json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
                encoding="utf-8",
            )
            if attempted is not None:
                attempted.add(ticker)

        if to_fetch:
            await asyncio.gather(*[_fetch_and_write(t) for t in to_fetch])

        return []
