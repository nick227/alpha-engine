from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROFILE_FIELDS = (
    "shortName",
    "longName",
    "website",
    "sector",
    "industry",
    "city",
    "country",
    "marketCap",
    "sharesOutstanding",
    "beta",
    "52WeekChange",
    "grossMargins",
    "operatingMargins",
    "profitMargins",
)


def _safe_filename(ticker: str) -> str:
    t = str(ticker).strip().upper()
    return "".join([c if (c.isalnum() or c in {"-", "_", "."}) else "_" for c in t])


async def ensure_yfinance_company_profiles(
    tickers: Iterable[str],
    *,
    out_dir: str | Path = Path("data") / "company_profiles",
    cache_handle: dict[str, Any] | None = None,
    concurrency: int = 4,
) -> None:
    """
    Best-effort profile fetch for tickers using yfinance.

    - Writes `data/company_profiles/{ticker}.json`
    - If file exists, skips
    - If `cache_handle` is provided, avoids reattempting within the same run
    """
    normalized = []
    for t in tickers:
        s = str(t or "").strip().upper()
        if s:
            normalized.append(s)
    if not normalized:
        return

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    attempted: set[str] | None = None
    if isinstance(cache_handle, dict):
        attempted = cache_handle.get("company_profiles_attempted")
        if not isinstance(attempted, set):
            attempted = set()
            cache_handle["company_profiles_attempted"] = attempted

    def profile_file(ticker: str) -> Path:
        return out_path / f"{_safe_filename(ticker)}.json"

    to_fetch: list[str] = []
    for t in normalized:
        if attempted is not None and t in attempted:
            continue
        if profile_file(t).exists():
            if attempted is not None:
                attempted.add(t)
            continue
        to_fetch.append(t)

    if not to_fetch:
        return

    try:
        import yfinance as yf  # type: ignore
    except Exception as e:
        raise ImportError("yfinance is not installed; cannot fetch company profiles.") from e

    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def fetch_one(ticker: str) -> None:
        async with sem:
            def _do() -> dict[str, Any]:
                info = yf.Ticker(ticker).info
                return info if isinstance(info, dict) else {}

            try:
                info = await asyncio.to_thread(_do)
            except Exception:
                if attempted is not None:
                    attempted.add(ticker)
                return

        if not info:
            if attempted is not None:
                attempted.add(ticker)
            return

        payload: dict[str, Any] = {
            "ticker": ticker,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "provider": "yfinance",
        }
        for k in PROFILE_FIELDS:
            payload[k] = info.get(k)

        try:
            profile_file(ticker).write_text(
                json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            if attempted is not None:
                attempted.add(ticker)
            return

        if attempted is not None:
            attempted.add(ticker)

    await asyncio.gather(*[fetch_one(t) for t in to_fetch])

