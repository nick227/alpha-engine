from __future__ import annotations
import re
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext
from app.ingest import adapter_helpers

_TICKER_TOKEN_RE = re.compile(r"(?:(?<=\$)|\b)([A-Z]{1,5})(?=\b)")
_FALSE_POSITIVES = {
    "A",
    "AI",
    "AND",
    "ARE",
    "ATH",
    "BUY",
    "CALL",
    "CEO",
    "CPI",
    "DD",
    "EDIT",
    "EOD",
    "EPS",
    "ETF",
    "FED",
    "FOMO",
    "GDP",
    "HOLD",
    "IMO",
    "IPO",
    "LOL",
    "LMAO",
    "MOON",
    "NASDAQ",
    "NYSE",
    "OF",
    "ON",
    "OTM",
    "PUT",
    "PT",
    "SEC",
    "SELL",
    "THE",
    "TO",
    "USA",
    "WSB",
    "YOLO",
    "YTD",
}


def _detect_tickers(text: str) -> list[str]:
    if not text:
        return []
    toks = [m.group(1) for m in _TICKER_TOKEN_RE.finditer(text.upper())]
    out: list[str] = []
    seen: set[str] = set()
    for t in toks:
        tt = str(t).strip().upper()
        if not tt or tt in _FALSE_POSITIVES:
            continue
        if tt in seen:
            continue
        seen.add(tt)
        out.append(tt)
    return out

class RedditSocialAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        subreddit = spec.options.get("subreddit", "wallstreetbets")
        start_dt = ctx.start_date or ctx.run_timestamp
        end_dt = ctx.end_date or start_dt
        after_ts = int(start_dt.timestamp())
        before_ts = int(end_dt.timestamp())
        
        reddit_fetch = {
            "kind": "http_json",
            "url": "https://api.pushshift.io/reddit/search/submission",
            "params": {
                "subreddit": subreddit,
                "size": 100,
                "sort": "desc",
                "sort_type": "created_utc",
                "after": after_ts,
                "before": before_ts,
                "fields": "title,selftext,created_utc,author,score,num_comments",
            },
            "timeout_s": 10,
        }
        
        reddit_data = await adapter_helpers.fetch_json(reddit_fetch, ctx)
        rows = reddit_data or []
        if not isinstance(rows, list):
            return []

        enriched: list[dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            title = str(r.get("title") or "")
            body = str(r.get("selftext") or "")
            # Align payload keys to sources.yaml ("title + body").
            r["body"] = body
            tickers = _detect_tickers(f"{title}\n{body}")
            if tickers:
                r["detected"] = ",".join(tickers[:8])
            enriched.append(r)

        return enriched
