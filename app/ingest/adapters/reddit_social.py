from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.fetch_context import FetchContext

class RedditSocialAdapter:
    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()
        _keys = ctx.key_manager.get("reddit")
        subreddit = spec.options.get("subreddit", "wallstreetbets")
        
        return [
            {
                "created_utc": ctx.run_timestamp,
                "detected": "NVDA",
                "title": f"Sample Reddit sentiment event from r/{subreddit}",
                "body": "It's going to the moon!",
                "subreddit": subreddit,
                "provider": "reddit",
            }
        ]

