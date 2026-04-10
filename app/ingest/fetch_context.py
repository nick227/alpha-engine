from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter

@dataclass
class FetchContext:
    provider: str
    key_manager: KeyManager
    rate_limiter: RateLimiter
    cache_handle: Any | None = None
    run_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    start_date: datetime | None = None
    end_date: datetime | None = None
    run_metadata: dict[str, Any] = field(default_factory=dict)
