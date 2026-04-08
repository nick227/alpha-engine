from __future__ import annotations
import asyncio
import time

RATE_LIMITS = {
    "reddit": 30,  # per minute
    "alpaca": 200, # per minute
    "fred": 5,     # per minute
    "yahoo": 100,  # per minute
}

class RateLimiter:
    def __init__(self, provider: str):
        self.provider = provider
        self.limit = RATE_LIMITS.get(provider, 60) # default 60/min
        self.interval = 60.0 / self.limit if self.limit > 0 else 0
        self.last_call = 0.0
        self.lock = asyncio.Lock()

    async def throttle(self):
        if self.interval <= 0:
            return
            
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            self.last_call = time.time()
