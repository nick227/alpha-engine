from __future__ import annotations
import asyncio

from app.ingest.async_runner import fetch_all_sources_async

def main() -> None:
    routed = asyncio.run(fetch_all_sources_async("config/sources.yaml"))
    total = sum(len(v) for v in routed.values())
    print(f"Loaded {total} normalized events across {len(routed)} routes")
    for route, events in routed.items():
        for event in events[:3]:
            print({"route": route, "source_id": event.source_id, "ticker": event.ticker, "timestamp": event.timestamp, "text": event.text})

if __name__ == "__main__":
    main()
