from __future__ import annotations
from app.ingest.registry import fetch_all_sources

def main() -> None:
    events = fetch_all_sources("config/sources.yaml")
    print(f"Loaded {len(events)} normalized events")
    for event in events[:10]:
        print({
            "source_id": event.source_id,
            "type": event.source_type,
            "ticker": event.ticker,
            "timestamp": event.timestamp,
            "text": event.text,
            "numeric_features": event.numeric_features,
            "tags": event.tags,
            "weight": event.weight,
        })

if __name__ == "__main__":
    main()
