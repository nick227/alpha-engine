from __future__ import annotations
import hashlib
from app.ingest.event_model import Event

def generate_hash_id(source_id: str, timestamp: str, text: str | None) -> str:
    """Generate deterministic hash for event deduplication."""
    content = f"{source_id}|{timestamp}|{text or ''}"
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

class Deduper:
    def __init__(self):
        # In-memory transient check for the current run.
        # Long term deduplication happens at the event_store level via INSERT IGNORE.
        self._seen_in_run: set[str] = set()
        
    def process(self, events: list[Event]) -> tuple[list[Event], int]:
        """
        Assigns IDs and returns (unique_events, dropped_count).
        """
        unique = []
        dropped = 0
        for e in events:
            if not e.id:
                e.id = generate_hash_id(e.source_id, e.timestamp, e.text)
                
            if e.id in self._seen_in_run:
                dropped += 1
            else:
                self._seen_in_run.add(e.id)
                unique.append(e)
                
        return unique, dropped
