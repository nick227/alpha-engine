from __future__ import annotations
from dataclasses import dataclass
import datetime
from typing import Dict

@dataclass
class SourceMetrics:
    last_fetch_time: str | None = None
    event_count: int = 0
    errors: int = 0
    latency_ms: float = 0.0
    dropped_events: int = 0

class MetricsRegistry:
    def __init__(self):
        self._metrics: Dict[str, SourceMetrics] = {}

    def _get(self, source_id: str) -> SourceMetrics:
        if source_id not in self._metrics:
            self._metrics[source_id] = SourceMetrics()
        return self._metrics[source_id]

    def record_fetch_success(self, source_id: str, new_events: int, dropped: int, latency_ms: float):
        m = self._get(source_id)
        m.last_fetch_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        m.event_count += new_events
        m.dropped_events += dropped
        # simple moving average for latency
        m.latency_ms = (m.latency_ms + latency_ms) / 2.0 if m.latency_ms > 0 else latency_ms

    def record_error(self, source_id: str):
        m = self._get(source_id)
        m.errors += 1

    def get_snapshot(self) -> Dict[str, dict]:
        return {
            k: {
                "last_fetch_time": v.last_fetch_time,
                "event_count": v.event_count,
                "errors": v.errors,
                "latency_ms": round(v.latency_ms, 2),
                "dropped_events": v.dropped_events,
            }
            for k, v in self._metrics.items()
        }

metrics_registry = MetricsRegistry()
