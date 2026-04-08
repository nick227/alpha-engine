from __future__ import annotations
from typing import Any
from app.ingest.source_spec import SourceSpec
from app.ingest.event_model import Event
from app.core.time_utils import normalize_timestamp

class Extractor:
    def parse(self, expression: str, payload: dict[str, Any]) -> str | None:
        """
        Evaluate simple extraction expressions like 'headline + summary'
        or direct field lookups like 'created_utc'.
        """
        if not expression:
            return None
            
        parts = [p.strip() for p in expression.split("+")]
        resolved = []
        for p in parts:
            val = payload.get(p)
            if val is not None:
                resolved.append(str(val))
        
        if not resolved:
            return None
        return " - ".join(resolved)  # Joining multiple string fields with a separator

    def normalize_many(self, raw_rows: list[dict[str, Any]], spec: SourceSpec) -> list[Event]:
        events = []
        for raw in raw_rows:
            events.append(self.normalize(raw, spec))
        return events

    def normalize(self, raw: dict[str, Any], spec: SourceSpec) -> Event:
        extract = spec.extract
        
        # Base event fields
        timestamp = self.parse(extract.timestamp, raw) if extract and extract.timestamp else raw.get("timestamp")
        timestamp = normalize_timestamp(timestamp)
            
        ticker = self.parse(extract.ticker, raw) if extract and extract.ticker else raw.get("ticker", raw.get("symbol"))
        text = self.parse(extract.text, raw) if extract and extract.text else raw.get("text")
        
        # Numeric features
        numeric_features = {}
        if extract and extract.numeric_features:
            for k, v in extract.numeric_features.items():
                parsed = self.parse(v, raw)
                if parsed:
                    try:
                        numeric_features[k] = float(parsed)
                    except ValueError:
                        numeric_features[k] = parsed
                        
        tags = []
        if extract and extract.tags:
            tags = extract.tags.copy()
            
        # Optional type injection
        if "type" not in tags:
            tags.append(spec.type)

        return Event(
            source_id=spec.id,
            source_type=spec.type,
            timestamp=timestamp,
            ticker=ticker,
            text=text,
            numeric_features=numeric_features,
            tags=tags,
            weight=spec.weight,
            raw_payload=raw,
        )
