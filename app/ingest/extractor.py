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

    def normalize_yfinance_columns(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Convert yfinance MultiIndex columns like 'Close_SPY' to normalized structure:
        {
            'symbol': 'SPY',
            'close': 523.11,
            'open': 522.50,
            'high': 524.00,
            'low': 521.75,
            'volume': 1234567
        }
        """
        # Preserve all original fields; this function should be non-destructive.
        normalized = dict(raw or {})
        
        # Copy basic fields
        for key in ['Datetime', 'Date', 'timestamp', 'symbol']:
            if key in raw:
                # Normalize timestamp field name
                if key in ['Datetime', 'Date']:
                    normalized['timestamp'] = raw[key]
                else:
                    normalized[key] = raw[key]
        
        # Normalize OHLCV fields
        field_mappings = {
            'Open': 'open',
            'High': 'high', 
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adj_close',
            'Volume': 'volume'
        }
        
        for yf_field, normal_field in field_mappings.items():
            # Look for fields like 'Close_SPY', 'Open_QQQ', etc.
            matching_keys = [k for k in raw.keys() if k.startswith(f"{yf_field}_")]
            if matching_keys:
                # Use the first match (should be only one per symbol)
                key = matching_keys[0]
                normalized[normal_field] = raw[key]
            elif yf_field in raw:  # Fallback for non-MultiIndex data
                normalized[normal_field] = raw[yf_field]
        
        return normalized
    
    def normalize_many(self, raw_rows: list[dict[str, Any]], spec: SourceSpec) -> list[Event]:
        events = []
        for raw in raw_rows:
            # Normalize yfinance columns first
            normalized_raw = self.normalize_yfinance_columns(raw)
            events.append(self.normalize(normalized_raw, spec))
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
