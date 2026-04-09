from __future__ import annotations
import yaml
from pathlib import Path
from pydantic import ValidationError

from app.ingest.source_spec import SourceSpec
from app.ingest.event_model import Event
from app.core.time_utils import to_utc_datetime

def validate_sources_yaml(path: str = "config/sources.yaml") -> list[SourceSpec]:
    """
    Parses and validates the sources configuration file.
    Fails fast if the shape is invalid.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Missing sources configuration at {path}")
        
    with open(config_path, "r", encoding="utf-8") as f:
        rows = yaml.safe_load(f) or []
        
    specs = []
    for row in rows:
        try:
            spec = SourceSpec(**row)
            specs.append(spec)
        except ValidationError as e:
            print(f"Failed to validate source config: {row.get('id', 'UNKNOWN_ID')}")
            raise e
            
    return specs

def validate_event(event: Event) -> bool:
    """
    Top-level logic to validate the business constraints of an event after it's parsed.
    """
    # Event is already validated for basic shape via Pydantic on construction.
    # We can add custom business logic validation here.
    if not event.timestamp:
        return False
    # Treat epoch timestamps as invalid; this usually indicates a missing/bad timestamp mapping.
    try:
        if to_utc_datetime(event.timestamp).year == 1970:
            return False
    except Exception:
        return False
    if event.source_type == "news" and not event.text:
        return False
    return True

def validate_events(events: list[Event]) -> list[Event]:
    return [e for e in events if validate_event(e)]


def validate_event_reason(event: Event) -> str | None:
    """
    Return a drop reason string for invalid events, otherwise None.

    Reasons are intended to be persisted for ingest health metrics.
    """
    if not getattr(event, "timestamp", None):
        return "bad_timestamp"
    try:
        if to_utc_datetime(event.timestamp).year == 1970:
            return "bad_timestamp"
    except Exception:
        return "bad_timestamp"

    if event.source_type == "news":
        if not getattr(event, "text", None) or not str(event.text or "").strip():
            return "empty_text"

    # Pydantic construction already enforces base shape, so this is a narrow fallback.
    return None


def validate_events_with_reasons(events: list[Event]) -> tuple[list[Event], dict[str, int]]:
    """
    Validate events and return (valid_events, dropped_reason_counts).
    """
    counts: dict[str, int] = {"empty_text": 0, "bad_timestamp": 0, "invalid_shape": 0}
    valid: list[Event] = []
    for e in events:
        try:
            reason = validate_event_reason(e)
        except Exception:
            reason = "invalid_shape"
        if reason is None:
            valid.append(e)
        else:
            if reason not in counts:
                counts[reason] = 0
            counts[reason] += 1
    return valid, counts
