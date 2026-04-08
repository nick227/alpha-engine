from __future__ import annotations
import yaml
from pathlib import Path
from pydantic import ValidationError

from app.ingest.source_spec import SourceSpec
from app.ingest.event_model import Event

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
    if event.source_type == "news" and not event.text:
        return False
    return True

def validate_events(events: list[Event]) -> list[Event]:
    return [e for e in events if validate_event(e)]
