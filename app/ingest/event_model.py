from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

class Event(BaseModel):
    source_id: str
    source_type: str
    timestamp: str
    ticker: str | None = None
    text: str | None = None
    numeric_features: dict[str, float | int | str] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    weight: float = 1.0
    raw_payload: dict[str, Any] = Field(default_factory=dict)
