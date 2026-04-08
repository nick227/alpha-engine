from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

class ExtractSpec(BaseModel):
    text: str | None = None
    timestamp: str | None = None
    ticker: str | None = None
    numeric_features: dict[str, str] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)

class SourceSpec(BaseModel):
    id: str
    type: str
    adapter: str
    enabled: bool = True
    poll: str | None = None
    weight: float = 1.0
    backfill_days: int | None = None
    symbols: str | list[str] | None = None
    endpoint: str | None = None
    extract: ExtractSpec | None = None
    options: dict[str, Any] = Field(default_factory=dict)
