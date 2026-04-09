from __future__ import annotations
from typing import Any, Literal
from pydantic import BaseModel, Field


class ExtractSpec(BaseModel):
    text: str | None = None
    timestamp: str | None = None
    ticker: str | None = None
    numeric_features: dict[str, str] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class FetchSpec(BaseModel):
    """
    Declarative fetch contract for the shared ingestion pipeline.

    Keep `kind` extremely small to avoid explosion.
    """

    kind: Literal["http_json", "rss", "local_file"]

    # local_file
    file: str | None = None

    # http_json / rss
    method: Literal["GET", "POST"] = "GET"
    url: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_s: int = 30

    # Optional: select a list of rows from a JSON response, e.g. "data.items".
    rows_path: str | None = None


class SourceSpec(BaseModel):
    id: str
    type: str
    adapter: str
    enabled: bool = True
    poll: str | None = None
    weight: float = 1.0
    priority: int
    backfill_days: int | None = None
    symbols: str | list[str] | None = None
    endpoint: str | None = None
    extract: ExtractSpec | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    fetch: FetchSpec | None = None
