"""Shared FastAPI dependency: parse `range` + `interval` for chart routes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import HTTPException, Query

from app.internal_read_v1.chart_range_interval import parse_interval_key, parse_range_key


@dataclass(frozen=True, slots=True)
class ChartQueryParams:
    range_key: str
    interval_key: str


def chart_range_interval(
    rng: Annotated[str | None, Query(alias="range")] = None,
    interval: str | None = None,
) -> ChartQueryParams:
    try:
        rk = parse_range_key(rng)
        ik = parse_interval_key(interval, rk)
        return ChartQueryParams(range_key=rk, interval_key=ik)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
