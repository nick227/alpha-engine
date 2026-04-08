from __future__ import annotations

from typing import Any

from app.db.repository import AlphaRepository


def rank_strategies(
    repo: AlphaRepository,
    *,
    tenant_id: str = "default",
    ticker: str | None = None,
    timeframe: str | None = None,
    forecast_days: int | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    return repo.rank_strategies(
        tenant_id=tenant_id,
        ticker=ticker,
        timeframe=timeframe,
        forecast_days=forecast_days,
        limit=limit,
    )

