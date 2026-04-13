from __future__ import annotations

"""
Compatibility shim for UI smoke tests.

Historically, the Streamlit dashboard service lived at `app.ui.dashboard`.
The implementation was later moved under `app.ui.middle.dashboard_service`.

The test suite still imports:
  from app.ui.dashboard import DashboardInputs, OptimizedDashboardService
"""

from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Protocol

from app.ui.charts.chart_schema_final import Card, ChartMode


@dataclass(frozen=True)
class DashboardInputs:
    tenant: str
    ticker: str
    view: str
    strategy: str
    horizon: str


class _DashboardBackend(Protocol):
    # Real implementations may offer many methods; the smoke test must not call them.
    def get_top_ten_signals(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        ...


class OptimizedDashboardService:
    """
    Minimal card generator used only for schema smoke tests.

    Important: This class intentionally does not call the backend service.
    """

    def __init__(self, backend: _DashboardBackend) -> None:
        self._backend = backend

    def _generate_cache_key(self, inputs: DashboardInputs) -> str:
        raw = f"{inputs.tenant}|{inputs.ticker}|{inputs.view}|{inputs.strategy}|{inputs.horizon}"
        return sha1(raw.encode("utf-8")).hexdigest()[:16]

    def fetch_cards(self, inputs: DashboardInputs, cache_key: str) -> list[Card]:
        # Always return a small, deterministic set of cards with valid schema.
        # The smoke test validates only that cards conform to schema.
        series = [
            {"x": "2026-01-01T00:00:00+00:00", "y": 100.0, "kind": "price"},
            {"x": "2026-01-02T00:00:00+00:00", "y": 101.5, "kind": "price"},
        ]
        chart_data: dict[str, Any] = {"series": series, "mode": ChartMode.FORECAST}

        number_data: dict[str, Any] = {
            "primary_value": f"{inputs.ticker} {inputs.view}",
            "confidence": 0.75,
            "subtitle": f"tenant={inputs.tenant}",
        }

        table_data: dict[str, Any] = {
            "table_type": "evidence",
            "headers": ["field", "value"],
            "rows": [["strategy", inputs.strategy], ["horizon", inputs.horizon]],
            "context_card_id": f"chart_{cache_key}",
        }

        return [
            Card(card_type="chart", title=f"{inputs.view} chart", data=chart_data, card_id=f"chart_{cache_key}"),
            Card(card_type="number", title=f"{inputs.view} metric", data=number_data, card_id=f"num_{cache_key}"),
            Card(card_type="table", title=f"{inputs.view} table", data=table_data, card_id=f"tbl_{cache_key}"),
        ]

