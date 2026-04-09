from __future__ import annotations

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("plotly")

from app.ui.charts.chart_schema_final import ChartData, NumberData, TableData
from app.ui.dashboard import DashboardInputs, OptimizedDashboardService


class _NoopDashboardService:
    def get_top_ten_signals(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("UI smoke test should not call get_top_ten_signals")


def test_dashboard_ui_cards_conform_to_schema() -> None:
    svc = OptimizedDashboardService(_NoopDashboardService())

    views = [
        "best_picks",
        "dips",
        "bundles",
        "compare",
        "backtest_analysis",
        "mixed_test",
    ]

    for view in views:
        inputs = DashboardInputs(
            tenant="default",
            ticker="AAPL",
            view=view,
            strategy="text_mra",
            horizon="ALL",
        )
        cache_key = svc._generate_cache_key(inputs)
        cards = svc.fetch_cards(inputs, cache_key)

        for card in cards:
            if card.card_type == "chart":
                ChartData(**card.data)
            elif card.card_type == "number":
                NumberData(**card.data)
            elif card.card_type == "table":
                TableData(**card.data)
            else:  # pragma: no cover
                raise AssertionError(f"Unexpected card type: {card.card_type}")
