from __future__ import annotations

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("plotly")


def test_ui_shell_imports_smoke() -> None:
    import app.ui.shell.filter_state  # noqa: F401
    import app.ui.shell.nav  # noqa: F401
    import app.ui.shell.top_bar  # noqa: F401
    import app.ui.pages.audit_page  # noqa: F401
    import app.ui.pages.dashboard_page  # noqa: F401
    import app.ui.pages.intelligence_hub_page  # noqa: F401
    import app.ui.predictions_views  # noqa: F401
    import app.ui.backtest_strategy_analysis  # noqa: F401
