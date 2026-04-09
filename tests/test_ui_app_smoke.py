from __future__ import annotations

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("plotly")


def test_ui_app_import_smoke() -> None:
    # Import should not execute page logic or crash due to optional deps.
    import app.ui.app  # noqa: F401

