from __future__ import annotations

"""
Streamlit multipage shim.

Tests import these modules to ensure optional UI pages exist and don't execute
page logic at import time.
"""

from app.ui.dashboard_compact import dashboard_compact_main

__all__ = ["dashboard_compact_main"]

