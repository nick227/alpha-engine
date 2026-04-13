from __future__ import annotations

"""
Streamlit multipage shim.

Tests import these modules to ensure optional UI pages exist and don't execute
page logic at import time.
"""

from app.ui.intelligence_hub import intelligence_hub_main

__all__ = ["intelligence_hub_main"]

