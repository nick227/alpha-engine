from __future__ import annotations

"""
Streamlit multipage shim.

Tests import these modules to ensure optional UI pages exist and don't execute
page logic at import time.
"""

from app.ui.audit import audit_main

__all__ = ["audit_main"]

