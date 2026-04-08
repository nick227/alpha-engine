"""
Repository facade.

The runtime currently uses the SQLite-backed Repository implementation.
"""

from __future__ import annotations

from app.core.repository_sql_old import Repository

__all__ = ["Repository"]
