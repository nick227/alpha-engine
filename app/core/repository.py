from __future__ import annotations

"""
Repository facade.

The runtime currently uses the SQLite-backed Repository implementation.
"""

from app.core.repository_sql_old import Repository

__all__ = ["Repository"]

