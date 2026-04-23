"""Ensure daily price downloader targets the same universe as rankings/recommendations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("pandas")


def test_download_symbol_list_covers_active_universe(tmp_path: Path) -> None:
    from app.core.active_universe import get_active_universe_tickers
    from app.db.repository import AlphaRepository

    db = tmp_path / "universe.db"
    AlphaRepository(db_path=str(db)).close()

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        expected = set(get_active_universe_tickers(tenant_id="default", sqlite_conn=conn))
        from dev_scripts.scripts.download_prices_daily import _get_symbols

        merged = set(_get_symbols(conn, tenant_id="default"))
    finally:
        conn.close()

    assert expected <= merged
