"""Per-ticker bar currency: stopped/unpriced tickers are excluded from rankings and the active universe."""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from starlette.testclient import TestClient

from app.core.active_universe import get_active_universe_tickers, stopped_admitted_tickers
from app.core.bar_freshness import current_tickers, latest_daily_bar_dates

pytest.importorskip("httpx")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE price_bars (tenant_id TEXT, ticker TEXT, timeframe TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, PRIMARY KEY (tenant_id, ticker, timeframe, timestamp))")
    c.execute("CREATE TABLE candidate_queue (tenant_id TEXT, ticker TEXT, status TEXT)")
    return c


def _bar(c, ticker, day, close=10.0, tenant="default", tf="1d"):
    c.execute("INSERT INTO price_bars VALUES (?,?,?,?,?,?,?,?,?)", (tenant, ticker, tf, f"{day}T00:00:00+00:00", close, close, close, close, 1.0))


def test_currency_is_relative_to_the_market_not_the_wall_clock() -> None:
    c = _conn()
    _bar(c, "AAPL", "2026-11-20")
    _bar(c, "OK", "2026-11-16")
    _bar(c, "GONE", "2020-07-02")
    _bar(c, "ZERO", "2026-11-20", close=0.0)  # non-positive close is not a valid bar
    _bar(c, "OTHER", "2026-11-20", tenant="ml_train")
    assert latest_daily_bar_dates(c, tenant_id="default", tickers=["aapl", "ZERO", "MISSING"]) == {"AAPL": date(2026, 11, 20)}
    current, market = current_tickers(c, tenant_id="default", tickers={"OK", "GONE", "ZERO", "OTHER", "MISSING"}, reference={"AAPL"})
    assert market == date(2026, 11, 20)
    assert current == {"OK"}
    # A pipeline outage ages the whole market equally (relative lag unchanged): nothing is singled out.
    c.execute("DELETE FROM price_bars WHERE ticker = 'AAPL'")
    _bar(c, "AAPL", "2026-11-16")
    assert current_tickers(c, tenant_id="default", tickers={"OK", "GONE"}, reference={"AAPL"})[0] == {"OK"}
    assert current_tickers(c, tenant_id="default", tickers={"OK"}, reference={"NONE"}) == (set(), None)


def test_stopped_admitted_tickers_leave_the_current_universe_only() -> None:
    c = _conn()
    _bar(c, "AAPL", "2026-11-20")  # AAPL is in the consumer universe (market reference)
    _bar(c, "LBRDA", "2026-06-01")
    _bar(c, "LIVE", "2026-11-19")
    for t in ("LBRDA", "LIVE", "NEWCO"):
        c.execute("INSERT INTO candidate_queue VALUES ('default', ?, 'admitted')", (t,))
    assert stopped_admitted_tickers(c, tenant_id="default", admitted={"LBRDA", "LIVE", "NEWCO"}, reference={"AAPL"}) == {"LBRDA"}
    uni = get_active_universe_tickers(tenant_id="default", sqlite_conn=c)
    assert "LBRDA" not in uni
    assert {"LIVE", "NEWCO", "AAPL"} <= set(uni)  # never-ingested admissions stay so they get ingested
    # Historical backfills (asof) keep every admission: LBRDA traded back then.
    assert "LBRDA" in get_active_universe_tickers(tenant_id="default", sqlite_conn=c, asof=date(2025, 1, 2))


def _seed(db: Path) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db))
    b = lambda ts, px: SimpleNamespace(timestamp=ts, open=px, high=px, low=px, close=px, volume=1e6)  # noqa: E731
    repo.save_price_bars("TST", "1d", [b("2022-06-01T00:00:00+00:00", 100.0), b("2022-06-02T00:00:00+00:00", 110.0)], tenant_id="default")
    repo.save_price_bars("GONE", "1d", [b("2022-04-01T00:00:00+00:00", 5.0)], tenant_id="default")
    repo.conn.execute(
        "INSERT OR REPLACE INTO candidate_queue (tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count, metadata_json) VALUES (?,?,?,?,?,?,?)",
        ("default", "TST", "admitted", "2022-06-01T00:00:00+00:00", "2022-06-02T00:00:00+00:00", 2, "{}"),
    )
    for t, score in (("TST", 0.55), ("GONE", 0.9), ("NOBARS", 0.8)):
        repo.conn.execute(
            "INSERT INTO ranking_snapshots (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp) VALUES (?,?,?,?,?,?,?,?)",
            (str(uuid4()), "default", t, score, score, "{}", "NORMAL", "2022-06-02T00:00:00+00:00"),
        )
    repo.conn.commit()
    repo.conn.close()


def test_ranking_top_excludes_and_reports_tickers_without_current_bars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "rank.db"
    _seed(db)
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        body = client.get("/ranking/top", params={"limit": 10}).json()
    assert [r["ticker"] for r in body["rankings"]] == ["TST"]
    assert body["excludedNoCurrentPrice"] == ["GONE", "NOBARS"]
