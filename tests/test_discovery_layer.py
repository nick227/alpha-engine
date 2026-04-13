from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.db.repository import AlphaRepository
from app.discovery.runner import run_discovery


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@pytest.mark.parametrize("top_n", [1, 3])
def test_discovery_runner_writes_candidates(tmp_path, top_n: int) -> None:
    db_path = tmp_path / "alpha.db"
    repo = AlphaRepository(db_path=db_path)
    try:
        tenant = "default"
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        # Build 260 daily bars for two tickers.
        rows = []
        for i in range(260):
            ts = _isoz(start + timedelta(days=i))
            # AAA: flat-ish
            close_aaa = 10.0 + (0.01 * i)
            vol_aaa = 100_000 + (i % 7) * 1_000
            # BBB: depressed with a recent spike in volume
            close_bbb = 3.0 - (0.005 * i)
            vol_bbb = 10_000 + (50_000 if i == 259 else 0)
            rows.append((tenant, "AAA", "1d", ts, close_aaa, close_aaa, close_aaa, close_aaa, vol_aaa))
            rows.append((tenant, "BBB", "1d", ts, close_bbb, close_bbb, close_bbb, close_bbb, vol_bbb))

        repo.conn.executemany(
            """
            INSERT OR REPLACE INTO price_bars
              (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        # Fundamentals (ensure sector tags exist for narrative lag).
        repo.upsert_fundamentals_snapshot(
            {
                "ticker": "AAA",
                "as_of_date": "2026-04-01",
                "revenue_ttm": 2_000_000_000.0,
                "revenue_growth": 0.10,
                "shares_outstanding": 1_000_000_000.0,
                "shares_growth": 0.02,
                "sector": "Technology",
                "industry": "Software",
            },
            tenant_id=tenant,
        )
        repo.upsert_fundamentals_snapshot(
            {
                "ticker": "BBB",
                "as_of_date": "2026-04-01",
                "revenue_ttm": 500_000_000.0,
                "revenue_growth": 0.05,
                "shares_outstanding": 200_000_000.0,
                "shares_growth": 0.01,
                "sector": "Technology",
                "industry": "Software",
            },
            tenant_id=tenant,
        )

        as_of = "2026-09-17"
        summary = run_discovery(db_path=db_path, tenant_id=tenant, as_of=as_of, top_n=top_n, min_avg_dollar_volume_20d=0)
        assert summary["as_of_date"] == as_of
        assert "strategies" in summary
        assert len(summary["strategies"]) == 5

        # With gated strategies, not every strategy will necessarily emit candidates in a toy dataset.
        nrows = repo.conn.execute("SELECT COUNT(*) as n FROM discovery_candidates").fetchone()["n"]
        assert nrows > 0
        assert nrows <= 5 * min(top_n, 2)

        strat_rows = repo.conn.execute("SELECT DISTINCT strategy_type FROM discovery_candidates").fetchall()
        got = {str(r["strategy_type"]) for r in strat_rows}
        assert got.issubset(
            {
                "realness_repricer",
                "silent_compounder",
                "narrative_lag",
                "balance_sheet_survivor",
                "ownership_vacuum",
            }
        )

        # Idempotent: running again doesn't change rowcount.
        run_discovery(db_path=db_path, tenant_id=tenant, as_of=as_of, top_n=top_n, min_avg_dollar_volume_20d=0)
        nrows2 = repo.conn.execute("SELECT COUNT(*) as n FROM discovery_candidates").fetchone()["n"]
        assert nrows2 == nrows
    finally:
        repo.close()
