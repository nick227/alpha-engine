from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db.repository import AlphaRepository


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_promotion_outcomes_stats_pipeline(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = AlphaRepository(db_path=db_path)
    try:
        tenant = "default"
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        # Bars for 40 days.
        rows = []
        for i in range(40):
            ts = _isoz(start + timedelta(days=i))
            # CCC trends up a bit
            close = 4.0 + (0.05 * i)
            vol = 50_000
            rows.append((tenant, "CCC", "1d", ts, close, close, close, close, vol))

        repo.conn.executemany(
            """
            INSERT OR REPLACE INTO price_bars
              (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        # Fundamentals for sector grouping.
        repo.upsert_fundamentals_snapshot(
            {
                "ticker": "CCC",
                "as_of_date": "2026-01-01",
                "revenue_ttm": 200_000_000.0,
                "revenue_growth": 0.10,
                "shares_outstanding": 100_000_000.0,
                "shares_growth": 0.01,
                "sector": "Technology",
                "industry": "Software",
            },
            tenant_id=tenant,
        )

        # Seed discovery_candidates on 3 days to satisfy days_seen >= 3 and overlap >= 2.
        for d in ("2026-01-05", "2026-01-06", "2026-01-07"):
            repo.upsert_discovery_candidates(
                as_of_date=d,
                candidates=[
                    {
                        "symbol": "CCC",
                        "strategy_type": "realness_repricer",
                        "score": 0.95,
                        "reason": "test",
                        "metadata_json": "{}",
                    },
                    {
                        "symbol": "CCC",
                        "strategy_type": "narrative_lag",
                        "score": 0.92,
                        "reason": "test",
                        "metadata_json": "{}",
                    },
                ],
                tenant_id=tenant,
            )

        # Promote watchlist
        from app.discovery.promotion import select_high_conviction, watchlist_to_queue_rows, watchlist_to_repo_rows

        wl = select_high_conviction(
            db_path=db_path,
            tenant_id=tenant,
            as_of_date="2026-01-07",
            window_days=5,
            min_overlap=2,
            min_days_seen=3,
            min_avg_score=0.85,
            top_k=20,
        )
        assert [r.symbol for r in wl] == ["CCC"]
        repo.upsert_discovery_watchlist("2026-01-07", watchlist_to_repo_rows(wl), tenant_id=tenant)
        repo.upsert_prediction_queue("2026-01-07", watchlist_to_queue_rows(wl), tenant_id=tenant)

        # Outcomes (watchlist + candidates)
        from app.discovery.outcomes import compute_candidate_outcomes, compute_watchlist_outcomes, outcomes_to_repo_rows

        watch_out = compute_watchlist_outcomes(db_path=db_path, tenant_id=tenant, watchlist_date="2026-01-07", horizons=[5])
        repo.upsert_discovery_outcomes("2026-01-07", outcomes_to_repo_rows(watch_out), tenant_id=tenant)
        cand_out = compute_candidate_outcomes(db_path=db_path, tenant_id=tenant, as_of_date="2026-01-07", horizons=[5])
        repo.upsert_discovery_candidate_outcomes("2026-01-07", cand_out, tenant_id=tenant)

        # Stats
        from app.discovery.stats import compute_discovery_stats, stats_to_repo_rows

        stats = compute_discovery_stats(db_path=db_path, tenant_id=tenant, end_date="2026-01-10", window_days=10, horizon_days=5)
        assert any(r.group_type == "cohort" for r in stats)
        repo.insert_discovery_stats(stats_to_repo_rows(stats), tenant_id=tenant)
    finally:
        repo.close()

