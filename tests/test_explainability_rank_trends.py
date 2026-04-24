from __future__ import annotations

import sqlite3
from uuid import uuid4

from app.ui.middle.explainability_rank_trends import build_ranking_movers


def test_build_ranking_movers_skips_null_scores_in_legacy_snapshot_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE ranking_snapshots (
            id TEXT PRIMARY KEY,
            tenant_id TEXT,
            ticker TEXT,
            score REAL,
            conviction REAL,
            attribution_json TEXT,
            regime TEXT,
            timestamp TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO ranking_snapshots
          (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(uuid4()),
                "default",
                "AAA",
                0.80,
                0.70,
                "{}",
                "NORMAL",
                "2026-04-24T00:00:00+00:00",
            ),
            (
                str(uuid4()),
                "default",
                "AAA",
                None,
                0.65,
                "{}",
                "NORMAL",
                "2026-04-23T00:00:00+00:00",
            ),
            (
                str(uuid4()),
                "default",
                "BBB",
                0.75,
                0.60,
                "{}",
                "NORMAL",
                "2026-04-23T00:00:00+00:00",
            ),
        ],
    )

    payload = build_ranking_movers(conn, tenant_id="default", top_n=20)

    assert payload["snapshot_ts_latest"] == "2026-04-24T00:00:00+00:00"
    assert payload["snapshot_ts_previous"] == "2026-04-23T00:00:00+00:00"
    assert isinstance(payload["all_deltas"], list)
