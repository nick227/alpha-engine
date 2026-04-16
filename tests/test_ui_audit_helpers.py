from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3

import pandas as pd

from app.ui import audit


def test_hit_color_thresholds() -> None:
    assert audit._hit_color(0.60).endswith("#2E7D32")  # noqa: SLF001
    assert audit._hit_color(0.50).endswith("#F57C00")  # noqa: SLF001
    assert audit._hit_color(0.10).endswith("#C62828")  # noqa: SLF001


def test_parse_ts_accepts_z_and_naive() -> None:
    dt = audit._parse_ts("2026-01-01T00:00:00Z")  # noqa: SLF001
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.isoformat().endswith("+00:00")

    naive = audit._parse_ts("2026-01-01T00:00:00")  # noqa: SLF001
    assert naive is not None
    assert naive.tzinfo is not None


def test_hours_since_returns_float(monkeypatch) -> None:
    fixed_now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(audit, "datetime", type("D", (), {"now": staticmethod(lambda tz=None: fixed_now), "fromisoformat": datetime.fromisoformat}))  # type: ignore[arg-type]

    hrs = audit._hours_since("2026-01-01T00:00:00Z")  # noqa: SLF001
    assert hrs is not None
    assert abs(hrs - 24.0) < 1e-6


def test_days_between_minimum_one() -> None:
    assert audit._days_between(None, None) == 1.0  # noqa: SLF001
    assert audit._days_between("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z") == 1.0  # noqa: SLF001
    assert audit._days_between("2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z") >= 2.0  # noqa: SLF001


def test_arrow_safe_display_df_keeps_numeric_and_fills_strings() -> None:
    df = pd.DataFrame(
        {
            "a": [1, None],
            "b": [None, "x"],
            "c": [None, None],
        }
    )
    out = audit._arrow_safe_display_df(df, numeric_cols=["a"])  # noqa: SLF001
    assert pd.api.types.is_numeric_dtype(out["a"])
    assert out.loc[1, "b"] == "x"
    assert out.loc[0, "b"] == "—"
    assert out.loc[0, "c"] == "—"


def test_db_helpers_table_exists_columns_query_scalar(tmp_path, monkeypatch) -> None:
    db = tmp_path / "alpha.db"
    monkeypatch.setattr(audit, "DB_PATH", db)

    with sqlite3.connect(str(db)) as conn:
        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO foo (name) VALUES ('a'), ('b')")
        conn.commit()

    assert audit._table_exists("foo") is True  # noqa: SLF001
    assert audit._table_exists("missing") is False  # noqa: SLF001
    cols = audit._columns_for("foo")  # noqa: SLF001
    assert {"id", "name"} <= cols

    df = audit._query("SELECT name FROM foo ORDER BY id")  # noqa: SLF001
    assert list(df["name"]) == ["a", "b"]
    assert audit._scalar("SELECT COUNT(1) FROM foo") == 2  # noqa: SLF001
