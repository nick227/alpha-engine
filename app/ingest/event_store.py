from __future__ import annotations
import sqlite3
import json
import datetime
import hashlib
from pathlib import Path
from app.ingest.event_model import Event

class EventStore:
    def __init__(self, db_path: str | Path = "data/alpha.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _table_has_column(self, *, table: str, column: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return any((r[1] if isinstance(r, tuple) else r["name"]) == column for r in rows)

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                ticker TEXT,
                text TEXT,
                tags TEXT,
                weight REAL,
                numeric_json TEXT,
                created_at TEXT NOT NULL
            )
            """)
            # Indexes for faster queries in learning loops
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker)")

            # Backfill slice markers to avoid refetching windows already completed.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_slice_markers (
                    source_id TEXT NOT NULL,
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    fetched_count INTEGER NOT NULL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, start_ts, end_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_backfill_slice_markers_range ON backfill_slice_markers(start_ts, end_ts);"
            )

            # Ingest run ledger (idempotency markers keyed by spec hash).
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest_runs (
                    source_id TEXT NOT NULL,
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    retry_count INTEGER NOT NULL,
                    fetched_count INTEGER NOT NULL,
                    emitted_count INTEGER NOT NULL,
                    empty_count INTEGER NOT NULL,
                    oldest_event_ts TEXT,
                    newest_event_ts TEXT,
                    last_error TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, start_ts, end_ts, spec_hash)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ingest_runs_range ON ingest_runs(start_ts, end_ts);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ingest_runs_source_range ON ingest_runs(source_id, start_ts, end_ts);"
            )

        # Backwards-compatible migrations for preexisting DBs created before these fields existed.
        # (SQLite has limited ALTER support; we add columns if missing.)
        with sqlite3.connect(self.db_path) as conn:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(ingest_runs)").fetchall()]
            if "provider" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN provider TEXT NOT NULL DEFAULT ''")
            if "status" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN status TEXT NOT NULL DEFAULT 'complete'")
            if "retry_count" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
            if "empty_count" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN empty_count INTEGER NOT NULL DEFAULT 0")
            if "oldest_event_ts" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN oldest_event_ts TEXT")
            if "newest_event_ts" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN newest_event_ts TEXT")
            if "started_at" not in cols:
                now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN started_at TEXT NOT NULL DEFAULT ''")
                conn.execute("UPDATE ingest_runs SET started_at = COALESCE(created_at, ?) WHERE started_at = ''", (now,))
            if "completed_at" not in cols:
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN completed_at TEXT")
            if "updated_at" not in cols:
                now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                conn.execute("ALTER TABLE ingest_runs ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
                # Best-effort backfill for existing rows.
                conn.execute("UPDATE ingest_runs SET updated_at = COALESCE(created_at, ?) WHERE updated_at = ''", (now,))

            # Backfill empty_count for old rows where last_error indicates empty.
            try:
                conn.execute("UPDATE ingest_runs SET empty_count = 1 WHERE empty_count = 0 AND last_error = 'empty' AND ok = 1")
            except Exception:
                pass

        # Per-window stats: dropped-row reasons, request cache hits, and response fingerprints.
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest_run_stats (
                    source_id TEXT NOT NULL,
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    request_hash TEXT,
                    request_cache_hit INTEGER NOT NULL,
                    response_fingerprint TEXT,
                    fetch_time_s REAL,
                    total_time_s REAL,
                    raw_rows_count INTEGER NOT NULL,
                    normalized_count INTEGER NOT NULL,
                    valid_count INTEGER NOT NULL,
                    bounded_count INTEGER NOT NULL,
                    dropped_empty_text INTEGER NOT NULL,
                    dropped_bad_timestamp INTEGER NOT NULL,
                    dropped_invalid_shape INTEGER NOT NULL,
                    dropped_out_of_bounds INTEGER NOT NULL,
                    dropped_duplicate INTEGER NOT NULL,
                    warnings_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, start_ts, end_ts, spec_hash)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ingest_run_stats_source_range ON ingest_run_stats(source_id, start_ts, end_ts);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ingest_run_stats_fingerprint ON ingest_run_stats(response_fingerprint);")

            # Backwards-compatible columns for preexisting DBs.
            cols = [r[1] for r in conn.execute("PRAGMA table_info(ingest_run_stats)").fetchall()]
            if "fetch_time_s" not in cols:
                conn.execute("ALTER TABLE ingest_run_stats ADD COLUMN fetch_time_s REAL")
            if "total_time_s" not in cols:
                conn.execute("ALTER TABLE ingest_run_stats ADD COLUMN total_time_s REAL")
            if "warnings_json" not in cols:
                conn.execute("ALTER TABLE ingest_run_stats ADD COLUMN warnings_json TEXT NOT NULL DEFAULT '[]'")

        # Backfill horizon markers (source has reached full range).
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_horizons (
                    source_id TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    backfilled_until_ts TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, spec_hash)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_backfill_horizons_until ON backfill_horizons(backfilled_until_ts);")

    @staticmethod
    def stable_spec_hash(spec_payload: dict) -> str:
        """
        Create a stable hash for a source spec (excluding secrets).

        Callers should pass a JSON-serializable dict with deterministic key ordering.
        """
        raw = json.dumps(spec_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def count_events_in_range(self, *, start_ts: str, end_ts: str) -> int:
        """
        Returns count of events in [start_ts, end_ts].
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM events WHERE timestamp BETWEEN ? AND ?",
                (start_ts, end_ts),
            ).fetchone()
            return int(row[0] or 0)

    def count_events_in_half_open_range(self, *, start_ts: str, end_ts: str) -> int:
        """
        Returns count of events in [start_ts, end_ts).
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM events WHERE timestamp >= ? AND timestamp < ?",
                (str(start_ts), str(end_ts)),
            ).fetchone()
            return int(row[0] or 0)

    def count_active_sources_in_half_open_range(self, *, start_ts: str, end_ts: str) -> int:
        """
        Returns distinct source count in [start_ts, end_ts).
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT source) FROM events WHERE timestamp >= ? AND timestamp < ?",
                (str(start_ts), str(end_ts)),
            ).fetchone()
            return int(row[0] or 0)

    def count_events_for_source_in_range(
        self,
        *,
        source_id: str,
        start_ts: str,
        end_ts: str,
        end_inclusive: bool = False,
    ) -> int:
        """
        Returns count of events for a source within a timestamp window.

        By default this uses a half-open range [start_ts, end_ts) to match typical slice semantics.
        Set `end_inclusive=True` to use [start_ts, end_ts].
        """
        with sqlite3.connect(self.db_path) as conn:
            if end_inclusive:
                row = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE source = ? AND timestamp BETWEEN ? AND ?",
                    (str(source_id), str(start_ts), str(end_ts)),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE source = ? AND timestamp >= ? AND timestamp < ?",
                    (str(source_id), str(start_ts), str(end_ts)),
                ).fetchone()
            return int(row[0] or 0)

    def save_batch(self, events: list[Event]) -> int:
        """
        Saves events to SQLite using INSERT OR IGNORE.
        Returns the number of events actually inserted (db-level uniqueness).
        """
        if not events:
            return 0
            
        rows = []
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for e in events:
            rows.append((
                e.id,
                e.source_id,
                e.timestamp,
                e.ticker,
                e.text,
                json.dumps(e.tags) if e.tags else "[]",
                e.weight,
                json.dumps(e.numeric_features) if e.numeric_features else "{}",
                now
            ))

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            before_changes = conn.total_changes
            cursor.executemany("""
            INSERT OR IGNORE INTO events 
            (id, source, timestamp, ticker, text, tags, weight, numeric_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            # sqlite3's cursor.rowcount is unreliable for executemany() + INSERT OR IGNORE
            # (it can reflect only the last statement). total_changes is stable per-connection.
            return conn.total_changes - before_changes

    def is_slice_completed(self, *, source_id: str, start_ts: str, end_ts: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT ok
                FROM backfill_slice_markers
                WHERE source_id = ? AND start_ts = ? AND end_ts = ?
                LIMIT 1
                """,
                (str(source_id), str(start_ts), str(end_ts)),
            ).fetchone()
            return bool(row and int(row[0] or 0) == 1)

    def is_ingest_window_completed(self, *, source_id: str, start_ts: str, end_ts: str, spec_hash: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT ok, status
                FROM ingest_runs
                WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ?
                LIMIT 1
                """,
                (str(source_id), str(start_ts), str(end_ts), str(spec_hash)),
            ).fetchone()
            if not row:
                return False
            ok = int(row[0] or 0) == 1
            status = str(row[1] or "")
            return bool(ok and status == "complete")

    def get_ingest_window_status(self, *, source_id: str, start_ts: str, end_ts: str, spec_hash: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT status
                FROM ingest_runs
                WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ?
                LIMIT 1
                """,
                (str(source_id), str(start_ts), str(end_ts), str(spec_hash)),
            ).fetchone()
            if not row:
                return None
            return str(row[0] or "") or None

    def begin_ingest_window(
        self,
        *,
        source_id: str,
        start_ts: str,
        end_ts: str,
        spec_hash: str,
        provider: str,
        running_ttl_s: int = 1800,
    ) -> bool:
        """
        Attempt to acquire an in-progress lock for a window.

        Returns True if the caller should perform the fetch, False if another worker
        is running it or it is already complete.

        If a window is stuck in running beyond running_ttl_s, it is treated as stale
        and can be restarted.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()
        ttl_cutoff = (now - datetime.timedelta(seconds=int(running_ttl_s))).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT status, updated_at, ok, retry_count
                FROM ingest_runs
                WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ?
                LIMIT 1
                """,
                (str(source_id), str(start_ts), str(end_ts), str(spec_hash)),
            ).fetchone()

            if row is not None:
                status = str(row["status"] or "")
                updated_at = str(row["updated_at"] or "")
                ok = int(row["ok"] or 0)
                retry_count = int(row["retry_count"] or 0)

                if status == "complete" and ok == 1:
                    return False

                if status == "running":
                    # If not stale, someone else is working it.
                    if updated_at and updated_at > ttl_cutoff:
                        return False
                    # Stale running -> restart.
                    conn.execute(
                        """
                        UPDATE ingest_runs
                        SET provider = ?, status = 'running', ok = 0, fetched_count = 0, emitted_count = 0,
                            empty_count = 0,
                            retry_count = ?, last_error = 'stale_running_restarted',
                            started_at = ?, completed_at = NULL, updated_at = ?
                        WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ?
                        """,
                        (
                            str(provider),
                            int(retry_count + 1),
                            now_iso,
                            now_iso,
                            str(source_id),
                            str(start_ts),
                            str(end_ts),
                            str(spec_hash),
                        ),
                    )
                    return True

                # failed or incomplete -> restart
                conn.execute(
                    """
                    UPDATE ingest_runs
                    SET provider = ?, status = 'running', ok = 0, fetched_count = 0, emitted_count = 0,
                        empty_count = 0,
                        retry_count = ?, last_error = COALESCE(last_error, 'restarted'),
                        started_at = ?, completed_at = NULL, updated_at = ?
                    WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ?
                    """,
                    (
                        str(provider),
                        int(retry_count + 1),
                        now_iso,
                        now_iso,
                        str(source_id),
                        str(start_ts),
                        str(end_ts),
                        str(spec_hash),
                    ),
                )
                return True

            # No row -> create running marker (in-progress lock).
            conn.execute(
                """
                INSERT INTO ingest_runs
                  (source_id, start_ts, end_ts, spec_hash, provider, status, ok, retry_count, fetched_count, emitted_count, empty_count, oldest_event_ts, newest_event_ts, last_error, started_at, completed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'running', 0, 0, 0, 0, 0, NULL, NULL, 'running', ?, NULL, ?, ?)
                """,
                (str(source_id), str(start_ts), str(end_ts), str(spec_hash), str(provider), now_iso, now_iso, now_iso),
            )
            return True

    def record_slice_marker(
        self,
        *,
        source_id: str,
        start_ts: str,
        end_ts: str,
        ok: bool,
        fetched_count: int,
        last_error: str | None = None,
    ) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO backfill_slice_markers
                  (source_id, start_ts, end_ts, ok, fetched_count, last_error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(source_id),
                    str(start_ts),
                    str(end_ts),
                    1 if ok else 0,
                    int(fetched_count),
                    (str(last_error) if last_error else None),
                    now,
                ),
            )

    def record_ingest_run(
        self,
        *,
        source_id: str,
        start_ts: str,
        end_ts: str,
        spec_hash: str,
        provider: str,
        ok: bool,
        fetched_count: int,
        emitted_count: int,
        oldest_event_ts: str | None = None,
        newest_event_ts: str | None = None,
        status_override: str | None = None,
        last_error: str | None = None,
    ) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        status = str(status_override) if status_override else ("complete" if ok else "failed")
        err = (str(last_error) if last_error else None)
        empty_count = 0
        if ok and int(fetched_count) == 0 and int(emitted_count) == 0 and not err:
            err = "empty"
            empty_count = 1
        with sqlite3.connect(self.db_path) as conn:
            # Preserve retry_count/started_at when updating an existing row.
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT retry_count, started_at
                FROM ingest_runs
                WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ?
                LIMIT 1
                """,
                (str(source_id), str(start_ts), str(end_ts), str(spec_hash)),
            ).fetchone()
            retry_count = int(row["retry_count"] or 0) if row is not None else 0
            started_at = str(row["started_at"] or now) if row is not None else now

            conn.execute(
                """
                INSERT OR REPLACE INTO ingest_runs
                  (source_id, start_ts, end_ts, spec_hash, provider, status, ok, retry_count, fetched_count, emitted_count, empty_count, oldest_event_ts, newest_event_ts, last_error, started_at, completed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(source_id),
                    str(start_ts),
                    str(end_ts),
                    str(spec_hash),
                    str(provider),
                    str(status),
                    1 if ok else 0,
                    int(retry_count),
                    int(fetched_count),
                    int(emitted_count),
                    int(empty_count),
                    (str(oldest_event_ts) if oldest_event_ts else None),
                    (str(newest_event_ts) if newest_event_ts else None),
                    err,
                    str(started_at),
                    now,
                    now,
                    now,
                ),
            )

    def record_ingest_run_stats(
        self,
        *,
        source_id: str,
        start_ts: str,
        end_ts: str,
        spec_hash: str,
        request_hash: str | None,
        request_cache_hit: bool,
        response_fingerprint: str | None,
        fetch_time_s: float | None,
        total_time_s: float | None,
        raw_rows_count: int,
        normalized_count: int,
        valid_count: int,
        bounded_count: int,
        dropped_empty_text: int = 0,
        dropped_bad_timestamp: int = 0,
        dropped_invalid_shape: int = 0,
        dropped_out_of_bounds: int = 0,
        dropped_duplicate: int = 0,
        warnings: list[str] | None = None,
    ) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        warnings_json = json.dumps(list(warnings or []), sort_keys=True, ensure_ascii=False)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO ingest_run_stats
                  (source_id, start_ts, end_ts, spec_hash, request_hash, request_cache_hit, response_fingerprint,
                   fetch_time_s, total_time_s,
                   raw_rows_count, normalized_count, valid_count, bounded_count,
                   dropped_empty_text, dropped_bad_timestamp, dropped_invalid_shape, dropped_out_of_bounds, dropped_duplicate,
                   warnings_json,
                   created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(source_id),
                    str(start_ts),
                    str(end_ts),
                    str(spec_hash),
                    (str(request_hash) if request_hash else None),
                    1 if bool(request_cache_hit) else 0,
                    (str(response_fingerprint) if response_fingerprint else None),
                    (float(fetch_time_s) if fetch_time_s is not None else None),
                    (float(total_time_s) if total_time_s is not None else None),
                    int(raw_rows_count),
                    int(normalized_count),
                    int(valid_count),
                    int(bounded_count),
                    int(dropped_empty_text),
                    int(dropped_bad_timestamp),
                    int(dropped_invalid_shape),
                    int(dropped_out_of_bounds),
                    int(dropped_duplicate),
                    warnings_json,
                    now,
                ),
            )

    def set_backfilled_until(self, *, source_id: str, spec_hash: str, backfilled_until_ts: str) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO backfill_horizons (source_id, spec_hash, backfilled_until_ts, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (str(source_id), str(spec_hash), str(backfilled_until_ts), now),
            )

    def get_backfilled_until(self, *, source_id: str, spec_hash: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT backfilled_until_ts FROM backfill_horizons WHERE source_id = ? AND spec_hash = ? LIMIT 1",
                (str(source_id), str(spec_hash)),
            ).fetchone()
            if not row:
                return None
            return str(row[0] or "") or None

    def get_events_chronological(
        self,
        start_ts: str | None = None,
        end_ts: str | None = None,
        *,
        start_inclusive: bool = True,
    ) -> list[Event]:
        """
        Retrieves events sorted by timestamp.
        """
        query = "SELECT id, source, timestamp, ticker, text, tags, weight, numeric_json FROM events"
        params = []
        
        if start_ts and end_ts:
            query += " WHERE timestamp BETWEEN ? AND ?"
            params = [start_ts, end_ts]
        elif start_ts:
            query += " WHERE timestamp >= ?" if start_inclusive else " WHERE timestamp > ?"
            params = [start_ts]
        elif end_ts:
            query += " WHERE timestamp <= ?"
            params = [end_ts]
            
        query += " ORDER BY timestamp ASC, id ASC"
        
        events = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            for row in cursor:
                events.append(Event(
                    id=row["id"],
                    source_id=row["source"],
                    source_type="unknown", # Metadata is lost in current SQLite schema but can be inferred or fixed
                    timestamp=row["timestamp"],
                    ticker=row["ticker"],
                    text=row["text"],
                    tags=json.loads(row["tags"]),
                    weight=row["weight"],
                    numeric_features=json.loads(row["numeric_json"])
                ))
        return events
