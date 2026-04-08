from __future__ import annotations
import sqlite3
import json
import datetime
from pathlib import Path
from app.ingest.event_model import Event

class EventStore:
    def __init__(self, db_path: str | Path = "data/alpha.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

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
