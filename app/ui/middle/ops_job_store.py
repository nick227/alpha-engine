from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class OpsJobRow:
    id: str
    started_at: str
    finished_at: str | None
    status: str
    command: str
    args_json: str
    exit_code: int | None
    cwd: str


@dataclass(frozen=True)
class OpsJobEventRow:
    job_id: str
    ts: str
    stream: str
    line: str


class OpsJobStore:
    """
    Small, dedicated job ledger for the Ops/Data Console.

    This intentionally lives outside `alpha.db` to avoid mixing operational state with research data.
    """

    def __init__(self, db_path: str | Path = "data/ops_jobs.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_jobs (
                id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                command TEXT NOT NULL,
                args_json TEXT NOT NULL,
                exit_code INTEGER,
                cwd TEXT NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_job_events (
                job_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                stream TEXT NOT NULL,
                line TEXT NOT NULL
            );
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_jobs_started_at ON ops_jobs(started_at);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_job_events_job ON ops_job_events(job_id);")
        self.conn.commit()

    def create_job(self, *, command: str, args: list[str], cwd: str) -> str:
        job_id = str(uuid.uuid4())
        started_at = _utcnow_iso()
        payload = json.dumps({"args": list(args)}, ensure_ascii=False)
        self.conn.execute(
            """
            INSERT INTO ops_jobs (id, started_at, finished_at, status, command, args_json, exit_code, cwd)
            VALUES (?, ?, NULL, ?, ?, ?, NULL, ?)
            """,
            (job_id, started_at, "running", str(command), payload, str(cwd)),
        )
        self.conn.commit()
        return job_id

    def append_event(self, *, job_id: str, stream: str, line: str) -> None:
        ts = _utcnow_iso()
        self.conn.execute(
            "INSERT INTO ops_job_events (job_id, ts, stream, line) VALUES (?, ?, ?, ?)",
            (str(job_id), ts, str(stream), str(line)),
        )
        self.conn.commit()

    def finish_job(self, *, job_id: str, exit_code: int | None, status: str) -> None:
        finished_at = _utcnow_iso()
        self.conn.execute(
            "UPDATE ops_jobs SET finished_at = ?, status = ?, exit_code = ? WHERE id = ?",
            (finished_at, str(status), (int(exit_code) if exit_code is not None else None), str(job_id)),
        )
        self.conn.commit()

    def list_jobs(self, *, limit: int = 50) -> list[OpsJobRow]:
        rows = self.conn.execute(
            """
            SELECT id, started_at, finished_at, status, command, args_json, exit_code, cwd
            FROM ops_jobs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out: list[OpsJobRow] = []
        for r in rows:
            out.append(
                OpsJobRow(
                    id=str(r["id"]),
                    started_at=str(r["started_at"]),
                    finished_at=(str(r["finished_at"]) if r["finished_at"] is not None else None),
                    status=str(r["status"]),
                    command=str(r["command"]),
                    args_json=str(r["args_json"]),
                    exit_code=(int(r["exit_code"]) if r["exit_code"] is not None else None),
                    cwd=str(r["cwd"]),
                )
            )
        return out

    def get_job(self, *, job_id: str) -> OpsJobRow | None:
        r = self.conn.execute(
            """
            SELECT id, started_at, finished_at, status, command, args_json, exit_code, cwd
            FROM ops_jobs
            WHERE id = ?
            """,
            (str(job_id),),
        ).fetchone()
        if not r:
            return None
        return OpsJobRow(
            id=str(r["id"]),
            started_at=str(r["started_at"]),
            finished_at=(str(r["finished_at"]) if r["finished_at"] is not None else None),
            status=str(r["status"]),
            command=str(r["command"]),
            args_json=str(r["args_json"]),
            exit_code=(int(r["exit_code"]) if r["exit_code"] is not None else None),
            cwd=str(r["cwd"]),
        )

    def list_job_events(self, *, job_id: str, limit: int = 2000) -> list[OpsJobEventRow]:
        rows = self.conn.execute(
            """
            SELECT job_id, ts, stream, line
            FROM ops_job_events
            WHERE job_id = ?
            ORDER BY ts ASC
            LIMIT ?
            """,
            (str(job_id), int(limit)),
        ).fetchall()
        return [
            OpsJobEventRow(
                job_id=str(r["job_id"]),
                ts=str(r["ts"]),
                stream=str(r["stream"]),
                line=str(r["line"]),
            )
            for r in rows
        ]

    @staticmethod
    def parse_args(args_json: str) -> list[str]:
        try:
            payload = json.loads(args_json)
            args = payload.get("args") if isinstance(payload, dict) else None
            if isinstance(args, list):
                return [str(a) for a in args]
        except Exception:
            pass
        return []

