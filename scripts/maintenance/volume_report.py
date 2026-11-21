#!/usr/bin/env python3
"""Read-only storage report for the Alpha Engine data volume.

Never deletes or modifies anything. Prints sizes of the DB, WAL and backup files,
volume usage, and the safe order in which space can be reclaimed.

Usage: python scripts/maintenance/volume_report.py [--db /data/alpha.db] [--integrity]
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from pathlib import Path

GIB = 1024**3


def _fmt(n: int) -> str:
    return f"{n / GIB:.2f} GiB"


def build_report(db_path: Path) -> dict:
    data_dir = db_path.resolve().parent
    usage = shutil.disk_usage(data_dir)
    files = []
    for p in sorted(data_dir.iterdir()):
        if p.is_file():
            files.append((p.name, p.stat().st_size))
    sizes = dict(files)
    wal = sizes.get(db_path.name + "-wal", 0)
    backups = {n: s for n, s in files if ".backup" in n or n.endswith((".bak", ".orig"))}
    used_pct = round(100 * usage.used / usage.total, 1) if usage.total else 0.0
    steps = []
    if wal > 256 * 1024**2:
        steps.append("1. WAL checkpoint (no data loss): PRAGMA wal_checkpoint(TRUNCATE) while the pipeline is idle.")
    if backups:
        steps.append(
            "2. Verify the newest backup with --integrity, copy it OFF the volume, then remove older/duplicate "
            "backups from the volume: " + ", ".join(sorted(backups))
        )
    steps.append("3. Only if still above ~85%: grow the Railway volume. Do not delete historical price/outcome tables.")
    return {
        "data_dir": str(data_dir),
        "total": usage.total,
        "used": usage.used,
        "free": usage.free,
        "used_pct": used_pct,
        "files": files,
        "wal": wal,
        "backups": backups,
        "steps": steps,
    }


def integrity(db_path: Path) -> str:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    try:
        return str(conn.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.environ.get("ALPHA_DB_PATH", "data/alpha.db"))
    ap.add_argument("--integrity", action="store_true", help="also run PRAGMA quick_check (slow on large DBs)")
    args = ap.parse_args()
    db = Path(args.db)
    if not db.exists():
        print(f"database not found: {db}", file=sys.stderr)
        return 2
    r = build_report(db)
    print(f"volume: {r['data_dir']}  used {r['used_pct']}%  free {_fmt(r['free'])} of {_fmt(r['total'])}")
    for name, size in r["files"]:
        print(f"  {name:50s} {_fmt(size)}")
    for s in r["steps"]:
        print(s)
    if args.integrity:
        print("quick_check:", integrity(db))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
