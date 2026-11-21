"""volume_report must be strictly read-only and report reclaimable files accurately."""
from __future__ import annotations

import hashlib
import importlib.util
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("volume_report", ROOT / "scripts" / "maintenance" / "volume_report.py")
vr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vr)


def _snapshot(d: Path) -> dict[str, tuple[int, str]]:
    return {p.name: (p.stat().st_size, hashlib.sha256(p.read_bytes()).hexdigest()) for p in d.iterdir() if p.is_file()}


def test_report_is_read_only_and_flags_wal_and_backups(tmp_path: Path, monkeypatch, capsys):
    db = tmp_path / "alpha.db"
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t(x)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    # Keep the writer open with an un-checkpointed WAL, as in a live deployment.
    (tmp_path / "alpha.db.backup_pre_x").write_bytes(b"x" * 1024)
    before = _snapshot(tmp_path)

    monkeypatch.setattr("sys.argv", ["volume_report.py", "--db", str(db), "--integrity"])
    assert vr.main() == 0
    out = capsys.readouterr().out
    assert "quick_check: ok" in out
    assert "alpha.db.backup_pre_x" in out
    assert "WAL" not in out or "checkpoint" in out  # advice only, never executed

    after = _snapshot(tmp_path)
    # Main DB and backup are byte-identical; no file was deleted; WAL was not truncated.
    for name in ("alpha.db", "alpha.db.backup_pre_x"):
        assert before[name] == after[name], name
    assert set(before) <= set(after)
    conn.close()


def test_missing_db_is_an_error_not_a_creation(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["volume_report.py", "--db", str(tmp_path / "nope.db")])
    assert vr.main() == 2
    assert not (tmp_path / "nope.db").exists()
