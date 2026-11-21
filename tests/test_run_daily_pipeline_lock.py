"""Legacy Linux pipeline script must use kernel flock, never stale-file detection."""
from __future__ import annotations

import fcntl
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "linux" / "run_daily_pipeline.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("flock") is None or shutil.which("bash") is None, reason="needs bash and flock"
)


def _stub_python(tmp_path: Path) -> Path:
    """Real python for the preflight (-c); every pipeline step fails fast so nothing real runs."""
    stub = tmp_path / "py"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        f'if [ "$1" = "-c" ]; then exec "{sys.executable}" "$@"; fi\n'
        "exit 1\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    return stub


def _run(tmp_path: Path, **env: str) -> subprocess.CompletedProcess:
    db = tmp_path / "alpha.db"
    db.write_bytes(b"")
    full = {
        **os.environ,
        "ALPHA_DB_PATH": str(db),
        "PYTHON": str(_stub_python(tmp_path)),
        "ALPHA_PIPELINE_ENABLED": "true",
        "ALPHA_PIPELINE_MIN_FREE_MB": "1",
        **env,
    }
    return subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=full, capture_output=True, text=True, timeout=60)


def test_script_has_no_stale_file_detection():
    text = SCRIPT.read_text()
    assert 'if [ -f "$LOCK_FILE" ]' not in text
    assert "rm -f \"$LOCK_FILE\"" not in text
    assert "flock -n" in text


def test_leftover_unheld_lock_file_does_not_block(tmp_path: Path):
    (tmp_path / "pipeline.lock").write_text("2026-04-01 00:00:00Z left behind by a crashed run")
    r = _run(tmp_path)
    assert r.returncode == 1, r.stdout + r.stderr  # got past the lock and failed at step 1 (stub)
    assert "holds the lock" not in r.stdout
    assert (tmp_path / "pipeline.lock").exists()  # never deleted by the script


def test_live_holder_blocks_second_run(tmp_path: Path):
    lock = tmp_path / "pipeline.lock"
    fh = open(lock, "a+")
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        r = _run(tmp_path)
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()
    assert r.returncode == 99, r.stdout + r.stderr
    assert "holds the lock" in r.stdout


def test_disabled_preflight_refuses_to_run(tmp_path: Path):
    r = _run(tmp_path, ALPHA_PIPELINE_ENABLED="false")
    assert r.returncode == 75, r.stdout + r.stderr
    assert not (tmp_path / "pipeline.lock").exists()
