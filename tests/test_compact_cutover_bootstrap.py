import gzip
import http.server
import json
import os
import shutil
import socketserver
import sqlite3
import tempfile
import threading
from pathlib import Path
import pytest

from app.ops.compact_cutover_bootstrap import (
    CutoverManager,
    compute_sha256,
    verify_sqlite_integrity,
    verify_compact_database,
    load_cutover_state,
    EXPECTED_CANDIDATE_SHA256,
    EXPECTED_CANDIDATE_BYTES,
)

CANDIDATE_FIXTURE = Path("/home/administrator/web/alpha-trading-platform/alpha-engine/backups_pre_compact/candidate.db")
CANDIDATE_GZ_FIXTURE = Path("/home/administrator/web/alpha-trading-platform/alpha-engine/backups_pre_compact/candidate.db.gz")


@pytest.fixture
def mock_env():
    with tempfile.TemporaryDirectory() as base_tmp:
        base = Path(base_tmp)
        volume_dir = base / "data"
        tmp_dir = base / "tmp"
        volume_dir.mkdir()
        tmp_dir.mkdir()

        # Create an initial mock legacy database
        legacy_db = volume_dir / "alpha.db"
        conn = sqlite3.connect(legacy_db)
        conn.execute("CREATE TABLE dummy (id INT PRIMARY KEY, name TEXT);")
        conn.execute("INSERT INTO dummy VALUES (1, 'legacy');")
        conn.commit()
        conn.close()

        # Place the verified candidate in tmp_dir
        cand_dir = tmp_dir / "cand"
        cand_dir.mkdir()
        cand_file = cand_dir / "candidate.db"
        shutil.copy2(CANDIDATE_FIXTURE, cand_file)

        yield volume_dir, tmp_dir, cand_file


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


class MockCandidateHandler(http.server.BaseHTTPRequestHandler):
    auth_token = "secret-cutover-token"
    serve_corrupt = False

    def do_GET(self):
        auth_header = self.headers.get("Authorization", "")
        if auth_header != f"Bearer {self.auth_token}":
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"Unauthorized")
            return

        if self.serve_corrupt:
            self.send_response(200)
            self.send_header("Content-Type", "application/gzip")
            self.end_headers()
            self.wfile.write(b"\x1f\x8b\x08corrupt-gzip-payload")
            return

        gz_size = CANDIDATE_GZ_FIXTURE.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", "application/gzip")
        self.send_header("Content-Length", str(gz_size))
        self.end_headers()

        with open(CANDIDATE_GZ_FIXTURE, "rb") as f_in:
            while chunk := f_in.read(1024 * 1024):
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break

    def log_message(self, format, *args):
        pass  # Quiet logging


def start_mock_candidate_server(serve_corrupt=False):
    MockCandidateHandler.serve_corrupt = serve_corrupt
    server = ThreadedTCPServer(("127.0.0.1", 0), MockCandidateHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}/candidate.db.gz"


def test_normal_cutover_lifecycle(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)

    # 1. Run cutover
    success = mgr.run()
    assert success is True

    # 2. Verify state
    state = load_cutover_state(volume_dir / ".cutover_state.json")
    assert state["state"] == "COMPLETED"
    assert state["final_sha256"] == EXPECTED_CANDIDATE_SHA256
    assert state["final_bytes"] == EXPECTED_CANDIDATE_BYTES

    # 3. Verify installed database
    installed_db = volume_dir / "alpha.db"
    assert compute_sha256(installed_db) == EXPECTED_CANDIDATE_SHA256
    assert verify_sqlite_integrity(installed_db) is True

    # 4. Idempotent re-run verifies database cryptographically and passes
    assert mgr.run() is True


def test_completed_state_strict_cryptographic_verification(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)

    # Successfully complete cutover
    assert mgr.run() is True

    # Now tamper with alpha.db by modifying 1 byte
    db_file = volume_dir / "alpha.db"
    data = bytearray(db_file.read_bytes())
    data[100] = (data[100] + 1) % 256
    db_file.write_bytes(data)

    # Re-running with tampered DB must FAIL CLOSED even though state says COMPLETED
    assert mgr.run() is False
    state = load_cutover_state(volume_dir / ".cutover_state.json")
    assert state["state"] == "FAILED"


def test_fail_closed_on_corrupt_state_file(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    state_file = volume_dir / ".cutover_state.json"
    state_file.write_text("{this is corrupted json", encoding="utf-8")

    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)
    success = mgr.run()
    assert success is False

    # Original database must not be touched
    assert (volume_dir / "alpha.db").exists()


def test_fail_closed_on_unrecognized_state_enum(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    state_file = volume_dir / ".cutover_state.json"
    state_file.write_text('{"state": "INVALID_STATE_XYZ", "history": []}', encoding="utf-8")

    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)
    success = mgr.run()
    assert success is False
    assert (volume_dir / "alpha.db").exists()


def test_fail_closed_on_active_writer(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    db_file = volume_dir / "alpha.db"

    # Open exclusive lock connection simulating active writer
    conn = sqlite3.connect(db_file)
    conn.execute("BEGIN EXCLUSIVE;")

    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)
    success = mgr.run()
    assert success is False

    conn.rollback()
    conn.close()


def test_interrupted_cutover_recovery_with_wiped_tmp(mock_env):
    """Simulates container restart after interruption: /tmp is completely wiped,
    candidate is retrieved via authenticated durable remote URL, verified, and installed.
    """
    volume_dir, tmp_dir, cand_file = mock_env

    # Start mock authenticated remote candidate server
    server, remote_url = start_mock_candidate_server()
    try:
        # Simulate interruption mid-swap: alpha.db unlinked, state is SWAPPING
        state_file = volume_dir / ".cutover_state.json"
        state_file.write_text(
            json.dumps({"state": "SWAPPING", "candidate_url": remote_url, "history": []}),
            encoding="utf-8",
        )
        (volume_dir / "alpha.db").unlink()

        # CRITICAL SIMULATION: WIPE /tmp completely to simulate container replacement
        shutil.rmtree(tmp_dir)
        tmp_dir.mkdir()

        # Ensure no candidate exists locally in tmp or data
        assert not (tmp_dir / "cand" / "candidate.db").exists()
        assert not (volume_dir / "candidate.db").exists()

        # Create manager pointing to empty tmp_dir and configured with candidate_url + auth_token
        mgr = CutoverManager(
            volume_dir=volume_dir,
            tmp_dir=tmp_dir,
            candidate_url=remote_url,
            candidate_token="secret-cutover-token",
        )

        # Run recovery
        success = mgr.run()
        assert success is True

        # Verify database successfully installed and verified
        installed_db = volume_dir / "alpha.db"
        assert installed_db.exists()
        assert compute_sha256(installed_db) == EXPECTED_CANDIDATE_SHA256
        assert verify_sqlite_integrity(installed_db) is True

        # Verify persistent state marked COMPLETED with recovered flag
        state = load_cutover_state(state_file)
        assert state["state"] == "COMPLETED"
        assert state.get("recovered") is True

    finally:
        server.shutdown()
        server.server_close()


def test_partial_staging_file_cleanup_and_recovery(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env

    # Simulate interruption leaving a partial staging file
    staging_file = volume_dir / ".alpha.db.cutover_staging"
    staging_file.write_bytes(b"partial-corrupted-staging-file-bytes")

    state_file = volume_dir / ".cutover_state.json"
    state_file.write_text('{"state": "SWAPPING", "history": []}', encoding="utf-8")
    (volume_dir / "alpha.db").unlink()

    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)
    success = mgr.run()
    assert success is True

    # Partial staging file must have been cleaned up and replaced
    assert not staging_file.exists()
    assert (volume_dir / "alpha.db").exists()
    assert compute_sha256(volume_dir / "alpha.db") == EXPECTED_CANDIDATE_SHA256


def test_fail_closed_on_corrupt_remote_candidate(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env

    server, remote_url = start_mock_candidate_server(serve_corrupt=True)
    try:
        shutil.rmtree(tmp_dir)
        tmp_dir.mkdir()

        mgr = CutoverManager(
            volume_dir=volume_dir,
            tmp_dir=tmp_dir,
            candidate_url=remote_url,
            candidate_token="secret-cutover-token",
        )

        success = mgr.run()
        assert success is False

        state = load_cutover_state(volume_dir / ".cutover_state.json")
        assert state["state"] == "FAILED"
        assert not (tmp_dir / "cand" / "candidate.db").exists()

    finally:
        server.shutdown()
        server.server_close()
