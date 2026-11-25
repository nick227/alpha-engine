import os
import shutil
import sqlite3
import tempfile
import pytest
from pathlib import Path
from app.ops.compact_cutover_bootstrap import (
    CutoverManager,
    compute_sha256,
    verify_sqlite_integrity,
    load_cutover_state,
    EXPECTED_CANDIDATE_SHA256,
)

CANDIDATE_FIXTURE = Path("/home/administrator/web/alpha-trading-platform/alpha-engine/backups_pre_compact/candidate.db")

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
    
    # 3. Verify installed database
    installed_db = volume_dir / "alpha.db"
    assert compute_sha256(installed_db) == EXPECTED_CANDIDATE_SHA256
    assert verify_sqlite_integrity(installed_db) is True
    
    # 4. Idempotent run
    assert mgr.run() is True

def test_interrupted_cutover_recovery(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=cand_file)
    
    # Simulate cutover interrupted mid-swap (alpha.db removed, state marked SWAPPING)
    state_file = volume_dir / ".cutover_state.json"
    state_file.write_text('{"state": "SWAPPING", "history": []}', encoding="utf-8")
    (volume_dir / "alpha.db").unlink()
    
    # Run recovery
    success = mgr.run()
    assert success is True
    
    # Verify recovered state
    state = load_cutover_state(state_file)
    assert state["state"] == "COMPLETED"
    assert state.get("recovered") is True
    assert (volume_dir / "alpha.db").exists()
    assert compute_sha256(volume_dir / "alpha.db") == EXPECTED_CANDIDATE_SHA256

def test_fail_closed_on_corrupt_candidate(mock_env):
    volume_dir, tmp_dir, cand_file = mock_env
    
    # Corrupt candidate file
    corrupt_cand = tmp_dir / "cand" / "corrupt.db"
    corrupt_cand.write_text("bad data", encoding="utf-8")
    
    mgr = CutoverManager(volume_dir=volume_dir, tmp_dir=tmp_dir, source_candidate_path=corrupt_cand)
    success = mgr.run()
    assert success is False
    
    state = load_cutover_state(volume_dir / ".cutover_state.json")
    assert state["state"] == "FAILED"
    # Legacy DB preserved untouched
    assert (volume_dir / "alpha.db").exists()
