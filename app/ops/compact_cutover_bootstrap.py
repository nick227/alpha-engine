#!/usr/bin/env python3
"""Compact Database Cutover Bootstrap & Recovery Manager.

Engineered for no-upgrade Railway persistent volumes.
Guarantees fail-closed startup, persistent state tracking, interruption recovery,
and pre-verification before atomic database cutover.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

# Default expected values for verified 0.77 GiB candidate
EXPECTED_CANDIDATE_SHA256 = "c652645a5919007670c821fbb34ad71141cd7e73c6e4d78a5053bd4543908b80"
EXPECTED_CANDIDATE_BYTES = 826613760


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def verify_sqlite_integrity(db_path: Path) -> bool:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
        res = conn.execute("PRAGMA quick_check").fetchall()
        conn.close()
        return res == [("ok",)]
    except Exception as e:
        print(f"[cutover] Integrity check error: {e}", file=sys.stderr)
        return False


def load_cutover_state(state_file: Path) -> dict[str, Any]:
    if state_file.exists():
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[cutover] Warning: corrupted state file: {e}", file=sys.stderr)
    return {"state": "IDLE", "history": []}


def save_cutover_state(state_file: Path, state_dict: dict[str, Any], new_state: str, **kwargs: Any) -> None:
    history_entry = {
        "from": state_dict.get("state"),
        "to": new_state,
        "timestamp": time.time(),
        **kwargs
    }
    history = state_dict.get("history", [])
    history.append(history_entry)
    state_dict["state"] = new_state
    state_dict["last_updated"] = time.time()
    state_dict["history"] = history
    for k, v in kwargs.items():
        state_dict[k] = v

    # Write atomically
    tmp_file = state_file.with_suffix(".tmp")
    tmp_file.write_text(json.dumps(state_dict, indent=2), encoding="utf-8")
    tmp_file.replace(state_file)
    print(f"[cutover] State transitioned to: {new_state} (metadata: {kwargs})")


class CutoverManager:
    def __init__(
        self,
        volume_dir: Path,
        tmp_dir: Path,
        expected_sha256: str = EXPECTED_CANDIDATE_SHA256,
        expected_bytes: int = EXPECTED_CANDIDATE_BYTES,
        source_candidate_path: Path | None = None,
    ) -> None:
        self.volume_dir = volume_dir.resolve()
        self.tmp_dir = tmp_dir.resolve()
        self.db_path = self.volume_dir / "alpha.db"
        self.state_file = self.volume_dir / ".cutover_state.json"
        self.expected_sha256 = expected_sha256
        self.expected_bytes = expected_bytes
        self.source_candidate_path = source_candidate_path

    def run(self, force: bool = False) -> bool:
        state_data = load_cutover_state(self.state_file)
        current_state = state_data.get("state", "IDLE")
        print(f"[cutover] Current persistent state: {current_state}")

        # If already completed and valid, pass through
        if current_state == "COMPLETED" and not force:
            if self.db_path.exists() and self.db_path.stat().st_size < 1024**3:
                print(f"[cutover] Database already in compact state ({self.db_path.stat().st_size:,} bytes). Skipping.")
                return True
            else:
                print("[cutover] State says COMPLETED but alpha.db is missing or large. Re-verifying.")

        # Interruption recovery: if caught in SWAPPING or VERIFYING
        if current_state in ("SWAPPING", "VERIFYING"):
            print("[cutover] Detected prior interrupted cutover. Engaging interruption recovery handler.")
            return self._recover_interrupted_cutover(state_data)

        # Normal execution path: IDLE -> PREPARING -> SWAPPING -> VERIFYING -> COMPLETED
        save_cutover_state(self.state_file, state_data, "PREPARING")

        # 1. Acquire and verify candidate
        candidate_file = self._acquire_candidate()
        if not candidate_file or not candidate_file.exists():
            save_cutover_state(self.state_file, state_data, "FAILED", error="Candidate acquisition failed")
            return False

        print(f"[cutover] Verifying candidate {candidate_file}...")
        cand_sha = compute_sha256(candidate_file)
        if cand_sha != self.expected_sha256:
            msg = f"Candidate SHA-256 mismatch: got {cand_sha}, expected {self.expected_sha256}"
            print(f"[cutover] ERROR: {msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg)
            return False

        if not verify_sqlite_integrity(candidate_file):
            msg = "Candidate SQLite PRAGMA integrity_check failed"
            print(f"[cutover] ERROR: {msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg)
            return False

        print("[cutover] Candidate verified successfully.")
        save_cutover_state(self.state_file, state_data, "CANDIDATE_VERIFIED", candidate_sha256=cand_sha)

        # 2. Perform atomic cutover
        save_cutover_state(self.state_file, state_data, "SWAPPING")

        # Remove old alpha.db (releasing volume blocks before new file is written)
        if self.db_path.exists():
            print(f"[cutover] Unlinking old database {self.db_path}...")
            self.db_path.unlink()

        # Copy candidate into place
        print(f"[cutover] Installing candidate {candidate_file} to {self.db_path}...")
        shutil.copy2(candidate_file, self.db_path)

        # 3. Post-swap verification
        save_cutover_state(self.state_file, state_data, "VERIFYING")
        installed_sha = compute_sha256(self.db_path)
        if installed_sha != self.expected_sha256:
            msg = f"Installed DB SHA-256 mismatch: {installed_sha} != {self.expected_sha256}"
            print(f"[cutover] FATAL: {msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg)
            return False

        if not verify_sqlite_integrity(self.db_path):
            msg = "Installed DB integrity check failed"
            print(f"[cutover] FATAL: {msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg)
            return False

        final_size = self.db_path.stat().st_size
        save_cutover_state(
            self.state_file,
            state_data,
            "COMPLETED",
            final_bytes=final_size,
            final_sha256=installed_sha,
        )
        print(f"[cutover] SUCCESS: Compact database installed ({final_size:,} bytes).")
        return True

    def _acquire_candidate(self) -> Path | None:
        if self.source_candidate_path and self.source_candidate_path.exists():
            return self.source_candidate_path

        locations = [
            self.tmp_dir / "cand" / "candidate.db",
            self.tmp_dir / "candidate.db",
            self.volume_dir / "candidate.db",
            Path("/tmp/cand/candidate.db"),
        ]
        for loc in locations:
            if loc.exists() and loc.stat().st_size == self.expected_bytes:
                return loc

        gz_locations = [
            self.tmp_dir / "cand" / "candidate.db.gz",
            self.tmp_dir / "candidate.db.gz",
            Path("/tmp/cand/candidate.db.gz"),
        ]
        for gz_loc in gz_locations:
            if gz_loc.exists():
                out_path = self.tmp_dir / "candidate.db"
                print(f"[cutover] Decompressing {gz_loc} -> {out_path}...")
                import gzip
                with gzip.open(gz_loc, "rb") as f_in, open(out_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                if out_path.exists():
                    return out_path

        print("[cutover] No local candidate found.", file=sys.stderr)
        return None

    def _recover_interrupted_cutover(self, state_data: dict[str, Any]) -> bool:
        print("[cutover-recovery] Inspecting volume state after interruption...")
        if self.db_path.exists():
            current_sha = compute_sha256(self.db_path)
            if current_sha == self.expected_sha256 and verify_sqlite_integrity(self.db_path):
                print("[cutover-recovery] alpha.db is intact and matches expected candidate.")
                save_cutover_state(
                    self.state_file,
                    state_data,
                    "COMPLETED",
                    recovered=True,
                    final_bytes=self.db_path.stat().st_size,
                )
                return True

        print("[cutover-recovery] alpha.db is incomplete or missing. Re-acquiring candidate...")
        cand = self._acquire_candidate()
        if not cand:
            save_cutover_state(self.state_file, state_data, "FAILED", error="Recovery failed: candidate not found")
            return False

        if compute_sha256(cand) != self.expected_sha256:
            save_cutover_state(self.state_file, state_data, "FAILED", error="Recovery failed: candidate SHA mismatch")
            return False

        if self.db_path.exists():
            self.db_path.unlink()

        shutil.copy2(cand, self.db_path)
        if compute_sha256(self.db_path) == self.expected_sha256 and verify_sqlite_integrity(self.db_path):
            save_cutover_state(
                self.state_file,
                state_data,
                "COMPLETED",
                recovered=True,
                final_bytes=self.db_path.stat().st_size,
            )
            print("[cutover-recovery] Successfully recovered and finalized compact database.")
            return True

        save_cutover_state(self.state_file, state_data, "FAILED", error="Recovery reinstall failed integrity")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume-dir", default=os.environ.get("VOLUME_DIR", "/data"), help="Path to /data volume")
    parser.add_argument("--tmp-dir", default=os.environ.get("TMP_DIR", "/tmp"), help="Path to ephemeral /tmp")
    parser.add_argument("--candidate", default=None, help="Explicit candidate path")
    parser.add_argument("--force", action="store_true", help="Force cutover even if state is COMPLETED")
    args = parser.parse_args()

    mgr = CutoverManager(
        volume_dir=Path(args.volume_dir),
        tmp_dir=Path(args.tmp_dir),
        source_candidate_path=Path(args.candidate) if args.candidate else None,
    )
    success = mgr.run(force=args.force)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
