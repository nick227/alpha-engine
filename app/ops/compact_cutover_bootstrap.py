#!/usr/bin/env python3
"""Compact Database Cutover Bootstrap & Recovery Manager.

Engineered for no-upgrade Railway persistent volumes.
Guarantees fail-closed startup, persistent state tracking, interruption recovery,
authenticated remote candidate retrieval after container replacement,
and cryptographic verification before accepting completed state.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# Default expected values for verified 0.77 GiB candidate
EXPECTED_CANDIDATE_SHA256 = "c652645a5919007670c821fbb34ad71141cd7e73c6e4d78a5053bd4543908b80"
EXPECTED_CANDIDATE_BYTES = 826613760

DEFAULT_CANDIDATE_URL = (
    "https://github.com/nick227/alpha-engine/releases/download/"
    "v0.9.0-compact-candidate/candidate.db.gz"
)

VALID_STATES = {
    "IDLE",
    "PREPARING",
    "CANDIDATE_ACQUIRED",
    "CANDIDATE_VERIFIED",
    "SWAPPING",
    "VERIFYING",
    "COMPLETED",
    "FAILED",
}


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
        print(f"[cutover] Integrity check error on {db_path}: {e}", file=sys.stderr)
        return False


def verify_compact_database(
    db_path: Path,
    expected_bytes: int = EXPECTED_CANDIDATE_BYTES,
    expected_sha256: str = EXPECTED_CANDIDATE_SHA256,
) -> tuple[bool, str]:
    if not db_path.exists():
        return False, f"File {db_path} does not exist"
    actual_bytes = db_path.stat().st_size
    if actual_bytes != expected_bytes:
        return False, f"Size mismatch: actual {actual_bytes:,} bytes != expected {expected_bytes:,} bytes"
    actual_sha = compute_sha256(db_path)
    if actual_sha.lower() != expected_sha256.lower():
        return False, f"SHA-256 mismatch: actual {actual_sha} != expected {expected_sha256}"
    if not verify_sqlite_integrity(db_path):
        return False, "PRAGMA quick_check failed"
    return True, "ok"


def load_cutover_state(state_file: Path) -> dict[str, Any]:
    """Load persistent state.
    
    CRITICAL: If the state file does not exist, returns initial IDLE state.
    If the state file exists but is corrupted, unparseable, or contains an invalid
    state enum, returns state 'CORRUPT_STATE' so caller can fail closed immediately.
    Never silently reset an existing corrupted state file to IDLE.
    """
    if not state_file.exists():
        return {"state": "IDLE", "history": []}

    try:
        content = state_file.read_text(encoding="utf-8")
        if not content.strip():
            return {"state": "CORRUPT_STATE", "error": "Empty state file"}
        data = json.loads(content)
        if not isinstance(data, dict):
            return {"state": "CORRUPT_STATE", "error": "State file is not a JSON object"}
        st = data.get("state")
        if st not in VALID_STATES:
            return {"state": "CORRUPT_STATE", "error": f"Unrecognized state enum: {st}"}
        return data
    except Exception as e:
        return {"state": "CORRUPT_STATE", "error": f"Failed to read/parse state file: {e}"}


def save_cutover_state(
    state_file: Path,
    state_dict: dict[str, Any],
    new_state: str,
    **kwargs: Any,
) -> None:
    history_entry = {
        "from": state_dict.get("state"),
        "to": new_state,
        "timestamp": time.time(),
        **kwargs,
    }
    history = state_dict.get("history", [])
    history.append(history_entry)
    state_dict["state"] = new_state
    state_dict["last_updated"] = time.time()
    state_dict["history"] = history
    for k, v in kwargs.items():
        state_dict[k] = v

    tmp_file = state_file.with_suffix(".tmp")
    tmp_file.write_text(json.dumps(state_dict, indent=2), encoding="utf-8")
    tmp_file.replace(state_file)
    print(f"[cutover] State transitioned to: {new_state} (metadata: {kwargs})")


def check_no_active_writers(db_path: Path, volume_dir: Path) -> tuple[bool, str]:
    """Verify that no active SQLite writers or locks exist before cutover."""
    lock_file = volume_dir / "pipeline.lock"
    if lock_file.exists():
        try:
            lock_data = json.loads(lock_file.read_text(encoding="utf-8"))
            locked_at = lock_data.get("locked_at")
            # If lock file exists and is less than 15 minutes old, report active
            file_age = time.time() - lock_file.stat().st_mtime
            if file_age < 900:
                return False, f"pipeline.lock is active ({locked_at}, age {file_age:.1f}s)"
        except Exception:
            pass

    if db_path.exists():
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=rw", uri=True, timeout=0.1)
            conn.execute("BEGIN EXCLUSIVE;")
            conn.rollback()
            conn.close()
        except sqlite3.OperationalError as e:
            return False, f"Database is locked by another connection: {e}"
        except Exception as e:
            return False, f"Database lock check error: {e}"

    return True, "ok"


def download_authenticated_candidate(
    url: str,
    dest_path: Path,
    expected_bytes: int,
    expected_sha256: str,
    auth_token: str | None = None,
    auth_header: str | None = None,
    timeout: int = 180,
) -> bool:
    """Download candidate from remote authenticated endpoint with streaming verification.
    
    Handles partial download cleanup on error, gzip decompression, exact SHA-256
    verification, and atomic placement.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    staging_file = dest_path.with_name(f".{dest_path.name}.downloading")
    staging_gz = dest_path.with_name(f".{dest_path.name}.downloading.gz")
    staging_file.unlink(missing_ok=True)
    staging_gz.unlink(missing_ok=True)

    print(f"[cutover-download] Connecting to {url}...")
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "AlphaEngine-Cutover-Bootstrap/2.0")
    if auth_header:
        req.add_header("Authorization", auth_header)
    elif auth_token:
        req.add_header("Authorization", f"Bearer {auth_token}")

    try:
        # Download payload to staging_gz
        with urllib.request.urlopen(req, timeout=timeout) as resp, open(staging_gz, "wb") as f_out:
            while chunk := resp.read(1024 * 1024):
                f_out.write(chunk)

        # Check if downloaded file is gzip compressed
        with open(staging_gz, "rb") as f_test:
            magic = f_test.read(2)

        if magic == b"\x1f\x8b":
            print(f"[cutover-download] Decompressing gzip payload ({staging_gz.stat().st_size:,} bytes)...")
            with gzip.open(staging_gz, "rb") as f_in, open(staging_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            staging_gz.unlink(missing_ok=True)
        else:
            staging_gz.replace(staging_file)

        # Cryptographically verify the downloaded file
        valid, msg = verify_compact_database(staging_file, expected_bytes, expected_sha256)
        if not valid:
            print(f"[cutover-download] ERROR: Downloaded file verification failed: {msg}", file=sys.stderr)
            staging_file.unlink(missing_ok=True)
            return False

        staging_file.replace(dest_path)
        print(f"[cutover-download] Successfully downloaded and verified {dest_path} ({dest_path.stat().st_size:,} bytes).")
        return True

    except Exception as e:
        print(f"[cutover-download] ERROR during download: {e}", file=sys.stderr)
        staging_file.unlink(missing_ok=True)
        staging_gz.unlink(missing_ok=True)
        return False


class CutoverManager:
    def __init__(
        self,
        volume_dir: Path,
        tmp_dir: Path,
        expected_sha256: str = EXPECTED_CANDIDATE_SHA256,
        expected_bytes: int = EXPECTED_CANDIDATE_BYTES,
        source_candidate_path: Path | None = None,
        candidate_url: str | None = None,
        candidate_token: str | None = None,
        candidate_auth_header: str | None = None,
    ) -> None:
        self.volume_dir = volume_dir.resolve()
        self.tmp_dir = tmp_dir.resolve()
        self.db_path = self.volume_dir / "alpha.db"
        self.state_file = self.volume_dir / ".cutover_state.json"
        self.staging_path = self.volume_dir / ".alpha.db.cutover_staging"
        self.expected_sha256 = expected_sha256
        self.expected_bytes = expected_bytes
        self.source_candidate_path = source_candidate_path
        self.candidate_url = candidate_url or os.environ.get("ALPHA_CANDIDATE_URL", DEFAULT_CANDIDATE_URL)
        self.candidate_token = candidate_token or os.environ.get("ALPHA_CANDIDATE_TOKEN")
        self.candidate_auth_header = candidate_auth_header or os.environ.get("ALPHA_CANDIDATE_AUTH_HEADER")

    def run(self, force: bool = False) -> bool:
        state_data = load_cutover_state(self.state_file)
        current_state = state_data.get("state", "IDLE")
        print(f"[cutover] Current persistent state: {current_state}")

        # Fail closed immediately if state file is corrupt or unrecognized
        if current_state == "CORRUPT_STATE":
            err = state_data.get("error", "Unknown state corruption")
            print(f"[cutover] FATAL: Corrupt or invalid state file: {err}. Failing closed.", file=sys.stderr)
            return False

        # If already COMPLETED and not forced, strictly verify cryptographic integrity
        if current_state == "COMPLETED" and not force:
            valid, msg = verify_compact_database(self.db_path, self.expected_bytes, self.expected_sha256)
            if valid:
                print(
                    f"[cutover] Database cryptographically verified in COMPLETED state "
                    f"({self.expected_bytes:,} bytes, {self.expected_sha256[:16]}...). Pass-through."
                )
                return True
            else:
                msg_fatal = f"State says COMPLETED but database verification failed: {msg}"
                print(f"[cutover] FATAL: {msg_fatal}. Failing closed.", file=sys.stderr)
                save_cutover_state(self.state_file, state_data, "FAILED", error=msg_fatal)
                return False

        # Interruption recovery: caught mid-swap or verifying
        if current_state in ("SWAPPING", "VERIFYING"):
            print("[cutover] Detected prior interrupted cutover. Engaging interruption recovery handler.")
            return self._recover_interrupted_cutover(state_data)

        # Normal execution path: IDLE -> PREPARING -> SWAPPING -> VERIFYING -> COMPLETED
        # Verify no active writers before starting
        writers_ok, writer_msg = check_no_active_writers(self.db_path, self.volume_dir)
        if not writers_ok:
            print(f"[cutover] FATAL: Cannot start cutover; active writer detected: {writer_msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=f"Active writer: {writer_msg}")
            return False

        save_cutover_state(
            self.state_file,
            state_data,
            "PREPARING",
            candidate_url=self.candidate_url,
            expected_sha256=self.expected_sha256,
            expected_bytes=self.expected_bytes,
        )

        # 1. Acquire and verify candidate
        candidate_file = self._acquire_candidate()
        if not candidate_file or not candidate_file.exists():
            save_cutover_state(self.state_file, state_data, "FAILED", error="Candidate acquisition failed")
            return False

        valid, msg = verify_compact_database(candidate_file, self.expected_bytes, self.expected_sha256)
        if not valid:
            print(f"[cutover] ERROR: Candidate verification failed: {msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=f"Candidate invalid: {msg}")
            return False

        save_cutover_state(self.state_file, state_data, "CANDIDATE_VERIFIED")

        # 2. Swap candidate into place with staging file
        return self._install_candidate_with_staging(candidate_file, state_data)

    def _acquire_candidate(self) -> Path | None:
        """Acquire candidate from explicit path, local cache, or durable remote URL."""
        # 1. Explicit source path
        if self.source_candidate_path and self.source_candidate_path.exists():
            valid, _ = verify_compact_database(self.source_candidate_path, self.expected_bytes, self.expected_sha256)
            if valid:
                return self.source_candidate_path

        # 2. Local uncompressed candidates
        local_candidates = [
            self.tmp_dir / "cand" / "candidate.db",
            self.tmp_dir / "candidate.db",
            self.volume_dir / "candidate.db",
            Path("/tmp/cand/candidate.db"),
        ]
        for loc in local_candidates:
            if loc.exists():
                valid, _ = verify_compact_database(loc, self.expected_bytes, self.expected_sha256)
                if valid:
                    return loc

        # 3. Local compressed candidates (.gz)
        gz_candidates = [
            self.tmp_dir / "cand" / "candidate.db.gz",
            self.tmp_dir / "candidate.db.gz",
            self.volume_dir / "candidate.db.gz",
            Path("/tmp/cand/candidate.db.gz"),
        ]
        for gz_loc in gz_candidates:
            if gz_loc.exists():
                out_path = self.tmp_dir / "cand" / "candidate.db"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                print(f"[cutover] Decompressing {gz_loc} -> {out_path}...")
                with gzip.open(gz_loc, "rb") as f_in, open(out_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                valid, _ = verify_compact_database(out_path, self.expected_bytes, self.expected_sha256)
                if valid:
                    return out_path
                else:
                    out_path.unlink(missing_ok=True)

        # 4. Off-volume durable remote retrieval (essential after container replacement)
        if self.candidate_url:
            print(f"[cutover] Candidate not present in /tmp or /data. Retrieving from {self.candidate_url}...")
            remote_dest = self.tmp_dir / "cand" / "candidate.db"
            success = download_authenticated_candidate(
                url=self.candidate_url,
                dest_path=remote_dest,
                expected_bytes=self.expected_bytes,
                expected_sha256=self.expected_sha256,
                auth_token=self.candidate_token,
                auth_header=self.candidate_auth_header,
            )
            if success and remote_dest.exists():
                return remote_dest

        print("[cutover] FATAL: Candidate could not be acquired locally or remotely.", file=sys.stderr)
        return None

    def _install_candidate_with_staging(
        self,
        candidate_file: Path,
        state_data: dict[str, Any],
        recovered: bool = False,
    ) -> bool:
        """Install candidate atomically using volume-level staging file."""
        save_cutover_state(self.state_file, state_data, "SWAPPING", recovered=recovered)

        # Clean up any partial staging file from prior runs
        self.staging_path.unlink(missing_ok=True)

        # Unlink original database to release ~3.7 GiB disk blocks
        if self.db_path.exists():
            print(f"[cutover] Unlinking {self.db_path} ({self.db_path.stat().st_size:,} bytes)...")
            self.db_path.unlink()

        # Copy candidate to staging file on volume
        print(f"[cutover] Staging candidate to {self.staging_path}...")
        try:
            shutil.copy2(candidate_file, self.staging_path)
        except Exception as e:
            self.staging_path.unlink(missing_ok=True)
            msg = f"Failed to stage candidate: {e}"
            print(f"[cutover] FATAL: {msg}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg)
            return False

        # Verify staging file before finalizing
        valid, msg = verify_compact_database(self.staging_path, self.expected_bytes, self.expected_sha256)
        if not valid:
            self.staging_path.unlink(missing_ok=True)
            msg_err = f"Staging database verification failed: {msg}"
            print(f"[cutover] FATAL: {msg_err}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg_err)
            return False

        # Atomically rename staging file into place
        save_cutover_state(self.state_file, state_data, "VERIFYING")
        self.staging_path.replace(self.db_path)

        # Final verification of target database
        valid, msg = verify_compact_database(self.db_path, self.expected_bytes, self.expected_sha256)
        if not valid:
            msg_err = f"Installed database failed final verification: {msg}"
            print(f"[cutover] FATAL: {msg_err}", file=sys.stderr)
            save_cutover_state(self.state_file, state_data, "FAILED", error=msg_err)
            return False

        final_size = self.db_path.stat().st_size
        save_cutover_state(
            self.state_file,
            state_data,
            "COMPLETED",
            recovered=recovered,
            final_bytes=final_size,
            final_sha256=self.expected_sha256,
        )
        print(f"[cutover] SUCCESS: Compact database installed ({final_size:,} bytes).")
        return True

    def _recover_interrupted_cutover(self, state_data: dict[str, Any]) -> bool:
        """Handle recovery from interrupted cutover across container replacements."""
        print("[cutover-recovery] Inspecting volume state after interruption...")

        # 1. Check if db_path already exists and is complete
        if self.db_path.exists():
            valid, msg = verify_compact_database(self.db_path, self.expected_bytes, self.expected_sha256)
            if valid:
                print("[cutover-recovery] Database on volume is already complete and cryptographically valid.")
                save_cutover_state(
                    self.state_file,
                    state_data,
                    "COMPLETED",
                    recovered=True,
                    final_bytes=self.db_path.stat().st_size,
                    final_sha256=self.expected_sha256,
                )
                return True
            else:
                print(f"[cutover-recovery] Found partial/corrupted alpha.db: {msg}. Cleaning up.")
                self.db_path.unlink()

        # 2. Check if staging file exists and is complete
        if self.staging_path.exists():
            valid, msg = verify_compact_database(self.staging_path, self.expected_bytes, self.expected_sha256)
            if valid:
                print("[cutover-recovery] Found valid staging file. Atomically finalizing...")
                self.staging_path.replace(self.db_path)
                save_cutover_state(
                    self.state_file,
                    state_data,
                    "COMPLETED",
                    recovered=True,
                    final_bytes=self.db_path.stat().st_size,
                    final_sha256=self.expected_sha256,
                )
                return True
            else:
                print(f"[cutover-recovery] Staging file is partial/corrupt: {msg}. Cleaning up.")
                self.staging_path.unlink()

        # 3. Restore candidate via local search or durable remote retrieval
        # If candidate_url was persisted in state_data, restore it
        if "candidate_url" in state_data and not self.candidate_url:
            self.candidate_url = state_data["candidate_url"]

        print("[cutover-recovery] Re-acquiring candidate (including remote retrieval if /tmp wiped)...")
        cand = self._acquire_candidate()
        if not cand:
            save_cutover_state(
                self.state_file,
                state_data,
                "FAILED",
                error="Recovery failed: candidate not found locally or remotely",
            )
            return False

        return self._install_candidate_with_staging(cand, state_data, recovered=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume-dir", default=os.environ.get("VOLUME_DIR", "/data"), help="Path to /data volume")
    parser.add_argument("--tmp-dir", default=os.environ.get("TMP_DIR", "/tmp"), help="Path to ephemeral /tmp")
    parser.add_argument("--candidate", default=None, help="Explicit candidate path")
    parser.add_argument("--candidate-url", default=None, help="Durable candidate download URL")
    parser.add_argument("--candidate-token", default=None, help="Optional auth token for candidate download")
    parser.add_argument("--force", action="store_true", help="Force cutover even if state is COMPLETED")
    args = parser.parse_args()

    mgr = CutoverManager(
        volume_dir=Path(args.volume_dir),
        tmp_dir=Path(args.tmp_dir),
        source_candidate_path=Path(args.candidate) if args.candidate else None,
        candidate_url=args.candidate_url,
        candidate_token=args.candidate_token,
    )
    success = mgr.run(force=args.force)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
