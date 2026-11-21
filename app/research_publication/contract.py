"""research-publication/v1 wire contract: canonical bytes, fingerprints, Ed25519 request auth.

Requests from a research worker are signed with Ed25519 over
    METHOD \\n PATH?QUERY \\n UNIX_TIMESTAMP \\n sha256(body)
Railway holds only public keys (ALPHA_PUBLISHER_KEYS = {"key-id": "<base64 raw 32 bytes>"}),
so a leaked Railway environment cannot forge research. A valid signature says which
worker sent the request — never that its claims are true; those are re-derived.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from typing import Any

from app.internal_read_v1.research_trace import _canonical

PUBLICATION_VERSION = "research-publication/v1"
SYNC_VERSION = "bar-sync/v1"
MAX_CLOCK_SKEW_SECONDS = 300
HEADER_KEY_ID = "x-research-key-id"
HEADER_TIMESTAMP = "x-research-timestamp"
HEADER_SIGNATURE = "x-research-signature"


def canonical(value: Any) -> bytes:
    return _canonical(value)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def bars_fingerprint(bars: list[list[Any]]) -> str:
    """bars: [[YYYY-MM-DD, open, high, low, close, volume], ...] in date order."""
    return sha256_hex(canonical(bars))


def publication_id(payload: dict[str, Any]) -> str:
    """Content address of the publication body (everything the worker claims)."""
    return "pub_" + sha256_hex(canonical(payload))


def signing_message(method: str, path_qs: str, timestamp: str, body: bytes) -> bytes:
    return "\n".join([method.upper(), path_qs, timestamp, sha256_hex(body)]).encode("utf-8")


def _public_keys() -> dict[str, bytes]:
    raw = os.environ.get("ALPHA_PUBLISHER_KEYS", "").strip()
    if not raw:
        return {}
    try:
        return {str(k): base64.b64decode(v) for k, v in json.loads(raw).items()}
    except (ValueError, TypeError):
        return {}


def verify_request(method: str, path_qs: str, headers: dict[str, str], body: bytes, *, now: float | None = None) -> tuple[bool, str, str | None]:
    """Returns (ok, reason, key_id). Fails closed on any missing or malformed part."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    key_id = headers.get(HEADER_KEY_ID) or ""
    ts = headers.get(HEADER_TIMESTAMP) or ""
    sig_b64 = headers.get(HEADER_SIGNATURE) or ""
    if not key_id or not ts or not sig_b64:
        return False, "missing_signature_headers", None
    keys = _public_keys()
    if key_id not in keys:
        return False, "unknown_publisher_key", key_id
    try:
        skew = abs((time.time() if now is None else now) - int(ts))
    except ValueError:
        return False, "invalid_timestamp", key_id
    if skew > MAX_CLOCK_SKEW_SECONDS:
        return False, "stale_or_future_timestamp", key_id
    try:
        Ed25519PublicKey.from_public_bytes(keys[key_id]).verify(
            base64.b64decode(sig_b64), signing_message(method, path_qs, ts, body)
        )
    except (InvalidSignature, ValueError, TypeError):
        return False, "invalid_signature", key_id
    return True, "ok", key_id


def sign_request(private_key: Any, key_id: str, method: str, path_qs: str, body: bytes, *, now: float | None = None) -> dict[str, str]:
    ts = str(int(time.time() if now is None else now))
    sig = private_key.sign(signing_message(method, path_qs, ts, body))
    return {HEADER_KEY_ID: key_id, HEADER_TIMESTAMP: ts, HEADER_SIGNATURE: base64.b64encode(sig).decode("ascii")}
