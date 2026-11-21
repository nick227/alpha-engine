"""Research worker CLI (localhost now; an Oracle Cloud worker later — same contracts).

    python -m app.research_publication.worker keygen
    python -m app.research_publication.worker sync      # Railway → local, full-window reconciliation
    python -m app.research_publication.worker run       # strategy on synced bars → outbox
    python -m app.research_publication.worker publish   # signed POST of the latest outbox entry
    python -m app.research_publication.worker all

Environment: ALPHA_RESEARCH_URL (Railway Alpha Engine), ALPHA_RESEARCH_KEY_ID,
ALPHA_RESEARCH_KEY_FILE (Ed25519 private key, PEM, mode 0600), ALPHA_RESEARCH_DB
(local worker database), ALPHA_RESEARCH_WORKER_ID. The private key never leaves the worker.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.research_publication.contract import PUBLICATION_VERSION, bars_fingerprint, canonical, publication_id, sign_request
from app.research_publication.live_universe import live_universe
from app.research_publication.service import load_bars
from app.research_publication.strategy import TREND_MOMENTUM_V1, evaluate, input_sessions

HOME = Path(os.environ.get("ALPHA_RESEARCH_HOME", Path.home() / "alpha-research"))
STRATEGY_KEY = "trend_momentum_v1"
TENANT = "default"


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if not v:
        raise SystemExit(f"{name} is not set")
    return v


def _db() -> sqlite3.Connection:
    path = Path(os.environ.get("ALPHA_RESEARCH_DB", HOME / "worker.db"))
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS price_bars (tenant_id TEXT NOT NULL, ticker TEXT NOT NULL, timeframe TEXT NOT NULL, timestamp TEXT NOT NULL,"
        " open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL,"
        " PRIMARY KEY (tenant_id, ticker, timeframe, timestamp))"
    )
    conn.execute("CREATE TABLE IF NOT EXISTS sync_log (synced_at TEXT, market_date TEXT, summary_json TEXT)")
    return conn


def _private_key():
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    return load_pem_private_key(Path(_env("ALPHA_RESEARCH_KEY_FILE")).read_bytes(), password=None)


def _request(method: str, path_qs: str, body: bytes = b"") -> tuple[int, Any]:
    url = _env("ALPHA_RESEARCH_URL").rstrip("/") + path_qs
    headers = sign_request(_private_key(), _env("ALPHA_RESEARCH_KEY_ID"), method, path_qs, body)
    headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body if method != "GET" else None, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as res:
            return res.status, json.loads(res.read())
    except urllib.error.HTTPError as err:
        try:
            return err.code, json.loads(err.read())
        except ValueError:
            return err.code, {"error": err.reason}


def cmd_keygen(args: argparse.Namespace) -> int:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    out = Path(args.out)
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to overwrite a key")
    out.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = Ed25519PrivateKey.generate()
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    pub = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    # The public key is not secret: it goes into Railway's ALPHA_PUBLISHER_KEYS.
    print(json.dumps({"keyFile": str(out), "publicKey": base64.b64encode(pub).decode("ascii")}))
    return 0


def cmd_sync(_: argparse.Namespace) -> dict[str, Any]:
    sessions = input_sessions(TREND_MOMENTUM_V1)
    status, data = _request("GET", f"/api/sync/bars?sessions={sessions}")
    if status != 200:
        raise SystemExit(f"sync failed: HTTP {status} {data}")
    conn = _db()
    summary: dict[str, Any] = {"marketDate": data["marketDate"], "tickers": {}}
    conn.execute("BEGIN IMMEDIATE")
    try:
        for t, block in data["tickers"].items():
            bars = block["bars"]
            if bars_fingerprint(bars) != block["sha256"]:
                raise SystemExit(f"{t}: payload fingerprint mismatch")
            first = bars[0][0]
            local = {r[0][:10]: list(r[1:]) for r in conn.execute(
                "SELECT timestamp, open, high, low, close, volume FROM price_bars WHERE tenant_id=? AND ticker=? AND timeframe='1d' AND timestamp >= ?",
                (TENANT, t, first))}
            remote = {b[0]: b[1:] for b in bars}
            ins = sum(1 for d in remote if d not in local)
            cor = sum(1 for d in remote if d in local and local[d] != remote[d])
            rem = sum(1 for d in local if d not in remote)
            # Full-window reconciliation: the local window becomes an exact mirror of Railway's.
            conn.execute("DELETE FROM price_bars WHERE tenant_id=? AND ticker=? AND timeframe='1d' AND timestamp >= ?", (TENANT, t, first))
            conn.executemany("INSERT INTO price_bars VALUES (?, ?, '1d', ?, ?, ?, ?, ?, ?)",
                             [(TENANT, t, f"{b[0]}T00:00:00+00:00", *b[1:]) for b in bars])
            summary["tickers"][t] = {"bars": len(bars), "inserted": ins, "corrected": cor, "removed": rem}
        conn.execute("INSERT INTO sync_log VALUES (?, ?, ?)", (datetime.now(timezone.utc).isoformat(), data["marketDate"], json.dumps(summary)))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    mirrored = load_bars(conn, sorted(data["tickers"]), sessions)
    bad = [t for t in data["tickers"] if bars_fingerprint(mirrored[t]) != data["tickers"][t]["sha256"]]
    if bad:
        raise SystemExit(f"local mirror differs after sync: {bad}")
    print(json.dumps({"sync": summary}, indent=2))
    return summary


def cmd_run(_: argparse.Namespace) -> Path:
    cfg = TREND_MOMENTUM_V1
    universe = live_universe()
    sessions = input_sessions(cfg)
    bars = load_bars(_db(), universe, sessions)
    short = [t for t in universe if len(bars[t]) != sessions]
    if short:
        raise SystemExit(f"not enough synced bars for {short}; run sync first")
    market = max(bars[t][-1][0] for t in universe)
    payload = {
        "schemaVersion": PUBLICATION_VERSION,
        "worker": {"id": os.environ.get("ALPHA_RESEARCH_WORKER_ID", "localhost")},
        "strategy": {"key": STRATEGY_KEY, "config": cfg},
        "universe": universe,
        "inputs": {"dataset": cfg["dataset"], "tenantId": TENANT, "sessions": sessions, "marketDate": market,
                   "fingerprints": {t: bars_fingerprint(bars[t]) for t in universe}},
        "result": evaluate(bars, cfg),
    }
    outbox = HOME / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    path = outbox / f"{publication_id(payload)}.json"
    path.write_bytes(canonical(payload))
    ev = payload["result"]["evaluation"]
    print(json.dumps({"publicationId": publication_id(payload), "marketDate": market,
                      "predictions": [p["ticker"] for p in payload["result"]["predictions"]],
                      "holdout": ev["holdout"], "edgeOverBaseline": ev["edgeOverBaseline"], "outbox": str(path)}, indent=2))
    return path


def cmd_publish(args: argparse.Namespace) -> int:
    outbox = HOME / "outbox"
    path = Path(args.file) if getattr(args, "file", None) else max(outbox.glob("pub_*.json"), key=lambda p: p.stat().st_mtime)
    status, out = _request("POST", "/api/research/publications", path.read_bytes())
    print(json.dumps({"httpStatus": status, **out}, indent=2))
    return 0 if status in (200, 201) else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="research-worker")
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("keygen")
    k.add_argument("--out", default=str(Path.home() / ".config" / "alpha-research" / "publisher_ed25519.pem"))
    sub.add_parser("sync")
    sub.add_parser("run")
    p = sub.add_parser("publish")
    p.add_argument("--file")
    sub.add_parser("all")
    args = ap.parse_args(argv)
    if args.cmd == "keygen":
        return cmd_keygen(args)
    if args.cmd == "sync":
        cmd_sync(args)
        return 0
    if args.cmd == "run":
        cmd_run(args)
        return 0
    if args.cmd == "publish":
        return cmd_publish(args)
    cmd_sync(args)
    args.file = str(cmd_run(args))
    return cmd_publish(args)


if __name__ == "__main__":
    sys.exit(main())
