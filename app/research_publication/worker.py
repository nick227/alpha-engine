"""Research worker CLI (localhost now; an Oracle Cloud worker later — same contracts).

    python -m app.research_publication.worker keygen
    python -m app.research_publication.worker sync      # Railway → local, full-window reconciliation
    python -m app.research_publication.worker run       # strategy on synced bars → outbox
    python -m app.research_publication.worker publish   # signed POST of the latest outbox entry
    python -m app.research_publication.worker all
    python -m app.research_publication.worker scheduled  # Railway cron: publish once per verified refresh

Environment: ALPHA_RESEARCH_URL (Railway Alpha Engine), ALPHA_RESEARCH_KEY_ID,
ALPHA_RESEARCH_KEY_FILE (Ed25519 private key, PEM, mode 0600) or ALPHA_RESEARCH_KEY_PEM (the same
PEM held as a managed secret, e.g. a Railway variable), ALPHA_RESEARCH_DB
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
REPLAY_HEADROOM_SESSIONS = 40  # sync a little extra history so recent sessions can be replayed


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

    pem = os.environ.get("ALPHA_RESEARCH_KEY_PEM", "").strip()
    if pem:
        # Managed secret: PEM text, with literal "\n" accepted for single-line variable stores.
        return load_pem_private_key(pem.replace("\\n", "\n").encode("ascii"), password=None)
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
    sessions = input_sessions(TREND_MOMENTUM_V1) + REPLAY_HEADROOM_SESSIONS
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


def cmd_run(args: argparse.Namespace) -> Path:
    cfg = TREND_MOMENTUM_V1
    universe = live_universe()
    sessions = input_sessions(cfg)
    as_of = getattr(args, "as_of", None)
    bars = load_bars(_db(), universe, sessions, until=as_of)
    short = [t for t in universe if len(bars[t]) != sessions]
    if short:
        raise SystemExit(f"not enough synced bars for {short}; run sync first")
    market = max(bars[t][-1][0] for t in universe)
    if as_of and market != as_of:
        raise SystemExit(f"{as_of} is not a session every live symbol traded (latest on/before it: {market})")
    payload = {
        "schemaVersion": PUBLICATION_VERSION,
        **({"mode": "historical_replay"} if as_of else {}),
        "worker": {"id": os.environ.get("ALPHA_RESEARCH_WORKER_ID", "localhost")},
        "strategy": {"key": STRATEGY_KEY, "config": cfg},
        "universe": universe,
        "inputs": {"dataset": cfg["dataset"], "tenantId": TENANT, "sessions": sessions, "marketDate": market,
                   **({"asOf": as_of} if as_of else {}),
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


def _log(**fields: Any) -> None:
    print(json.dumps({"at": datetime.now(timezone.utc).isoformat(), **fields}, sort_keys=True), flush=True)


def _health() -> dict[str, Any]:
    url = _env("ALPHA_RESEARCH_URL").rstrip("/") + "/health"
    with urllib.request.urlopen(urllib.request.Request(url), timeout=30) as res:
        return json.loads(res.read())


def cmd_scheduled(_: argparse.Namespace) -> int:
    """One scheduled pass (Railway cron). Safe to run often:

    - waits (exit 0) until Alpha Engine reports a verified daily refresh: live_bars succeeded,
      nothing missing, and the synced window ends on that market date;
    - publishes once per (market date, inputs): the local state file skips work already
      accepted, and Railway's content-addressed publish is idempotent anyway;
    - any failure exits non-zero without recording state, so the next pass retries.
    Publication is not eligibility: Railway re-derives every claim, and weak research is
    still published (with its evidence) but never qualifies for trading downstream.
    """
    state_path = HOME / "scheduled-state.json"
    try:
        lb = (_health().get("live_bars") or {})
    except Exception as err:  # unreachable Alpha Engine is a failure, not a wait
        _log(action="error", stage="health", error=str(err)[:200])
        return 1
    market = lb.get("market_date")
    if lb.get("state") != "succeeded" or lb.get("missing") or not market:
        _log(action="wait", reason="daily_refresh_not_verified", liveBars={k: lb.get(k) for k in ("state", "market_date", "missing", "finished_at")})
        return 0
    last = json.loads(state_path.read_text()) if state_path.exists() else {}
    if last.get("marketDate") == market and last.get("refreshFinishedAt") == lb.get("finished_at"):
        _log(action="skip", reason="already_published", marketDate=market, publicationId=last.get("publicationId"))
        return 0
    try:
        summary = cmd_sync(_)
        if summary["marketDate"] != market:
            _log(action="wait", reason="sync_behind_refresh", refreshMarketDate=market, syncMarketDate=summary["marketDate"])
            return 0
        path = cmd_run(argparse.Namespace(as_of=None))
        status, out = _request("POST", "/api/research/publications", path.read_bytes())
    except SystemExit as err:
        _log(action="error", stage="pipeline", error=str(err)[:300])
        return 1
    if status not in (200, 201):
        _log(action="error", stage="publish", httpStatus=status, status=out.get("status"),
             failedGates=[g.get("gate") for g in out.get("gates", []) if g.get("status") == "fail"])
        return 1
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"marketDate": market, "refreshFinishedAt": lb.get("finished_at"), "publicationId": out.get("publicationId"),
                               "publishedAt": datetime.now(timezone.utc).isoformat()}))
    tmp.replace(state_path)
    _log(action="published", httpStatus=status, idempotent=bool(out.get("idempotent")), marketDate=market,
         publicationId=out.get("publicationId"), runStatus=out.get("runStatus"), researchQuality=out.get("researchQuality"),
         predictions=len(out.get("predictions", [])))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="research-worker")
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("keygen")
    k.add_argument("--out", default=str(Path.home() / ".config" / "alpha-research" / "publisher_ed25519.pem"))
    sub.add_parser("sync")
    r = sub.add_parser("run")
    r.add_argument("--as-of", dest="as_of", help="historical replay: research as of this past session (YYYY-MM-DD)")
    p = sub.add_parser("publish")
    p.add_argument("--file")
    a = sub.add_parser("all")
    a.add_argument("--as-of", dest="as_of")
    sub.add_parser("scheduled")
    args = ap.parse_args(argv)
    if args.cmd == "scheduled":
        return cmd_scheduled(args)
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
