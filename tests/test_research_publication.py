"""P0 research publication: signed sync, worker run, independent validation, atomic storage."""
from __future__ import annotations

import base64
import json
import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")
pytest.importorskip("cryptography")

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

from app.research_publication import contract, service, worker  # noqa: E402
from app.research_publication.strategy import TREND_MOMENTUM_V1, evaluate, input_sessions  # noqa: E402

UNIVERSE = ["AAA", "BBB", "CCC"]
SESSIONS = input_sessions(TREND_MOMENTUM_V1)


def _weekdays_ending(end: date, n: int) -> list[str]:
    out, d = [], end
    while len(out) < n + 20:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return sorted(out)


def _seed(db: Path, end: date) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db))
    days = _weekdays_ending(end, SESSIONS)
    for k, t in enumerate(UNIVERSE):
        rows = []
        for i, d in enumerate(days):
            px = 100.0 * (1 + 0.002 * i * (1 if k != 2 else -0.4)) * (1 + 0.005 * math.sin(i / (4 + k)))
            rows.append(("default", t, "1d", f"{d}T00:00:00+00:00", px * 0.99, px * 1.01, px * 0.98, round(px, 4), 1e6 + i))
        repo.conn.executemany("INSERT INTO price_bars VALUES (?,?,?,?,?,?,?,?,?)", rows)
    repo.conn.commit()
    repo.conn.close()


@pytest.fixture
def env(tmp_path, monkeypatch):
    key = Ed25519PrivateKey.generate()
    pub = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    db = tmp_path / "railway.db"
    today = date.today()
    _seed(db, today)
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    monkeypatch.setenv("ALPHA_LIVE_UNIVERSE", ",".join(UNIVERSE))
    monkeypatch.setenv("ALPHA_PUBLISHER_KEYS", json.dumps({"test-worker": base64.b64encode(pub).decode()}))
    monkeypatch.setenv("ALPHA_RESEARCH_HOME", str(tmp_path / "worker"))
    monkeypatch.setenv("ALPHA_RESEARCH_DB", str(tmp_path / "worker" / "worker.db"))
    monkeypatch.setattr(worker, "HOME", tmp_path / "worker")
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        def signed(method, path_qs, body=b"", key_id="test-worker", private=key):
            headers = contract.sign_request(private, key_id, method, path_qs, body)
            return client.request(method, path_qs, content=body or None, headers=headers)

        def fake_request(method, path_qs, body=b""):
            res = signed(method, path_qs, body)
            return res.status_code, res.json()

        monkeypatch.setattr(worker, "_request", fake_request)
        yield {"client": client, "signed": signed, "db": db, "key": key, "today": today}


def _run_worker():
    worker.cmd_sync(None)
    return worker.cmd_run(None)


def _pub_rows(db):
    c = sqlite3.connect(db)
    try:
        return c.execute("SELECT status, run_status FROM research_publications").fetchall(), c.execute(
            "SELECT id, ticker, mode FROM predictions WHERE mode = 'published' ORDER BY ticker").fetchall()
    finally:
        c.close()


def test_sync_requires_a_valid_ed25519_signature(env):
    c = env["client"]
    path = f"/api/sync/bars?sessions={SESSIONS}"
    assert c.get(path).status_code == 401
    assert env["signed"]("GET", path, key_id="unknown").status_code == 401
    other = Ed25519PrivateKey.generate()
    assert env["signed"]("GET", path, private=other).status_code == 401
    stale = contract.sign_request(env["key"], "test-worker", "GET", path, b"", now=1_000_000)
    assert c.get(path, headers=stale).status_code == 401
    # The read key path does not open worker routes either.
    assert c.get(path, headers={"X-Internal-Key": "anything"}).status_code == 401
    ok = env["signed"]("GET", path)
    assert ok.status_code == 200
    body = ok.json()
    assert sorted(body["tickers"]) == UNIVERSE
    for t in UNIVERSE:
        assert len(body["tickers"][t]["bars"]) == SESSIONS
        assert body["tickers"][t]["sha256"] == contract.bars_fingerprint(body["tickers"][t]["bars"])


def test_worker_publication_is_validated_stored_and_traceable(env):
    path = _run_worker()
    assert worker.cmd_publish(type("A", (), {"file": str(path)})()) == 0
    pubs, preds = _pub_rows(env["db"])
    assert pubs and pubs[0][0] == "accepted"
    payload = json.loads(path.read_bytes())
    pub_id = contract.publication_id(payload)
    record = env["client"].get(f"/api/research/publications/{pub_id}").json()
    assert {g["gate"]: g["status"] for g in record["gates"]} == {
        "schema": "pass", "universe": "pass", "provenance": "pass", "freshness": "pass",
        "reproduction": "pass", "market_data": "pass",
        "holdout_sufficient": "pass" if payload["result"]["evaluation"]["sufficientHoldout"] else "fail",
        "edge_over_baseline": "evidence"}
    assert [p[1] for p in preds] == [p["ticker"] for p in payload["result"]["predictions"]]
    assert preds, "synthetic uptrend must produce at least one prediction"

    ticker = preds[0][1]
    latest = env["client"].get("/api/research/latest", params={"ticker": ticker}).json()
    assert latest["found"] is True and latest["publicationId"] == pub_id and latest["predictionId"] == f"{pub_id}:{ticker}"
    assert latest["quality"]["genuineOutOfSample"] is False

    trace = env["client"].get(f"/api/predictions/{latest['predictionId']}/trace").json()
    from app.internal_read_v1.research_trace import verify_evidence_digest

    snap = trace["evidence"]["snapshot"]
    assert verify_evidence_digest(snap, trace["evidence"]["sha256"])
    assert snap["ranking"]["publicationId"] == pub_id and snap["features"]["publicationId"] == pub_id
    assert snap["ranking"]["evaluation"]["genuineOutOfSample"] is False

    no_signal = payload["result"]["noSignal"]
    if no_signal:
        assert env["client"].get("/api/research/latest", params={"ticker": no_signal[0]}).json()["reason"] == "no_signal"


def test_republishing_is_idempotent(env):
    path = _run_worker()
    body = path.read_bytes()
    first = env["signed"]("POST", "/api/research/publications", body)
    second = env["signed"]("POST", "/api/research/publications", body)
    assert first.status_code == 201 and second.status_code == 200 and second.json()["idempotent"] is True
    pubs, preds = _pub_rows(env["db"])
    assert len(pubs) == 1 and len(preds) == len(json.loads(body)["result"]["predictions"])


def _publish_modified(env, mutate):
    path = _run_worker()
    payload = json.loads(path.read_bytes())
    mutate(payload)
    return env["signed"]("POST", "/api/research/publications", contract.canonical(payload))


@pytest.mark.parametrize("mutate,failed_gate", [
    (lambda p: p["result"]["evaluation"]["holdout"].__setitem__("precision", 0.99), "reproduction"),
    (lambda p: p["result"]["predictions"].append({**p["result"]["predictions"][0], "ticker": "CCC"}), "reproduction"),
    (lambda p: p["strategy"]["config"]["signal"].__setitem__("smaSessions", 50), "schema"),
    (lambda p: p.__setitem__("universe", ["AAA", "BBB"]), "universe"),
    (lambda p: p["inputs"]["fingerprints"].__setitem__("AAA", "0" * 64), "provenance"),
    (lambda p: p["inputs"].__setitem__("marketDate", "2000-01-03"), "freshness"),
])
def test_claims_that_railway_cannot_reproduce_are_rejected(env, mutate, failed_gate):
    res = _publish_modified(env, mutate)
    assert res.status_code == 422
    out = res.json()
    assert out["status"] == "rejected" and out["predictions"] == []
    assert out["gates"][-1] == {**out["gates"][-1], "gate": failed_gate, "status": "fail"}
    pubs, preds = _pub_rows(env["db"])
    assert pubs == [("rejected", None)] and preds == []  # kept for audit, never published


def test_railway_data_changing_after_sync_is_a_provenance_failure(env):
    path = _run_worker()
    c = sqlite3.connect(env["db"])
    c.execute("UPDATE price_bars SET close = close * 1.01 WHERE ticker = 'BBB' AND timestamp = (SELECT MAX(timestamp) FROM price_bars WHERE ticker='BBB')")
    c.commit()
    c.close()
    res = env["signed"]("POST", "/api/research/publications", path.read_bytes())
    assert res.status_code == 422 and res.json()["gates"][-1]["gate"] == "provenance"


def test_stale_market_data_is_rejected(env):
    path = _run_worker()
    conn = sqlite3.connect(env["db"], isolation_level=None)
    try:
        status, out = service.publish(conn, path.read_bytes(), key_id="test-worker", today=env["today"] + timedelta(days=30))
    finally:
        conn.close()
    assert status == 422 and out["gates"][-1]["gate"] == "freshness"


def test_strategy_is_deterministic_and_embargoes_the_holdout():
    days = _weekdays_ending(date(2026, 11, 20), SESSIONS)[-SESSIONS:]
    bars = {"X": [[d, 1.0, 1.0, 1.0, 100 + i * 0.1 + (i % 7), 1.0] for i, d in enumerate(days)]}
    a, b = evaluate(bars), evaluate(json.loads(json.dumps(bars)))
    assert contract.canonical(a) == contract.canonical(b)
    ev = a["evaluation"]
    assert ev["inSample"]["end"] < ev["holdout"]["start"]
    gap = days.index(ev["holdout"]["start"]) - days.index(ev["inSample"]["end"])
    assert gap > TREND_MOMENTUM_V1["evaluation"]["embargoSessions"]
    assert ev["genuineOutOfSample"] is False and ev["parametersFitted"] is False
    with pytest.raises(ValueError):
        evaluate({"X": bars["X"][1:]})


def test_live_bars_refresh_reconciles_full_window(tmp_path, monkeypatch):
    from app.jobs import live_bars

    db = tmp_path / "a.db"
    _seed(db, date(2026, 11, 20))
    monkeypatch.setenv("ALPHA_LIVE_UNIVERSE", "AAA,NEWX")
    c = sqlite3.connect(db)
    old = c.execute("SELECT timestamp, open, high, low, close, volume FROM price_bars WHERE ticker='AAA' ORDER BY timestamp DESC LIMIT 3").fetchall()
    c.close()
    # Provider re-adjusted an older bar (dividend), kept two, and has a brand-new symbol.
    fetched = {
        "AAA": [(old[2][0], *old[2][1:4], old[2][4] * 0.99, old[2][5]), *[(r[0], *r[1:]) for r in reversed(old[:2])]],
        "NEWX": [("2026-11-20T00:00:00+00:00", 10.0, 11.0, 9.0, 10.5, 100.0)],
    }
    rec = live_bars.refresh(str(db), fetch=lambda tickers, days: fetched)
    assert rec["state"] == "succeeded" and (rec["inserted"], rec["corrected"], rec["unchanged"]) == (1, 1, 2)
    assert live_bars.read_status(str(db))["state"] == "succeeded"
    c = sqlite3.connect(db)
    assert c.execute("SELECT close FROM price_bars WHERE ticker='AAA' AND timestamp=?", (old[2][0],)).fetchone()[0] == pytest.approx(old[2][4] * 0.99)
    total = c.execute("SELECT COUNT(*) FROM price_bars").fetchone()[0]
    c.close()
    assert total == 3 * (SESSIONS + 20) + 1  # nothing deleted

    monkeypatch.setattr(live_bars, "MIN_FREE_MB", 10**9)
    assert live_bars.refresh(str(db), fetch=lambda t, d: fetched)["reason"] == "insufficient_storage"


def test_session_close_is_daily_only_and_live_universe_only(env):
    c = env["client"]
    latest = c.get("/api/market/session-close", params={"ticker": "AAA"}).json()
    assert latest["timeframe"] == "1d" and latest["session"] <= env["today"].isoformat() and latest["close"] > 0
    earlier = c.get("/api/market/session-close", params={"ticker": "AAA", "session": "2026-01-01"})
    assert earlier.status_code in (200, 404)
    if earlier.status_code == 200:
        assert earlier.json()["session"] <= "2026-01-01"
    assert c.get("/api/market/session-close", params={"ticker": "ZZZ"}).status_code == 404
    assert c.get("/api/market/session-close", params={"ticker": "AAA", "session": "bad"}).status_code == 422

