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


def _run_worker(as_of=None):
    worker.cmd_sync(None)
    return worker.cmd_run(type("A", (), {"as_of": as_of})())


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
        "schema": "pass", "mode": "pass", "universe": "pass", "provenance": "pass", "freshness": "pass",
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
    assert latest["volume"] > 0 and latest["avgVolume20"] > 0 and latest["avgVolume20"] < latest["volume"]  # seeded volume rises daily
    earlier = c.get("/api/market/session-close", params={"ticker": "AAA", "session": "2026-01-01"})
    assert earlier.status_code in (200, 404)
    if earlier.status_code == 200:
        assert earlier.json()["session"] <= "2026-01-01"
    assert c.get("/api/market/session-close", params={"ticker": "ZZZ"}).status_code == 404
    assert c.get("/api/market/session-close", params={"ticker": "AAA", "session": "bad"}).status_code == 422


def test_live_bars_updates_legacy_format_rows_in_place_instead_of_duplicating(tmp_path, monkeypatch):
    from app.jobs import live_bars

    db = tmp_path / "legacy.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE price_bars (tenant_id TEXT, ticker TEXT, timeframe TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, PRIMARY KEY (tenant_id, ticker, timeframe, timestamp))")
    c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d','2026-11-19',1,1,1,10,1)")                       # legacy bare date
    c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d','2026-11-20T00:00:00+00:00',1,1,1,11,1)")
    c.commit()
    c.close()
    monkeypatch.setenv("ALPHA_LIVE_UNIVERSE", "AAA")
    fetched = {"AAA": [("2026-11-19T00:00:00+00:00", 1.0, 1.0, 1.0, 9.5, 1.0), ("2026-11-20T00:00:00+00:00", 1.0, 1.0, 1.0, 11.0, 1.0),
                       ("2026-11-21T00:00:00+00:00", 1.0, 1.0, 1.0, 12.0, 1.0)]}
    rec = live_bars.refresh(str(db), fetch=lambda t, d: fetched)
    assert (rec["inserted"], rec["corrected"], rec["unchanged"]) == (1, 1, 1)
    c = sqlite3.connect(db)
    rows = c.execute("SELECT timestamp, close FROM price_bars ORDER BY timestamp").fetchall()
    c.close()
    assert rows == [("2026-11-19", 9.5), ("2026-11-20T00:00:00+00:00", 11.0), ("2026-11-21T00:00:00+00:00", 12.0)]


def test_load_bars_counts_sessions_not_rows_and_prefers_canonical(tmp_path):
    c = sqlite3.connect(tmp_path / "dups.db")
    c.execute("CREATE TABLE price_bars (tenant_id TEXT, ticker TEXT, timeframe TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)")
    days = _weekdays_ending(date(2026, 11, 20), 30)[-30:]
    for i, d in enumerate(days):
        c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d',?,1,1,1,?,1)", (f"{d}T00:00:00+00:00", 100.0 + i))
        for stamp in (d, f"{d} 00:00:00", f"{d}T21:19:03.641365+00:00"):  # legacy duplicates, stale values
            c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d',?,1,1,1,?,1)", (stamp, 1.0))
    bars = service.load_bars(c, ["AAA"], 20)["AAA"]
    assert [b[0] for b in bars] == days[-20:]
    assert [b[4] for b in bars] == [100.0 + i for i in range(10, 30)]


def _previous_session(env):
    c = sqlite3.connect(env["db"])
    try:
        return c.execute("SELECT DISTINCT substr(timestamp,1,10) d FROM price_bars ORDER BY d DESC LIMIT 1 OFFSET 1").fetchone()[0]
    finally:
        c.close()


def test_historical_replay_is_validated_as_of_its_session_and_kept_apart_from_live(env):
    session = _previous_session(env)
    path = _run_worker(as_of=session)
    payload = json.loads(path.read_bytes())
    assert payload["mode"] == "historical_replay" and payload["inputs"]["asOf"] == session
    res = env["signed"]("POST", "/api/research/publications", path.read_bytes())
    assert res.status_code == 201, res.json()
    out = res.json()
    assert out["mode"] == "historical_replay"
    assert {g["gate"] for g in out["gates"]} >= {"mode", "replay_session", "reproduction", "market_data"}
    assert "freshness" not in {g["gate"] for g in out["gates"]}
    c = env["client"]
    ticker = payload["result"]["predictions"][0]["ticker"]
    # Never served as live research.
    assert c.get("/api/research/latest", params={"ticker": ticker}).json()["reason"] == "no_accepted_publication"
    replay = c.get("/api/research/latest", params={"ticker": ticker, "mode": "historical_replay", "session": session}).json()
    assert replay["found"] is True and replay["mode"] == "historical_replay" and replay["marketDate"] == session
    trace = c.get(f"/api/predictions/{replay['predictionId']}/trace").json()
    assert trace["evidence"]["snapshot"]["observedAt"].startswith(session)
    assert trace["evidence"]["snapshot"]["ranking"]["mode"] == "historical_replay"
    assert c.get("/api/research/latest", params={"ticker": ticker, "mode": "bogus"}).status_code == 422


def test_replay_must_match_a_real_session_and_cannot_pose_as_live(env):
    session = _previous_session(env)
    path = _run_worker(as_of=session)
    payload = json.loads(path.read_bytes())
    wrong = {**payload, "inputs": {**payload["inputs"], "asOf": "2001-01-02"}}
    res = env["signed"]("POST", "/api/research/publications", contract.canonical(wrong))
    assert res.status_code == 422 and res.json()["gates"][-1]["gate"] in ("provenance", "replay_session")
    as_live = {k: v for k, v in payload.items() if k != "mode"}
    res = env["signed"]("POST", "/api/research/publications", contract.canonical(as_live))
    assert res.status_code == 422 and res.json()["gates"][-1]["gate"] in ("provenance", "freshness")


def test_load_bars_ignores_non_session_rows(tmp_path):
    c = sqlite3.connect(tmp_path / "junk.db")
    c.execute("CREATE TABLE price_bars (tenant_id TEXT, ticker TEXT, timeframe TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)")
    days = _weekdays_ending(date(2026, 11, 20), 10)[-10:]
    for i, d in enumerate(days):
        c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d',?,1,1,1,?,1)", (f"{d}T00:00:00+00:00", 100.0 + i))
    # Mock contamination: intraday-stamped "daily" rows, including on a Saturday and a real weekday.
    for stamp in ("2026-11-14T21:19:03.641365+00:00", "2026-11-14T21:27:36.470408+00:00", f"{days[-1]}T21:19:03.641365+00:00"):
        c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d',?,1,1,1,999,1)", (stamp,))
    c.execute("INSERT INTO price_bars VALUES ('default','AAA','1d','2026-11-15',1,1,1,999,1)")  # Sunday, midnight-keyed
    bars = service.load_bars(c, ["AAA"], 10)["AAA"]
    assert [b[0] for b in bars] == days and 999 not in [b[4] for b in bars]
    assert service.is_session_bar("2026-11-20") and service.is_session_bar("2026-11-20 00:00:00") and service.is_session_bar("2026-11-20T05:00:00+00:00")
    assert not service.is_session_bar("2026-11-20T21:19:03.641365+00:00") and not service.is_session_bar("2026-11-21T00:00:00+00:00")



# ── Publication health vs research quality ─────────────────────────────────────

@pytest.mark.parametrize("run_status, edge, oos, verdict", [
    ("HEALTHY", False, False, "no_demonstrated_edge"),  # staging today: 48.4% vs 49.9%
    ("HEALTHY", True, False, "edge_not_out_of_sample"),
    ("HEALTHY", True, True, "edge_demonstrated"),
    ("DEGRADED", True, True, "insufficient_evidence"),
])
def test_operational_health_is_not_research_quality(run_status, edge, oos, verdict):
    gates = [{"gate": "edge_over_baseline", "status": "evidence", "detail": {
        "holdoutPrecision": 0.484, "baselineRate": 0.499, "edgeOverBaseline": edge, "genuineOutOfSample": oos}}]
    q = service.research_quality(gates, run_status)
    assert q["operationalStatus"] == run_status and q["verdict"] == verdict
    assert q["evidenceQualifies"] is (verdict == "edge_demonstrated")


def test_quality_is_reported_on_every_read_surface(env):
    path = _run_worker()
    res = env["signed"]("POST", "/api/research/publications", path.read_bytes()).json()
    assert res["runStatus"] == "HEALTHY" and res["researchQuality"]["evidenceQualifies"] is False
    record = env["client"].get(f"/api/research/publications/{res['publicationId']}").json()
    assert record["researchQuality"] == res["researchQuality"]
    ticker = res["predictions"][0].split(":")[1]
    latest = env["client"].get("/api/research/latest", params={"ticker": ticker}).json()
    assert latest["run"]["runStatus"] == "HEALTHY"
    assert latest["researchQuality"]["verdict"] in {"no_demonstrated_edge", "edge_not_out_of_sample"}


# ── Scheduled publisher (Railway cron) ──────────────────────────────────────────

def _scheduled(env, monkeypatch, live_bars):
    monkeypatch.setattr(worker, "_health", lambda: {"live_bars": live_bars})
    return worker.cmd_scheduled(None)


def _refreshed(env, finished="2026-11-24T22:05:00+00:00"):
    sync = env["signed"]("GET", f"/api/sync/bars?sessions={SESSIONS}").json()
    return {"state": "succeeded", "market_date": sync["marketDate"], "missing": [], "finished_at": finished}


def test_scheduled_waits_for_a_verified_refresh(env, monkeypatch, capsys):
    assert _scheduled(env, monkeypatch, {"state": "running", "market_date": None, "missing": []}) == 0
    assert _scheduled(env, monkeypatch, {**_refreshed(env), "state": "partial", "missing": ["BBB"]}) == 0
    assert '"daily_refresh_not_verified"' in capsys.readouterr().out
    assert not (worker.HOME / "outbox").exists()  # nothing computed, nothing published


def test_scheduled_publishes_once_per_refresh_and_is_idempotent(env, monkeypatch, capsys):
    lb = _refreshed(env)
    assert _scheduled(env, monkeypatch, lb) == 0
    out = capsys.readouterr().out
    assert '"action": "published"' in out and '"idempotent": false' in out
    assert _scheduled(env, monkeypatch, lb) == 0
    assert '"already_published"' in capsys.readouterr().out
    # Lost local state (fresh volume): Railway's content-addressed publish dedupes.
    (worker.HOME / "scheduled-state.json").unlink()
    assert _scheduled(env, monkeypatch, lb) == 0
    assert '"idempotent": true' in capsys.readouterr().out
    assert len(_pub_rows(env["db"])[0]) == 1


def test_scheduled_failures_exit_nonzero_and_the_next_pass_recovers(env, monkeypatch):
    lb = _refreshed(env)

    def down():
        raise OSError("connection refused")
    monkeypatch.setattr(worker, "_health", down)
    assert worker.cmd_scheduled(None) == 1

    real = worker._request
    monkeypatch.setattr(worker, "_request", lambda m, p, b=b"": (503, {"error": "busy"}) if m == "POST" else real(m, p, b))
    assert _scheduled(env, monkeypatch, lb) == 1
    assert not (worker.HOME / "scheduled-state.json").exists()  # nothing recorded on failure

    monkeypatch.setattr(worker, "_request", real)
    assert _scheduled(env, monkeypatch, lb) == 0
    assert len(_pub_rows(env["db"])[0]) == 1


def test_private_key_can_come_from_a_managed_secret(env, monkeypatch):
    pem = env["key"].private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    monkeypatch.delenv("ALPHA_RESEARCH_KEY_FILE", raising=False)
    monkeypatch.setenv("ALPHA_RESEARCH_KEY_PEM", pem.replace("\n", "\\n"))
    signed = contract.sign_request(worker._private_key(), "test-worker", "GET", "/x", b"")
    assert env["client"].get("/x", headers=signed) is not None  # loads and signs without a key file
    assert worker._private_key().public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw) == \
        env["key"].public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
