"""Railway side: serve bars for sync, and independently validate + store publications.

Nothing a worker claims is trusted. Railway re-derives each claim from its own bars:
  schema        — known strategy key, config identical to the registered config
  universe      — exactly the configured live universe
  provenance    — per-ticker fingerprints of the worker's inputs equal Railway's bars
  freshness     — inputs end at Railway's market date, which is recent
  reproduction  — Railway re-runs the strategy; result must be identical
  market data   — every entry price is Railway's close on the observation date
Any failure rejects the publication (kept for audit, never published as a prediction).
Quality evidence (holdout size, edge over baseline, genuineOutOfSample) is recomputed
and published; an insufficient holdout makes the run DEGRADED.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.research_publication.contract import (
    PUBLICATION_VERSION, SYNC_VERSION, bars_fingerprint, canonical, publication_id,
)
from app.research_publication.live_universe import live_universe
from app.research_publication.strategy import STRATEGIES, evaluate, input_sessions

TENANT = "default"
MAX_MARKET_DATA_AGE_DAYS = 5     # calendar days: covers weekends and one holiday
MAX_BODY_BYTES = 512 * 1024

_DDL = """
CREATE TABLE IF NOT EXISTS research_publications (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    publisher_key_id TEXT NOT NULL,
    worker_id TEXT,
    strategy_key TEXT,
    market_date TEXT,
    status TEXT NOT NULL,              -- accepted | rejected
    run_status TEXT,                   -- HEALTHY | DEGRADED (accepted only)
    gates_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_publications_latest
    ON research_publications(tenant_id, status, market_date, received_at);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)


def load_bars(conn: sqlite3.Connection, tickers: list[str], sessions: int, *, tenant_id: str = TENANT) -> dict[str, list[list[Any]]]:
    """Last `sessions` daily bars per ticker, one per date, ascending: [date, o, h, l, c, v]."""
    out: dict[str, list[list[Any]]] = {}
    for t in tickers:
        rows = conn.execute(
            """
            SELECT timestamp, open, high, low, close, volume FROM price_bars
            WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d' AND close > 0
            ORDER BY timestamp DESC LIMIT ?
            """,
            (tenant_id, t, sessions * 2),
        ).fetchall()
        by_date: dict[str, list[Any]] = {}
        for r in rows:
            d = str(r[0])[:10]
            # Prefer the canonical midnight-UTC row when a date has several formats.
            if d not in by_date or str(r[0]).endswith("T00:00:00+00:00"):
                by_date[d] = [d, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
        out[t] = [by_date[d] for d in sorted(by_date)][-sessions:]
    return out


def sync_payload(conn: sqlite3.Connection, tickers: list[str] | None, sessions: int) -> dict[str, Any]:
    universe = live_universe()
    wanted = universe if not tickers else [t for t in tickers if t in universe]
    bars = load_bars(conn, wanted, sessions)
    market = max((b[-1][0] for b in bars.values() if b), default=None)
    return {
        "schemaVersion": SYNC_VERSION, "tenantId": TENANT, "timeframe": "1d", "sessions": sessions,
        "marketDate": market,
        "tickers": {t: {"bars": b, "sha256": bars_fingerprint(b)} for t, b in bars.items()},
    }


def _gate(name: str, ok: bool, detail: Any = None) -> dict[str, Any]:
    return {"gate": name, "status": "pass" if ok else "fail", **({"detail": detail} if detail is not None else {})}


def validate(conn: sqlite3.Connection, payload: Any, *, today: date | None = None) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Returns (gates, recomputed_result). Gates stop at the first structural failure."""
    gates: list[dict[str, Any]] = []
    today = today or datetime.now(timezone.utc).date()

    keys = {"schemaVersion", "worker", "strategy", "universe", "inputs", "result"}
    shape_ok = isinstance(payload, dict) and keys <= set(payload) and payload.get("schemaVersion") == PUBLICATION_VERSION
    strat = payload.get("strategy") if shape_ok else None
    cfg = STRATEGIES.get(strat.get("key")) if isinstance(strat, dict) else None
    shape_ok = shape_ok and cfg is not None and canonical(strat.get("config")) == canonical(cfg)
    gates.append(_gate("schema", shape_ok, None if shape_ok else "unknown schema, strategy key or config"))
    if not shape_ok:
        return gates, None

    universe = live_universe()
    uni_ok = payload["universe"] == universe
    gates.append(_gate("universe", uni_ok, None if uni_ok else {"expected": universe}))
    if not uni_ok:
        return gates, None

    inputs = payload["inputs"] if isinstance(payload["inputs"], dict) else {}
    sessions = input_sessions(cfg)
    bars = load_bars(conn, universe, sessions)
    complete = all(len(bars[t]) == sessions for t in universe)
    ours = {t: bars_fingerprint(bars[t]) for t in universe}
    claimed = inputs.get("fingerprints") or {}
    mismatched = sorted(t for t in universe if claimed.get(t) != ours[t])
    prov_ok = complete and inputs.get("sessions") == sessions and inputs.get("tenantId") == TENANT and not mismatched
    gates.append(_gate("provenance", prov_ok, None if prov_ok else {"mismatchedTickers": mismatched, "complete": complete}))
    if not prov_ok:
        return gates, None

    market = max(bars[t][-1][0] for t in universe)
    lagging = sorted(t for t in universe if bars[t][-1][0] != market)
    age = (today - date.fromisoformat(market)).days
    fresh_ok = inputs.get("marketDate") == market and not lagging and age <= MAX_MARKET_DATA_AGE_DAYS
    gates.append(_gate("freshness", fresh_ok, {"marketDate": market, "ageDays": age, "laggingTickers": lagging}))
    if not fresh_ok:
        return gates, None

    recomputed = evaluate(bars, cfg)
    repro_ok = canonical(payload["result"]) == canonical(recomputed)
    gates.append(_gate("reproduction", repro_ok, None if repro_ok else "worker result differs from Railway recomputation"))
    if not repro_ok:
        return gates, None

    close_on = {t: {b[0]: b[4] for b in bars[t]} for t in universe}
    bad_px = [p["ticker"] for p in recomputed["predictions"]
              if p["observedAt"][:10] != market or close_on[p["ticker"]].get(market) != p["entryPrice"]]
    gates.append(_gate("market_data", not bad_px, None if not bad_px else {"tickers": bad_px}))
    if bad_px:
        return gates, None

    ev = recomputed["evaluation"]
    gates.append(_gate("holdout_sufficient", ev["sufficientHoldout"], {"signals": ev["holdout"]["signals"]}))
    gates.append({"gate": "edge_over_baseline", "status": "evidence", "detail": {
        "holdoutPrecision": ev["holdout"]["precision"], "baselineRate": ev["holdout"]["baselineRate"],
        "edgeOverBaseline": ev["edgeOverBaseline"], "genuineOutOfSample": ev["genuineOutOfSample"]}})
    return gates, recomputed


def _integrity_ok(gates: list[dict[str, Any]]) -> bool:
    required = {"schema", "universe", "provenance", "freshness", "reproduction", "market_data"}
    passed = {g["gate"] for g in gates if g["status"] == "pass"}
    return required <= passed


def publish(conn: sqlite3.Connection, body: bytes, *, key_id: str, today: date | None = None) -> tuple[int, dict[str, Any]]:
    """Validate and store atomically. Idempotent by content address."""
    if len(body) > MAX_BODY_BYTES:
        return 413, {"status": "rejected", "reason": "payload_too_large"}
    try:
        payload = json.loads(body)
    except ValueError:
        return 400, {"status": "rejected", "reason": "invalid_json"}
    ensure_schema(conn)
    pub_id = publication_id(payload) if isinstance(payload, dict) else None
    if pub_id is None:
        return 400, {"status": "rejected", "reason": "invalid_payload"}
    existing = conn.execute("SELECT status, run_status, gates_json FROM research_publications WHERE id = ?", (pub_id,)).fetchone()
    if existing:
        return (200 if existing[0] == "accepted" else 422), {
            "publicationId": pub_id, "status": existing[0], "runStatus": existing[1],
            "gates": json.loads(existing[2]), "idempotent": True}

    gates, result = validate(conn, payload, today=today)
    accepted = result is not None and _integrity_ok(gates)
    run_status = None
    if accepted:
        run_status = "HEALTHY" if result["evaluation"]["sufficientHoldout"] else "DEGRADED"
    now = datetime.now(timezone.utc).isoformat()
    market = payload.get("inputs", {}).get("marketDate") if isinstance(payload.get("inputs"), dict) else None
    strat_key = payload.get("strategy", {}).get("key") if isinstance(payload.get("strategy"), dict) else None
    worker_id = payload.get("worker", {}).get("id") if isinstance(payload.get("worker"), dict) else None

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO research_publications (id, tenant_id, schema_version, publisher_key_id, worker_id, strategy_key, market_date, status, run_status, gates_json, payload_json, received_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (pub_id, TENANT, str(payload.get("schemaVersion")), key_id, worker_id, strat_key, market,
             "accepted" if accepted else "rejected", run_status, json.dumps(gates), canonical(payload).decode("utf-8"), now),
        )
        if accepted:
            ev = result["evaluation"]
            for p in result["predictions"]:
                ranking_ctx = {
                    "publicationId": pub_id, "strategy": strat_key, "marketDate": market, "runStatus": run_status,
                    "evaluation": {k: ev[k] for k in ("method", "parametersFitted", "genuineOutOfSample", "edgeOverBaseline", "sufficientHoldout", "inSample", "holdout")},
                }
                conn.execute(
                    "INSERT INTO predictions (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction, confidence, horizon, entry_price, mode, feature_snapshot_json, rank_score, ranking_context_json)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', ?, ?, ?)",
                    (f"{pub_id}:{p['ticker']}", TENANT, strat_key, pub_id, p["ticker"], p["observedAt"], p["direction"],
                     p["confidence"], p["horizon"], p["entryPrice"],
                     json.dumps({**p["features"], "model_id": strat_key, "publicationId": pub_id}, sort_keys=True),
                     p["confidence"], json.dumps(ranking_ctx, sort_keys=True)),
                )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    body_out = {"publicationId": pub_id, "status": "accepted" if accepted else "rejected", "runStatus": run_status, "gates": gates,
                "predictions": [f"{pub_id}:{p['ticker']}" for p in (result or {}).get("predictions", [])] if accepted else []}
    return (201 if accepted else 422), body_out


def latest_for_ticker(conn: sqlite3.Connection, ticker: str) -> dict[str, Any]:
    """Locator for bots: the prediction for `ticker` in the latest accepted publication."""
    ensure_schema(conn)
    row = conn.execute(
        "SELECT id, market_date, run_status, received_at, gates_json FROM research_publications"
        " WHERE tenant_id = ? AND status = 'accepted' ORDER BY market_date DESC, received_at DESC LIMIT 1",
        (TENANT,),
    ).fetchone()
    t = str(ticker).strip().upper()
    if not row:
        return {"found": False, "ticker": t, "reason": "no_accepted_publication"}
    pub_id, market, run_status, received, gates_json = row
    pred = conn.execute("SELECT id FROM predictions WHERE tenant_id = ? AND scored_event_id = ? AND ticker = ?", (TENANT, pub_id, t)).fetchone()
    edge = next((g["detail"] for g in json.loads(gates_json) if g["gate"] == "edge_over_baseline"), {})
    base = {
        "ticker": t, "publicationId": pub_id, "marketDate": market, "publishedAt": received,
        "run": {"intelligenceConfidenceTier": "full" if run_status == "HEALTHY" else "suppressed",
                "rankedUnderDegradedRun": run_status != "HEALTHY", "runStatus": run_status},
        "quality": edge,
    }
    if not pred:
        return {"found": False, "reason": "no_signal", **base}
    return {"found": True, "predictionId": pred[0], **base}


def publication_record(conn: sqlite3.Connection, pub_id: str) -> dict[str, Any] | None:
    ensure_schema(conn)
    row = conn.execute("SELECT id, status, run_status, market_date, publisher_key_id, worker_id, strategy_key, gates_json, received_at FROM research_publications WHERE id = ?", (pub_id,)).fetchone()
    if not row:
        return None
    keys = ["publicationId", "status", "runStatus", "marketDate", "publisherKeyId", "workerId", "strategyKey", "gates", "receivedAt"]
    out = dict(zip(keys, row))
    out["gates"] = json.loads(out["gates"])
    return out
