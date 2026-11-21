"""Matched, read-only historical strategy comparison; not proof of live predictive edge."""
from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from typing import Any


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def compare_prediction_strategies(
    conn: sqlite3.Connection, *, tenant_id: str,
    ml_strategy_id: str, rules_strategy_id: str,
    horizon: str | None = None, max_recent_rows: int = 10000
) -> dict[str, Any]:
    """Only compare predictions with the same ticker, horizon, date, target and return.

    Non-paired, quarantined, ambiguous or legacy outcomes lacking a target time
    never contribute. Strategies must be named explicitly: no guesswork based on
    strategy labels. Differences are descriptive, not validated out-of-sample.
    """
    if not ml_strategy_id or not rules_strategy_id or ml_strategy_id == rules_strategy_id:
        raise ValueError("two different strategy IDs are required")
    out: dict[str, Any] = {
        "status": "unavailable", "reason": None, "tenant_id": tenant_id,
        "mlStrategyId": ml_strategy_id, "rulesStrategyId": rules_strategy_id,
        "horizon": horizon, "matchedSamples": 0,
        "mlDirectionalAccuracy": None, "rulesDirectionalAccuracy": None,
        "alwaysUpBaselineAccuracy": None, "mlMinusRulesAccuracy": None,
        "mlMinusBaselineAccuracy": None, "agreementRate": None,
        "coverage": {"mlOutcomes": 0, "rulesOutcomes": 0, "pairs": 0},
        "methodology": {
            "evaluation": "historical_matched_observations",
            "pairing": "ticker_horizon_prediction_date_target_time_identical_return",
            "isProspective": False, "outOfSampleVerified": False,
            "quarantinedExcluded": True, "nonMatchedOutcomesExcluded": True,
            "calibratedProbabilities": False, "returnUnits": "fraction",
        },
    }
    pcols = {"id", "tenant_id", "strategy_id", "ticker", "timestamp", "horizon", "prediction"}
    ocols = {"id", "tenant_id", "prediction_id", "is_quarantined",
             "evaluated_at", "target_time", "direction_correct", "actual_return"}
    if not pcols <= _columns(conn, "predictions") or not ocols <= _columns(conn, "prediction_outcomes"):
        out["reason"] = "required_point_in_time_outcomes_unavailable"
        return out
    args: list[Any] = [tenant_id, tenant_id, ml_strategy_id, rules_strategy_id]
    extra = ""
    if horizon:
        extra = " AND p.horizon = ?"
        args.append(horizon)
    cap = min(max(1, int(max_recent_rows)), 20000)
    args.append(cap + 1)
    try:
        rows = conn.execute(f"""
            SELECT p.id AS prediction_id, p.strategy_id, p.ticker, p.horizon,
                   p.timestamp, p.prediction, o.target_time, o.actual_return,
                   o.direction_correct, o.id AS outcome_id
            FROM prediction_outcomes o JOIN predictions p
              ON p.tenant_id = o.tenant_id AND p.id = o.prediction_id
            WHERE p.tenant_id = ? AND o.tenant_id = ?
              AND p.strategy_id IN (?, ?) AND o.is_quarantined = 0
              AND julianday(o.evaluated_at) >= julianday(p.timestamp)
              AND julianday(o.target_time) >= julianday(p.timestamp)
              {extra}
            ORDER BY o.evaluated_at DESC, o.id DESC LIMIT ?
        """, args).fetchall()
    except sqlite3.Error:
        out["reason"] = "outcomes_query_unavailable"
        return out
    truncated = len(rows) > cap
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows[:cap]:
        direction = str(r["prediction"] or "").lower()
        if direction not in {"up", "down", "buy", "sell"} or r["direction_correct"] not in (0, 1):
            continue
        try:
            ret = float(r["actual_return"])
            if not math.isfinite(ret):
                continue
        except (TypeError, ValueError):
            continue
        date = str(r["timestamp"] or "")[:10]
        target = str(r["target_time"] or "")
        if not date or not target:
            continue
        key = (str(r["ticker"]).upper(), str(r["horizon"]), date, target)
        groups[key][str(r["strategy_id"])].append((int(r["direction_correct"]), ret, direction))
    ml, rules, baseline, agree = [], [], [], []
    for sides in groups.values():
        a, b = sides.get(ml_strategy_id, []), sides.get(rules_strategy_id, [])
        if len(a) != 1 or len(b) != 1:
            continue
        if not math.isclose(a[0][1], b[0][1], rel_tol=1e-8, abs_tol=1e-10):
            continue
        ml.append(a[0][0])
        rules.append(b[0][0])
        baseline.append(int(a[0][1] > 0))
        agree.append(int(a[0][2] == b[0][2]))
    n = len(ml)
    out["matchedSamples"] = n
    out["coverage"] = {
        "mlOutcomes": sum(len(s.get(ml_strategy_id, [])) for s in groups.values()),
        "rulesOutcomes": sum(len(s.get(rules_strategy_id, [])) for s in groups.values()),
        "pairs": n, "truncated": truncated, "maxRecentRows": cap,
    }
    if truncated:
        out["reason"] = "sample_window_truncated"
        return out
    if n < 2:
        out["reason"] = "insufficient_matched_outcomes"
        return out
    ml_acc, rules_acc, baseline_acc = sum(ml)/n, sum(rules)/n, sum(baseline)/n
    out.update({
        "status": "available", "reason": None,
        "mlDirectionalAccuracy": round(ml_acc, 6),
        "rulesDirectionalAccuracy": round(rules_acc, 6),
        "alwaysUpBaselineAccuracy": round(baseline_acc, 6),
        "mlMinusRulesAccuracy": round(ml_acc - rules_acc, 6),
        "mlMinusBaselineAccuracy": round(ml_acc - baseline_acc, 6),
        "agreementRate": round(sum(agree)/n, 6),
        "sampleSizeWarning": "descriptive_only_not_statistically_validated" if n < 30 else None,
    })
    return out
