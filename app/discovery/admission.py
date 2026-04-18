"""
Diversity-aware admission: candidate_queue → status='admitted' (dynamic slice of active universe).

Ranking still uses only static YAML ∪ admitted; this does not feed ranking scores.
"""

from __future__ import annotations

from typing import Any

from app.db.repository import AlphaRepository


def _sort_key(r: dict[str, Any]) -> tuple[float, float, str]:
    m = r.get("multiplier_score")
    d = r.get("discovery_score")
    ms = float(m) if m is not None else float("-inf")
    ds = float(d) if d is not None else float("-inf")
    t = str(r.get("ticker") or "").strip().upper()
    return (ms, ds, t)


def _mcap_counts_admitted(repo: AlphaRepository, tenant_id: str) -> dict[str, int]:
    rows = repo.conn.execute(
        """
        SELECT COALESCE(market_cap_bucket, 'unknown') AS bucket, COUNT(*) AS n
        FROM candidate_queue
        WHERE tenant_id = ? AND status = 'admitted'
        GROUP BY COALESCE(market_cap_bucket, 'unknown')
        """,
        (tenant_id,),
    ).fetchall()
    return {str(r["bucket"]): int(r["n"] or 0) for r in rows}


def run_diversity_admission(
    repo: AlphaRepository,
    *,
    tenant_id: str = "default",
    max_admitted: int = 20,
    per_lens_cap: int = 4,
    per_mcap_cap: int | None = 5,
    eligible_statuses: tuple[str, ...] = ("recurring", "shortlisted"),
) -> dict[str, Any]:
    """
    Promote eligible rows to admitted until the dynamic cap is reached.

    1) Per discovery_lens (sorted): up to per_lens_cap tickers (best by multiplier_score), respecting per_mcap_cap.
    2) Global sort: fill remaining slots, respecting per_mcap_cap.
    3) If slots remain: fill by global sort without mcap cap (avoid starving).
    """
    row = repo.conn.execute(
        "SELECT COUNT(*) AS n FROM candidate_queue WHERE tenant_id = ? AND status = ?",
        (tenant_id, "admitted"),
    ).fetchone()
    n_admitted = int(row["n"] if row else 0)
    slots = max(0, int(max_admitted) - n_admitted)
    if slots == 0:
        return {
            "ok": True,
            "slots": 0,
            "newly_admitted": [],
            "reason": "at_cap",
            "max_admitted": int(max_admitted),
            "already_admitted": n_admitted,
        }

    ph = ",".join(["?"] * len(eligible_statuses))
    rows = repo.conn.execute(
        f"""
        SELECT ticker, status, discovery_lens, market_cap_bucket, multiplier_score, discovery_score, signal_count
        FROM candidate_queue
        WHERE tenant_id = ?
          AND status IN ({ph})
        """,
        (tenant_id, *eligible_statuses),
    ).fetchall()
    pool = [dict(x) for x in rows]
    pool.sort(key=_sort_key, reverse=True)

    by_lens: dict[str, list[dict[str, Any]]] = {}
    for r in pool:
        lens = str(r.get("discovery_lens") or "").strip() or "unknown"
        by_lens.setdefault(lens, []).append(r)
    for lx in by_lens.values():
        lx.sort(key=_sort_key, reverse=True)

    selected: list[str] = []
    used: set[str] = set()
    mcap_totals = _mcap_counts_admitted(repo, tenant_id)

    def mcap_ok(bucket: str) -> bool:
        if per_mcap_cap is None:
            return True
        return int(mcap_totals.get(bucket, 0)) < int(per_mcap_cap)

    def take(ticker: str, bucket: str) -> bool:
        if ticker in used:
            return False
        if not mcap_ok(bucket):
            return False
        selected.append(ticker)
        used.add(ticker)
        mcap_totals[bucket] = mcap_totals.get(bucket, 0) + 1
        return True

    # Pass 1 — per lens
    for lens in sorted(by_lens.keys()):
        if len(selected) >= slots:
            break
        taken = 0
        for r in by_lens[lens]:
            if len(selected) >= slots or taken >= int(per_lens_cap):
                break
            t = str(r.get("ticker") or "").strip().upper()
            b = str(r.get("market_cap_bucket") or "unknown")
            if not t:
                continue
            if t in used:
                continue
            if take(t, b):
                taken += 1

    # Pass 2 — global (mcap-aware)
    for r in pool:
        if len(selected) >= slots:
            break
        t = str(r.get("ticker") or "").strip().upper()
        b = str(r.get("market_cap_bucket") or "unknown")
        if not t or t in used:
            continue
        take(t, b)

    # Pass 3 — fill without mcap if still short
    if len(selected) < slots and per_mcap_cap is not None:
        for r in pool:
            if len(selected) >= slots:
                break
            t = str(r.get("ticker") or "").strip().upper()
            if not t or t in used:
                continue
            selected.append(t)
            used.add(t)

    if not selected:
        return {
            "ok": True,
            "slots": slots,
            "newly_admitted": [],
            "reason": "no_eligible",
            "max_admitted": int(max_admitted),
            "already_admitted": n_admitted,
        }

    for t in selected:
        repo.conn.execute(
            f"""
            UPDATE candidate_queue
            SET status = 'admitted'
            WHERE tenant_id = ? AND ticker = ? AND status IN ({ph})
            """,
            (tenant_id, t, *eligible_statuses),
        )
    repo.conn.commit()

    return {
        "ok": True,
        "slots": slots,
        "newly_admitted": selected,
        "count": len(selected),
        "max_admitted": int(max_admitted),
        "already_admitted_before": n_admitted,
    }
