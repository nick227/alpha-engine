"""
Diversity-aware admission: candidate_queue → status='admitted' (dynamic slice of active universe).

Ranking still uses only static YAML ∪ admitted; this does not feed ranking scores.

When at cap, optional overrule swaps weakest admitted for high-potential eligibles.
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


def _fetch_admitted_rows(repo: AlphaRepository, tenant_id: str) -> list[dict[str, Any]]:
    rows = repo.conn.execute(
        """
        SELECT ticker, multiplier_score, discovery_score, market_cap_bucket
        FROM candidate_queue
        WHERE tenant_id = ? AND status = 'admitted'
        """,
        (tenant_id,),
    ).fetchall()
    return [dict(x) for x in rows]


def _overrule_at_cap(
    repo: AlphaRepository,
    *,
    tenant_id: str,
    max_admitted: int,
    eligible_statuses: tuple[str, ...],
    overrule_min_multiplier: float,
    overrule_min_discovery_score: float,
    max_overrule_swaps: int,
) -> dict[str, Any]:
    """
    If admitted count >= max_admitted, swap weakest admitted for best eligible 'stars'
    that meet minimum multiplier + discovery_score and beat the weakest admitted.
    """
    row = repo.conn.execute(
        "SELECT COUNT(*) AS n FROM candidate_queue WHERE tenant_id = ? AND status = ?",
        (tenant_id, "admitted"),
    ).fetchone()
    n_admitted = int(row["n"] if row else 0)
    if n_admitted < int(max_admitted):
        return {"ran": False, "reason": "below_cap", "swaps": []}

    ph = ",".join(["?"] * len(eligible_statuses))
    pool = repo.conn.execute(
        f"""
        SELECT ticker, status, discovery_lens, market_cap_bucket, multiplier_score, discovery_score, signal_count
        FROM candidate_queue
        WHERE tenant_id = ?
          AND status IN ({ph})
        """,
        (tenant_id, *eligible_statuses),
    ).fetchall()
    stars: list[dict[str, Any]] = []
    for x in pool:
        r = dict(x)
        m = r.get("multiplier_score")
        d = r.get("discovery_score")
        if m is None or d is None:
            continue
        if float(m) < float(overrule_min_multiplier):
            continue
        if float(d) < float(overrule_min_discovery_score):
            continue
        stars.append(r)
    stars.sort(key=_sort_key, reverse=True)

    admitted = _fetch_admitted_rows(repo, tenant_id)
    admitted.sort(key=_sort_key)  # weakest first

    swaps: list[dict[str, str]] = []
    for _ in range(max(0, int(max_overrule_swaps))):
        if not stars or not admitted:
            break
        best = stars[0]
        worst = admitted[0]
        bt = str(best.get("ticker") or "").strip().upper()
        wt = str(worst.get("ticker") or "").strip().upper()
        if not bt or not wt or bt == wt:
            stars.pop(0)
            continue
        if _sort_key(best) <= _sort_key(worst):
            break

        repo.conn.execute(
            """
            UPDATE candidate_queue
            SET status = 'recurring'
            WHERE tenant_id = ? AND ticker = ? AND status = 'admitted'
            """,
            (tenant_id, wt),
        )
        repo.conn.execute(
            f"""
            UPDATE candidate_queue
            SET status = 'admitted'
            WHERE tenant_id = ? AND ticker = ? AND status IN ({ph})
            """,
            (tenant_id, bt, *eligible_statuses),
        )
        repo.conn.commit()
        swaps.append({"demoted": wt, "admitted": bt})
        stars = [s for s in stars if str(s.get("ticker") or "").strip().upper() != bt]
        admitted = _fetch_admitted_rows(repo, tenant_id)
        admitted.sort(key=_sort_key)

    return {
        "ran": True,
        "reason": "at_cap_overrule",
        "swaps": swaps,
        "count": len(swaps),
    }


def run_diversity_admission(
    repo: AlphaRepository,
    *,
    tenant_id: str = "default",
    max_admitted: int = 20,
    per_lens_cap: int = 4,
    per_mcap_cap: int | None = 5,
    eligible_statuses: tuple[str, ...] = ("recurring", "shortlisted"),
    overrule_at_cap: bool = True,
    overrule_min_multiplier: float = 0.78,
    overrule_min_discovery_score: float = 0.72,
    max_overrule_swaps: int = 3,
) -> dict[str, Any]:
    """
    Promote eligible rows to admitted until the dynamic cap is reached.

    1) Per discovery_lens (sorted): up to per_lens_cap tickers (best by multiplier_score), respecting per_mcap_cap.
    2) Global sort: fill remaining slots, respecting per_mcap_cap.
    3) If slots remain: fill by global sort without mcap cap (avoid starving).
    4) If at cap: optional overrule — swap weakest admitted for high-threshold eligibles.
    """
    row = repo.conn.execute(
        "SELECT COUNT(*) AS n FROM candidate_queue WHERE tenant_id = ? AND status = ?",
        (tenant_id, "admitted"),
    ).fetchone()
    n_admitted = int(row["n"] if row else 0)
    slots = max(0, int(max_admitted) - n_admitted)

    ph = ",".join(["?"] * len(eligible_statuses))
    selected: list[str] = []

    if slots > 0:
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

        for lens in sorted(by_lens.keys()):
            if len(selected) >= slots:
                break
            taken = 0
            for r in by_lens[lens]:
                if len(selected) >= slots or taken >= int(per_lens_cap):
                    break
                t = str(r.get("ticker") or "").strip().upper()
                b = str(r.get("market_cap_bucket") or "unknown")
                if not t or t in used:
                    continue
                if take(t, b):
                    taken += 1

        for r in pool:
            if len(selected) >= slots:
                break
            t = str(r.get("ticker") or "").strip().upper()
            b = str(r.get("market_cap_bucket") or "unknown")
            if not t or t in used:
                continue
            take(t, b)

        if len(selected) < slots and per_mcap_cap is not None:
            for r in pool:
                if len(selected) >= slots:
                    break
                t = str(r.get("ticker") or "").strip().upper()
                if not t or t in used:
                    continue
                selected.append(t)
                used.add(t)

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

    overrule: dict[str, Any] = {"ran": False}
    if overrule_at_cap and max_overrule_swaps > 0:
        overrule = _overrule_at_cap(
            repo,
            tenant_id=tenant_id,
            max_admitted=max_admitted,
            eligible_statuses=eligible_statuses,
            overrule_min_multiplier=overrule_min_multiplier,
            overrule_min_discovery_score=overrule_min_discovery_score,
            max_overrule_swaps=max_overrule_swaps,
        )

    if not selected and slots > 0:
        return {
            "ok": True,
            "slots": slots,
            "newly_admitted": [],
            "reason": "no_eligible",
            "max_admitted": int(max_admitted),
            "already_admitted_before": n_admitted,
            "overrule": overrule,
        }

    row2 = repo.conn.execute(
        "SELECT COUNT(*) AS n FROM candidate_queue WHERE tenant_id = ? AND status = ?",
        (tenant_id, "admitted"),
    ).fetchone()
    n_after = int(row2["n"] if row2 else 0)

    out: dict[str, Any] = {
        "ok": True,
        "slots": slots,
        "newly_admitted": selected,
        "count": len(selected),
        "max_admitted": int(max_admitted),
        "already_admitted_before": n_admitted,
        "admitted_after": n_after,
        "overrule": overrule,
    }
    if slots == 0 and not selected:
        out["reason"] = "at_cap"
    return out
