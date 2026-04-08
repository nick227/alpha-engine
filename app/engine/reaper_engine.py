from __future__ import annotations


def should_reap(strategy_snapshot: dict) -> tuple[bool, str]:
    stability = float(strategy_snapshot.get("stability_score", 0.0))
    consecutive = int(strategy_snapshot.get("consecutive_bad_windows", 0))
    parent_delta = float(strategy_snapshot.get("parent_underperformance_pct", 0.0))

    if stability < 0.60 and consecutive >= 3:
        return True, "Stability death: below 0.60 for 3 consecutive windows"

    if parent_delta < -15.0:
        return True, "Performance death: underperforming parent by >15%"

    return False, ""
