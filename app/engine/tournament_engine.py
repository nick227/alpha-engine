from __future__ import annotations

from typing import Iterable


def rank_candidates(results: Iterable[dict], metric: str = "stability_score") -> list[dict]:
    return sorted(results, key=lambda row: row.get(metric, 0.0), reverse=True)


def choose_winner(results: Iterable[dict], metric: str = "stability_score") -> dict | None:
    ranked = rank_candidates(results, metric=metric)
    return ranked[0] if ranked else None
