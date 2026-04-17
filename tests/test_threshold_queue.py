from __future__ import annotations

from app.engine.threshold_queue import build_threshold_queue_rows


def test_build_threshold_queue_respects_cap_and_threshold() -> None:
    summary = {
        "strategies": {
            "silent_compounder": {
                "top": [
                    {
                        "symbol": "AAA",
                        "score": 0.95,
                        "metadata": {"close": 25.0},
                    },
                    {
                        "symbol": "BBB",
                        "score": 0.41,
                        "metadata": {"close": 30.0},
                    },
                ]
            },
            "balance_sheet_survivor": {
                "top": [
                    {
                        "symbol": "CCC",
                        "score": 0.88,
                        "metadata": {"close": 15.0},
                    }
                ]
            },
        }
    }
    rows, counts = build_threshold_queue_rows(
        disc_summary=summary,
        as_of_str="2026-04-17",
        target_signals=10,
        per_strategy_cap=5,
        min_confidence=0.42,
        promoted_overrides={
            "silent_compounder": {
                "direction": "UP",
                "horizon_days": 20,
                "min_close": 20.0,
                "priority_base": 20,
            },
            "balance_sheet_survivor": {
                "direction": "UP",
                "horizon_days": 5,
                "min_close": 10.0,
                "max_close": 20.0,
                "priority_base": 15,
            },
        },
        exclude_symbols=set(),
        source_pipeline="test",
    )
    symbols = {r["symbol"] for r in rows}
    assert "BBB" not in symbols
    assert "AAA" in symbols
    assert "CCC" in symbols
    assert sum(counts.values()) == len(rows)
