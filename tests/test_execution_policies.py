from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from app.core.execution_policies import apply_execution_policy


def test_clustering_constraint_keeps_strongest_signals() -> None:
    idx = pd.to_datetime([datetime(2026, 1, 1), datetime(2026, 1, 2)])
    signals = pd.DataFrame(
        {
            "AAA": [0.2, 0.1],
            "BBB": [1.0, 0.0],
            "CCC": [-0.8, -0.3],
        },
        index=idx,
    )
    constrained = apply_execution_policy(signals, "clustering", max_positions=1)
    assert constrained.loc[idx[0], "BBB"] != 0
    assert constrained.loc[idx[0], "AAA"] == 0
    assert constrained.loc[idx[0], "CCC"] == 0


def test_sizing_equal_preserves_direction_and_zeros() -> None:
    idx = pd.to_datetime([datetime(2026, 1, 1)])
    signals = pd.DataFrame({"AAA": [1.0], "BBB": [-1.0], "CCC": [0.0]}, index=idx)
    sized = apply_execution_policy(signals, "sizing", base_size=0.5, sizing_method="equal")
    assert sized.loc[idx[0], "AAA"] == 0.5
    assert sized.loc[idx[0], "BBB"] == -0.5
    assert sized.loc[idx[0], "CCC"] == 0.0


def test_apply_execution_policy_unknown_type_raises() -> None:
    with pytest.raises(ValueError):
        apply_execution_policy(pd.DataFrame(), "nope")

