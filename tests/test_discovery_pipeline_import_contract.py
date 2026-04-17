"""
Regression guard for the daily pipeline import chain (STEP 2: nightly discovery).

Fails fast if app.discovery.strategies or scripts.analysis wiring breaks again
(ModuleNotFoundError under Task Scheduler with no Python stack in stderr).
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_discovery_strategies_package_loads() -> None:
    from app.discovery.strategies import STRATEGIES, TemporalCorrelationStrategy

    assert isinstance(STRATEGIES, dict)
    assert len(STRATEGIES) >= 1
    assert TemporalCorrelationStrategy.__name__ == "TemporalCorrelationStrategy"


def test_base_strategy_types_usable() -> None:
    from app.discovery.strategies.base_strategy import BaseStrategy, Signal, SignalType

    class _Stub(BaseStrategy):
        def analyze(self, market_data: Dict[str, Any]) -> List[Signal]:
            return [
                Signal(
                    symbol="TEST",
                    signal_type=SignalType.HOLD,
                    strength=0.5,
                    confidence=0.9,
                    timestamp=datetime(2026, 1, 1),
                    metadata={},
                )
            ]

    s = _Stub("stub")
    out = s.analyze({})
    assert len(out) == 1 and out[0].symbol == "TEST"


def test_scripts_analysis_temporal_stack_imports() -> None:
    from scripts.analysis.insights_engine import InsightsEngine
    from scripts.analysis.strategy_performance_periods import StrategyPerformanceAnalyzer
    from scripts.analysis.temporal_correlation_analyzer import TemporalCorrelationAnalyzer

    assert TemporalCorrelationAnalyzer is not None
    assert InsightsEngine is not None
    assert StrategyPerformanceAnalyzer is not None


def test_nightly_discovery_module_imports() -> None:
    import dev_scripts.scripts.nightly_discovery_pipeline as n

    assert hasattr(n, "main")


def test_nightly_discovery_cli_help_exits_zero() -> None:
    script = REPO_ROOT / "dev_scripts" / "scripts" / "nightly_discovery_pipeline.py"
    r = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert r.returncode == 0, r.stderr
    assert "dry-run" in (r.stdout + r.stderr).lower()


def test_discovery_cli_nightly_help_exits_zero() -> None:
    r = subprocess.run(
        [sys.executable, "-m", "app.discovery.discovery_cli", "nightly", "--help"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert r.returncode == 0, r.stderr
    assert "supplement" in (r.stdout + r.stderr).lower()


def test_prediction_cli_run_queue_help_exits_zero() -> None:
    r = subprocess.run(
        [sys.executable, "-m", "app.engine.prediction_cli", "run-queue", "--help"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert r.returncode == 0, r.stderr
