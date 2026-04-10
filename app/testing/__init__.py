"""
Testing module for safe backfill execution.

10-Phase Testing Methodology:
0. Config Validation (adapters, keys, cache, DB)
1. Dry Run (no API calls)
2. Single Slice Test (SPY, 1 day)
3. Calculation Smoke Tests + Weight Shift
4. End-to-End Controlled Flow
5. Prediction Traceability Test (CRITICAL)
6. Cache Validation (output equality)
7. Incremental Backfill
8. Final Pass Criteria
9. Full Backfill

Usage:
    from app.testing import SafeExecutionFramework, TestPhase
    
    framework = SafeExecutionFramework(phase=TestPhase.CONFIG_VALIDATION)
    success = framework.run_phase()
    
    if success:
        framework = SafeExecutionFramework(phase=TestPhase.DRY_RUN)
        success = framework.run_phase()
"""

from app.testing.safe_backfill import (
    SafeExecutionFramework,
    TestPhase,
    ExecutionCounters,
    BudgetGuard,
    ConfigValidator,
    main,
)
from app.testing.pipeline_adapter import (
    PipelineAdapter,
    PipelineContext,
    BudgetExceeded,
)
from app.testing.guarded_fetcher import (
    guarded_fetch,
    FetchContext,
    NetworkBlockedError,
    BudgetExceededError,
    create_guarded_bars_provider,
    create_guarded_ingest_adapter,
    create_guarded_fallback_provider,
    with_retry_and_budget,
    GuardedCache,
)
from app.testing.prediction_integrity import (
    PredictionIntegrityTest,
    PredictionTrace,
)

__all__ = [
    "SafeExecutionFramework",
    "TestPhase",
    "ExecutionCounters",
    "BudgetGuard",
    "ConfigValidator",
    "PipelineAdapter",
    "PipelineContext",
    "BudgetExceeded",
    "guarded_fetch",
    "FetchContext",
    "NetworkBlockedError",
    "BudgetExceededError",
    "create_guarded_bars_provider",
    "create_guarded_ingest_adapter",
    "create_guarded_fallback_provider",
    "with_retry_and_budget",
    "GuardedCache",
    "PredictionIntegrityTest",
    "PredictionTrace",
    "main",
]
