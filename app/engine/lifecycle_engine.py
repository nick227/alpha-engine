from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LifecycleDecision:
    promote: bool = False
    rollback: bool = False
    archive: bool = False
    next_status: str = "CANDIDATE"
    reason: str = ""


def candidate_to_probation() -> LifecycleDecision:
    return LifecycleDecision(promote=False, next_status="PROBATION", reason="Passed gate; entering probation")


def probation_to_active() -> LifecycleDecision:
    return LifecycleDecision(promote=True, next_status="ACTIVE", reason="Probation successful")


def rollback_to_parent() -> LifecycleDecision:
    return LifecycleDecision(rollback=True, next_status="ROLLED_BACK", reason="Candidate degraded; rollback to parent")


def archive_candidate() -> LifecycleDecision:
    return LifecycleDecision(archive=True, next_status="ARCHIVED", reason="Candidate failed gate")
