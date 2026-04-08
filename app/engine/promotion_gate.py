from __future__ import annotations


def passes_forward_gate(
    candidate: dict,
    parent: dict,
    *,
    min_stability_required: float = 0.6,
    min_sample_size: int = 10,
) -> tuple[bool, dict]:
    gate_logs = {
        "candidate_forward_alpha": candidate.get("forward_alpha", 0.0),
        "parent_forward_alpha": parent.get("forward_alpha", 0.0),
        "candidate_stability": candidate.get("stability_score", 0.0),
        "min_stability_required": min_stability_required,
        "min_sample_size": min_sample_size,
    }

    passed = (
        candidate.get("forward_alpha", 0.0) >= parent.get("forward_alpha", 0.0)
        and candidate.get("stability_score", 0.0) >= min_stability_required
        and candidate.get("sample_size", 0) >= max(min_sample_size, parent.get("sample_size", 0) // 2)
    )
    return passed, gate_logs
