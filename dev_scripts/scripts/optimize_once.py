from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd
from dateutil.parser import isoparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository
from app.core.types import RawEvent, StrategyConfig
from app.engine.genetic_optimizer_service import GeneticOptimizerService


def load_sample_events(path: str | Path = "data/sample/raw_events.jsonl") -> list[RawEvent]:
    events: list[RawEvent] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        events.append(
            RawEvent(
                id=str(payload["id"]),
                timestamp=isoparse(str(payload["timestamp"])),
                source=str(payload.get("source", "sample")),
                text=str(payload.get("text", "")),
                tickers=list(payload.get("tickers") or []),
            )
        )
    return events


def load_parent_strategy(path: str | Path) -> StrategyConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return StrategyConfig(
        id=str(payload["id"]),
        name=str(payload["name"]),
        version=str(payload["version"]),
        strategy_type=str(payload["strategy_type"]),
        mode=str(payload.get("mode", "backtest")),
        active=bool(payload.get("active", True)),
        config=dict(payload.get("config") or {}),
    )


def main() -> None:
    repo = Repository("data/alpha.db")
    service = GeneticOptimizerService(repo)

    raw_events = load_sample_events()
    bars = pd.read_csv("data/sample/bars.csv")

    parent = load_parent_strategy("experiments/strategies/technical_vwap_v1.json")
    candidates = service.propose_candidates(parent, max_children=8)

    train_pre, fwd_pre = service.precompute_windows(raw_events=raw_events, bars=bars, forward_ratio=0.3)
    parent_train = service.evaluate_strategy_on_window(strategy=parent, window=train_pre)
    parent_fwd = service.evaluate_strategy_on_window(strategy=parent, window=fwd_pre)

    print(f"Parent: {parent.name} ({parent.strategy_type})")
    print(f"Candidates: {len(candidates)}")

    for cand in candidates:
        service.persist_candidate(parent=parent, candidate=cand, status="CANDIDATE")
        cand_train = service.evaluate_strategy_on_window(strategy=cand, window=train_pre)
        cand_fwd = service.evaluate_strategy_on_window(strategy=cand, window=fwd_pre)
        passed, gate_logs = service.gate_decision(
            parent=parent,
            candidate=cand,
            parent_train=parent_train,
            parent_forward=parent_fwd,
            candidate_train=cand_train,
            candidate_forward=cand_fwd,
            min_stability_required=0.55,
            min_sample_size=2,
        )
        service.record_gate_result(parent=parent, candidate=cand, passed=passed, gate_logs=gate_logs)
        print(f"- {cand.name} -> {'PASS' if passed else 'FAIL'} (forward_avg_return={gate_logs.get('candidate_forward_avg_return')}, stability={gate_logs.get('candidate_stability_score')})")

    repo.close()
    print("Done. Check `promotion_events` and `strategy_state` in `data/alpha.db`.")


if __name__ == "__main__":
    main()
