from __future__ import annotations

import json
from pathlib import Path
import sys

from dateutil.parser import isoparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository
from app.core.types import RawEvent


def main(
    *,
    events_jsonl: str | Path = "data/sample/raw_events.jsonl",
    db_path: str | Path = "data/alpha.db",
) -> None:
    events_jsonl = Path(events_jsonl)
    if not events_jsonl.exists():
        raise FileNotFoundError(events_jsonl)

    repo = Repository(db_path=db_path)
    inserted = 0
    for line in events_jsonl.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        evt = RawEvent(
            id=str(payload["id"]),
            timestamp=isoparse(str(payload["timestamp"])),
            source=str(payload.get("source", "sample")),
            text=str(payload.get("text", "")),
            tickers=list(payload.get("tickers") or []),
            metadata=dict(payload.get("metadata") or {}),
        )
        repo.persist_raw_event(evt)
        inserted += 1
    repo.close()

    print(f"Upserted {inserted} raw events into {db_path}.")


if __name__ == "__main__":
    main()

