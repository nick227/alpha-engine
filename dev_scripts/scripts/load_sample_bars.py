from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository


def main(
    *,
    bars_csv: str | Path = "data/sample/bars.csv",
    db_path: str | Path = "data/alpha.db",
) -> None:
    bars_csv = Path(bars_csv)
    if not bars_csv.exists():
        raise FileNotFoundError(bars_csv)

    df = pd.read_csv(bars_csv)
    required = {"ticker", "timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {bars_csv}: {sorted(missing)}")

    repo = Repository(db_path=db_path)
    inserted = 0
    for row in df.itertuples(index=False):
        repo.upsert_price_bar(
            ticker=str(row.ticker),
            timestamp=str(row.timestamp),
            open_price=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=float(row.volume),
        )
        inserted += 1
    repo.close()

    print(f"Upserted {inserted} bars into {db_path}.")


if __name__ == "__main__":
    main()

