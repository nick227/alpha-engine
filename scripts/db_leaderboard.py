from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.repository import Repository


def main(db_path: str | Path = "data/alpha.db") -> None:
    repo = Repository(db_path=db_path)

    df = repo.query_df(
        """
        SELECT
          s.name as strategy_name,
          s.version as strategy_version,
          s.strategy_type as strategy_type,
          p.horizon as horizon,
          COUNT(*) as total_predictions,
          AVG(CAST(o.direction_correct AS REAL)) as accuracy,
          AVG(o.return_pct) as avg_return
        FROM predictions p
        JOIN prediction_outcomes o ON o.prediction_id = p.id
        JOIN strategies s ON s.id = p.strategy_id
        WHERE p.tenant_id = 'default' AND o.tenant_id = 'default' AND s.tenant_id = 'default'
        GROUP BY s.name, s.version, s.strategy_type, p.horizon
        ORDER BY accuracy DESC, avg_return DESC, total_predictions DESC
        """
    )

    repo.close()

    if df.empty:
        print(f"No rows found in {db_path}. Run `python scripts/demo_run.py` first.")
        return

    df["strategy"] = df["strategy_type"].astype(str) + ":" + df["strategy_version"].astype(str)
    out = df[["strategy", "horizon", "total_predictions", "accuracy", "avg_return"]]
    pd.set_option("display.max_rows", 200)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
