# Alpha Engine

Alpha Engine is the deep market data and intelligence repository behind consumer services such as TradeLoom. It runs a daily research-to-prediction pipeline, stores decision evidence in SQLite, and exposes a large internal read API for UI and integrations.

This README is for admins and developers who maintain or extend the server.

## What This System Is

- Data warehouse + decision engine for market intelligence.
- Daily scheduled pipeline for ingestion, discovery, ranking, prediction materialization, and outcome replay.
- Internal read API (`FastAPI`) with market data, recommendations, explainability, and operational health endpoints.
- Primary persistence in `data/alpha.db` (configurable via `ALPHA_DB_PATH`).

## Core Runtime Surfaces

- Internal read API app: `app/internal_read_v1/app.py`
- `/api/*` routes: `app/internal_read_v1/api_routes.py`
- Recommendations model layer: `app/internal_read_v1/recommendations.py`
- Discovery CLI and nightly flow: `app/discovery/discovery_cli.py`
- Prediction ranking: `app/engine/prediction_rank_sqlite.py`
- Ranking snapshot publisher: `app/engine/ranking_snapshots_from_predictions.py`
- Windows daily scheduler script: `scripts/windows/run_daily_pipeline.bat`

## Platform Concepts

- **Discovery**: decides what symbols/situations deserve attention.
- **Prediction**: generates and ranks model outputs with confidence and context.
- **Ranking**: relative priority surface, explicitly non-actionable by itself.
- **Recommendations**: actionable house layer built from ranking + consensus + momentum + admission + quality.
- **Outcomes**: truth layer that closes the loop and supports learning/validation.
- **Operations health**: heartbeat, data-health, and latest run diagnostics for trust and freshness.

## Daily Operating Process

The canonical production script executes this sequence:

1. Validate DB path and acquire lock.
2. Download prices.
3. Sync fundamentals snapshot and company profiles.
4. Run discovery nightly (candidates, admission, watchlist, queue, outcomes/stats).
5. Deep-fill admitted symbols if needed.
6. Rank and trim queue.
7. Run prediction queue.
8. Materialize predictions.
9. Rank prediction rows.
10. Publish `ranking_snapshots`.
11. Replay matured predictions and backfill outcomes.
12. Write health/report artifacts.

Reference implementation: `scripts/windows/run_daily_pipeline.bat`.

## API Summary

Key endpoint families:

- **Market**: `/api/quote/{ticker}`, `/api/history/{ticker}`, `/api/candles/{ticker}`, `/api/stats/{ticker}`, `/api/company/{ticker}`
- **Recommendations**: `/api/recommendations/latest`, `/api/recommendations/best`, `/api/recommendations/{ticker}`, `/api/recommendations/under/{price_cap}`
- **Strategy/Model**: `/api/strategies/catalog`, `/api/strategies/{strategy_id}/stability`, `/api/strategies/{strategy_id}/performance`
- **Experiments/Meta-Ranker**: `/api/experiments/leaderboard`, `/api/experiments/trends`, `/api/experiments/summary`, `/api/experiments/meta-ranker/latest`, `/api/experiments/meta-ranker/intents/latest`, `/api/experiments/meta-ranker/intents/replay`, `/api/experiments/meta-ranker/promotion-readiness`, `/api/experiments/meta-ranker/alt-data/coverage`, `/api/experiments/meta-ranker/strategy-queue-share`
- **Consensus/Attribution/Accuracy**: `/api/consensus/signals`, `/api/ticker/{symbol}/attribution`, `/api/ticker/{symbol}/accuracy`
- **Ops/Freshness**: `/api/system/heartbeat`, `/api/system/data-health`, `/api/predictions/runs/latest`, `/api/predictions/{prediction_id}/context`, `/api/engine/calendar`
- **Ranking Explainability**: `/ranking/top`, `/ranking/movers`, `/ticker/{symbol}/why`, `/ticker/{symbol}/performance`

Deep endpoint behavior and data lineage:

- `docs/public/api-data-warehouse-and-pipelines.md`
- `docs/api/internal-read-server-routes.md`

## Security and Access Model

- Internal-read auth header: `X-Internal-Key`
- Configure via `INTERNAL_READ_KEY`
- Local-only bypass: `INTERNAL_READ_INSECURE=1` (development only)

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
npm run read-api
npm run dashboard
```

## Operations Commands

- Full daily batch: `scripts/windows/run_daily_pipeline.bat`
- Discovery nightly: `python -m app.discovery.discovery_cli nightly --db data/alpha.db --tenant-id default`
- Queue run: `python -m app.engine.prediction_cli run-queue --as-of YYYY-MM-DD --db data/alpha.db --tenant-id default`
- Prediction ranking: `python -m app.engine.prediction_rank_sqlite --as-of YYYY-MM-DD --db data/alpha.db --tenant-id default`
- Snapshot publish: `python -m app.engine.ranking_snapshots_from_predictions --as-of YYYY-MM-DD --db data/alpha.db --tenant-id default`

## Data Warehouse Tables

- Predictions lifecycle: `predictions`, `prediction_runs`, `prediction_outcomes`, `prediction_scores`
- Ranking + consensus: `ranking_snapshots`, `consensus_signals`
- Selection + recommendations: `candidate_queue`, `house_recommendations`
- Market layer: `price_bars`, `fundamentals_snapshot`, company profiles on disk
- Ops observability: `loop_heartbeats`, `reports/pipeline-last-status.txt`

## Documentation Map

### Public

- `docs/public/api-data-warehouse-and-pipelines.md`
- `docs/public/components.md`
- `docs/public/jobs-and-scheduling.md`
- `docs/public/how-discovery-and-playbooks-work.md`
- `docs/public/how-predictions-work.md`
- `docs/public/how-to-use-and-evaluate.md`
- `docs/public/data-sources-at-a-glance.md`
- `docs/public/cli.md`

### Internal

- `docs/internal/alpha-engine-internal-read-api-v1.md`
- `docs/internal/upstream-pipeline-operational-contract.md`
- `docs/internal/ops/daily-process.md`
- `docs/internal/system-design.md`
- `docs/internal/strategies-overview.md`
- `docs/internal/audit/schema-map.md`
- `docs/internal/audit/data-lineage.md`

## Maintainer Checklist

- Verify `reports/pipeline-last-status.txt`
- Verify `/api/system/data-health` and `/api/predictions/runs/latest`
- Validate `/ranking/top` quality + confidence tier
- Confirm ranking/recommendation freshness against schedule

Alpha Engine is most reliable when operated as a disciplined daily pipeline with explicit observability checks and strict separation between ranking context and actionable recommendation layers.
