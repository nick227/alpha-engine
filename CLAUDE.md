# Alpha Engine Quick Map

## Purpose
- Internal read server exposes API views to downstream consumers (UI, automation, integrations).
- System runs a daily scheduled pipeline to ingest market data, run prediction/ranking calculations, and refresh read models.
- ML + discovery/candidate pipelines generate prediction candidates, rank them, and publish ranking/recommendation surfaces.

## Core Runtime Surfaces
- Read API entrypoint: `app/internal_read_v1/app.py`
- API route group (`/api/*`): `app/internal_read_v1/api_routes.py`
- Read service/store layer: `app/ui/middle/dashboard_service.py`, `app/ui/middle/engine_read_store.py`

## Daily Scheduled Pipeline
- Main scheduler script (Windows): `scripts/windows/run_daily_pipeline.bat`
- High-value steps:
  - price/fundamentals/profile ingestion
  - discovery nightly candidate generation
  - queue ranking + prediction materialization
  - prediction ranking + ranking snapshot publish
  - replay/backfill outcomes

## Prediction + Ranking Flow
- Discovery CLI: `app/discovery/discovery_cli.py`
- Queue ranking: `app/engine/queue_rank_trim.py`
- Prediction rank scoring: `app/engine/prediction_rank_sqlite.py`
- Snapshot publish to `ranking_snapshots`: `app/engine/ranking_snapshots_from_predictions.py`
- Recommendation builder/read model: `app/internal_read_v1/recommendations.py`

## Key Data Outputs (warehouse)
- `predictions`, `prediction_runs`, `prediction_outcomes`
- `ranking_snapshots`, `consensus_signals`
- `candidate_queue`, `house_recommendations`
- `price_bars` (market coverage base layer)

## Common Commands
- Start read API: `npm run read-api`
- Run ranking contract smoke: `python dev_scripts/scripts/smoke_ranking_contract.py`
- Run production health snapshot: `python dev_scripts/scripts/generate_data_health_prod_report.py`

## Working Style (repo idioms)
- Prefer extending existing read models/contracts over adding parallel endpoints.
- Keep ranking semantics explicit: ranking is relative priority; recommendations/actions are separate surfaces.
- Reuse pipeline stages/scripts already in daily job before introducing new orchestration.

## Development Style
- Declarative, model driven, fail-fast code
- Frequently commits never push

## Project Objectives
- Data back-end for trading-platform consumer app
- Creating quality market-leading predictions
- Serving data through consistent fast-api