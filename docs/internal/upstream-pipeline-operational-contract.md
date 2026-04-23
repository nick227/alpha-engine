# Upstream vs Read API: Operational Contract

## Core idea

A healthy **read API** (200s, JSON shape) only proves the service and database are reachable. **Intelligence quality** depends on **upstream batch and job health**: market data, discovery, queue, predictions, ranking materialization, and recommendation rebuild.

The data health artifact is the **RCA + funnel** for that split: *starved upstream* often looks like a *working product* if the API fail-opens to shallow or legacy rows.

## Symptom priority (typical)

1. **Fresh 1d bar coverage** on the **canonical universe** (`target_stocks.yaml` ∪ admitted candidates) below policy — strongest “automation starvation” signal.
2. **`predictions` row count** at 0 while **`ranking_snapshots`** still has rows — rankings are **not** coming from the live prediction pipeline for that snapshot.
3. **`runStatus` / `coverageRatio` / `runQuality`** from `/api/predictions/runs/latest` — trust layer for prediction batch quality, orthogonal to endpoint uptime.

## Canonical upstream order

Intended cadence (see `scripts/windows/run_daily_pipeline.bat`):

1. Price download (**active universe** — `dev_scripts/scripts/download_prices_daily.py`)
2. Discovery nightly
3. Queue rank trim
4. Prediction queue run
5. Materialize predictions
6. Prediction rank (`prediction_rank_sqlite`)
7. Ranking snapshots from predictions (`ranking_snapshots_from_predictions`)
8. Replay / outcomes / optional reports

Any broken early step starves downstream; the read API may still return **narrow** rankings or recommendations built from whatever rows exist.

## Operational policy (encoded in Data Health report)

Configurable in `tests/internal_read_inventory/config.py`:

- **`BAR_COVERAGE_SLA_RATIO`** (default **0.90**) — report prints **PASS/FAIL** for fresh 1d bars vs expected universe.
- **`MIN_RECOMMENDATION_UNIQUE_TICKERS_WARNING`** — breadth guardrail on unique tickers in `/api/recommendations/latest`.

The report adds **stage freshness** timestamps from the warehouse (latest bar among universe symbols, predictions, rankings, consensus, candidate queue activity) so schedulers can answer “when did each layer last write?” without tailing fifteen logs.

## Probability ranking when signals are weak

Likely explanations (investigate in order):

1. **Downloader not targeting canonical universe or not scheduled** — fixed in code (`download_prices_daily.py` defaults to active universe).
2. **Prediction job not running or failing before insert** — check pipeline logs and `predictions.MAX(timestamp)`.
3. **`ALPHA_DB_PATH` / environment mismatch** between jobs — all writers must hit the same DB the API reads.
4. **Provider throttling** — partial batch success; downloader uses smaller chunks + pause + retry.
5. **Filters / admission leaving queue empty or non-admitted** — check `candidate_queue` by status.

Model tuning **after** ingest + prediction pipeline are reliably feeding breadth.

## Hard enforcement (future / product choice)

Explicit **ranking provenance**, **blocking rankings when SLA fails**, or **suppress shallow outputs** require product decisions and API/schema changes — the report documents **policy hints** first; wiring hard gates belongs in pipeline orchestration or API contracts when you are ready.
