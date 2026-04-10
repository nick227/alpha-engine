# Incident Triage Checklist (Internal)

## Purpose
Provide a fast, consistent triage checklist for ingestion and pipeline incidents.

## Audience
- Operators
- Developers on-call

## When to use this
- Something “broke” (missing data, failing runs, bad outputs, repeated timeouts).

## Prereqs
- Access to logs and the repo environment

---

## 1) Establish scope
- Which subsystem: ingestion, backfill, pipeline, UI?
- Which sources/tickers are impacted?
- When did it start (approx timestamp)?

## 2) Quick reproduction
- Deterministic pipeline demo (no network): `python scripts/demo_run.py`
  - If this fails, the issue is likely local environment/dependencies.
- Ingestion single run: `python -m app.ingest.async_runner`
  - If a specific source fails, capture the `source_id` + `adapter` + error string.

## 3) Data integrity checks
- Check `data/alpha.db` exists and is writable.
- If ingestion is “running” but inserts are zero, suspect:
  - Validation drops (timestamp/text issues)
  - Dedupe (same payload re-ingested)
  - Adapter returning unexpected shapes (normalize failures)

## 4) Source-specific debugging
- Run diagnose (fast adapter check):
  - `python -m app.ingest.diagnose` (network disabled by default)
  - Set `ALPHA_DIAGNOSE_ALLOW_NETWORK=1` to enable fetch calls

## 5) Stabilize
- Temporarily disable failing sources in `config/sources.yaml` (set `enabled: false`) to restore partial service.
- Increase `poll` intervals for noisy sources if rate limits are suspected.

## 6) Document
- Record:
  - git commit hash
  - affected sources and configs
  - exact commands run
  - the first observed bad timestamp and any error logs

