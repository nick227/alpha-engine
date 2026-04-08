# Declarative Data Sources Overlay

This overlay adds a **declarative ingestion framework** so new data sources can be onboarded with a simple config-first process.

## Core idea
All sources are defined in `config/sources.yaml` and normalized into a single `Event` schema.

## Included
- centralized API key manager
- source spec + event model
- adapter registry
- scaffold adapters for:
  - Alpaca
  - Yahoo Finance
  - FRED
  - Reddit
  - Custom developer bundles
- sample `sources.yaml`
- sample `keys.yaml`
- custom bundle example
- demo ingest script

## Add a new source
1. add key config in `config/keys.yaml`
2. add a source block in `config/sources.yaml`
3. create a small adapter under `app/ingest/adapters/`
4. register adapter in `app/ingest/registry.py`

## Unified event schema
```python
Event(
  source_id,
  source_type,
  timestamp,
  ticker,
  text,
  numeric_features,
  tags,
  weight,
  raw_payload
)
```

## Run demo
```bash
python scripts/ingest_sources_demo.py
```
