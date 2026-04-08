# v2.1 changelog

## Consolidation
- cleaned scoring + MRA flow into smaller helper functions
- aligned summary math with prediction-first architecture
- standardized timestamps and horizon handling

## Fixes
- prediction timestamps now anchor to source event time
- added strategy config validation during load
- added persistence helpers for SQLite-backed local runs
- added confidence-weighted accuracy metric

## Improvements
- richer deterministic category tagging
- stronger explanation term extraction
- dashboard input remains CSV-based for dead-simple inspection
