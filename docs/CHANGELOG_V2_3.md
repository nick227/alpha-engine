# v2.3 Changelog

## Main theme
Improved **back-test dual track time analysis**.

## Changes
- added rolling-window and forward-window comparison utilities
- added slice-level metrics for prediction consistency
- added dual-track aggregation (`sentiment`, `quantitative`, `hybrid`)
- clarified separation of:
  - live prediction generation
  - continuous backtest replay
  - optimization / learning loop
- updated runner to expose track-aware execution helpers
- added starter hybrid dual-track strategy config
- added dashboard sections for:
  - rolling slice comparison
  - track stability
  - backtest vs live drift

## Architectural effect
The system now supports:
- live signal generation
- continuous historical replay
- track-level consistency measurement
- future UI proofing for signal trust and stability
