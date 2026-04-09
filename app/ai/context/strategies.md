# Strategies Logic mapping
- **Architecture**: Strategies belong to one of two parent tracks: `Sentiment` or `Quant`. They operate in multiple regimes (e.g., breakout, mean-reversion).
- **Status Lifecycle**: Strategies progress through `CANDIDATE` -> `PROBATION` -> `ACTIVE`. Underperformers move to `DEGRADED` or `ARCHIVED`.
- **Promotion Engine**: A strategy is tagged as `isChampion: true` when it successfully clears performance and stability gates.
- **Scoring Dimensions**: Evaluated based on `forwardScore`, `backtestScore`, and live execution `residualAlpha`.
- **Genetics / Evolution**: The platform continuously tests new hyperparameters to spawn candidate children off established champion parents.
