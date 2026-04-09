# Pipeline Flow Architecture
- **Ingestion**: System ingests `RawEvent` items (news, market data) periodically.
- **Scoring**: The scorer processes RawEvents into `ScoredEvent` instances by determining materiality, confidence, direction, and concept tags using a taxonomy.
- **MRA (Multi-Regime Analysis)**: Extracts market features (`MraOutcome`) around the event like return/volume trajectories and regime alignment scoring.
- **Predictions**: Child strategies parse ScoredEvents to output `Prediction` records containing predicted direction, confidence, and target durations.
- **Consensus**: Signals from various sentiment and quantitative strategies are merged in `ConsensusSignal` via a weighted sum array factoring in stability out-of-bounds metrics.
- **Backfill**: Historical regime data populates SQLite `PriceBar` and `RegimePerformance` schemas for regime-aware backtesting.
