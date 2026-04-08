# Alpha Engine POC

Research-first foundation for a news/media + market-reaction strategy lab.

Core flow:

```text
ingest text + market data
→ structured event scoring
→ MRA (market reaction analysis)
→ strategies create predictions
→ evaluate outcomes
→ rank strategies
```

## What is included

- Python scaffold for scoring, MRA, strategies, simulation, and ranking
- Streamlit dashboard starter
- Prisma schema as the canonical data model
- Sample news events and sample OHLCV bars
- Demo scripts for local proof-of-concept runs

## What is intentionally not complete yet

- Live Alpaca ingestion
- Live paper trading
- Production DB wiring
- Authentication / multitenant API
- Real LLM scoring calls

This is the **day-one foundation**, not the finished system.

## Tech choices

- **Python** for research, scoring, strategy logic, and UI
- **SQLite** for local proof-of-concept data
- **Prisma schema** from day one so the model stays portable to Postgres later
- **Streamlit** for a dead-simple analytics UI

## Quick start

### 1. Python environment

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

### 2. Optional Prisma tooling

```bash
npm install
npx prisma format
```

### 3. Run the demo pipeline

```bash
python scripts/demo_run.py
```

This writes:

- `outputs/scored_events.csv`
- `outputs/predictions.csv`
- `outputs/prediction_outcomes.csv`
- `outputs/strategy_performance.csv`

### 4. Launch the dashboard

```bash
streamlit run app/ui/dashboard.py
```

## Recommended build order

1. Replace sample data with real historical price + news ingest
2. Replace heuristic scorer with deterministic + LLM structured scoring
3. Tighten MRA features
4. Add more baseline strategies
5. Connect results to SQLite / Prisma-backed persistence
6. Add paper mode using the same prediction engine

## Directory layout

```text
app/
  core/         scoring + mra + shared types
  engine/       prediction generation, evaluation, ranking
  strategies/   baseline + experimental strategies
  ui/           streamlit dashboard
prisma/         canonical schema
scripts/        demo / local runners
data/sample/    sample events and bars
outputs/        generated reports
```
