# Alpha Engine UI Redesign — Implementation Plan

Deliverable: this document (`docs/ui_redesign_plan.md`)

## Summary
Redesign the Streamlit UI to match the intended product structure:
- **Main Dashboard** = compact, forward-looking cards driven by `DashboardService.get_top_ten_signals`, plus **per-horizon (1D/7D/30D) consensus** and champion indicators.
- **Intelligence / Predictions Views** = curated exploration views (best picks, dips, compare, overlays, timeline).
- **Signal Audit** = debug/diagnostic UI.
- **Backtest / Strategy Analysis** = run-scoped ranking + variance + champion vs challenger.
- **Data Controls** = global sidebar controls (tenant/run/ticker/timeframe/strategy/horizon).
- **Chat Assistant (v1)** = stateless, reads 4 md files only, app navigation help, no history.

This redesign includes read-model + DB evolution to support per-horizon consensus.

## IA Overview (current structure)
1. **Main Dashboard**
   - top predictions (buy/sell with confidence)
   - strategy consensus view
   - ticker selection + horizon
   - champion strategy indicators
2. **Intelligence / Predictions Views**
   - best picks
   - dips / reversals
   - compare tickers
   - strategy overlays
   - timeline of predictions
3. **Signal Audit (debug UI)**
   - adapter activity (which sources fired)
   - raw event stream
   - strategy leaderboard
   - prediction log with hit rates
4. **Backtest / Strategy Analysis**
   - strategy ranking
   - hit rate
   - alpha / efficiency
   - sample counts
   - champion vs challenger
5. **Data Controls**
   - tenant / run selection
   - ticker filter
   - timeframe filter
   - strategy filter
6. **Planned Chat Assistant**
   - stateless
   - reads 4 md files
   - app navigation help
   - explains predictions
   - no history

## Supported entrypoint
Use a single Streamlit entrypoint:
- `streamlit run app/ui/app.py`

## Data Controls contract
Global sidebar state lives in `st.session_state.ui_filters`:
```python
{
  "tenant_id": str,
  "run_id": str | None,       # None means "Latest"
  "ticker": str | None,
  "timeframe": str,           # "1M" | "3M" | "6M" | "1Y"
  "horizon_days": int,        # 1 | 7 | 30
  "strategy_id": str | None,
}
```
All pages read from this dictionary; they should not introduce duplicate top-level filters unless strictly page-specific.

## Per-horizon consensus spec
- Canonical horizon keys: `["1d", "7d", "30d"]`
- Dashboard renders **1D / 7D / 30D** consensus cards.
- Missing data behavior: explicit “No data” (no fallback).

API:
```python
DashboardService.get_consensus_by_horizon(
  *, tenant_id: str, ticker: str, horizons: list[str]
) -> dict[str, ConsensusView | None]
```

## DB / Read-model evolution
### A) Add `horizon` to `consensus_signals`
`consensus_signals` is a UI read-model convenience table. It must support multiple horizons per ticker.

Migration approach:
- Best-effort additive migration at runtime:
  - `ALTER TABLE consensus_signals ADD COLUMN horizon TEXT;`

### B) Horizon-aware reads
Extend:
- `EngineReadStore.get_latest_consensus(..., horizon: str | None = None)`
- Add helper:
  - `EngineReadStore.get_latest_consensus_by_horizon(...)`

### C) Ensure the pipeline produces 1d/7d/30d consensus rows
The engine should emit predictions for horizons `1d`, `7d`, `30d` and materialize consensus rows per horizon.

Acceptance checks:
- `SELECT DISTINCT horizon FROM predictions` includes `1d`, `7d`, `30d`
- `consensus_signals` contains rows with `horizon` populated for those horizons.

## Test plan
- DB/read-model:
  - Migration safe on existing DBs.
  - `get_latest_consensus(..., horizon="7d")` returns correct row or `None`.
- UI smoke:
  - Import smoke for `app/ui/app.py`.
  - Dashboard handles missing 1d/7d/30d consensus as “No data”.

## Acceptance checklist (manual)
- `streamlit run app/ui/app.py`
- Each nav item exists: Main Dashboard / Intelligence-Predictions / Intelligence Hub / Backtest / Signal Audit / Chat Assistant.
- Data Controls apply across pages without losing state.
- Dashboard:
  - Top cards come from `get_top_ten_signals` (horizon does not rerank).
  - 1D/7D/30D consensus panel renders and shows “No data” when missing.
  - Champion indicators show track champions and efficiency champion (when available).

