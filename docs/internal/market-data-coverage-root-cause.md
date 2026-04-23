# Market Data Coverage Root Cause (Internal Read / Rankings)

## Symptom

Data health reconciliation showed **23 expected universe symbols** but only **~3 with fresh `1d` bars**, yielding shallow rankings (~3 rows) and concentrated recommendations.

## Root cause

**Universe mismatch between the daily price downloader and the trading/active universe.**

| Surface | Symbol source |
|--------|----------------|
| Rankings, recommendations, `expectedUniverseCount`, coverage ratio | `get_active_universe_tickers()` → `config/target_stocks.yaml` ∪ admitted `candidate_queue` |
| **Previous** `download_prices_daily.py` | **`feature_snapshot` table only** |

If `feature_snapshot` contains few symbols (or none), Yahoo downloads ran only for that small set. The other watchlist symbols **never received `price_bars`**, so `build_stats_payload` / ranking inputs saw no market data for most names.

This is **not** primarily provider failure or tenant mismatch: the job simply **did not target** the same symbol list the product uses.

## Fix (code)

`dev_scripts/scripts/download_prices_daily.py` **defaults** the download list to:

**active universe only** (`get_active_universe_tickers`)

so it matches rankings, recommendations, and `coverageRatio`.

Optional **`--include-feature-snapshot`** restores the legacy behavior (add every distinct `feature_snapshot` symbol). Use that only for ML backfills — large DBs can have thousands of symbols and will hit Yahoo rate limits.

Also:

- Default DB path respects **`ALPHA_DB_PATH`** when set (aligned with the internal read API).

## Operational verification

1. Backfill recent history so incremental fetch has a baseline (first run after fix):

   ```powershell
   .\.venv\Scripts\python.exe dev_scripts\scripts\download_prices_daily.py --db data\alpha.db --days 45
   ```

2. Re-run downstream steps (or full daily pipeline) so `ranking_snapshots` and recommendations refresh:

   ```powershell
   .\scripts\windows\run_daily_pipeline.bat
   ```

3. Re-run data health or query: fresh `1d` bar count for the 23-symbol universe should move toward **20+/23** (subject to Yahoo availability and symbol validity).

## What we ruled out (for this incident)

- **Tenant mismatch**: bars and reads both use `tenant_id='default'` in the downloader and read API paths reviewed.
- **Stale filter in health report**: reconciliation uses a 7-day fresh window on **latest `1d` bar**; the gap was missing rows, not the window alone.

## Residual risks

- **yfinance** batch failures or rate limits: downloader logs `got data for X/Y`; retry or smaller batches if needed.
- **bad / delisted symbols** in YAML: may never return data; trim config if persistent.
