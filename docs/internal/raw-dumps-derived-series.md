# Raw Dumps -> Derived Series

This repo can ingest certain "non-price" datasets (options, earnings, shorts, breadth) by converting them into **derived daily series** written into `price_bars` with synthetic tickers.

Why: ML feature generation already knows how to read `source: price` from `price_bars`. By loading these datasets into `price_bars`, we can add ML factors without introducing new DB tables.

Loader script: `scripts/load_derived_series.py`

## Derived Ticker Conventions

- Options / positioning: `OPT:{SYM}:{METRIC}`
- Earnings / expectations: `EARN:{SYM}:{METRIC}`
- Short interest / crowding: `SHORT:{SYM}:{METRIC}`
- Market internals: `INT:{VENUE}:{METRIC}`

All derived series are stored as `timeframe='1d'` with `open=high=low=close=value` and `volume=0`.

Writes are idempotent via `INSERT OR IGNORE` on `(tenant_id,ticker,timeframe,timestamp)`.

## CSV Formats

### 1) Options (`--kind options`)

Supported inputs:
- A single CSV (combined): `data/raw_dumps/options/options_metrics.csv`
- A directory of per-symbol CSVs (recommended for download jobs): `data/raw_dumps/options/`

Downloader (Polygon): `scripts/download_polygon_options_daily.py` writes:
- `data/raw_dumps/options/{SYMBOL}_options_daily.csv`

Required columns:
- `ticker` (or `symbol`)
- `date`

Recommended metric columns (any numeric columns are accepted):
- `iv` (from `scripts/download_polygon_options_daily.py`)
- `iv_rank`
- `gamma`
- `put_call_ratio`
- `oi`
- `volume`
- `iv_7d` (ATM-ish IV proxy near ~7D expiry)
- `iv_30d` (ATM-ish IV proxy near ~30D expiry)
- `iv_term_slope` (`iv_30d - iv_7d`)
- `iv_skew` (OTM put IV minus OTM call IV, same expiry)

### 2) Earnings (`--kind earnings`)

Path suggestion: `data/raw_dumps/earnings/earnings_metrics.csv`

Required columns:
- `ticker`
- `date` (earnings report date or the date the metric becomes known)

Recommended metric columns:
- `eps_surprise_pct`
- `rev_surprise_pct`
- `guidance_delta_pct`
- `estimate_revision_30d`

Tip: use `--forward-fill-days 90` so the last known surprise/revision persists until the next report.

### 3) Shorts (`--kind shorts`)

Supported inputs:
- A single CSV (combined): `data/raw_dumps/short_interest/short_interest.csv`
- A directory of per-symbol CSVs (recommended for download jobs): `data/raw_dumps/shorts/`

Downloader (FMP): `scripts/download_fmp_shorts.py` writes:
- `data/raw_dumps/shorts/{SYMBOL}_shorts.csv`

Required columns:
- `ticker` (or `symbol`)
- `date`

Recommended metric columns:
- `short_float`
- `days_to_cover`
- `borrow_rate`
- `utilization`
- `short_volume` (optional)

Tip: use `--forward-fill-days 30` so the last known short metrics persist.

### 4) Internals (`--kind internals`)

Path suggestion: `data/raw_dumps/internals/breadth.csv`

Required columns:
- `date`

Recommended metric columns:
- `advancers`
- `decliners`
- `new_highs`
- `new_lows`
- `nh_nl` (new_highs - new_lows)

The loader writes `INT:NYSE:{METRIC}` by default.

## Examples

Dry-run:
```bash
python scripts/load_derived_series.py --kind options --input data/raw_dumps/options/options_metrics.csv --dry-run
```

Directory input (per-symbol dumps):
```bash
python scripts/load_derived_series.py --kind options --input data/raw_dumps/options --dry-run
```

Load options into `ml_train` tenant and forward-fill one month:
```bash
python scripts/load_derived_series.py --kind options --input data/raw_dumps/options/options_metrics.csv --tenant ml_train --forward-fill-days 30
```
