# Window & memory decisions (one-pager)

**One-line truth:** you are not choosing one history length—you are designing **how the system remembers** at each layer.

**Naming:** prefer **discovery memory** over vague “history.” The milestone CLI flag remains `--history-days`; treat it mentally as **`discovery_memory_days`** (how far back the **resurfacing sweep** reaches).

---

## Operating model

1. **Freeze one layer, tune another** — do not optimize everything at once. Example: tune **discovery memory** while **ranking / rank_score** recipe is fixed; then rotate.
2. **Condition by regime** — avoid one global answer. Example heuristic: **high VIX → shorter memory** for noisy layers; **calm → longer memory** where stationarity holds. Implement via buckets (VIX / regime from `ranking_context_json` / bars), not hand-waving.
3. **Retune cadence (suggested defaults)**

| Cadence | What typically moves |
|--------|----------------------|
| **Daily** | Rank context / temporal adjustment inputs (as-of freshness, VIX bar age) |
| **Weekly** | Strategy performance stats rollups, supplemental thresholds |
| **Monthly** | Discovery **memory** (resurfacing sweep span), milestone parameters |
| **Quarterly** | Universe structure (`target_stocks.yaml`, discovery universe rules, admission caps) |

---

## Decision matrix (maintain this)

| Decision | Knob (examples) | Metric (pick one primary) | Source tables | Retune frequency |
|----------|-----------------|----------------------------|---------------|------------------|
| **Outcome horizon** | `predictions.horizon`; exit / label window in outcome scoring | Calibration vs forward return or direction hit rate at that H | `predictions`, `prediction_outcomes` | **Quarterly** or when execution horizon changes |
| **Strategy stats lookback** | `strategy_performance.horizon` rows (e.g. ALL, 5d, 20d); discovery stats horizons in CLI | Rank quality: rank_score vs realized outcomes **out-of-sample**; stability of accuracy | `strategy_performance`, `strategy_stability`, `predictions`, `prediction_outcomes` | **Weekly** (rolling) to **monthly** (policy) |
| **Discovery resurfacing memory** | **`discovery_memory_days`** (CLI `--history-days`), `--step-days`, `--top-n`, `--min-adv` | Diversity + stability of `candidate_queue`; admission quality; runtime budget | `candidate_queue`, `admission_metrics`, (inputs) `price_bars` | **Monthly** milestone / soak |
| **Ranking snapshot alignment** | Daily step order: rank predictions → persist `ranking_snapshots` | Movers / top-N explainability match ranked day | `predictions`, `ranking_snapshots` | **Daily** (automated in daily batch) |
| **Admission policy** | caps, overrule thresholds, per-lens / mcap | Swaps rate, lens mix, weakest admitted vs promoted | `admission_metrics`, `candidate_queue` | **Monthly** with discovery memory reviews |
| **Universe structure** | static list + dynamic admits | Coverage vs mandate; correlation budget | `candidate_queue`, config `target_stocks.yaml` | **Quarterly** |

---

## Highest-ROI window choices (now)

1. **Outcome horizon** — defines the label your whole stack is trying to hit.
2. **Strategy stats lookback** — feeds `prediction_rank_sqlite` lookups and thus **picks**.
3. **Discovery resurfacing memory** (`discovery_memory_days` / step) — who keeps getting **surfaced** into the queue over time.

Ranking **context** (VIX age, temporal multiplier) is high leverage for **day-of** behavior but is a different knob than multi-month discovery memory.

---

## Tables reference

| Table | Role in memory story |
|-------|------------------------|
| `predictions` | Per-signal rows, `rank_score`, `ranking_context_json`, `horizon` |
| `prediction_outcomes` | Realized label for horizon / calibration |
| `candidate_queue` | Discovery memory **surface area** (who was seen / admitted) |
| `admission_metrics` | Admission runs: swaps, thresholds, lens mix |
| `ranking_snapshots` | Persisted top ordering for movers / top-N read API (from ranked predictions) |

---

## Blunt recommendation

Stop optimizing **one number**. Maintain the matrix, **freeze other layers** when tuning one, and let **regime** split policies when data supports it.
