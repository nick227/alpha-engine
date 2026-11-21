# TradeLoom recommendation integrity contract v2

Alpha Engine is TradeLoom's internal intelligence service. This change is limited to publication semantics, quality gates, consumer adapters, and tests. It does not establish predictive advantage or introduce calibration, strategies, execution behavior, data backfills, or historical migrations.

## Verified defects

Original references below are against Alpha Engine `7f75bc0` and Trading Platform `64f9b04`, before this change. Current implementation references use repository-relative links.

| Original location | Verified defect | Correction |
| --- | --- | --- |
| `app/internal_read_v1/recommendations.py:534,590` | Absolute adjusted composite multiplied by 100, clipped to 5–99, published as confidence. | [Serializer](../../app/internal_read_v1/recommendations.py): nullable legacy confidence; explicit uncalibrated heuristic score and unavailable success probability. Sorting uses absolute score directly. |
| `recommendations.py:176,196,234` | One daily move defined Low/Moderate/High risk; fixed percentage entry bands and additional percentage stop levels implied analytical precision. Missing daily change became Low risk. | Risk, entry zone, expected return are null; avoidIf is empty, with explicit availability reasons. |
| `recommendations.py:205,481,535` | Template sentences described a thesis; generated before final selection adjustment, potentially disagreeing with final direction. | Deterministic component explanations are generated from final score; no investment thesis is claimed. |
| `recommendations.py:784–801` | Profile/age/price-position blend named `fiftyDollarPotential`; distance below a historical high named `undervaluation`. Neither establishes a price target or fair value. | Legacy fields null; renamed descriptive proxy scores and methodology metadata. |
| `recommendations.py:624,674,823` | Long-only could fall back to SELL/HOLD; under-price results filled shortfalls with ineligible near misses. | No direction fallback or near-miss promotion. Short lists are valid. |
| `app/core/pipeline_gates.py:116–117,146–149` | Any historical prediction meant pipeline active; missing-prediction blocking opt-in. | Recent prediction timestamps required; essential publication checks cannot be disabled by environment configuration. |
| `app/internal_read_v1/api_routes.py:436–573` | Latest, ticker, under-price paths did not consistently enforce recommendation quality; best checked only optional missing predictions. | All share publication gate through the builder. Suppressed rows persist as WATCH and serialize as non-actionable. Best returns 503 if no eligible recommendation. |
| `app/internal_read_v1/intelligence_read.py:384,395` | Latest run ordered by forecast end; an older run with a farther horizon could hide a newer run. | Order by run creation; publication also verifies actual queue-job completion. |
| `app/internal_read_v1/app.py:241–317` | Driver/risk `percentile` fields were scaled individual scores, not empirical percentiles. | Deprecated fields null; `normalizedScore` and explicit heuristic methodology. Actual peer percentiles remain. |
| Trading Platform `server/src/clients/engine.js:754–789` | Adapter rejected `degraded`, but accepted absent/limited/suppressed metadata; legacy confidence promoted to order-signal confidence. | Require healthy, recent, explicit envelope and row metadata plus an empirically calibrated probability. Current uncalibrated recommendations yield no active order signals. |
| Trading Platform `server/src/clients/engine.js:711–736` | Price-cap fallback reused confidence, invented entry from quote, and dropped upstream suppression metadata. | WATCH-only fallback, null confidence/entry, observed reference price, retained metadata. |
| TradeLoom confidence normalizers and recommendation components | `Number(null)` became zero; missing risk became Medium; explanations labeled thesis. | Null-aware displays, unavailable risk/probability, explanation labels. |

## Field inventory and semantics

| Surface / fields | Meaning in v2 |
| --- | --- |
| Recommendations `confidence` | Always null; deprecated 0–100 heuristic representation. Never coerce null to zero. |
| `successProbability`, `confidenceMethodology` | Probability unavailable, `method=not_calibrated`; no evidence-backed calibration is attached. No endpoint in this change produces a calibrated probability. |
| `heuristicConfidence.value` | Absolute final composite in score units; not bounded probability, not percentage. |
| `score`, selection diagnostics | Relative heuristic scores/adjustments; preserve ranking utility. |
| Under-price `compositeScore`, `semanticCredibility`, `liquidityEfficiency` | Explicit 0–1 heuristic proxies, described by `researchScoreMethodology`. Profile completeness is not verified credibility; liquidity proxy is not executable capacity. |
| Under-price `fiftyDollarPotential`, `undervaluation` | Null deprecated claims. `profileCompositeScore` and `pricePositionScore` preserve the proxy values without price-target/fair-value implications. |
| Ranking `score`, `conviction`, `edgeScore`, confidence subdriver | Relative priority/heuristic strength, not success probability or expected return. Row-level methodology explicitly identifies this. |
| Ranking driver/risk `percentile` | Null where previously just rescaled; use `normalizedScore`. Rank/peer-context percentiles computed from peers remain descriptive relative position, not probability of investment success. |
| Consensus `pFinal` | Legacy name retained for signed consensus score; `isProbability=false`, `successProbability=null`, `actionable=false`. |
| Prediction context, event attribution, calendar `confidence` | Raw strategy/event/ranking/consensus scores retained for research compatibility, with explicit uncalibrated methodology and `actionable=false`. Do not present as calibrated chance of success or convert calendar direction into instructions. |
| Accuracy `hitRate`, measured performance rates | Historical outcome summaries, not forward probabilities; unchanged. Historical-data validity is outside this work. |
| `intelligenceConfidenceTier`, `runQuality`, coverage ratios | Operational quality and measured coverage, not model probability. |

## Recommendation response changes

All existing recommendation paths remain. Rows add `contractVersion: "2.0"`, `actionable`, `researchDirection`, `qualityStatus`, `suppressionReasons`, score/confidence methodology, `referencePrice`, `referencePriceAsOf`, `explanations`, and guidance availability.

- `confidence`, `risk`, `entryZone`, `expectedReturn` are null; `thesis` and `avoidIf` are empty lists. Deterministic explanations remain useful research. Mode-based `horizon` is explicitly labeled policy, not a validated forecast.
- `asOf` is research build time, explicitly **not** source freshness. Use quality evaluation and source timestamps.
- `/latest` and `/under/{price_cap}` retain their recommendation arrays and add global quality metadata and `actionableCount`. Global healthy status does not override row-level suppression.
- `/{ticker}` retains `found`; suppressed research returns WATCH with row-specific quality metadata. Missing symbols have `found=false`, `actionable=false`.
- `/best` returns 503 with machine-readable reasons if global quality is inadequate or no eligible row exists. No fallback to a stale pick.
- `long_only` never falls back to a bearish/neutral name. Under-price eligibility failures are never promoted to fill result counts. Price caps use observed reference prices, not invented entry bands. Invalid non-finite caps return 400.
- The internal NOT NULL integer confidence column is retained as zero for schema compatibility only. It is neither serialized nor used to order results. No historical migration is performed.

## Mandatory publication gates

Default policy: at least **90%** valid fresh daily-price coverage over the active universe; timestamps no older than **72 hours** and not future-dated. Predictions must independently be recent. These essential minima cannot be relaxed through legacy environment settings. Optional research-ranking display filters remain separate.

Require a recent daily run with HEALTHY derived status, plus the latest `run_queue` job recording `success` and a recent completion. The run must lie within that job's start/completion interval; a populated job run_id must match. Forecast end is never proof of completion. Missing job records fail closed.

For each symbol independently require valid recent daily prices, a recent selected reference quote, prediction, ranking and consensus. The prediction must link to the selected run. Missing/non-finite consensus and invalid prices fail closed. Rows failing any required check become WATCH and preserve scores/explanations.

Reasons include `INSUFFICIENT_FRESH_PRICE_COVERAGE`, `PREDICTIONS_STALE_OR_MISSING`, `PIPELINE_HEALTH_UNAVAILABLE`, `PIPELINE_RUN_STALE_OR_INVALID`, `PIPELINE_NOT_HEALTHY`, `PREDICTION_JOB_UNAVAILABLE`, `PREDICTION_JOB_NOT_SUCCESSFUL`, `PREDICTION_JOB_STALE_OR_INVALID`, `PREDICTION_JOB_RUN_MISMATCH`, `SYMBOL_*_STALE_OR_MISSING`, `SYMBOL_*_INVALID`, `SYMBOL_PREDICTION_RUN_MISMATCH`, `REFERENCE_QUOTE_STALE_OR_MISSING`, and `NO_ELIGIBLE_RECOMMENDATION`. Existing derived health reasons are also included.

Passing publication quality permits directional research; it is not execution approval. The TradeLoom active-signal adapter separately requires explicit calibrated probability availability and quality metadata evaluated within 60 seconds. This version supplies no calibrated probability, so unvalidated research cannot pass that adapter. Broker execution code is unchanged.

## Available evidence and limitations

Existing price bars, daily changes, 30-session volume, profile beta and historical highs/lows could support separately labeled descriptive statistics. This review found no validated recommendation-specific entry/stop/risk method or provenance sufficient to turn those into precise guidance. ATR multiples alone would still be policy choices, not calibrated success or risk estimates. No new estimates were fabricated.

The 72-hour policy is conservative and not exchange-calendar-aware; extended holidays may suppress publication. Existing derived health may also suppress narrow universes or runs whose horizon fields trigger latency warnings. No source data is repaired by these gates. Older/alternative runners without the required job and prediction linkage stay research-only.

Generic prediction/calendar UI outside recommendation flows may still format their raw research scores as percentages; their APIs now identify the uncalibrated semantics. That broader display migration, mock-data cleanup, historical evaluation, licensing and production operational verification are outside this change. The recommendation pages and order-bootstrap adapter touched here use unavailable states instead of invented probabilities.

TradeLoom's existing batch recommendation request has no matching Alpha Engine batch handler; this pre-existing integration gap was observed, not expanded into a new endpoint.

## Verification record

Commands executed from `alpha-engine/`:

```sh
.venv/bin/python -m pip install 'pytest>=8.0.0' 'httpx>=0.27.0'
.venv/bin/python -m pytest -o addopts='' -q tests/test_recommendation_integrity.py tests/test_internal_read_api_market.py tests/test_internal_read_api.py tests/test_internal_read_api_intelligence.py tests/test_consensus_signals_null.py tests/test_explainability_rank_trends.py
.venv/bin/python -m pytest -o addopts='' -q tests/test_recommendation_integrity.py
git diff --check
```

The broader API run passed **74 tests**. After the final chronological source-selection correction and its additional regression, the focused integrity rerun passed **31 tests**. The focused suite covers healthy, stale/missing independent inputs, failed/running/missing jobs, per-symbol partial availability, tenant separation, future/invalid timestamps, null consensus, stale intraday quotes, unavailable guidance, cap eligibility and non-finite price caps. Tests use isolated temporary/in-memory databases; no test report is treated as production-health evidence.

Python 3.12 project `.venv` was used. Unlike the earlier review, pytest was present at this turn's environment check; httpx was missing and installed. Earlier assertion failures exposed obsolete actionable-2022-fixture expectations and a rebuild ordering defect; both were corrected and rerun. The remaining Starlette/httpx deprecation warning is not a failed test.

TradeLoom checks (commands in its contract document): **20 server tests**, **3 frontend tests**, and **production Vite build** passed. The original integration fixture lacked the new explicit quality/calibration metadata; it was updated as a synthetic contract fixture. An initial frontend test invocation from the monorepo root passed the app tests but failed workspace dispatch (server had no matching tests; worker has no test script). Running from `app/` passed. Existing Vite configuration warnings remain.

Inspection used `rg`, `sed`, `cat`, `git status`, `git diff`, and `git log/show`; local edits used scripts and file writes. No backfill, VACUUM, outcome migration, live execution, or push command was run for this assignment.

Shared-workspace note: another process committed the main Alpha Engine patch as `5987cd0` and follow-up API methodology edits as `6d3a427` while this work was being tested. Those commits were preserved; the final local commit adds remaining contract corrections, chronology regression, and this documentation. No other agent's data/model work was reverted or restaged by this task.
