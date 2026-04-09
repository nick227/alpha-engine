# Champion Promotion Rules: Strategy Lifecycle Policy

This document defines the quantitative requirements for promoting a trading strategy from **CANDIDATE** to **CHAMPION** status. These rules are powered by the **Canonical Alpha Scoring** system to ensure consistent, risk-adjusted performance.

## 1. Core Promotion Thresholds

A strategy must exceed all the following thresholds over its rolling window (default: last 50 predictions) to be considered for promotion.

| Metric | Threshold | Rationale |
| :--- | :--- | :--- |
| **Minimum Alpha Strategy** | `> 0.60` | Ensures high risk-adjusted alpha after variance penalties. |
| **Minimum Sample size** | `> 50` | Required for statistical significance of the score. |
| **Minimum Win Rate** | `> 52%` | Baseline directional accuracy requirement. |
| **Maximum Drawdown** | `< 5.5%` | Risk gate to prevent volatile "blowup" strategies. |
| **Stability (1-Variance)** | `> 0.75` | Ensures returns are consistent, not driven by single outliers. |

## 2. Recency & Regime Consistency
- **Data Recency**: The strategy must have at least one prediction within the last **7 calendar days**.
- **Regime Alignment**: If the current market regime is `HIGH_VOLATILITY`, only strategies with a `regime_focus` of `HIGH` or `ALL` are eligible for promotion.

## 3. Demotion Logic (The "Cooling" Period)
A Champion will be automatically demoted to Candidate if:
- **Alpha Strategy** drops below `0.40` for more than 48 hours.
- **Drawdown** exceeds `8.0%` on a single trade's impact.
- **Sample Staleness**: No predictions for `14 days`.

---

> [!IMPORTANT]
> **Scoring Version**: All calculations above assume `alpha_version = 'canonical_v1'`. If the scoring logic is updated, all strategies must be re-evaluated against the new baseline before promotion.

> [!TIP]
> **Auto-Trader Integration**: Only active **CHAMPION** strategies are permitted to issue trade orders to the execution engine.
