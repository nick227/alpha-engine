# How Discovery + Playbooks Work (Public Overview)

## Purpose
Explain the selection layer that sits **before** prediction timing: discovery → promotion → watchlist → outcomes → stats.

## Audience
- End users and evaluators
- Operators who want a public-safe map of the workflow

## When to use this
- You want to understand how Alpha Engine decides **what is worth paying attention to** before predicting **when** to act.

## Prereqs
- `docs/public/legal/disclaimer.md`
- `docs/public/help/how-predictions-work.md`

---

## High-level flow
```mermaid
flowchart LR
  A[Universe + Bars + Fundamentals] --> B[Discovery Strategies]
  B --> C[Discovery Candidates]
  C --> D[Promotion Rules]
  D --> E[Daily Watchlist + Playbooks]
  E --> F[Outcomes (1d/5d/20d)]
  F --> G[Stats (Lift + Win Rate)]
```

---

## Key ideas (plain language)
- **Discovery**: finds *situations* (e.g., “distressed repricer”, “narrative lag”) worth investigating.
- **Prediction**: handles *timing/confirmation* (e.g., mean reversion, breakout, VWAP reclaim).
- **Playbook**: the connective tissue that maps a discovery situation to a recommended prediction behavior + horizon.
- **Promotion**: filters discovery candidates into a shorter watchlist using transparent rules (overlap + persistence).
- **Outcomes/Stats**: measure whether promotion and playbooks are improving forward returns (evidence > intuition).

---

## What a “discovery candidate” is
A discovery candidate is **not** a trade. It is a row that answers:
- Which symbol is interesting **today**?
- Which discovery strategy found it?
- How strong is the signal (score) and why (drivers)?

In this repo, candidates are persisted in SQLite (`data/alpha.db`) as:
- `discovery_candidates` (ranked per discovery strategy per day)

---

## What promotion does (why it exists)
Promotion is a deterministic filter that tries to improve quality:
- **Overlap**: a symbol shows up in multiple discovery strategies on the same day.
- **Persistence**: the symbol continues to appear over multiple days.

Promoted results are stored as:
- `discovery_watchlist` (daily shortlist)
- `prediction_queue` (optional downstream consumption)

---

## What “promotion lift” means (the one truth metric)
Promotion lift answers: **does filtering help?**

Definition:
- `promotion_lift = watchlist_avg_return − candidates_avg_return`

Interpretation:
- If lift is consistently **positive**, promotion is doing useful work.
- If lift is near **zero**, promotion is neutral.
- If lift is consistently **negative**, promotion rules are hurting selection quality.

---

## Playbooks (discovery → prediction wiring)
Playbooks are a small set of reusable mappings:
- **Discovery situation** → **recommended prediction strategy types** + **horizons**

Example (illustrative):
- `distressed_repricer` → mean reversion + VWAP confirmation → horizons 1d/7d/30d

Why this matters:
- It turns “RSI says buy” into “RSI says buy *in the distressed repricer situation*”.

---

## How to run it (CLI)
Nightly (single command):
- `python -m app.discovery.discovery_cli nightly`

Manual steps (if you prefer explicit stages):
- `python -m app.discovery.discovery_cli run --date YYYY-MM-DD`
- `python -m app.discovery.discovery_cli promote --date YYYY-MM-DD`
- `python -m app.discovery.discovery_cli outcomes --date YYYY-MM-DD --scope both`
- `python -m app.discovery.discovery_cli stats --end-date YYYY-MM-DD --window 30`

Note: outcomes require sufficient future bars (e.g., 20 trading days later for 20d outcomes).

---

## Where to see results (Streamlit)
- **Discovery → Watchlist**: daily top picks (human-consumable)
- **Discovery → Stats**: watchlist vs candidates vs non-promoted + lift
- **Discovery → Playbooks**: playbook performance table (5d/20d) + best/worst headline

---

## Job status (removes ambiguity)
The system logs job runs to SQLite so you can see:
- Last nightly run status (running/success/failed)
- Completed time and duration
- Error message if failed

This is surfaced in the Playbooks dashboard UI.

