# LLM Side-Car Analyst Verification (Internal)

## Purpose
Document and reproduce the verification that the LLM “Shadow Analyst” runs as a **non-blocking** side-car on every eligible trade signal, and that both qualitative reasoning and a structured enum decision are persisted for benchmarking.

## Audience
- Developers
- Auditors / reviewers

## When to use this
- You need evidence that the LLM layer is **observational** (does not block execution) and that `analysis` + `llm_prediction` are persisted per trade.

## Prereqs
- Python available
- Repo dependencies installed

---

## What “Side-Car” means here
- The deterministic engine still executes trades when the signal passes deterministic qualification.
- The LLM produces:
  - `analysis`: narrative justification (free text)
  - `llm_prediction`: structured decision enum (`QUALIFIED`, `CAUTION`, `REJECT`)
- Divergences are logged (e.g., deterministic execution proceeds while LLM says `REJECT`).

## Persistence (Database)
Trades are persisted to SQLite in the `trades` table with:
- `analysis` (TEXT)
- `llm_prediction` (TEXT)

## Reproduce the verification
Run:
- `python scripts/test_llm_analysis.py`

What it does:
- Case 1: LLM returns `QUALIFIED` → trade executes and is persisted.
- Case 2: LLM returns `REJECT` → trade still executes (side-car behavior) and divergence is logged.

Expected output signals:
- Console prints confirming persistence of `analysis` + `llm_prediction`.
- Log line containing `STRATEGY DISAGREEMENT` for the `REJECT` case.

## Notes
- The LLM layer is configured under `config['llm_validation']` and supports `enabled` and `min_confidence_for_llm`.
- If the LLM call fails, the system is fail-safe and proceeds without the narrative/decision.

