# Streamlit Visibility + Platform Controls Backlog (Internal)

## Purpose
Define a concrete backlog to evolve the Streamlit UI from “POC observability” into “operator-grade platform controls,” with measurable acceptance criteria.

## Audience
- Developers
- Operators / on-call
- Audit/security reviewers (internal)

## When to use this
- You are planning UI work, prioritizing reliability controls, or scoping “what it would take to look like a real quant platform.”

## Prereqs
- Familiarity with the Streamlit shell entrypoint: `streamlit run app/ui/app.py`

---

## Current baseline (what we already offer)

### Pages
- Unified shell + shared filters (tenant/run/ticker/horizon): `app/ui/app.py`, `app/ui/shell/filter_state.py`
- Signal Audit (flat-table diagnostics): `app/ui/audit.py`
- Ops / Data Console (data-plane health + job runner with logs): `app/ui/ops_data_console.py`
- Intelligence Hub (strategy matrix/overlays/timeline): `app/ui/intelligence_hub.py`
- Paper Trades (positions/history/P&L analytics): `app/ui/paper_trades.py`

### Notable strengths
- Operator actions are reproducible: command preview + explicit confirmation + logs/exit codes (Ops/Data Console).
- DB-backed health visibility exists (ingest windows, staleness, freshness, prediction runs).

### Known missing “platform controls” (summary)
- No auth/RBAC / read-only vs operator mode
- No end-to-end lineage drill-down (event → features → prediction → outcome → trade)
- No explicit quality gates / SLO enforcement that blocks downstream actions
- No alerting (push) or scheduled monitoring; mostly “look at dashboards”
- Limited run comparison, regression detection, and release-style governance

---

## North-star outcomes (what “professional” feels like)
- An operator can answer “what changed and why?” in < 2 minutes (lineage + diffs + suggested fixes).
- The platform can safely run in a shared environment (auth, roles, redaction, audit log).
- Data quality gates prevent accidental garbage-in/garbage-out and explicitly block execution.
- Failures page someone, link to context, and provide a safe one-click remediation path.

---

## Milestones (suggested order)

### M0 — Quick wins (1–3 days)
1) **Unified “System Status” header card**
   - Add a compact banner across pages: DB path, last ingest ok time, last prediction run time, staleness count.
   - Acceptance:
     - Shows on Dashboard/IH/Ops/Audit/Paper pages.
     - Derived from existing service calls and/or simple DB queries.
   - Effort: S

2) **Deep-linking between pages**
   - “View in Audit” / “View in Ops” links/buttons for: adapter id, run id, strategy id, ticker.
   - Acceptance:
     - Clicking sets `st.session_state` route + filters and reruns.
   - Effort: S

3) **Consistent exports**
   - Add CSV download buttons for the highest-signal tables in Audit and Ops.
   - Acceptance:
     - Downloads are schema-stable (fixed columns + explicit sorting).
   - Effort: S

---

### M1 — Lineage Explorer (1–2 weeks)
Goal: make “why did we trade this?” explainable without leaving Streamlit.

4) **Lineage Explorer page**
   - Input: `event_id` / `prediction_id` / `trade_id` (auto-complete where possible).
   - Output: a single “thread” view with tabs:
     - Raw event payload/extracted fields + validation/dedupe reasons (when available)
     - Feature snapshot used for prediction (and a “no look-ahead” badge)
     - Per-strategy signals + consensus weights/regime snapshot
     - Outcome evaluation and scoring metrics
     - Downstream trade (if any): sizing, risk checks, lifecycle, realized P&L
   - Acceptance:
     - Works even when some tables are missing: UI degrades gracefully with “not available” sections.
     - Every section has “source table(s)” and timestamps.
   - Dependencies:
     - Existing SQLite tables already referenced by Audit/Ops/Paper + `DashboardService`.
   - Effort: L

5) **Drop/validation reason accounting**
   - Persist and display “why emitted_count < fetched_count” by reason bucket (timestamp parse, empty text, dedupe hit, extractor mismatch).
   - Acceptance:
     - Audit shows top reason buckets per adapter over a selected window.
   - Dependencies:
     - Might require adding columns/tables in ingest ledger for reason counts (if not already tracked).
   - Effort: M–L

---

### M2 — Quality Gates + “Safe to Execute” (1–2 weeks)
Goal: replace “eyeballing health” with explicit pass/fail gates.

6) **Data Quality Gates panel**
   - Define a gate set (configurable) and compute pass/fail for:
     - Event freshness by adapter (max age)
     - Drop-rate spike vs baseline
     - Bars completeness for selected tickers (coverage threshold)
     - Prediction run freshness
     - Schema expectations / drift
   - Acceptance:
     - A single status: `PASS` / `WARN` / `FAIL`.
     - Each failing gate lists: evidence, likely causes, and a suggested remediation job preset.
   - Effort: M

7) **Execution gating**
   - If gates are `FAIL`, Paper Trades page defaults to read-only and blocks new job actions that could increase risk (configurable override for operators).
   - Acceptance:
     - Explicit “override with reason” field is required to bypass.
   - Effort: M

---

### M3 — Auth/RBAC + Audit Trail (2–4 weeks)
Goal: make it deployable/shared without fear.

8) **RBAC: read-only vs operator vs admin**
   - Add a minimal auth layer and role checks around:
     - Viewing raw event text
     - Running jobs (backfills, cleanup, downloads, ML builds)
     - Viewing keys/secrets diagnostics
   - Acceptance:
     - “Operator mode” toggle is only available to authorized roles.
     - Default is read-only; dangerous actions hidden or disabled.
   - Effort: L

9) **Operator audit log**
   - Write an append-only record of operator actions:
     - who, when, what command, parameters, UI context (tenant/ticker/run), free-text reason
   - Acceptance:
     - Exportable table in UI, filterable by date/user/action.
   - Effort: M–L

10) **Redaction controls**
   - Role-based redaction of event text / provider identifiers in UI exports.
   - Acceptance:
     - “Share-safe export” mode produces redacted CSVs.
   - Effort: M

---

### M4 — Run Diff + Regression Detection (1–2 weeks)
Goal: explain changes and catch regressions fast.

11) **Run comparison view**
   - Compare two `prediction_runs` for a ticker/horizon:
     - coverage deltas (events/bars)
     - strategy ranking changes
     - champion switches
     - consensus distribution shift
   - Acceptance:
     - A “top movers” table with drill-down to affected predictions/events.
   - Effort: M–L

12) **Regression checks**
   - Define “expected invariants” (e.g., demo artifacts stable; certain metrics within bounds) and show pass/fail per run.
   - Acceptance:
     - A run gets a `healthy` badge only when checks pass.
   - Effort: M

---

### M5 — Alerting + Scheduled Monitoring (1–2 weeks)
Goal: move from dashboards to “platform that wakes you up when broken.”

13) **Alert hooks**
   - Send alerts on: staleness > threshold, repeated adapter failures, bars coverage drops, gate FAIL.
   - Acceptance:
     - Links back into the relevant UI views with filters pre-applied.
   - Effort: M

14) **Scheduled jobs**
   - Add a lightweight scheduler (or external cron) to run:
     - ingest-health report
     - gate checks
     - periodic backfill for recent windows
   - Acceptance:
     - Ops UI shows schedule + last run + last status.
   - Effort: M–L

---

## Recommended first slice (if we start this week)
- M0.1 System Status header card
- M0.2 Deep-linking between pages
- M2.6 Data Quality Gates panel (minimal gate set)
- M1.4 Lineage Explorer (thin vertical slice: prediction_id → strategies/consensus → trade)

