# Alpha Engine — Documentation / Marketing / Knowledge Base Proposal (Markdown)

## Purpose
Define a consistent and well-organized documentation system that supports:
- Baseline **technical audit** documentation (developers/auditors)
- **Action-oriented problem solving** material for investors and end users
- “Help-light” component guides for the ingestion pipeline and prediction engine
- A **short-form** content library that explains value of the app

## Audience
- Public: investors, evaluators, end users
- Internal: developers, operators, auditors

## When to use this
- You are building, reviewing, or governing Alpha Engine’s documentation/marketing/knowledge base structure.

## Prereqs
- None (public)
- Repo access recommended (internal)

---

## Summary (Decisions Locked)
- Docs are authored as **Markdown in repo**.
- Docs are **split** into `docs/public/` and `docs/internal/`.
- Primary positioning: **research & analytics platform** (tier 1).
- Fully automated bot trading/execution is treated as a **future/advanced goal** (roadmap), not a current claim.
- Existing materials in `docs/archive/` and `docs/plans/` remain as sources; we link to them rather than reorganize them in the first pass.

## Information Architecture (Decision Complete)

### Repo layout
- `docs/README.md` — single entry point; explains audiences + navigation
- `docs/public/`
  - `docs/public/README.md` — public landing + “start here”
  - `docs/public/marketing/` — narrative/value/positioning
  - `docs/public/kb/` — problem/solution articles (“if X, do Y”)
  - `docs/public/help/` — help-light + component cards + troubleshooting
  - `docs/public/shortform/` — short explainers, FAQs, snippets
  - `docs/public/legal/` — disclaimers (research platform; not investment advice)
- `docs/internal/` (internal/audit)
  - `docs/internal/README.md` — internal landing + audit map
  - `docs/internal/audit/` — security, data lineage, controls, risk register
  - `docs/internal/architecture/` — deeper design docs, diagrams, contracts
  - `docs/internal/ops/` — runbooks, debugging, deployments, incident playbooks
  - `docs/internal/dev/` — contributor/dev workflow, testing, releases

### Navigation rules (coherence rules)
- Every folder has a `README.md` mini-index.
- Every page starts with: **Purpose**, **Audience**, **When to use this**, **Prereqs**.
- Prefer:
  - Task titles for KB/help (`How to…`, `Troubleshoot…`, `Interpret…`)
  - Concept titles for architecture (`What is…`, `How it works…`)
- Use Mermaid for diagrams where helpful.

## Content System (What We Write)

### 1) Technical audit baseline (“Audit Pack”)
Required internal pages:
- `docs/internal/audit/security-overview.md`
- `docs/internal/audit/data-lineage.md`
- `docs/internal/audit/dependencies-and-licenses.md`
- `docs/internal/audit/reproducibility.md`
- `docs/internal/audit/model-limitations.md`
- `docs/internal/audit/risk-register.md`

Public-safe mirrors:
- `docs/public/help/how-predictions-work.md`
- `docs/public/help/data-sources-at-a-glance.md`
- `docs/public/legal/disclaimer.md`

Audit template (required sections):
- Purpose / Audience / Scope boundaries
- System boundary diagram
- Key controls (what prevents bad outcomes)
- Known risks + mitigations
- Verification steps (how an auditor confirms)

### 2) Action-oriented investor + end user material (public)
Positioning: **Research & analytics platform (tier 1)**; automated trading as **future/advanced goal**.

Cornerstone pages:
- `docs/public/marketing/one-pager.md`
- `docs/public/marketing/investor-faq.md`
- `docs/public/kb/how-to-use-and-evaluate.md` (consolidated playbook)

Action/KB template (required sections):
- Problem statement (“You are seeing X”)
- Fast checklist (3–7 bullets)
- Root causes (ranked)
- Resolution steps
- “What good looks like” (expected outputs/artifacts)

### 3) Help-light: component guides (public + internal depth)
Public component cards (baseline set):
- `docs/public/help/components.md` (consolidated component cards)

Component card template:
- What it does (2–3 sentences)
- Inputs/outputs (including file paths/config keys)
- Key knobs (what you can tune safely)
- Common issues + fixes
- Where to read next (links)

### 4) Short-form value library (public)
Content kit backlog:
- 10× “What is X?” (MRA, consensus engine, strategy ranking, horizons)
- 10× “Why it matters” (risk controls, reproducibility, extensibility)
- 10× “Myth vs fact” (AI misconceptions, confidence vs performance)
- 20× “Feature bullets” grouped by persona (investor, operator, developer, analyst)
- 10× “Use-case snapshots” (earnings/news events, regime changes, volatility spikes)

Short-form rules:
- One-line headline
- 5–7 bullets max
- One “Next” link to a deeper KB/help page

## Consistency: voice, claims, and governance

### Writing/claim rules
- Avoid performance promises; focus on research workflow, measurement, transparency.
- Always include and link the disclaimer from public marketing pages.
- Keep the confidence vs outcomes distinction explicit.

### Shared terms
- Public glossary: `docs/public/help/README.md` (glossary section)
- Internal glossary: `docs/internal/architecture/glossary.md`

### Ownership + update cadence
Owners (recommended):
- Ingestion: engineering owner
- Engine: engineering owner
- UI: engineering/design owner
- Ops/audit: operations/security owner
- Marketing: product/positioning owner

Review rules:
- Public pages: require “product/positioning” review
- Internal/audit pages: require “engineering/ops” review

## Milestones (Shippable)
1) Create proposal doc + directory tree + index pages (`docs/README.md`, folder READMEs)
2) Publish cornerstone public docs (one-pager, investor FAQ, 3 KB articles, disclaimer)
3) Publish internal audit minimum set (security overview, data lineage, reproducibility)
4) Add component-card help-light set for ingestion + prediction engine (+ outputs + UI)
5) Fill short-form library (first 20 items) and link into marketing/KB

## Acceptance criteria
- Every doc page includes Purpose/Audience/When-to-use/Prereqs.
- `docs/README.md` routes a reader to the right place in ≤2 clicks.
- Public docs contain disclaimer link and avoid unverifiable performance claims.
- Internal audit pack provides reproducibility steps and maps to code/config + artifacts (`config/`, `data/`, `outputs/`, `prisma/`).
