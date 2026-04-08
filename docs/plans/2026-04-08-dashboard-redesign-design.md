## Alpha Engine Dashboard Redesign (Arctic Premium)

### North-star goals
- **Premium, brand-competitive UI**: strong hierarchy, extreme scale contrast, disciplined spacing, minimal color, consistent grid alignment, low-noise surfaces.
- **Results first**: predictions/results are the primary UX surface; everything else supports that.
- **Runs are first-class**: tuning + reruns are designed as a workflow, not an afterthought.
- **Repeatable system**: small token set + small component primitive set; no ad-hoc styling per panel.

### Locked design decisions
- **Layout model**: Tabs — `Results` (default) | `Runs` | `Settings`.
- **Theme**: **Arctic premium** (bright surfaces, subtle borders, dark typography).
- **Accent color**: **Emerald** (primary actions/selection only).
- **Imagery**: **Micro-icons only**, used sparingly (no illustrations/gradients).
- **Core rule**: **one focal idea per section** (each section has exactly one dominant element).
- **Color semantics rule**: **Emerald is reserved for UI interaction semantics only** (primary buttons, active tab, selection). Market direction (Long/Short) uses a separate muted pair to avoid “Christmas UI”.

### Information architecture
#### Tab: Results (primary)
**Purpose**: immediate, high-confidence read of “what the engine believes now” + fast scanning of ranked signals.

- **Vertical flow (strict)**: **Hero → Signals table → compact health strips**.

- **Hero header (single focal element)**:
  - Dominant: **Action + Confidence** (e.g., `Long 0.84` / `Short 0.84`), not just `P_final`.
  - Supporting meta (low weight): regime badge, timestamp, champion/challenger IDs.
  - Micro-icon usage: one contextual icon at most (e.g., “consensus”, “regime”).
  - Regime badge coloring: **neutral grayscale** (no emerald; no direction colors).
  - Freshness: include **ConfidenceDecay** indicator (e.g., “Fresh / Aging / Stale”) derived from last optimizer/run timestamp.

- **Top-level sort (global)**:
  - Applies to the primary signals list/table and any secondary lists on this tab.
  - Default sort: **confidence/consensus strength** (descending).
  - Candidate sort keys (finalize during implementation plan):
    - confidence / consensus strength
    - expected return (if available)
    - risk-adjusted score (if available)
    - recency
    - freshness / staleness (ConfidenceDecay)

- **Primary section: Signals table (dominant element)**:
  - Columns (minimal, fixed): **symbol, direction, confidence, horizon, regime, updated**.
  - Optional: tiny, grayscale micro-sparkline if/when data exists.
  - Visual discipline: no per-row color floods; only direction chip + confidence number formatting.
  - Interaction: **row click opens a `DetailPanel`** (no navigation to a new page).

- **Secondary sections (subordinate, compact)**:
  - Placement: **below the signals table** so they never compete for attention.
  - Track health (sentiment/quant) as compact strips.
  - Guardrails (stability monitor/promotion gate) as compact strips.

#### Tab: Runs (tuning + reruns)
**Purpose**: manage run lifecycle, compare outcomes, rerun quickly with controlled changes.

- **2-column workflow (strict)**: **run list (left) → detail + rerun (right)**.

- **Run list (left)**:
  - Recent runs with status chip (running/success/failed), created time, dataset/time range.
- **Compare mode**:
  - A `Compare mode` checkbox enables selecting **two runs** from the list.
  - When exactly two are selected, the detail area switches to a **side-by-side diff view**:
    - parameter diff (grouped; only show changed params by default)
    - key outcome deltas (stability/alpha/confidence metrics)

- **Run detail (right)**:
  - Uses `DetailPanel` primitive.
  - Shows: parameters snapshot (read-only) + deltas vs baseline/previous + **single CTA**.
  - **No charts yet** (keep it text/table based until UX is proven).
  - CTA: “Re-run with changes” (emerald) opens the tuning controls in-context (same panel).

- **Rerun/tuning controls**:
  - Small number of grouped controls; defaults are stable and conservative.
  - Submit is the single primary action in this panel.

#### Tab: Settings (global)
**Purpose**: calm, form-like configuration; no charts.

- 2–3 sections max:
  - Data sources / universe
  - Default horizons / evaluation windows
  - Model/strategy toggles
- Rule: Settings must be **stacked, calm forms** (avoid side-by-side density).

### Visual system (tokens + primitives)
#### Tokens (single source of truth)
Expand `app/ui/tokens.py` to include:
- **Typography**: display/title/body/label/meta with strict sizes/weights/line heights.
- **Spacing**: 4/8/16/24/40… (no arbitrary gaps).
- **Surfaces**: background, surface, surface-alt; subtle borders.
- **Radii**: one or two radii values only.
- **Color**:
  - Text grayscale
  - Accent emerald (buttons, active tab, selected row)
  - Direction chips (market semantics): **muted slate-blue for Long** and **muted coral for Short** (distinct from emerald)
  - Semantic pos/neg used only when meaning is critical (and never to indicate “primary action”)

#### Primitives (repeatable building blocks)
Upgrade/replace existing `app/ui/components/*` into a small set of enforced primitives:
- `PageShell` (applies theme + grid container + page spacing)
- `HeroHeader` (single focal KPI + supporting meta)
- `Section` (title + one dominant child region)
- `Card` (surface + padding + optional header)
- `KpiRow` (uniform KPI layout, no duplicated labels)
- `SignalsTable` (sortable, consistent column formatting)
- `DetailPanel` (shared: signal detail + run detail; supports in-context rerun actions)
- `Badge/Chip` (regime, status, direction)
- `Strip` (compact health/guardrail meter)

### Streamlit implementation constraints (design-aware)
- Prefer composition over custom HTML where possible; use a small CSS layer only to enforce spacing/typography/surfaces consistently.
- Avoid visual noise: reduce redundant headings and repeated labels (e.g., don’t show “Sentiment” twice in the same card/metric).
- Grid alignment: Streamlit `st.columns` defaults can introduce inconsistent padding; mitigate via a **single global CSS injection** applied by `PageShell`.
- Signals table styling: `st.dataframe` is preferred for sorting/filtering; expect theming friction and plan to align visuals via theme/CSS (keep overrides centralized).
- Micro-sparklines: treat as optional; avoid per-row heavy render paths until performance is proven acceptable.

### Non-goals (for this redesign slice)
- No new backend pipeline/data integration work is required to ship the UI redesign; mock data can remain until pipeline work lands.
- No additional pages beyond the 3 tabs unless a hard requirement emerges.

### Acceptance criteria
- **Results tab** clearly reads as “one hero focal + one dominant signals table”; secondary health/guardrails never compete for attention.
- **Runs tab** supports run selection, parameter review, and rerun-with-changes workflow with a single clear primary action.
- **Settings tab** is calm, minimal, and consistent with the theme.
- Visual consistency holds across the whole dashboard using tokens/primitives (no one-off styling).

