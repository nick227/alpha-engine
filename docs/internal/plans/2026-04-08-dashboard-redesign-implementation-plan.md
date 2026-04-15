# Dashboard Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement an Arctic Premium, institutional-grade Streamlit dashboard with a strict Results-first vertical flow (Hero → Signals table → health strips), a shared `DetailPanel` pattern, and a 2-column Runs workflow.

**Architecture:** Build a token-driven UI layer (`tokens` + theme CSS) and a small set of reusable primitives (`PageShell`, `HeroHeader`, `SignalsTable`, `DetailPanel`, etc.). Refactor `app/ui/dashboard.py` to compose these primitives via tabs: Results (default), Runs, Settings.

**Tech Stack:** Python + Streamlit (existing), light CSS injection via `st.markdown(..., unsafe_allow_html=True)`.

---

### Task 0: Baseline checks (do not widen scope)

**Files:**
- Read: `app/ui/dashboard.py`
- Read: `app/ui/tokens.py`
- Read: `app/ui/theme.py`
- Read: `app/ui/components/*.py`

**Step 1: Run current dashboard**

Run (pick the existing entrypoint if it exists, otherwise run the file directly):
- `python -m streamlit run app/ui/dashboard.py`

Expected:
- Current mock dashboard renders (hardcoded content).

**Step 2: Capture a “before” screenshot (optional but useful)**
- Use OS screenshot tooling (no code changes).

**Step 3: Commit checkpoint (optional)**
- Skip if repo already has unrelated dirty state.

---

### Task 1: Create `PageShell` + global Arctic theme injection (foundation)

**Files:**
- Modify: `app/ui/tokens.py`
- Modify or replace: `app/ui/theme.py`
- Create: `app/ui/components/page_shell.py`
- Modify: `app/ui/dashboard.py`

**Step 1: Update tokens to include full Arctic theme primitives**
- Add: surface colors, border, radii, shadow/elevation, expanded text colors.
- Add: **direction chip colors** (muted slate-blue for Long, muted coral for Short).
- Keep: emerald as UI interaction accent only.

**Step 2: Implement theme CSS generator**
- In `app/ui/theme.py`, implement a function like `inject_theme_css()` that:
  - Sets base typography scale for headings/body.
  - Normalizes section spacing.
  - Reduces Streamlit chrome noise (margins/padding) carefully.
  - Defines reusable CSS classes for:
    - hero container
    - chips/badges
    - detail panel surface
    - compact strip rows
  - Ensures the **Hero KPI** can be rendered with **extreme scale contrast**.

**Step 3: Implement `PageShell` primitive**
- `PageShell(title, subtitle, body_fn)` should:
  - call `st.set_page_config(layout="wide")` once
  - call `inject_theme_css()`
  - render a consistent page header container (title/subtitle)
  - call `body_fn()`

**Step 4: Wire `dashboard.py` to use `PageShell`**
- Keep content minimal; just prove:
  - theme applies
  - spacing is disciplined
  - typography hierarchy is visible

**Step 5: Manual verification**
- Run Streamlit and verify:
  - page spacing feels premium and consistent
  - headings don’t look like default Streamlit

**Step 6: Commit milestone**
- Only commit the files listed in this task.

---

### Task 2: Implement `HeroHeader` (Action + Confidence + meta + neutral regime)

**Files:**
- Create: `app/ui/components/hero_header.py`
- Modify: `app/ui/dashboard.py`

**Step 1: Implement `HeroHeader`**
- Inputs:
  - `action_label` (e.g., Long/Short/Hold)
  - `confidence` (0..1)
  - meta: `regime`, `updated_at`, `champion_id`, `challenger_id`
  - `confidence_decay` label (Fresh/Aging/Stale)
- Rendering rules:
  - action+confidence is the focal element (largest type)
  - regime badge is **neutral grayscale**
  - no emerald in hero except possibly for UI controls outside the hero

**Step 2: Use mock data to validate layout**
- Replace existing 4-column metrics row with the hero header.

**Step 3: Manual verification**
- Ensure “extreme scale contrast” actually reads:
  - hero > table > strips

**Step 4: Commit**

---

### Task 3: Implement `SignalsTable` with default sort + row-click → `DetailPanel`

**Files:**
- Create: `app/ui/components/signals_table.py`
- Create: `app/ui/components/detail_panel.py`
- Modify: `app/ui/dashboard.py`

**Step 1: Decide interaction approach for “row click” in Streamlit**
- Implement as one of:
  - `st.dataframe` + `st.session_state` selection (if feasible), or
  - a list/table-like rendering where each row is a button, or
  - a hybrid: minimal table + “Select” column.
- Constraint: must not navigate away; must open detail in-place.

**Step 2: Implement `DetailPanel` primitive**
- Shared component used by:
  - selected signal details (Results tab)
  - selected run details (Runs tab)
- Surface: bordered, subtle shadow, consistent padding.
- Content slots: header, body, footer CTA region.

**Step 3: Implement `SignalsTable`**
- Minimal fixed columns:
  - symbol, dir, confidence, horizon, regime, updated
- Default sort:
  - confidence desc
- Row selection updates `st.session_state["selected_signal_id"]`.

**Step 4: Compose Results vertical flow**
- Order (strict):
  1) `HeroHeader`
  2) `SignalsTable` (dominant)
  3) compact health strips (Task 4)
- Show `DetailPanel` alongside the table (or under it) without breaking vertical hierarchy.
  - Recommendation: keep table dominant; panel should not visually compete.

**Step 5: Commit**

---

### Task 4: Compact health + guardrail strips (below table)

**Files:**
- Create: `app/ui/components/health_strips.py` (or extend existing `strip.py`)
- Modify: `app/ui/dashboard.py`

**Step 1: Implement compact strips**
- Render as compact rows:
  - label + small progress bar + value caption
- Minimal color; no big blocks.

**Step 2: Place below `SignalsTable`**
- Confirm they do not compete with the table.

**Step 3: Commit**

---

### Task 5: Runs tab — strict 2-column workflow + `DetailPanel` + single CTA

**Files:**
- Create: `app/ui/components/runs_workflow.py` (or `runs_list.py` + `run_detail.py`)
- Modify: `app/ui/dashboard.py`

**Step 1: Implement 2-column layout**
- Left: run list
- Right: `DetailPanel` for selected run

**Step 2: Compare mode**
- Checkbox `Compare mode`
- Allow selecting two runs; render side-by-side parameter diff + outcome deltas inside the detail area.
- No charts.

**Step 3: Rerun CTA**
- Single primary CTA (emerald) in the run detail panel: “Re-run with changes”.
- When clicked: show tuning controls in the same panel (still one primary submit).

**Step 4: Commit**

---

### Task 6: Settings tab — calm stacked forms only

**Files:**
- Create: `app/ui/components/settings_forms.py`
- Modify: `app/ui/dashboard.py`

**Step 1: Implement stacked sections**
- 2–3 sections max:
  - universe/data
  - horizons/windows
  - toggles
- Avoid side-by-side density.

**Step 2: Commit**

---

### Task 7: Cleanup/refactor legacy UI components

**Files:**
- Modify: `app/ui/components/card.py`
- Modify: `app/ui/components/section.py`
- Modify: `app/ui/components/table.py`
- Remove/replace: any component that duplicates labels or fights the new primitives

**Step 1: Remove duplicated headings/labels**
- Example: `card.py` currently uses `st.subheader(title)` and `st.metric(title, ...)` which repeats labels—update to new primitives or delete if unused.

**Step 2: Commit**

---

### Task 8: Verification + performance sanity

**Step 1: Run dashboard**
- `python -m streamlit run app/ui/dashboard.py`

Expected:
- Results defaults to confidence-desc sorted signals.
- Clicking a signal opens `DetailPanel` in-place.
- Runs tab behaves as list→detail workflow with single CTA.
- Settings is calm stacked forms.

**Step 2: Check render performance**
- Ensure no per-row heavy rendering is added (skip micro-sparklines for now).

---

## Notes / guardrails
- Keep scope UI-only; keep mock data until pipeline work lands.
- Keep CSS centralized (theme injection) to avoid “CSS spaghetti.”
- Preserve semantic separation:
  - **Emerald = interaction**
  - **Long/Short = muted slate-blue / muted coral**
  - **Regime = neutral grayscale**

