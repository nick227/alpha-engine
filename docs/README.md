# Alpha Engine Documentation

## Purpose
Provide a single, organized documentation hub for Alpha Engine with **two tracks**:
- **Public**: marketing, knowledge base (KB), and help-light documentation for investors and end users.
- **Internal**: technical audit, architecture, operations, and developer workflow documentation for engineers and auditors.

## Audience
- Investors, end users, evaluators (public)
- Developers, operators, security/audit reviewers (internal)

## When to use this
- You need to understand what Alpha Engine is, how to evaluate outputs, or how to operate/audit the system.

## Prereqs
- None for public docs
- For internal docs: local repo access recommended

---

## Start Here

### Public (investors + end users)
- `docs/public/README.md`
- One-pager: `docs/public/marketing/one-pager.md`
- Investor FAQ: `docs/public/marketing/investor-faq.md`
- Disclaimer: `docs/public/legal/disclaimer.md`

### Internal (developers + auditors)
- `docs/internal/README.md`
- Audit Pack index: `docs/internal/audit/README.md`
- Reproducibility: `docs/internal/audit/reproducibility.md`
- Ops runbooks: `docs/internal/ops/README.md`

---

## Navigation Rules (project-wide)
- Every folder includes a `README.md` index page.
- Every page starts with **Purpose**, **Audience**, **When to use this**, **Prereqs**.
- Exception: `docs/public/shortform/*` is intentionally brief and may omit the header block in favor of a headline + bullets + one next link.
- Exception: `docs/archive/*` and `docs/plans/*` are historical notes and planning documents and may not follow the header template.
- Prefer:
  - **Task titles** for KB/help (e.g., “How to…”, “Troubleshoot…”).
  - **Concept titles** for architecture (e.g., “What is…”, “How it works…”).
- Use Mermaid for diagrams where helpful.
