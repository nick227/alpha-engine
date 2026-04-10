# Dependencies and Licenses (Internal)

## Purpose
Provide a baseline inventory of dependencies and how they are managed.

## Audience
- Auditors
- Developers

## When to use this
- You need to review supply chain risk and licensing posture.

## Prereqs
- Repo access

---

## Dependency surfaces (current repo)

### Python (runtime) — `requirements.txt`
- `streamlit`, `streamlit-autorefresh` (UI)
- `pandas`, `numpy` (data processing)
- `pydantic` (ingestion/source spec validation)
- `python-dateutil`
- `PyYAML` (config parsing)
- `plotly` (charts)
- `yfinance` (market data)

### Python (dev) — `requirements-dev.txt`
- `pytest`
- `pylint`

### Node/npm (Prisma tooling) — `package.json`
- `prisma` (dev dependency)
- `@prisma/client`

## External services (high level)
- Market/news sources depend on configured adapters in `config/sources.yaml`.
- Any keys/secrets should be provided via environment variables (see `.env.example`).

## License posture (current repo)
- `README.md` references an MIT license badge, but there is currently **no `LICENSE` file** at repo root. Treat this as an audit gap to resolve before external distribution.

## Dependency evidence (what reviewers can verify quickly)

### Python requirements (as committed)
Source of truth:
- `requirements.txt`
- `requirements-dev.txt`

Current runtime minimums (non-exhaustive list; see file for full set):
- `streamlit>=1.44.0`
- `pandas>=2.2.2`
- `numpy>=1.26.4`
- `pydantic>=2.7.0`
- `PyYAML>=6.0.1`
- `plotly>=5.17.0`
- `yfinance>=0.2.0`

### Node requirements (as committed)
Source of truth:
- `package.json`
- `package-lock.json`

Current versions:
- `prisma` (dev dependency) `^6.19.2`
- `@prisma/client` `^6.19.2`

## Update policy (recommended baseline)
- For auditability, prefer pinning exact versions (or using a lockfile) for Python in production environments.
- Record dependency updates in a changelog entry and rerun reproducibility steps (`docs/internal/audit/reproducibility.md`).
- For any network adapters, re-run diagnose with network enabled (`ALPHA_DIAGNOSE_ALLOW_NETWORK=1`) after dependency bumps.

## Verification steps
- Review `requirements.txt` and `package-lock.json` for pinned versions.
- Confirm secrets are not committed; rely on `.env` locally and `.env.example` as a template.
- Confirm no `LICENSE` drift: if distributing externally, add the intended license file before release.
