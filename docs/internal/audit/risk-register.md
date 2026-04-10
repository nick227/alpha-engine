# Risk Register (Internal)

## Purpose
Maintain a quantified, reviewable list of major risks and mitigations.

## Audience
- Auditors
- Engineering leadership
- Operators

## When to use this
- You need to track and communicate current risk posture and mitigation work.

## Prereqs
- None

---

## Scale
- Likelihood: Low / Medium / High
- Impact: Low / Medium / High
- Priority: computed qualitatively (High impact + High/Medium likelihood)

## Risks (baseline)
| Risk | Likelihood | Impact | Mitigation | Verification |
|---|---:|---:|---|---|
| Data gaps / rate limits cause missing inputs | Medium | Medium | Metrics + rate limiting + retries; use mock datasets for isolation | Confirm ingestion metrics; reproduce with fixed bundles |
| Timestamp misalignment causes misleading outcomes | Medium | High | Normalize timestamps; enforce minimum series points; lineage checks | Run reproducibility flow; compare event vs bar timestamps |
| Look-ahead bias in evaluation (future returns leaking into features) | Medium | High | Separate features vs outcomes; strip `future_return_*` from feature inputs | Inspect `app/engine/runner.py:_split_context`; validate strategy inputs |
| Overclaiming performance to stakeholders | Medium | High | Public disclaimer; separate confidence vs outcomes; consistent language | Review public docs before release |
| Dependency supply chain vulnerabilities | Medium | Medium | Pin versions; review deps; periodic audit | Compare lockfiles; run vulnerability checks in CI (future) |
| Secrets exposure | Low | High | `.env` usage; avoid committing secrets; access controls | Repo secret scan; review `.gitignore` |
| Misconfigured env var names cause silent auth failures | Medium | Medium | Align `.env.example` variable names with `config/keys.yaml` expectations | Validate `KeyManager.get()` resolves non-empty keys; run `app/ingest/diagnose.py` with network enabled |
