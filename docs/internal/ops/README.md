# Ops (Internal)

## Purpose
Runbooks and operational guidance for running, monitoring, and debugging Alpha Engine.

## Audience
- Operators
- Developers on-call

## When to use this
- You are deploying/running the system or investigating issues.

## Prereqs
- Repo + environment access

---

## Primary operator reference
- `ADMIN_GUIDE.md`

## Runbooks
- Daily Windows batch pipeline (scheduled task + five steps): `docs/internal/ops/daily-process.md`
- Ingestion health checks: `docs/internal/ops/ingestion-health-checks.md`
- Source mapping + keys/secrets: `docs/internal/ops/secrets-and-keys.md`
- Backfill + replay operations: `docs/internal/ops/backfill-and-replay.md`
- Incident triage checklist: `docs/internal/ops/incident-triage.md`

