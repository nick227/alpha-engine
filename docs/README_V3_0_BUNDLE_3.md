
v3.0 Bundle 3 — Evolution Layer

Adds:
- mutation engine
- tournament selection
- promotion state machine
- reaper guardrails
- optimizer loop

Flow:
active
→ mutate children
→ tournament
→ candidate
→ probation
→ active OR rollback
