# Research publisher (Railway cron service)

The signed research worker (`app/research_publication/worker.py`) runs on Railway as its own
service, `research-publisher`, built from this repo with `railway.publisher.json`
(`railwayConfigFile`). It is a cron job: every 30 minutes it runs `worker scheduled` once and exits.

Each pass:
1. Reads Alpha Engine `/health`. Unless `live_bars.state == "succeeded"` with nothing missing,
   it logs `wait` and exits 0 — publication only follows a verified daily refresh (~22:00 UTC).
2. Skips if this refresh was already published (`$ALPHA_RESEARCH_HOME/scheduled-state.json`).
3. Signed sync (full-window mirror) → strategy run → signed publish. Railway re-derives every
   claim; publishing is content-addressed, so repeats are idempotent.
4. Any failure exits non-zero without recording state; the next pass retries.

Publication is not eligibility. A publication can be operationally `HEALTHY` (integrity gates
passed, enough holdout) and still have no demonstrated edge — see `researchQuality` on
`/api/research/latest` and `/api/research/publications/{id}`. TradeLoom trades research only
when it beat its baseline on genuinely out-of-sample evidence.

## Configuration
| Variable | Value |
|---|---|
| `ALPHA_RESEARCH_URL` | Alpha Engine over the private network |
| `ALPHA_RESEARCH_KEY_ID` | key id registered in Alpha Engine `ALPHA_PUBLISHER_KEYS` |
| `ALPHA_RESEARCH_KEY_PEM` | Ed25519 private key (PEM) — Railway secret; never leaves the service |
| `ALPHA_RESEARCH_WORKER_ID` | recorded on each publication |
| `ALPHA_RESEARCH_HOME` / `ALPHA_RESEARCH_DB` | on the service volume (mirror DB, outbox, state) |

Key rotation: generate a key (`worker keygen`), add its public key to `ALPHA_PUBLISHER_KEYS`
alongside the old one, swap `ALPHA_RESEARCH_KEY_ID`/`ALPHA_RESEARCH_KEY_PEM`, then remove the old id.
