# Compact operational database cutover (Railway `alpha-engine`)

Status: **prepared, not approved.** Do not swap, delete or modify `/data/alpha.db` without explicit approval.

## Why

`/data/alpha.db` is 4.18 GiB on a 5 GB Hobby volume (≈94% used). 97% of `price_bars` is the
`ml_train` tenant (historical ML training data, which belongs to the local research warehouse).
A verified compact copy is **0.81 GiB** with every operational table identical.

## What the candidate changes (copy only)

| Change | Rows | Why |
|---|---|---|
| Drop `price_bars` tenant `ml_train` | ~31.9 M | Training history lives off-volume (local warehouse / backups) |
| Remove legacy-format daily duplicates with a canonical twin (live symbols) | 8,230 | Stale/unadjusted (median 0.5%, p95 74%, max 903% off the fresh adjusted close) |
| Remove intraday-stamped "daily" rows | 2,124 | Mock/test contamination (e.g. `T21:19:03.641365`, random prices) |
| Rebuild `price_bars` `WITHOUT ROWID`, `VACUUM` | — | Removes the duplicate primary-key index |

Kept: `default` and `backfill` tenants, crypto weekend bars, every non-price table.
Not changed: 1,965 legacy duplicate sessions for symbols outside the live universe.

## Prerequisites (all must be true)

1. **Approval** for the swap and for the row removals above.
2. **Two independent, verified backups of the database taken after writers are paused** (step 2 below):
   - *Backup A — local workstation:* chunked pull over `railway ssh` (`dd | gzip | base64`, 128 MiB chunks),
     reassembled; SHA-256 equals the remote file; `PRAGMA quick_check` = `ok`; file set read-only.
   - *Backup B — a different device or location:* a copy of A on separate media or another machine
     (not the same disk), re-hashed there. Same SHA-256 as A and the remote file.
   - The 2026-11-21 backup (`424c057c…`) is **stale**: production changed after it was taken.
3. **Proven restore paths:**
   - *Backup is usable:* a backup copy opened by the Alpha Engine read API (done locally: `/health` ok,
     research sync/publication accepted against it).
   - *Railway inbound transfer:* streaming into the container over `railway ssh` stdin was verified
     (64 MiB in 6.2 s, SHA-256 match). Full 4.5 GB rollback ≈ 7 min; 0.81 GiB candidate ≈ 1.5 min.
   - Railway volume backups are not available on this plan; the restore path is the stream above.
4. **Space plan:** with 0.3 GiB free, the candidate cannot sit beside the 4.2 GiB file on the volume.
   The swap is "verify backups A and B → delete old → place new". The rollback copy is off-volume.
5. **Maintenance window:** Alpha Engine API briefly unavailable; TradeLoom research lookups fail closed
   (no trades) during the window.

## Procedure

1. Announce the window. Confirm TradeLoom worker is in simulator mode and `ALLOW_LIVE_TRADING=false`.
2. **Pause writers:** `ALPHA_LIVE_BARS_ENABLED=false`, `ALPHA_PIPELINE_ENABLED=false`,
   `ALPHA_SCHEDULER_ENABLED=false`; stop the research worker; confirm `pipeline.lock` is not held.
3. Take backups A and B; verify both (prerequisite 2). Record the SHA-256.
4. **Build the candidate** in the container's ephemeral `/tmp` from a read-only `VACUUM INTO`
   (script: `candidate_build.py`, run with `cd /app && python3 build.py`). `/tmp` is wiped by any redeploy:
   build immediately before the swap.
5. **Go/no-go checks** (all must pass, from `/tmp/cand/report.json`):
   - `integrity_check = ok`; no duplicate sessions for live symbols; no intraday-stamped rows;
   - all non-price tables identical (row count + content hash) to the backup;
   - research windows (`load_bars`, 644 sessions, live universe) identical;
   - every published research trace has an identical evidence SHA-256;
   - production file hash unchanged during the build; candidate SHA-256 recorded.
6. **Swap:** stop the `alpha-engine` service process; `rm /data/alpha.db`; `mv /tmp/cand/candidate.db /data/alpha.db`;
   verify SHA-256 equals the recorded candidate hash; start the service.
7. **Post-checks:** `/health` ok and `latest_bar_date` unchanged; `/api/sync/bars` signed sync ok;
   worker `sync` reports zero corrections; an existing prediction's `/trace` has the same digest;
   TradeLoom bot page shows the research evidence as verified; free space ≥ 3.5 GiB.
8. **Re-enable writers;** watch one `live_bars` run (`inserted`/`corrected` reported, no errors).
9. **Afterwards:** set a bounded retention for daily bars (2 years, live universe); lower
   `ALPHA_PIPELINE_MIN_FREE_MB` to suit the new size.

## Rollback

Stop the service → stream Backup A into `/data/alpha.db.restore` → verify SHA-256 → replace →
start → `/health`, trace and sync checks as in step 7. Backup B is the fallback if A fails verification.
