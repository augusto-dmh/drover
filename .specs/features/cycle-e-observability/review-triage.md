# Review triage — PR #9 (observability)

Triage of review comments against the code as it exists on `feat/observability`. Comments will be deleted in Stage 6; this file is the surviving record.

| # | Source | File:line | Verdict | Action | Rationale |
|---|---|---|---|---|---|
| 1 | inline `3741082025` concurrency | `loop.go:41` | real | fix | Listen runs before the already-started check. With a fixed `OpsAddr`, a second `Start` fails with `EADDRINUSE` wrapped as a listen error; callers using `errors.Is(..., ErrAlreadyStarted)` miss. Check under the lock first (or restructure listen + re-check) while keeping bind-before-goroutine. |
| 2 | inline `3741082050` concurrency | `ops.go:79` | real | fix | `Shutdown` with a deadline does not abort in-flight handlers; `Stop` can return while handler goroutines still run. Call `Close()` when `Shutdown` returns a non-nil error, then still join `done`. |
| 3 | inline `3741082068` architecture | `ops.go:38` | real | fix | ADR-0004 commits to ServeMux 1.22+ patterns; ADR-0005 / README document GET. Register `GET /metrics`, `GET /healthz`, `GET /readyz`. |
| 4 | inline `3741082114` architecture | `ops.go:70` | real | fix | `err != http.ErrServerClosed` is the errorlint anti-pattern under ADR-0004; use `errors.Is`. |
| 5 | inline `3741082135` architecture | `pool.go:247` | real | fix | Ops shutdown failure is logged and dropped; `Stop` can report nil after a failed ops drain. Surface via `errors.Join` so the start/stop honesty is symmetric. |
| 6 | inline `3741118837` tests | `stats.go:117` | real | fix | Every test stops after the first `Stats` call or uses an hour-long interval. A one-shot refresher that never ticks would still pass. Add a Docker-free periodic-refresh sensor (`testing/synctest` preferred) asserting ≥2 `Stats` calls. |
| 7 | inline `3741118861` tests | `stats_test.go:411` | real | fix | `TestRunRefreshesImmediatelyBeforeFirstTick` cancels without joining and without goleak. Join on a `done` channel and add goleak (fold into #6 if convenient). |
| 8 | inline `3741118881` tests | `pool.go:117` | real | fix | Draining→503 is unit-tested; the `"queue stats stale"` branch is integration-only. Add a memdriver unit test that forces failed refreshes past `2×interval` and asserts `/readyz` 503 with `stale`. |
| 9 | inline `3741130692` regression | `doc.go:131` | real | fix | Docs claim `/readyz` returns 503 when not started; ops is only bound inside `Start`, so not-started is connection refused. Align package docs with the ready closure (`draining` / `queue stats stale`). |
| 10 | inline `3741130733` regression | `client.go:165` | real | fix | Godoc says any non-positive value is corrected *and reported*; `0` is silent by design (`TestStatsIntervalZeroTakesTheDefaultSilently`). Fix the comment to match the switch. |
| 11 | inline `3741134428` sql-performance | `003_index_dead_jobs.sql:12` | real | fix | Comment claims the build is "over only the rows that have died"; a non-`CONCURRENTLY` partial `CREATE INDEX` still heap-scans the table to evaluate the predicate. Correct the comment; do **not** switch to `CONCURRENTLY` (transactional migrator constraint is intentional). |
| — | issue `5227105502` requirements | (PR-level) | informational | won't-fix | Requirements pass inventory; no defect. Survives in this triage as context only. |
| — | issue `5227127524` summary | (PR-level) | informational | won't-fix | Roll-up of the inline findings above; no independent defect. |

**Counts:** 11 real / 0 false among inline findings; 11 fix / 0 won't-fix among actionable; 2 informational issue comments won't-fix.
