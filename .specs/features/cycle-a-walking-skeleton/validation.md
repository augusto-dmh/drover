# Cycle A — Walking Skeleton Validation

**Date**: 2026-07-25
**Spec**: `.specs/features/cycle-a-walking-skeleton/spec.md`
**Diff range**: `e79d281..HEAD` (12 commits, `b4572c5..2212c13`, 31 files, +3163)
**Verifier**: independent sub-agent (author ≠ verifier), evidence-or-zero

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1 core types | ✅ Done | `b4572c5` — `job.go`, `errors.go` |
| T2 driver contract | ✅ Done | `96d1223` — 6-method interface confirmed (`internal/driver/driver.go:67-74`) |
| T3 memdriver | ✅ Done | `3f9fb88` |
| T4 migrations | ✅ Done | `dd8a6e4` |
| T5 pgdriver | ✅ Done | `a974c6a` — `FOR UPDATE SKIP LOCKED` at `internal/pgdriver/queries.sql:18` |
| T6 worker registry | ✅ Done | `be8fc45` |
| T7 client enqueue | ✅ Done | `375f452` |
| T8 worker loop | ✅ Done | `0fd5c55` (+ fixes `d33519b`, `e4440c7`) |
| T9 e2e proof | ✅ Done | `8edca28` |
| T10 CI/lint | ✅ Done | `2212c13` — `.github/workflows/ci.yml`, `.golangci.yml` |

---

## Spec-Anchored Acceptance Criteria

All paths relative to repo root. Line numbers verified against working tree at HEAD.

### CORE-01: Schema and migrator

| AC | Spec-defined outcome | Evidence | Result |
| -- | -------------------- | -------- | ------ |
| AC1 fresh DB → table + enum + version table, version 1 | `drover_jobs`, state enum, `drover_schema_version` exist; version = 1 | `internal/migrate/migrate_integration_test.go:26-32` — `SELECT MAX(version) FROM drover_schema_version` → `version != 1` fails; `:34-56` columns of `drover_jobs`; `:58-80` — enum labels `slices.Equal(states, wantStates)` for all 7 states | ✅ PASS |
| AC2 second run → no changes, nil | second `Migrate` returns nil; no re-apply | `internal/migrate/migrate_integration_test.go:90-101` — `second Migrate returned %v, want nil` + `COUNT(*) FROM drover_schema_version` → `applied != 1` fails | ✅ PASS |
| AC3 exact column set | 12 named columns incl. `queue` (default `'default'`), `args` (jsonb) | `internal/migrate/migrate_integration_test.go:49-55` — `slices.Equal(columns, wantColumns)` with the exact 12-column set. Column **names** exactly asserted; the parenthetical `queue` DEFAULT `'default'` and `args` jsonb **type** are in the DDL (`internal/migrate/migrations/001_create_jobs.sql:14-15`) but not asserted by any test | ✅ PASS (minor: default/type unasserted — gap G2) |

### CORE-02: Transactional typed enqueue

| AC | Spec-defined outcome | Evidence | Result |
| -- | -------------------- | -------- | ------ |
| AC1 Insert persists typed row | `kind = args.Kind()`, JSON args, state `available`, returns id/state | `client_test.go:45-59` — `row.Kind != "greet"`, `row.State != StateAvailable`, `string(row.Args) != '{"name":"ada"}'`, stored row `available`; Postgres path `internal/pgdriver/pgdriver_integration_test.go:46-66` | ✅ PASS |
| AC2 InsertTx + commit → visible, available | row count 1, state `available` | `client_integration_test.go:48-64` — `after commit: %d jobs, want 1` and `row.State != StateAvailable`; driver-level `internal/pgdriver/pgdriver_integration_test.go:94-107` | ✅ PASS |
| AC3 InsertTx + rollback → no row | count 0 | `client_integration_test.go:34-46` — `after rollback: %d jobs, want 0`; driver-level `pgdriver_integration_test.go:80-92` | ✅ PASS |
| AC4 marshal failure → wrapped error, nothing persisted | error non-nil naming kind; 0 rows | `client_test.go:75-89` — `TestInsertWrapsMarshalFailure`: err non-nil, `strings.Contains(err.Error(), 'kind "bad"')`, `assertNothingPersisted` (`client_test.go:24-33`). Only `Insert` exercised; `InsertTx` shares `insertParamsFor` (`client.go:102-112`, `114-124`) so structurally guarded, but no direct test — gap G3 | ✅ PASS (minor) |
| AC5 empty kind → `ErrInvalidKind`, nothing persisted | `errors.Is(err, ErrInvalidKind)`; 0 rows | `client_test.go:62-73` — `!errors.Is(err, ErrInvalidKind)` fails + `assertNothingPersisted`. Same InsertTx caveat as AC4 (G3) | ✅ PASS (minor) |

### CORE-03: Claim-and-execute worker loop

| AC | Spec-defined outcome | Evidence | Result |
| -- | -------------------- | -------- | ------ |
| AC1 claim → running, decode T, Work, nil → completed + finalized_at | state `running` on claim; typed decode; `completed`; `finalized_at` set | claim→running+attempt: `internal/memdriver/memdriver_test.go:90-97` (`State = %q, want running`, `Attempt = %d, want 1`), `internal/pgdriver/pgdriver_integration_test.go:138-143`; typed decode: `worker_test.go:50-59` (`worker.got.Args.Name != "ada"`); completion: `loop_test.go:133-139` — `waitFor(... "completed")` + `stored.FinalizedAt == nil` fails; Postgres finalize: `pgdriver_integration_test.go:239-248` | ✅ PASS |
| AC2 two claimers, N jobs → exactly once each | 50 jobs, 2 loops, 50 distinct, each once, all `completed` | `e2e_integration_test.go:86-109` — waits `countInState("completed") == 50`, `len(worker.runs) != jobs` fails, `n != 1` fails per job; driver-level SKIP LOCKED: `pgdriver_integration_test.go:185-224` (2 goroutines × 25 jobs, zero double-claims) | ✅ PASS |
| AC3 Work error → error msg + attempt recorded, `dead` | `errors[0] = {Attempt:1, Error:"boom"}`, state `dead` | `loop_test.go:164-170` — `len(recorded) != 1 \|\| recorded[0].Error != "boom" \|\| recorded[0].Attempt != 1` fails; Postgres e2e: `e2e_integration_test.go:132-149` (`smtp unreachable`, Attempt 1) | ✅ PASS |
| AC4 panic → recover, record value + stack, `dead`, loop continues | panicked job `dead` with "kaboom" + non-empty Trace; subsequent job completes | `loop_test.go:196-206` — bad→`dead`, `!strings.Contains(recorded[0].Error, "kaboom")` fails, `recorded[0].Trace == ""` fails; good job reaches `completed` (loop continues) | ✅ PASS |
| AC5 unregistered kind → `dead` + "unregistered kind" error | error entry naming the kind | `loop_test.go:218-224` — `dead` + `strings.Contains(recorded[0].Error, 'no worker registered for kind "ghost"')` (spec's phrase "unregistered kind" is semantic, exact wording unspecified — actual message names the kind) | ✅ PASS |
| AC6 ctx cancel → stop polling, drain in-flight, return; goleak-clean | Start blocks until in-flight finishes; returns nil; no leaked goroutines | `loop_test.go:269-286` — `Start returned while a job was in flight` fails if it returns early; returns nil after `close(release)`; drained job `completed`; `goleak.VerifyNone(t, goleak.IgnoreCurrent())` on all 7 loop tests (`loop_test.go:123,152,177,210,228,252,290`) | ✅ PASS |
| AC7 idle → sleep configured poll interval before next fetch | wait = `PollInterval` between fetches when idle | `loop_test.go:289-298` — `TestStartKeepsPollingWhileIdle` asserts only `counting.fetches.Load() >= 3` (polling continues). **No test asserts the interval is actually waited** — sensor mutant M5 (busy-poll) survives the full unit suite. Gap G1 | ⚠️ WEAK (covered for "keeps polling", not discriminated for "sleeps the configured interval") |

### CORE-04: Registry ergonomics and observability floor

| AC | Spec-defined outcome | Evidence | Result |
| -- | -------------------- | -------- | ------ |
| AC1 duplicate Register → panic naming kind | panic; message contains the kind | `worker_test.go:67-77` — `recover()` non-nil required, `strings.Contains(msg, '"greet"')` required | ✅ PASS |
| AC2 slog records for start/complete/fail with `job_id`, `kind`, `attempt`, duration, outcome | one record each with the listed attrs | start+complete: `loop_test.go:140-148` — asserts `msg="drover: job started"`, `msg="drover: job completed"`, `job_id=`, `kind=greet`, `attempt=1`, `duration=`; fail: `loop_test.go:171-173` asserts only `msg="drover: job failed"` — failure-record attributes unasserted (code emits them, `loop.go:71-73`). Gap G4 | ✅ PASS (minor) |
| AC3 no logger → `slog.Default()` | `c.logger == slog.Default()` | `client_test.go:106-108` — `if c.logger != slog.Default()` fails | ✅ PASS |

### Edge Cases

| Edge case | Evidence | Result |
| --------- | -------- | ------ |
| Empty table → no job, no error | `internal/memdriver/memdriver_test.go:112,125-131` ("empty store" case: err nil, 0 rows); `internal/pgdriver/pgdriver_integration_test.go:154-160` (exhausted queue → 0 rows) | ✅ |
| Args decode failure at execution → `dead` with decode error, loop continues | `loop_test.go:242-248` — `dead` + `strings.Contains(recorded[0].Error, "decode args")`; workFunc-level: `worker_test.go:87-97` (Work not called on bad args) | ✅ |
| DB unreachable at poll → log ERROR, retry next tick | `loop_test.go:306-317` — flaky driver fails 2 fetches, job still completes; asserts `msg="drover: fetch jobs"` (level=ERROR emitted by `loop.go:33` but level not asserted — see G5) | ✅ (minor) |
| Nil pool → constructor error, no panic | `client_test.go:91-98` — `NewClient(nil, Config{})` must return error (a panic would fail the test) | ✅ |

**Status**: 17/18 ACs+edges fully anchored; 1 weak (CORE-03 AC7 timing not discriminated); no unclaimed tests found (every test maps to an AC, edge case, or Done-when).

---

## Gate Check

| Gate | Command | Result |
| ---- | ------- | ------ |
| Build | `go build ./... && go vet ./...` | exit 0 |
| Quick (unit, no Docker) | `go test -race -count=1 ./...` | **PASS** — `ok drover` (1.194s), `ok internal/memdriver` (1.044s); 33 test runs incl. subtests (root 17, memdriver 8 top-level + 8 subtests); 0 failed, 0 skipped |
| Full (integration) | `go test -race -tags=integration -count=1 ./...` | **PASS** — `ok drover` (15.2s), `ok internal/memdriver` (1.1s), `ok internal/migrate` (13.7s), `ok internal/pgdriver` (16.1s); 14 integration test functions (root 3, migrate 2, pgdriver 9); 0 failed, 0 skipped |

- **Test count before feature**: 0 (greenfield) → **after**: 33 unit runs + 14 integration functions. Delta all-new; no deletions or weakened assertions possible.
- Spec success criteria: unit suite Docker-free ✅; `-race` integration incl. concurrent-claim ✅; `example_test.go` builds as a testable example (compile-only — no `// Output:`, appropriate since it needs a live DB) ✅; goleak on loop shutdown ✅ (scoped with `IgnoreCurrent`, commit `d33519b`).

---

## Discrimination Sensor

Method: mutation applied in working tree (tracked files verified clean before/after each), relevant unit package run, then `git checkout --` revert. No commits made.

| # | Mutation | File:line (original) | Killed? | Killing test |
| - | -------- | -------------------- | ------- | ------------ |
| M1 | Claim does not increment `Attempt` (`row.Attempt++` removed) | `internal/memdriver/memdriver.go:84` | ✅ Killed | `TestFetchAvailableClaimsInFIFOOrderWithLease` — `memdriver_test.go:93: Attempt = 0, want 1` |
| M2 | Failed job finalized as `completed` instead of `markDead` | `loop.go:70` | ✅ Killed | `TestStartMarksFailingJobDeadWithRecordedError` / `TestStartMarksUndecodableArgsDead` (timeout waiting for `dead`) + goleak |
| M3 | Empty-kind validation removed from `insertParamsFor` | `client.go:116-118` | ✅ Killed | `TestInsertRejectsEmptyKind` — `client_test.go:70: error = <nil>, want ErrInvalidKind` |
| M4 | Panic recovery swallows panic (`err` left nil → panicked job treated as success) | `loop.go:92` | ✅ Killed | `TestStartRecoversPanicAndKeepsRunning` (timeout waiting for `dead`) |
| M5 | `sleep` returns immediately without waiting `pollInterval` (busy-poll) | `loop.go:120-127` | ❌ **SURVIVED** — full root unit suite passes (`ok drover 0.081s`) | none — `TestStartKeepsPollingWhileIdle` only asserts fetch count ≥ 3, never the spacing |

**Sensor depth**: lightweight+ (5 behavior-level mutants across 3 components; one earlier M4 variant discarded as a compile error, not counted)
**Result**: 4/5 killed — CORE-03 AC7 is not discriminated → fix task required.

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| Minimum code / no scope creep (no retries, pools, middleware, metrics — matches Out of Scope table) | ✅ |
| Surgical diff (31 new files, 0 modified pre-existing) | ✅ |
| Matches documented patterns (root pkg + `internal/`, pgx+sqlc committed codegen, slog injection, `%w` sentinel errors — ADR-0004) | ✅ |
| Spec-anchored outcome check | ✅ with 5 minor notes (G1–G5) |
| Per-layer Coverage Expectation (matrix in tasks.md) met: memdriver all methods + guards + concurrency; pgdriver happy+error+concurrent-claim; e2e happy+failure+2-loop | ✅ |
| Every test maps to a spec requirement | ✅ |
| Documented guidelines followed: `CLAUDE.md` Workflow (table-driven, `t.Parallel` on unit tests, `-race`, testcontainers, Docker-free unit suite) | ✅ |

---

## Ranked Gaps

1. **G1 (Major — surviving mutant, CORE-03 AC7)**: No test discriminates "sleep for the configured poll interval". Removing the wait entirely (busy-poll) passes the whole unit suite. `loop_test.go:289-298` proves polling continues but not its pacing. **Fix task**: assert an upper bound on fetch count over a measured window with a larger `PollInterval` (or use a fake clock / `testing/synctest`), so a missing/shortened sleep fails.
2. **G2 (Minor — CORE-01 AC3)**: Column-set assertion (`migrate_integration_test.go:49-55`) checks names only; the spec's `queue` DEFAULT `'default'` and `args` jsonb type are never asserted (DDL is correct: `001_create_jobs.sql:14-15`). Add `information_schema.columns` checks for `column_default`/`data_type`.
3. **G3 (Minor — CORE-02 AC4/AC5)**: Spec says "`Insert`/`InsertTx` SHALL return…" but marshal-failure and empty-kind are tested only through `Insert`. `InsertTx` shares `insertParamsFor` (`client.go:102-112`), so the guard structurally applies; a two-line unit test on `InsertTx` would close it.
4. **G4 (Minor — CORE-04 AC2)**: Failure log record asserts only `msg="drover: job failed"` (`loop_test.go:171-173`); `job_id`/`kind`/`attempt`/`duration` attrs asserted for start/complete only.
5. **G5 (Minor — D-6 edge)**: DB-unreachable test asserts the message, not `level=ERROR` (`loop_test.go:315-317`).

No spec-precision gaps: every AC defines a checkable outcome (AC5's "unregistered kind" wording is intentionally semantic; actual message names the kind).

---

## Requirement Traceability Update

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| CORE-01 | Pending | ✅ Verified (minor G2) |
| CORE-02 | Pending | ✅ Verified (minor G3) |
| CORE-03 | Pending | ⚠️ Needs Fix — AC7 not discriminated (G1) |
| CORE-04 | Pending | ✅ Verified (minor G4) |

---

## Summary

**Overall**: ⚠️ Issues → **FAIL** (by the surviving-mutant rule; functionally everything works and both gates are green)

**Spec-anchored check**: 17/18 ACs+edges matched spec outcomes; CORE-03 AC7 weak
**Gate**: unit 33/33, integration 14/14, build+vet clean — 0 failures, 0 skips
**Sensor**: 4/5 mutants killed; M5 (busy-poll) survived

**What works**: full enqueue→claim→execute→finalize path on memdriver and Postgres; SKIP LOCKED exactly-once proven at driver and e2e level; panic recovery, unregistered-kind, decode-failure, transition guards, drain-on-cancel, goleak-clean shutdown, idempotent migrator.

**Next steps**: one fix task for G1 (strengthen AC7 idle-interval assertion, then re-run sensor M5); G2–G5 optional assertion tightening.

---

## Re-verification (iteration 1)

**Date**: 2026-07-25
**Fix commit**: `063bd2e` "test: strengthen assertions flagged by feature validation" (3 files, +72/−10 — test-only)
**Diff range now covered**: `e79d281..063bd2e` (13 commits)
**Verifier**: independent sub-agent (author ≠ verifier), evidence-or-zero

### Per-gap verdicts

| Gap | Spec anchor | New evidence | Verdict |
| --- | ----------- | ------------ | ------- |
| G1 (Major) CORE-03 AC7 — poll-interval sleep not discriminated | spec.md:78 "sleep for the configured poll interval before the next fetch" | `loop_test.go:295-327` rewritten: `PollInterval: 30ms` (`:300`), measured 150 ms window (`:306`), lower bound `fetches < 2` fails (`:319`), **upper bound `fetches > 20` fails** (`:324`) — a loop that skips the sleep busy-polls far past the bound | ✅ Closed |
| G2 (Minor) CORE-01 AC3 — `queue` default / `args` type unasserted | spec.md:48 "`queue` (default `'default'`), `args` (jsonb)" | `migrate_integration_test.go:82-97` — `information_schema.columns` lookup (`columnMeta`, `:82-91`); asserts `queue` is `text` with default `'default'::text` (`:92-94`) and `args` is `jsonb` with default `'{}'::jsonb` (`:95-97`; default is beyond spec, matches DDL) | ✅ Closed |
| G3 (Minor) CORE-02 AC4/AC5 — InsertTx validation untested | spec.md:61-62 "`Insert`/`InsertTx` SHALL return…" | `client_test.go:91-104` — `TestInsertTxValidatesBeforeTouchingTransaction`: empty kind → `!errors.Is(err, ErrInvalidKind)` fails (`:96-98`); marshal failure → wrapped error containing `` `kind "bad"` `` (`:99-101`); `assertNothingPersisted` (`:103`). `tx` passed as nil — proves validation precedes any tx use | ✅ Closed |
| G4 (Minor) CORE-04 AC2 — failure-log attributes unasserted | spec.md:89 "one `slog` record each with `job_id`, `kind`, `attempt`, duration … and outcome" | `loop_test.go:171-179` — failure record now requires all of `msg="drover: job failed"`, `job_id=<row.ID>`, `kind=greet`, `attempt=1`, `duration=`, `error=boom` | ✅ Closed |
| G5 (Minor) D-6 edge — fetch-error level unasserted | spec.md:96 "log at ERROR and retry next tick" | `loop_test.go:345-348` — asserts `` `level=ERROR msg="drover: fetch jobs"` `` | ✅ Closed |

### Sensor re-run (M5)

Working tree verified clean before mutation (only untracked `.specs/`). Mutation: `loop.go:119-128` `sleep` body (timer+select) replaced with `if ctx.Err() != nil { return false }; return true` (busy-poll).

- `go test -race -count=1 -run TestStartKeepsPollingWhileIdle .` → **FAIL**: `loop_test.go:325: loop fetched 149960 times in 150ms with a 30ms interval — polling without sleeping`
- Reverted via `git checkout -- loop.go`; same test → `ok drover 1.177s`; `git status --porcelain` → only `?? .specs/` (tree clean)

**M5: ✅ KILLED** — sensor now 5/5.

### Gate re-run

| Gate | Command | Result |
| ---- | ------- | ------ |
| Vet | `go vet ./...` | exit 0 |
| Quick (unit, no Docker) | `go test -race -count=1 ./...` | **PASS** — `ok drover` (1.261s), `ok internal/memdriver` (1.028s); 0 failed |
| Full (integration, Docker) | `go test -race -tags=integration -count=1 ./...` | **PASS** — `ok drover` (8.7s), `ok internal/memdriver` (1.1s), `ok internal/migrate` (8.2s), `ok internal/pgdriver` (9.0s); 0 failed |

### Requirement Traceability Update

| Requirement | Previous | New |
| ----------- | -------- | --- |
| CORE-01 | ✅ Verified (minor G2) | ✅ Verified |
| CORE-02 | ✅ Verified (minor G3) | ✅ Verified |
| CORE-03 | ⚠️ Needs Fix (G1) | ✅ Verified |
| CORE-04 | ✅ Verified (minor G4) | ✅ Verified |

### Verdict

**Overall**: ✅ **PASS** — all 5 gaps closed with spec-anchored assertions, surviving mutant M5 killed (sensor 5/5), all gates green, fix commit is test-only (no production code touched).
