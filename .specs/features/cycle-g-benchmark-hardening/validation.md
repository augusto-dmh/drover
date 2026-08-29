# Benchmark + Hardening Validation

**Date**: 2026-08-16
**Spec**: `.specs/features/cycle-g-benchmark-hardening/spec.md`
**Diff range**: `main..HEAD` (`afabb50..04c8339`)
**Verifier**: independent sub-agent (author ≠ verifier)

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1 | ✅ Done | `47ee360` driver + staging SQL |
| T2 | ✅ Done | `8866c7b` memdriver |
| T3 | ✅ Done | `ae2a449` pgdriver COPY |
| T4 | ✅ Done | lint close skipped (no empty commit) |
| T5 | ✅ Done | `26c777a` Client InsertMany |
| T6 | ✅ Done | lint close skipped |
| T7 | ✅ Done | `3a7bd12` local wake |
| T8 | ✅ Done | `f1bc106` LISTEN/NOTIFY |
| T9 | ✅ Done | lint close skipped |
| T10 | ✅ Done | `aa3c338` drover-bench |
| T11 | ✅ Done | `8e3fb6b` README + examples |
| Follow-up | ✅ Done | `aade081` listen-reconnect / InsertTx nudge / write-failure tests; `04c8339` README GOARCH |

---

## Spec-Anchored Acceptance Criteria

### P1: Atomic batch insert

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `InsertMany` with a valid non-empty batch THEN persist every item and return `[]*JobRow` of equal length, input order, distinct positive IDs | `len(rows)==len(items)`; each `ID>0` and unique; stored kinds/args match input order | `client_test.go:349-372` — `len(rows) != len(items)`; `row.ID <= 0`; duplicate ID; `string(row.Args) != wantArgs[i]`. Also `internal/memdriver/memdriver_test.go:169-183` and `internal/pgdriver/pgdriver_integration_test.go:1514-1528` (`row.Kind != batch[i].Kind`) | ✅ PASS |
| WHEN any item has nil `Args`, empty kind, or marshal error THEN return that error, insert zero rows, table unchanged | error (`ErrInvalidKind` / wrapped marshal); store empty | `client_test.go:433-434` — `errors.Is(err, ErrInvalidKind)`; `456` — error names `kind "bad"`; `474` — `assertNothingPersisted` (`len(rows) != 0` on fetch). Tx path: `client_test.go:556-565` | ✅ PASS |
| WHEN `InsertMany` with nil or empty slice THEN empty result, nil error, without writing | empty slice, `err==nil`, no rows | `client_test.go:406-415` — `err != nil`; `rows == nil`; `len(rows) != 0`; `assertNothingPersisted`. Driver: `internal/memdriver/memdriver_test.go:205-215` (next ID not consumed) | ✅ PASS |
| WHEN an item's `Opts` is nil THEN queue `"default"`, due immediately, matching single `Insert` | `Queue=="default"`, `State==available`, `ScheduledAt` not in the future | `client_test.go:499-503` — `rows[0].Queue != "default" \|\| rows[0].State != StateAvailable`; `ScheduledAt.After(now+1m)` | ✅ PASS |
| WHEN `ScheduledAt` is future THEN `scheduled`; WHEN zero or past THEN `available`; dueness by DB clock (AD-035) | future → `scheduled` with that time; zero/past → `available` | `client_test.go:508-515`; `internal/pgdriver/pgdriver_integration_test.go:1599-1612` — `rows[1].State != "scheduled"`; `rows[0]/[2] != "available"`. SQL CASE uses `now()` (`internal/pgdriver/queries.sql:38-41`) | ✅ PASS |
| WHEN `InsertManyTx` then caller rolls back THEN none visible to `FetchAvailable` | `FetchAvailable` claims 0 after rollback | `internal/pgdriver/pgdriver_integration_test.go:1654-1655` — `len(claimed) != 0`. Client: `client_integration_test.go:107-108` — `countJobs() != 0` after rollback | ✅ PASS |
| WHEN `InsertMany` runs on Postgres THEN load via `COPY FROM` (session-temp staging), not N `InsertJob` | one `CopyFrom`; zero `INSERT … VALUES` | `internal/pgdriver/pgdriver_integration_test.go:1777-1781` — `trace.copyFrom != 1`; `trace.insertJob != 0` | ✅ PASS |
| WHEN two `InsertManyTx` share one transaction THEN both batches persist (no temp-table-exists failure) | 3 jobs after commit, kinds `a,b,c` | `internal/pgdriver/pgdriver_integration_test.go:1738-1744` — `len(claimed) != 3`; `!slices.Equal(got, want)` | ✅ PASS |

### P1: Optional notify wake-up

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `NotifyWakeup` is unset or false THEN idle fetch waits `PollInterval`; inserts do not emit `NOTIFY` | default false; no local wake; fetch count unchanged; no Postgres NOTIFY | `client_test.go:134-135` — `c.notifyWakeup` true. `client_test.go:1144-1145` — `len(c.wake) != 0`. `pool_test.go:984-992` — fetch count changed; state not `available`. `client_integration_test.go:361-363` — `err == nil` on `WaitForNotification` | ✅ PASS |
| WHEN `NotifyWakeup` is true and same-client `Insert`/`InsertMany` succeeds THEN idle fetch resumes without waiting out `PollInterval` (elapsed upper bound) | claim elapsed `< 1s` against `3s` poll | `pool_test.go:941-950` — `time.After(notifyWakeUpperBound)`; `elapsed > notifyWakeUpperBound`. Uses `Insert`; `InsertMany` shares `wakeAfterInsert` and is covered for nudge in `client_test.go:1126-1130` | ✅ PASS |
| WHEN `NotifyWakeup` is true and another process commits `Insert` THEN listener wakes on channel `drover` (empty payload) and claims without waiting out `PollInterval` | claim elapsed well under poll; channel `drover` | `client_integration_test.go:265-274` — job id + `elapsed > notifyWakeUpperBound`. Channel name: `internal/pgdriver/pgdriver_integration_test.go:1871-1872` — `n.Channel != "drover"`. Payload `""` is produced by `droverNotifySQL` (`internal/pgdriver/pgdriver.go:101`) and is not separately asserted | ✅ PASS |
| WHEN `InsertTx` / `InsertManyTx` with the flag set THEN emit `NOTIFY` in the caller transaction; listeners wake only if that tx commits | no local nudge before commit; no run before commit; run after commit within upper bound | Local wake: `client_test.go:1197-1205` — `len(c.wake) != 0` after `InsertTx` and `InsertManyTx`. Cross-client: `client_integration_test.go:311-331` — `job ran before the producer committed`; post-commit `elapsed > notifyWakeUpperBound`. Driver: `internal/pgdriver/pgdriver_integration_test.go:1857-1872`. `InsertManyTx` notify is the same `notifyTx` call | ✅ PASS |
| WHEN `LISTEN` cannot be established at `Start` THEN `Start` returns nil, logs, keeps polling | `Start` err nil; log contains listen failure; job claimed via poll | `pool_test.go:1056-1077` — `Start: %v, want nil`; log substring `drover: listen for job notifications`; `job was not claimed after LISTEN failed` | ✅ PASS |
| WHEN the listen connection drops after `Start` THEN log, reconnect, keep polling; SHALL NOT stop the worker pool | Listen retried after drop; pool still claims via poll | `pool_test.go:1126-1142` — `drv.calls.Load() >= 2`; log substring `drover: listen for job notifications`; `job was not claimed after LISTEN dropped` | ✅ PASS |
| WHEN `Stop` is called during an idle wait THEN SHALL not wait out `PollInterval` | `Stop` elapsed `< 1s` against `3s` poll with flag on | `pool_test.go:1021-1022` — `elapsed > notifyWakeUpperBound` | ✅ PASS |
| WHEN `NotifyWakeup` is true, `Start` SHALL acquire a dedicated connection for `LISTEN`; docs SHALL state PgBouncer transaction pooling is incompatible | session-scoped LISTEN; docs name session pooling / PgBouncer | Behaviour: `internal/pgdriver/pgdriver.go:131` (`pool.Acquire`) exercised by `internal/pgdriver/pgdriver_integration_test.go:1882-1893` (`ListenWakeups` + cancel) and `1897-1928` (nudge on NOTIFY). Docs: `README.md:29`; `client.go:72-75`; `example_test.go:117` (`NotifyWakeup: false` + session pooling comment) | ✅ PASS |

### P1: Benchmark harness

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `drover-bench` `--mode enqueue` THEN insert `--jobs` via `InsertMany` in `--batch` chunks and print jobs/sec + methodology (GOOS/GOARCH/NumCPU, Postgres version, jobs/batch/concurrency, no-op handlers) | those keys present; chunk sizes | `cmd/drover-bench/main_test.go:116-130` — stdout contains each key including `GOARCH=` and `jobs/sec=`. Chunks: `cmd/drover-bench/bench_test.go:52-61` | ✅ PASS |
| WHEN `--mode drain` THEN insert, start no-op pool of `--concurrency`, wait until every job completes, print jobs/sec + p50/p95/p99 | percentiles + jobs/sec printed; completion signal after last job | `cmd/drover-bench/main_test.go:183-186` — `p50=`/`p95=`/`p99=`/`jobs/sec=`. Wait: `cmd/drover-bench/bench_test.go:92-104` (`allDone` after 2 of 2). Default `--mode` is drain (`cmd/drover-bench/main.go:58`) | ✅ PASS |
| WHEN `--database` missing and `DATABASE_URL` empty THEN non-zero exit, insert nothing | exit 2; execute not called | `cmd/drover-bench/main_test.go:70-74` — `code != 2`; `ran.Load()` | ✅ PASS |
| WHEN `--jobs` or `--batch` < 1 THEN non-zero usage exit | exit 2; no insert | `cmd/drover-bench/main_test.go:31-47` cases + same `code != 2` / `ran.Load()` | ✅ PASS |
| WHEN `--mode` is neither `enqueue` nor `drain` THEN non-zero usage exit | exit 2 | `cmd/drover-bench/main_test.go:50-57` + `70-74` | ✅ PASS |
| Binary lives under `cmd/drover-bench/` and SHALL NOT be added to GoReleaser | only `./cmd/drover` in GoReleaser | `.goreleaser.yaml:8` — `main: ./cmd/drover`; `README.md:198` | ✅ PASS |

### P1: README benchmark table

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN README is opened THEN `## Benchmarks` with enqueue jobs/sec, drain p50/p95/p99, GOOS/GOARCH, CPU, Postgres version, job count, batch, concurrency, no-op single-node caveat | table + methodology + reproduce command matching a real run | `README.md:166-198` — table `16,230` / `704` / `8.19s` / `13.74s` / `14.14s` matches recorded `jobs/sec=16229.96` and drain percentiles. Machine names `GOOS=linux`, `GOARCH=amd64`, Postgres 16.14, 10,000 jobs, batch 256, concurrency 10, no-op single-node caveat | ✅ PASS |
| WHEN the section names `NotifyWakeup` / `InsertMany` THEN `example_test.go` has compile-checked Examples naming those fields | Examples compile | `example_test.go:117` — `NotifyWakeup: false`; `example_test.go:127` — `client.InsertMany(...)` | ✅ PASS |
| WHEN Planned API sample is read THEN it shows `InsertMany` alongside `Insert` / `InsertTx` | all three appear | `README.md:60-74`; Example `example_test.go:121-130` | ✅ PASS |
| WHEN Roadmap blurb is read THEN Cycle G bench work is shipped, not "then: benchmarks…" | A–G includes published methodology; "Then:" is later work | `README.md:202` — `cycles A–G` … `**benchmarks with published methodology**. Then: periodic jobs…` | ✅ PASS |

**Status**: ✅ All ACs covered

---

## Discrimination Sensor

Scratch: `git worktree add /tmp/drover-g-sensor-reverify HEAD` (detached `04c8339`). Restored after each mutant; worktree removed. Primary tree never mutated.

| Mutation | File:line | Description | Killed? |
| -------- | --------- | ----------- | ------- |
| 1 | `client.go:407` | `c.nudge()` on `InsertTx` after the driver write (before caller commit) | ✅ Killed — `TestInsertTxDoesNotNudgeBeforeCommit` (`wake channel length = 1 after InsertTx, want 0`) |
| 2 | `pool.go:579` | `listenForWakeups` calls `ListenWakeups` once and returns (no retry loop) | ✅ Killed — `TestListenReconnectsAfterDropAndKeepsPolling` (`timed out waiting for LISTEN to be retried after the drop`) |
| 3 | `internal/pgdriver/pgdriver.go:191` | Skip `CopyFrom`; loop `InsertJob` instead | ✅ Killed — `TestInsertManyUsesCopyFrom` (`CopyFrom 0`, `InsertJob VALUES 5`) |
| 4 | `client.go:413` | Drop validate-all-first; `Insert` each item so a later bad item leaves a prefix | ✅ Killed — `TestInsertManyRejectsInvalidItems` (`found 1 persisted jobs, want 0`) |
| 5 | `client.go:418` | Empty `InsertMany` inserts a dummy row | ✅ Killed — `TestInsertManyEmptyOrNilWritesNothing` (`got 1 rows, want 0`) |
| 6 | `pool.go:593` | `sleep` ignores the wake channel (timer + stop only) | ✅ Killed — `TestNotifyWakeupClaimsInsertBeforePollInterval` (`not claimed within 1s`) |

**Sensor depth**: P0-full (≥5 behaviour-level mutations on insert atomicity + fetch wake-up, including the two required follow-up mutants)
**Result**: 6/6 killed — PASS ✅

---

## Interactive UAT Results (if performed)

Not performed — backend/library feature; automated checks are sufficient per validate.md.

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| Minimum code | ✅ |
| Surgical changes | ✅ |
| No scope creep | ✅ |
| Matches patterns | ✅ |
| Spec-anchored outcome check (asserted values match spec) | ✅ |
| Per-layer Coverage Expectation met (domain 1:1 ACs; routes happy+edge+error) | ✅ |
| Every test maps to a spec requirement — no unclaimed tests | ✅ extras map to edges / Done-when (empty-batch notify, LISTEN cancel, bench chunks, write-failure after validation) |
| Documented guidelines followed: `CLAUDE.md` (table-driven, `t.Parallel`, `-race`, testcontainers, goleak, unit suite without Docker); `.claude/skills/tlc-spec-driven/references/coding-principles.md` | ✅ |

---

## Edge Cases

- [x] Mixed queues persist on the named queue (or `"default"`): `client_test.go:505-537`; `internal/memdriver/memdriver_test.go:238-264`
- [x] `InsertMany` fails after validation (COPY or INSERT error) → wrapped error and zero rows: `client_test.go:1223-1229` — error contains `copy failed`; `assertNothingPersisted`. Rolled-back tx write: `internal/pgdriver/pgdriver_integration_test.go:1696-1706` — `err == nil`; `n != 0`
- [x] Future-only scheduled batch MAY wake and claim nothing: allowed (`MAY`); not required
- [x] Extra wakes coalesce (capacity 1): `client_test.go:1129-1130` — `len(c.wake) != 1`
- [x] `memdriver.InsertTx` / `InsertManyTx` return `ErrTxUnsupported`: `internal/memdriver/memdriver_test.go:151-152`, `304-305`; `client_test.go:600-601`

---

## Gate Check

- **Gate command**: `go build ./... && go vet ./...` (tasks.md Build)
- **Result**: PASS (exit 0)
- **Quick**: `go test -race ./...` — all packages ok, 0 failed. Recount `go test -count=1 -json` on unit packages: 492 passed, 0 failed, 0 skipped
- **COPY/NOTIFY evidence**: `go test -race -tags=integration -count=1 . ./internal/pgdriver` — both packages ok, 0 failed
- **Test count before feature**: 307 `func Test` on `main`
- **Test count after feature**: 352 `func Test` on `HEAD`
- **Delta**: +45 (no `func Test` deleted)
- **Skipped tests**: none
- **Failures**: none

---

## Fix Plans (if issues found)

None.

---

## Requirement Traceability Update

Verifier does not edit `spec.md`. Recommended statuses after this report:

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| BATCH-01 … BATCH-06 | In Tasks | ✅ Verified |
| WAKE-01 … WAKE-05 | In Tasks | ✅ Verified |
| BENCH-01, BENCH-02 | In Tasks | ✅ Verified |
| DOC-01 | In Tasks | ✅ Verified |

---

## Summary

**Overall**: ✅ Ready

**Spec-anchored check**: 26/26 ACs matched spec outcome | 0 spec-precision gaps
**Sensor**: 6/6 mutations killed
**Gate**: build+vet PASS; unit 492 passed; integration `.` + `internal/pgdriver` packages ok

**What works**: Atomic `InsertMany`/`InsertManyTx` (order, validation all-or-nothing, empty batch, opts/schedules, rollback, two batches in one tx, COPY vs InsertJob, post-validation write failure leaves zero rows). Same-process and cross-client wake under `PollInterval`. `InsertTx`/`InsertManyTx` do not local-nudge before commit. `Start` degrades to poll when LISTEN cannot be established, and reconnects after a drop. `Stop` still interrupts idle wait. Bench flag validation and methodology printout including GOARCH. README table matches the recorded harness numbers and names GOARCH; Examples name `InsertMany` and `NotifyWakeup`.

**Issues found**: none

**Next steps**: none — feature ready for orchestrator publish/merge
