# Benchmark + Hardening Validation

**Date**: 2026-08-15
**Spec**: `.specs/features/cycle-g-benchmark-hardening/spec.md`
**Diff range**: `main..HEAD` (`afabb50..8e3fb6b`)
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

---

## Spec-Anchored Acceptance Criteria

### P1: Atomic batch insert

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `InsertMany` with a valid non-empty batch THEN persist every item and return `[]*JobRow` of equal length, input order, distinct positive IDs | `len(rows)==len(items)`; each `ID>0` and unique; stored kinds/args match input order | `client_test.go:348-372` — `len(rows) != len(items)`; `row.ID <= 0`; duplicate ID; `string(row.Args) != wantArgs[i]`. Also `internal/memdriver/memdriver_test.go:169-176` and `internal/pgdriver/pgdriver_integration_test.go:1514-1524` (`row.Kind != batch[i].Kind`) | ✅ PASS |
| WHEN any item has nil `Args`, empty kind, or marshal error THEN return that error, insert zero rows, table unchanged | error (`ErrInvalidKind` / wrapped marshal); store empty | `client_test.go:432-434` — `errors.Is(err, ErrInvalidKind)`; `455-456` — error names `kind "bad"`; `473` — `assertNothingPersisted` (`len(rows) != 0` on fetch). Tx path: `client_test.go:555-564` | ✅ PASS |
| WHEN `InsertMany` with nil or empty slice THEN empty result, nil error, without writing | empty slice, `err==nil`, no rows | `client_test.go:405-414` — `err != nil`; `rows == nil`; `len(rows) != 0`; `assertNothingPersisted`. Driver: `internal/memdriver/memdriver_test.go:205-215` (next ID not consumed) | ✅ PASS |
| WHEN an item's `Opts` is nil THEN queue `"default"`, due immediately, matching single `Insert` | `Queue=="default"`, `State==available`, `ScheduledAt` not in the future | `client_test.go:498-503` — `rows[0].Queue != "default" \|\| rows[0].State != StateAvailable`; `ScheduledAt.After(now+1m)` | ✅ PASS |
| WHEN `ScheduledAt` is future THEN `scheduled`; WHEN zero or past THEN `available`; dueness by DB clock (AD-035) | future → `scheduled` with that time; zero/past → `available` | `client_test.go:507-515`; `internal/pgdriver/pgdriver_integration_test.go:1599-1612` — `rows[1].State != "scheduled"`; `rows[0]/[2] != "available"`. SQL CASE uses `now()` (`internal/pgdriver/queries.sql:38-41`); no InsertMany test skews client vs database clocks | ✅ PASS |
| WHEN `InsertManyTx` then caller rolls back THEN none visible to `FetchAvailable` | `FetchAvailable` claims 0 after rollback | `internal/pgdriver/pgdriver_integration_test.go:1650-1655` — `len(claimed) != 0`. Client: `client_integration_test.go:107-108` — `countJobs() != 0` after rollback | ✅ PASS |
| WHEN `InsertMany` runs on Postgres THEN load via `COPY FROM` (session-temp staging), not N `InsertJob` | one `CopyFrom`; zero `INSERT … VALUES` | `internal/pgdriver/pgdriver_integration_test.go:1749-1753` — `trace.copyFrom != 1`; `trace.insertJob != 0` | ✅ PASS |
| WHEN two `InsertManyTx` share one transaction THEN both batches persist (no temp-table-exists failure) | 3 jobs after commit, kinds `a,b,c` | `internal/pgdriver/pgdriver_integration_test.go:1710-1716` — `len(claimed) != 3`; `!slices.Equal(got, want)` | ✅ PASS |

### P1: Optional notify wake-up

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `NotifyWakeup` is unset or false THEN idle fetch waits `PollInterval`; inserts do not emit `NOTIFY` | default false; no local wake; fetch count unchanged; no Postgres NOTIFY | `client_test.go:133-134` — `c.notifyWakeup` true. `client_test.go:1143-1144` — `len(c.wake) != 0`. `pool_test.go:983-992` — fetch count changed; state not `available`. `client_integration_test.go:361-363` — `err == nil` on `WaitForNotification` | ✅ PASS |
| WHEN `NotifyWakeup` is true and same-client `Insert`/`InsertMany` succeeds THEN idle fetch resumes without waiting out `PollInterval` (elapsed upper bound) | claim elapsed `< 1s` against `3s` poll | `pool_test.go:941-950` — `time.After(notifyWakeUpperBound)`; `elapsed > notifyWakeUpperBound`. Uses `Insert`; `InsertMany` shares `wakeAfterInsert` and is covered for nudge in `client_test.go:1125-1129` | ✅ PASS |
| WHEN `NotifyWakeup` is true and another process commits `Insert` THEN listener wakes on channel `drover` (empty payload) and claims without waiting out `PollInterval` | claim elapsed well under poll; channel `drover` | `client_integration_test.go:265-274` — job id + `elapsed > notifyWakeUpperBound`. Channel name: `internal/pgdriver/pgdriver_integration_test.go:1843-1844` — `n.Channel != "drover"`. **Payload `""` is not asserted** | ✅ PASS (payload unasserted) |
| WHEN `InsertTx` / `InsertManyTx` with the flag set THEN emit `NOTIFY` in the caller transaction; listeners wake only if that tx commits | no run before commit; run after commit within upper bound | `client_integration_test.go:311-331` — `job ran before the producer committed`; post-commit `elapsed > notifyWakeUpperBound`. Driver: `internal/pgdriver/pgdriver_integration_test.go:1829-1841`. `InsertManyTx` notify is the same `notifyTx` call; no dedicated InsertManyTx wake test | ✅ PASS |
| WHEN `LISTEN` cannot be established at `Start` THEN `Start` returns nil, logs, keeps polling | `Start` err nil; log contains listen failure; job claimed via poll | `pool_test.go:1056-1077` — `Start: %v, want nil`; log substring `drover: listen for job notifications`; `job was not claimed after LISTEN failed` | ✅ PASS |
| WHEN the listen connection drops after `Start` THEN log, reconnect, keep polling; SHALL NOT stop the worker pool | Listen retried after drop; pool still claims via poll | **no `file:line`**. `TestStartSucceedsWhenListenFails` covers *initial* listen failure (AC5), not drop-after-Start / retry. Sensor mutant 7 (retry loop removed) survived | ❌ GAP |
| WHEN `Stop` is called during an idle wait THEN SHALL not wait out `PollInterval` | `Stop` elapsed `< 1s` against `3s` poll with flag on | `pool_test.go:1021-1022` — `elapsed > notifyWakeUpperBound` | ✅ PASS |
| WHEN `NotifyWakeup` is true, `Start` SHALL acquire a dedicated connection for `LISTEN`; docs SHALL state PgBouncer transaction pooling is incompatible | session-scoped LISTEN; docs name session pooling / PgBouncer | Behaviour: `internal/pgdriver/pgdriver_integration_test.go:1869-1900` (`ListenWakeups` + `WaitForNotification`). Docs: `README.md:29`; `client.go:72-75`; `example_test.go:117` (`NotifyWakeup: false` + session pooling comment). No connection-count assertion | ✅ PASS |

### P1: Benchmark harness

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `drover-bench` `--mode enqueue` THEN insert `--jobs` via `InsertMany` in `--batch` chunks and print jobs/sec + methodology (GOOS/GOARCH/NumCPU, Postgres version, jobs/batch/concurrency, no-op handlers) | those keys present; chunk sizes | `cmd/drover-bench/main_test.go:116-130` — stdout contains each key including `jobs/sec=`. Chunks: `cmd/drover-bench/bench_test.go:52-61` | ✅ PASS |
| WHEN `--mode drain` THEN insert, start no-op pool of `--concurrency`, wait until every job completes, print jobs/sec + p50/p95/p99 | percentiles + jobs/sec printed; completion signal after last job | `cmd/drover-bench/main_test.go:183-186` — `p50=`/`p95=`/`p99=`/`jobs/sec=`. Wait: `cmd/drover-bench/bench_test.go:92-104` (`allDone` after 2 of 2). Default `--mode` is drain (`main.go:58`) | ✅ PASS |
| WHEN `--database` missing and `DATABASE_URL` empty THEN non-zero exit, insert nothing | exit 2; execute not called | `cmd/drover-bench/main_test.go:70-74` — `code != 2`; `ran.Load()` | ✅ PASS |
| WHEN `--jobs` or `--batch` < 1 THEN non-zero usage exit | exit 2; no insert | `cmd/drover-bench/main_test.go:31-47` cases + same `code != 2` / `ran.Load()` | ✅ PASS |
| WHEN `--mode` is neither `enqueue` nor `drain` THEN non-zero usage exit | exit 2 | `cmd/drover-bench/main_test.go:50-57` + `70-74` | ✅ PASS |
| Binary lives under `cmd/drover-bench/` and SHALL NOT be added to GoReleaser | only `./cmd/drover` in GoReleaser | `.goreleaser.yaml:8` — `main: ./cmd/drover`; `README.md:198` | ✅ PASS |

### P1: README benchmark table

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN README is opened THEN `## Benchmarks` with enqueue jobs/sec, drain p50/p95/p99, GOOS/GOARCH, CPU, Postgres version, job count, batch, concurrency, no-op single-node caveat | table + methodology + reproduce command matching a real run | `README.md:166-198` — table `16,230` / `704` / `8.19s` / `13.74s` / `14.14s` matches recorded `jobs/sec=16229.96` and drain percentiles. Machine/Postgres/workload present. **GOARCH is not named** (GOOS present as "Linux") | ⚠️ Spec-precision gap (GOARCH omitted) |
| WHEN the section names `NotifyWakeup` / `InsertMany` THEN `example_test.go` has compile-checked Examples naming those fields | Examples compile | `example_test.go:117` — `NotifyWakeup: false`; `example_test.go:127` — `client.InsertMany(...)` | ✅ PASS |
| WHEN Planned API sample is read THEN it shows `InsertMany` alongside `Insert` / `InsertTx` | all three appear | `README.md:60-74`; Example `example_test.go:121-130` | ✅ PASS |
| WHEN Roadmap blurb is read THEN Cycle G bench work is shipped, not "then: benchmarks…" | A–G includes published methodology; "Then:" is later work | `README.md:202` — `cycles A–G` … `**benchmarks with published methodology**. Then: periodic jobs…` | ✅ PASS |

**Status**: ❌ Gaps present / ⚠️ Spec-precision gaps flagged

---

## Discrimination Sensor

Scratch: `git worktree add /tmp/drover-g-sensor HEAD` (detached `8e3fb6b`). Restored after each mutant; worktree removed. Primary tree never mutated.

| Mutation | File:line | Description | Killed? |
| -------- | --------- | ----------- | ------- |
| 1 | `internal/pgdriver/pgdriver.go:191` | Skip `CopyFrom`; loop `InsertJob` instead | ✅ Killed — `TestInsertManyUsesCopyFrom` (`copyFrom 0`, `InsertJob VALUES 5`) |
| 2 | `client.go:413` | Drop validate-all-first; `Insert` each item so a later bad item leaves a prefix | ✅ Killed — `TestInsertManyRejectsInvalidItems` (`found 1 persisted jobs, want 0`) |
| 3 | `client.go:407` | `nudge()` on `InsertTx` after the driver write (before caller commit) | ❌ Survived — `TestInsertTxDoesNotWakeUntilCommit` and unit wake tests still passed (local wake is on the producer, not the worker) |
| 4 | `pool.go:609` | `sleep` ignores the wake channel (timer + stop only) | ✅ Killed — `TestNotifyWakeupClaimsInsertBeforePollInterval` (`not claimed within 1s`) |
| 5 | `client.go:242` | `notifyWakeup: true` ignoring `Config` | ✅ Killed — `TestConfigZeroValuesGetDefaults`; `TestNotifyWakeupOffDoesNotNudge`; `TestNotifyWakeupOffDoesNotWakeFetchOnInsert` |
| 6 | `client.go:418` | Empty `InsertMany` inserts a dummy row | ✅ Killed — `TestInsertManyEmptyOrNilWritesNothing` (`got 1 rows, want 0`) |
| 7 | `pool.go:579` | `listenForWakeups` calls `ListenWakeups` once and returns (no reconnect loop) | ❌ Survived — `TestStartSucceedsWhenListenFails` and cross-client LISTEN tests still passed |

**Sensor depth**: P0-full (≥5 behaviour-level mutations on insert atomicity + fetch wake-up)
**Result**: 5/7 killed — FAIL ❌

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
| Spec-anchored outcome check (asserted values match spec) | ❌ AC6 reconnect has no assertion; GOARCH/NOTIFY payload not pinned |
| Per-layer Coverage Expectation met (domain 1:1 ACs; routes happy+edge+error) | ❌ WAKE AC6; COPY-after-validation edge |
| Every test maps to a spec requirement — no unclaimed tests | ✅ extras map to edges / Done-when (empty-batch notify, LISTEN cancel, bench chunks) |
| Documented guidelines followed: `CLAUDE.md` (table-driven, `t.Parallel`, `-race`, testcontainers, goleak, unit suite without Docker); `.claude/skills/tlc-spec-driven/references/coding-principles.md` | ✅ |

---

## Edge Cases

- [x] Mixed queues persist on the named queue (or `"default"`): `client_test.go:504-537`; `internal/memdriver/memdriver_test.go:238-264`
- [ ] `InsertMany` fails after validation (COPY or INSERT error) → wrapped error and zero rows: **no test** (implementation rolls back the driver tx, but evidence-or-zero is empty)
- [x] Future-only scheduled batch MAY wake and claim nothing: allowed (`MAY`); not required
- [x] Extra wakes coalesce (capacity 1): `client_test.go:1128-1129` — `len(c.wake) != 1`
- [x] `memdriver.InsertTx` / `InsertManyTx` return `ErrTxUnsupported`: `internal/memdriver/memdriver_test.go:150-153`, `304-305`; `client_test.go:599-600`

---

## Gate Check

- **Gate command**: `go build ./... && go vet ./...` (tasks.md Build)
- **Result**: PASS (exit 0)
- **Quick (counts)**: `go test -race ./...` — 489 passed, 0 failed, 0 skipped
- **COPY/NOTIFY evidence**: `go test -race -tags=integration -count=1 . ./internal/pgdriver` — 408 passed, 0 failed, 0 skipped (includes that package's unit tests under the tag)
- **Test count before feature**: 307 `func Test` on `main`
- **Test count after feature**: 348 `func Test` on `HEAD`
- **Delta**: +41 (no `func Test` deleted)
- **Skipped tests**: none
- **Failures**: none

---

## Fix Plans (if issues found)

### Fix 1: Prove LISTEN reconnect after a drop (WAKE AC6)

- **Root cause**: AC6 has no test. `TestStartSucceedsWhenListenFails` only covers first-call failure at `Start`. Removing the retry loop in `listenForWakeups` still passes.
- **Fix task**: Add a stub `wakeupListener` that fails (or returns) on call 1 and succeeds on call 2; assert `ListenWakeups` is invoked again, a listen error is logged, and a job inserted without local nudge is still claimed by poll. Optionally close a live listen conn in pgdriver integration.
- **Verify**: sensor mutant 7 (retry loop removed) is killed
- **Done when**: WAKE AC6 has `file:line` asserting retry + keep-polling
- **Priority**: Blocker

### Fix 2: Assert `InsertTx` does not local-nudge before commit

- **Root cause**: Sensor mutant 3 (`nudge()` on `InsertTx`) survived. Cross-client tests watch Postgres NOTIFY, not the inserting client's cap-1 wake channel.
- **Fix task**: With `NotifyWakeup: true`, successful `InsertTx` (pgdriver) or a fake driver that implements `InsertTx` must leave `len(c.wake)==0` until commit; after commit, NOTIFY still wakes listeners.
- **Verify**: mutant 3 is killed; AC4 still passes
- **Done when**: Tx path cannot signal the local wake channel without a failing test
- **Priority**: Major

### Fix 3: COPY/INSERT failure after validation (edge)

- **Root cause**: Validation-reject path is tested; post-validation write failure is not.
- **Fix task**: Force `CopyFrom` or `InsertJobsFromStaging` to fail (tracer, cancelled ctx, or constraint) and assert wrapped error + `COUNT(*)==0`.
- **Priority**: Major

### Fix 4: Name GOARCH in README methodology

- **Root cause**: Spec lists GOOS/GOARCH; README names Linux/CPU/Postgres but not GOARCH.
- **Fix task**: Add `GOARCH` (and keep GOOS) next to the published run.
- **Priority**: Minor

---

## Requirement Traceability Update

Verifier does not edit `spec.md`. Recommended statuses after this report:

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| BATCH-01 … BATCH-06 | In Tasks | ✅ Verified (after implementer checkbox pass; tests match) |
| WAKE-01 | In Tasks | ✅ Verified |
| WAKE-02 | In Tasks | ✅ Verified |
| WAKE-03 | In Tasks | ✅ Verified (payload unasserted; claim latency holds) |
| WAKE-04 | In Tasks | ✅ Verified |
| WAKE-05 | In Tasks | ❌ Needs Fix (AC6 reconnect) |
| BENCH-01, BENCH-02 | In Tasks | ✅ Verified |
| DOC-01 | In Tasks | ⚠️ GOARCH omitted from methodology block |

---

## Summary

**Overall**: ❌ Not Ready

**Spec-anchored check**: 24/26 ACs matched spec outcome | 1 GAP (WAKE AC6) | 1 spec-precision gap (README GOARCH)
**Sensor**: 5/7 mutations killed (2 survived)
**Gate**: build+vet PASS; unit 489 passed; integration `.` + `internal/pgdriver` 408 passed

**What works**: Atomic `InsertMany`/`InsertManyTx` (order, validation all-or-nothing, empty batch, opts/schedules, rollback, two batches in one tx, COPY vs InsertJob). Same-process and cross-client wake under `PollInterval`. `Start` degrades to poll when LISTEN cannot be established. `Stop` still interrupts idle wait. Bench flag validation and methodology printout. README table matches the recorded harness numbers; Examples name `InsertMany` and `NotifyWakeup`.

**Issues found**:
1. WAKE AC6 reconnect-after-drop has no evidence; mutant 7 survived.
2. `InsertTx` local `nudge()` before commit is not discriminated; mutant 3 survived.
3. Post-validation COPY/INSERT failure edge has no test.
4. README methodology omits GOARCH.

**Next steps**: Implement Fix 1–2 (required for PASS); Fix 3–4 with them or immediately after; re-verify (iteration 1 of 3).
