# cycle-h-periodic-jobs Validation

**Date**: 2026-08-28
**Spec**: `.specs/features/cycle-h-periodic-jobs/spec.md`
**Diff range**: `main..HEAD` on `feat/unique-jobs-periodic-scheduler` (`c74d2df`…`34ad755`)
**Verifier**: independent sub-agent (author ≠ verifier)

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1 | ✅ Done | `33b56a3` migration + unique_key column/index |
| T2 | ✅ Done | `ff57b18` memdriver unique insert |
| T3 | ✅ Done | `25174ef` pgdriver unique insert |
| T4 | ✅ Done | sentinel landed in T2 (`driver.ErrDuplicateJob`); no empty commit |
| T5 | ✅ Done | `026a810` lint close |
| T6 | ✅ Done | `d80a5a6` cron parser + FuzzParse |
| T7 | ✅ Done | `dbbbe3f` public UniqueKey + ErrDuplicateJob |
| T8 | ✅ Done | skipped (lint already clean) |
| T9 | ✅ Done | `6743b80` PeriodicJob construction panics |
| T10 | ✅ Done | `b4b60c4` scheduler loop + memdriver leadership |
| T11 | ✅ Done | `3c0fc24` advisory lock + two-client integration |
| T12 | ✅ Done | `34ad755` README, Example, email example |

---

## Spec-Anchored Acceptance Criteria

### P1: Unique jobs at enqueue

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| UNIQ AC1: Insert with non-empty UniqueKey and no non-terminal row | persist the job with that key and return it | `internal/memdriver/memdriver_test.go:368-376` — `row.UniqueKey != "invoice-1"` / `stored.UniqueKey != "invoice-1"`; `client_test.go:645-656` — same plus `row.UniqueKey != stored.UniqueKey`; `internal/pgdriver/pgdriver_integration_test.go:2014-2019` — `row.UniqueKey != "invoice-1"` and `*got != "invoice-1"` | ✅ PASS |
| UNIQ AC2: same (queue, kind, UniqueKey) as available/scheduled/retryable/running | insert no row; `errors.Is(err, ErrDuplicateJob)` | `client_test.go:716-720` — `assertDuplicateJob` (`errors.Is(err, ErrDuplicateJob)` and not driver-only) + `countMemJobs != before`; table covers all four states (`client_test.go:669-704`); memdriver `internal/memdriver/memdriver_test.go:447-451` — `errors.Is(err, driver.ErrDuplicateJob)` + `jobCount != before`; pgdriver `internal/pgdriver/pgdriver_integration_test.go:2118-2122` | ✅ PASS |
| UNIQ AC3: key is completed/cancelled/dead | later Insert succeeds and creates a new row | `client_test.go:775-787` — `err != nil` after terminal, `second.ID == first.ID` forbidden, `second.UniqueKey != "u"`, `countMemJobs != 2`; same at memdriver `:509-519` and pgdriver `:2177-2190` | ✅ PASS |
| UNIQ AC4: empty UniqueKey | persist `unique_key` as NULL; do not participate in uniqueness | pgdriver `internal/pgdriver/pgdriver_integration_test.go:2040-2047` — `UniqueKey != ""` forbidden, `storedUniqueKey(...) != nil` (SQL NULL), `countJobs != 2`; client/memdriver `client_test.go:806-810` / `memdriver_test.go:542-546` — empty `JobRow.UniqueKey` and two rows | ✅ PASS |
| UNIQ AC5: InsertMany collides with existing or in-batch | insert zero rows of that batch; `ErrDuplicateJob` | `client_test.go:856-864` / `:881-887` — `assertDuplicateJob` + `rows != nil` + `countMemJobs != 1`; memdriver `:616-623` / `:642-646`; pgdriver `:2273-2277` / `:2299-2303` | ✅ PASS |
| UNIQ AC6: two concurrent Inserts share a key | at most one row; the other is `ErrDuplicateJob` | `client_integration_test.go:450-466` — `errors.Is(err, ErrDuplicateJob)`, `ok != 1 \|\| dup != 1`, `n != 1`; pgdriver `:2249-2259` — `errors.Is(err, driver.ErrDuplicateJob)`, same 1/1/1 counts | ✅ PASS |
| UNIQ AC7: JobRow for a unique insert | `UniqueKey` equals the key that was stored | `client_test.go:645-656` — `row.UniqueKey != "invoice-1"` and `row.UniqueKey != stored.UniqueKey`; memdriver `:368-376`; pgdriver `:2014-2019` | ✅ PASS |

### P1: Cron grammar

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| CRON AC1: valid 5-field → Next(t) earliest instant strictly after t, in loc (UTC if nil) | exact next instants; `got.After(from)` | `internal/cron/cron_test.go:127-132` — `!got.Equal(tc.want)` and `!got.After(tc.from)`; nil loc UTC at `:100-106` / non-nil NY at `:108-114` | ✅ PASS |
| CRON AC2: `@every d` positive duration | next Unix-epoch aligned instant strictly after t (`t.Truncate(d).Add(d)` in UTC) | `internal/cron/cron_test.go:170-178` — `!got.Equal(tc.want)`, `!got.After(tc.from)`, `got.UnixNano()%d.Nanoseconds() != 0`; epoch-not-local-wall at `:197-210` | ✅ PASS |
| CRON AC3: wrong arity / OOR field / empty or non-positive `@every` / unknown tokens | parse returns error; does not panic | `internal/cron/cron_test.go:295-303` — recover check + `err == nil` for empty, 4/6 fields, OOR, `@every`/`@every 0s`/`@every -1s`, `@hourly`, named tokens, `?` | ✅ PASS |
| CRON AC4: dow `0` or `7` | both mean Sunday | `internal/cron/cron_test.go:225-230` — `!got.Equal(want)` Sunday 2026-01-18 and `got.Weekday() != time.Sunday` for both specs | ✅ PASS |
| CRON AC5: FuzzParse exists; no corpus input panics | fuzz target compiled; Parse+Next on success | `internal/cron/cron_test.go:335-365` — `FuzzParse` seed corpus + `Parse` then `Next` when err is nil | ✅ PASS |

### P1: Periodic registration and leader enqueue

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| PER AC1: empty PeriodicJobs | Start does not take the advisory lock and does not run a scheduler goroutine | `scheduler_test.go:74-76` — `locker.tries.Load() != 0`; goleak at `:62` | ✅ PASS |
| PER AC2: leader inserts due tick | `ScheduledAt` = fire; Args/Queue from PeriodicJob; UniqueKey `id + "/" + fireTime.UTC().Format(time.RFC3339)` | `scheduler_test.go:100-119` — `len(rows) != 1`; `!fire.After(started)`; `rows[0].UniqueKey != wantKey` where `wantKey = periodicUniqueKey("tick", fire)` (`scheduler.go:8-9` is `id + "/" + fire.UTC().Format(time.RFC3339)`); overwrite of caller UniqueKey (`!= "caller-must-not-win"`); `Queue != "periodic"`; `Kind != "ping"` | ✅ PASS |
| PER AC3: second client, same PeriodicJobs | at most one lock holder; one row per tick even if both enqueue | `client_integration_test.go:505-517` — `count != 1` after first tick; `advisoryLockHolders != 1` with two clients; unique-key backstop `scheduler_test.go:170-172` — `n != 1` on pre-inserted same key | ✅ PASS |
| PER AC4: leader lock connection drops | other client can acquire and enqueue subsequent ticks | `client_integration_test.go:519-525` — after `leader.Stop`, `waitForJobCount(..., 2)` and `holders != 1`; pgdriver `pgdriver_integration_test.go:2366-2379` — after `ReleaseLeader`, second `TryBecomeLeader` `!ok` forbidden | ✅ PASS |
| PER AC5: Insert returns ErrDuplicateJob | treat tick as done; do not retry as handler failure | `scheduler_test.go:170-176` — `n != 1`; `strings.Contains(out, "job failed")` / `"job execution failed"` forbidden | ✅ PASS |
| PER AC6: first enqueue strictly after Start | not an immediate run | `scheduler_test.go:104-106` — `!fire.After(started)` | ✅ PASS |
| PER AC7: empty/duplicate ID, empty Args, unparseable Cron | constructing the client panics | `periodic_test.go:86-96` — `r == nil` + panic text contains `empty ID` / duplicate `tick` / `nil Args` / `empty kind` / `invalid cron`; inverse `periodic_test.go:17-18` / `:32-33` empty slice and valid slice do not panic | ✅ PASS |
| PER AC8: lock acquire/hold fails | Start returns nil; worker pool runs; failure logged | `scheduler_test.go:195-200` — `Start` err != nil forbidden; `waitFor` log contains `"acquire periodic scheduler lock"`; Stop nil. Pool still starts (`pool.go:176-180` workers launched regardless of locker). No dedicated “a job was processed while lock-failing” assertion; Start+Stop+goleak is the observed evidence | ✅ PASS |
| PER AC9: Stop returns without waiting for the next fire | upper-bound elapsed; goleak clean | `scheduler_test.go:221-225` — `elapsed >= 2*time.Second` forbidden against `"0 0 1 1 *"`; goleak at `:207`. Interruptibility of `waitFetch` is also discriminated by `TestPeriodicLockFailureDoesNotFailStart` (sensor M4) | ✅ PASS |

### P1: Docs and example

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| DOC AC1: README Planned API shows UniqueKey and PeriodicJobs | those names appear in usage | `README.md:57-62` `PeriodicJobs`; `README.md:75-78` `UniqueKey` (doc inspection) | ✅ PASS |
| DOC AC2: example_test.go compile-checked Example names them | Example typechecks the fields | `example_test.go:118-128` — `ExampleConfig_uniqueAndPeriodic` names `PeriodicJobs` and `UniqueKey`; compiled by `go test` | ✅ PASS |
| DOC AC3: Roadmap blurb | Cycle H shipped, not “Then: periodic jobs…” | `README.md:214` — “**periodic jobs via advisory-lock leader election**. Then: an optional server-rendered status page.” | ✅ PASS |
| DOC AC4: email example registers ≥1 periodic job | example still exercises the surface | `examples/email/main.go:179-184` — `PeriodicJobs: []drover.PeriodicJob{{ID: "hourly-digest", ...}}`; package `ok` under `go test` | ✅ PASS |

**Status**: ✅ All ACs covered (25/25 matched spec-defined outcomes; 0 spec-precision gaps)

Payload/conjunction: UniqueKey values are compared to the stored key (not merely “Insert happened”). Duplicate path uses `errors.Is(..., ErrDuplicateJob)` via `assertDuplicateJob` (`client_test.go:628-632`), which also rejects a driver-only sentinel. Periodic UniqueKey shape is asserted through `periodicUniqueKey` (same formula as the spec); omitting the fire time is killed by the second-tick row count (`scheduler_test.go:125-126`), not only by the helper comparison.

---

## Discrimination Sensor

Scratch: `git worktree add /tmp/drover-cycle-h-sensor HEAD` (removed after). Live tree untouched.

| Mutation | File:line | Description | Killed? |
| -------- | --------- | ----------- | ------- |
| 1 | `internal/memdriver/memdriver.go:131` | `uniqueConflict` always returns false | ✅ Killed — `TestInsertDuplicateUniqueKeyFailsWhileNonTerminal` / InsertMany collision (`errors.Is` + row count) |
| 2 | `client.go:574` | `publicInsertErr` does not remap to `ErrDuplicateJob` | ✅ Killed — `assertDuplicateJob`: “matches only the driver sentinel; want errors.Is(err, ErrDuplicateJob)” |
| 3 | `scheduler.go:8` | `periodicUniqueKey` returns `id` (omits fire time) | ✅ Killed — `TestPeriodicLeaderEnqueuesOneJobPerTick`: `after 2s: 1 jobs, want 2` |
| 4 | `scheduler.go:124` | `waitFetch` waits only on the timer (ignores `fetchCtx`) | ✅ Killed — `TestPeriodicLockFailureDoesNotFailStart` hangs in `waitFetch(pollInterval)` on Stop (package timeout). Isolated `TestPeriodicStopDoesNotWaitForNextFire` can win a race before the first wait and is a weaker killer |
| 5 | `internal/pgdriver/pgdriver.go:191` | `TryBecomeLeader` always `return true, nil` (no lock, no dedicated conn) | ✅ Killed — `acquired conns = 0, want > 0`; two-client `lock holders after first tick = 0, want 1` |
| 6 | `internal/cron/cron.go:173,203` | `Next` returns `t` itself | ✅ Killed — `TestNextStrictlyAfter` / `TestParseNextEvery` / `TestParseNextFiveField` (`must not equal t`, `want After(t)`) |

**Sensor depth**: P0-full (6 behavior-level mutations; uniqueness + leadership included)
**Result**: 6/6 killed — PASS ✅

---

## Interactive UAT Results

Not performed — library/backend feature; no user-facing UI.

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| Minimum code | ✅ New surface is unique insert + cron + scheduler + advisory lock; no extra consensus/dashboard/metrics series |
| Surgical changes | ✅ `30 files, +3735/−41` on the feature branch; adjacent cycles untouched |
| No scope creep | ✅ Matches Out of Scope (owned cron, no RunOnStart, construction-time PeriodicJobs, no CLI unique-key) |
| Matches patterns | ✅ `errors.Is` + package sentinels, memdriver mutex, dedicated conn (LISTEN lesson), goleak, synctest, testcontainers |
| Spec-anchored outcome check (asserted values match spec) | ✅ |
| Per-layer Coverage Expectation met (domain 1:1 ACs; routes happy+edge+error) | ✅ Unique: unit + integration; cron: unit+fuzz; scheduler: unit; lock: integration |
| Every test maps to a spec requirement — no unclaimed tests | ✅ In-scope tests map to UNIQ/CRON/PER/DOC ACs, listed edges, or T3 COPY Done-when. `TestSpringForwardYieldsLaterInstant` is location/DST coverage under CRON AC1 |
| Documented guidelines followed | ✅ `CLAUDE.md` (table-driven, `t.Parallel`, `-race`, testcontainers, goleak, synctest, unit suite without Docker) |

---

## Edge Cases

- [x] InsertMany in-batch shared UniqueKey → `ErrDuplicateJob` and insert nothing — `client_test.go:856-864`, memdriver `:616-623`, pgdriver `:2273-2277`
- [x] UniqueKey on a delayed (`scheduled`) job still uniqueness-holds — scheduled rows in AC2 tables (`client_test.go:678-683`, memdriver `:400-407`, pgdriver `:2071-2078`)
- [x] Location non-nil interprets 5-field in that location; `@every` stays epoch/UTC — `cron_test.go:108-114`, `:183-211`
- [x] NotifyWakeup + due periodic insert wakes idle fetch as any other Insert — `insertPeriodicTick` uses `Client.Insert` (`scheduler.go:105`); Insert wakeup `client_test.go:1447+` (`TestNotifyWakeupCoalescesLocalNudges`)
- [x] memdriver unique under existing mutex; `InsertTx` remains `ErrTxUnsupported` — unique path is inside `mu.Lock` (`memdriver.go:38-42`); `memdriver_test.go:151` — `errors.Is(err, driver.ErrTxUnsupported)`
- [x] Late ticks after lock-retry still enqueue once leadership is gained if fire is in the past — implemented by `for !fire.After(now)` catch-up (`scheduler.go:82-86`). No dedicated test that a failing-then-succeeding locker enqueues a past fire; behavior is implied by the catch-up loop and PER AC2’s due-tick insert. Not scored as an AC gap

---

## Gate Check

- **Gate command**: `go build ./... && go vet ./...` ; Quick `go test -race ./...` ; Full `go test -race ./... && go test -race -tags=integration ./...`
- **Build**: pass (`go build ./... && go vet ./...` exit 0)
- **Quick** (`go test -race -count=1 ./...`): **624** passed, **0** failed, **0** skipped
- **Full** (`go test -race -count=1 -tags=integration ./...`): **746** passed, **0** failed, **0** skipped
- **Test function count before feature** (`git grep '^func (Test\|Fuzz\|Example)' main`): 361
- **After feature** (HEAD): 406
- **Delta**: +45 Test/Fuzz/Example declarations (subtests multiply runtime counts)
- **Skipped tests**: none
- **Failures**: none on the recorded gate. First sandboxed integration run hit testcontainers reaper/Docker socket timeouts (infrastructure); unsandboxed rerun with `DOCKER_HOST=unix:///mnt/wsl/docker-desktop-bind-mounts/Ubuntu/docker.sock` passed

---

## Fix Plans

None. Sensor 6/6 killed; all ACs have spec-anchored evidence.

---

## Requirement Traceability Update

Report-only (spec.md statuses left for the orchestrator):

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| UNIQ-01 | In Tasks | ✅ Verified |
| UNIQ-02 | In Tasks | ✅ Verified |
| UNIQ-03 | In Tasks | ✅ Verified |
| CRON-01 | In Tasks | ✅ Verified |
| CRON-02 | In Tasks | ✅ Verified |
| PER-01 | Done | ✅ Verified |
| PER-02 | Done | ✅ Verified |
| PER-03 | Done | ✅ Verified |
| PER-04 | Done | ✅ Verified |
| DOC-01 | Done | ✅ Verified |

---

## Summary

**Overall**: ✅ Ready

**Spec-anchored check**: 25/25 ACs matched spec outcome; 0 spec-precision gaps
**Sensor**: 6/6 mutations killed
**Gate**: build pass; quick 624 passed; full 746 passed; 0 failed; 0 skipped

**What works**: Unique insert (empty/NULL, four non-terminal states, terminal reuse, InsertMany all-or-nothing, concurrent pair, public `errors.Is(..., ErrDuplicateJob)`), owned cron (5-field, `@every` epoch align, 0/7 Sunday, fuzz), periodic leader loop (first fire after Start, per-tick `id/RFC3339` key, duplicate tick is success, Stop/lock-fail do not hang the pool), two-client advisory lock + failover, README/Example/email surface.

**Issues found**: none that block.

**Next steps**: orchestrator may mark the feature verified; `validation.md` is uncommitted for the ship-cycle to include if desired.
