# Cycle B — Reliability Core Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute
flow and Critical Rules.** Do not search for skill files by filesystem path. If the skill cannot be
activated, STOP.

**Spec**: `.specs/features/cycle-b-reliability-core/spec.md`
**Design**: `.specs/features/cycle-b-reliability-core/design.md`
**Status**: Ready

| Task | Commit | Status |
|---|---|---|
| T1 retry policy | | ⬜ |
| T2 sentinels | | ⬜ |
| T3 driver contract + memdriver transitions | | ⬜ |
| T4 migration 002 + sqlc queries + pgdriver | | ⬜ |
| T5 loop disposition (retry / die / cancel / snooze) | | ⬜ |
| T6 in-flight set + heartbeat | | ⬜ |
| T7 rescuer | | ⬜ |
| T8 supervisor lifecycle + config defaults | | ⬜ |
| T9 end-to-end integration + docs | | ⬜ |

## Test Coverage Matrix

> Guidelines found: `CLAUDE.md` (table-driven, `t.Parallel`, `-race` always, testcontainers-go for
> integration, `testing/synctest` for time-dependent logic, goleak on lifecycle tests, unit suite
> Docker-free via memdriver). Existing suite: 44 test/example functions, green at baseline.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
|---|---|---|---|---|
| Retry policy (`retry.go`) | unit | Bound, variation, monotonicity, nil/custom policy selection, past-time clamp, panicking policy | `retry_test.go` | `go test -race ./...` |
| Sentinels (`errors.go`) | unit | Classification through `%w` wrapping; non-sentinel errors unaffected; zero/negative snooze | `errors_test.go` | `go test -race ./...` |
| Loop disposition (`loop.go`) | unit (memdriver) | Every row of the state-transition table; each edge case in the spec | `loop_test.go` | `go test -race ./...` |
| Heartbeat + in-flight set (`heartbeat.go`, `inflight.go`) | unit (synctest + `-race`) | Extension cadence, stop on finalize, error tolerance, no resurrection of finalized rows | `heartbeat_test.go` | `go test -race ./...` |
| Rescuer (`rescue.go`) | unit (memdriver) | Expired→retryable/dead, `attempt` unchanged, unexpired untouched, sweep survives errors, concurrent sweeps disposition once | `rescue_test.go` | `go test -race ./...` |
| Lifecycle (`Start`) | unit + goleak | No goroutine outlives `Start`; heartbeat survives cancellation until drain completes | `loop_test.go` | `go test -race ./...` |
| `internal/memdriver` | unit | Each new transition + its guard + concurrent use | `internal/memdriver/*_test.go` | `go test -race ./...` |
| `internal/pgdriver`, `internal/migrate` | integration | Each new query, widened fetch predicate, index migration idempotence, concurrent `FetchExpired` exactly-once | `*_integration_test.go`, `//go:build integration` | `go test -race -tags=integration ./...` |
| End-to-end | integration | Fail-fail-succeed reaches `completed`; exhaustion reaches `dead`; crashed worker rescued; long job never rescued | root `*_integration_test.go` | `go test -race -tags=integration ./...` |

**Timing-assertion rule (from `.specs/LESSONS.md` L-001):** every interval or count assertion states
**both** an upper and a lower bound, or uses `testing/synctest` for exact cadence. A lower bound
alone passes under a busy loop and discriminates nothing.

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
|---|---|---|---|
| unit (root + memdriver) | Yes | per-test driver instance; no shared state | Cycle A precedent |
| synctest bubbles | Yes across tests, no `t.Parallel` inside a bubble | `synctest.Test` owns its own fake clock | `testing/synctest` contract |
| integration | No (sequential per package) | one testcontainer per package via `TestMain`, fresh database per test | `internal/testdb` |

## Gate Check Commands

| Gate Level | When to Use | Command |
|---|---|---|
| Quick | after unit-only tasks | `go test -race ./...` |
| Full | after integration-bearing tasks, and at every phase close | `go test -race ./... && go test -race -tags=integration ./...` |
| Build | types/config-only tasks | `go build ./... && go vet ./...` |
| Publish | before pushing | Full, plus `golangci-lint run`, plus regenerated sqlc committed |

## Execution Plan

### Phase 1: Policy and classification vocabulary (sequential)
```
T1 → T2
```
### Phase 2: Storage transitions (sequential)
```
T3 → T4
```
### Phase 3: Loop behavior (sequential)
```
T5 → T6 → T7 → T8
```
### Phase 4: End-to-end and documentation
```
T9
```

---

## Tasks

### T1 — Default retry policy and the `RetryPolicy` seam

**Requirements**: POLICY-01, POLICY-02
**Files**: `retry.go` (new), `retry_test.go` (new)

**Do**: Introduce the `RetryPolicy` interface and the exported exponential default described in the
design, plus the unexported application helper that clamps a past answer to now and recovers a
panicking policy onto the default.

**Verify**: The delay for attempt N lands within `[0.9·N⁴, 1.1·N⁴]` seconds across many samples,
varies between calls, and grows with N; a nil `Config.RetryPolicy` selects the default and a
supplied one fully replaces it; a policy returning a past time yields an immediately-claimable job;
a panicking policy does not escape.

**Gate**: Quick.
**Commit**: `feat(retry): add pluggable retry policy with exponential backoff`

---

### T2 — `Cancel` and `Snooze` sentinels

**Requirements**: SENT-01, SENT-02, SENT-03 (classification half)
**Files**: `errors.go`, `errors_test.go` (new)

**Do**: Add `Cancel(err) error`, `Snooze(d) error`, the `ErrCancelled` sentinel, and the internal
classifier that maps a handler's returned error to one of: cancelled, snoozed (with its duration),
or ordinary failure.

**Verify**: A sentinel is classified correctly when returned bare and when wrapped with `%w` in
additional context; an ordinary error is classified as an ordinary failure; a zero or negative
snooze duration yields an immediately-claimable wake time.

**Gate**: Quick.
**Commit**: `feat(jobs): add cancel and snooze outcome sentinels for handlers`

---

### T3 — Driver contract additions and in-memory implementations

**Requirements**: RETRY-01, RETRY-03, RESCUE-01, RESCUE-02, LEASE-01, LEASE-02, SENT-01, SENT-02
**Files**: `internal/driver/driver.go`, `internal/memdriver/memdriver.go`,
`internal/memdriver/memdriver_test.go`

**Do**: Extend the `Driver` interface with `FetchExpired`, `ExtendLeases`, `MarkRetryable`,
`MarkCancelled` and `MarkSnoozed` per the design, and implement them in `memdriver` with the same
guards the existing finalizers use. `MarkCompleted` and `MarkDead` also clear the lease.

**Verify**: Each new transition succeeds from `running` and reports `ErrInvalidTransition` from any
other state and `ErrNotFound` for an unknown id; `MarkSnoozed` decrements `attempt` with a floor of
zero and appends no error; `FetchExpired` returns only `running` rows whose lease has passed, leaves
`attempt` untouched, and refreshes the lease; `ExtendLeases` moves only `running` rows and silently
ignores ids that no longer qualify; concurrent use is race-clean.

**Gate**: Quick.
**Commit**: `feat(storage): add lease, retry, cancel and snooze transitions`

---

### T4 — Migration 002, queries, and the Postgres driver

**Requirements**: RETRY-04, RESCUE-03, plus the storage half of every requirement in T3
**Files**: `internal/migrate/migrations/002_widen_fetch_predicate.sql` (new),
`internal/pgdriver/queries.sql`, regenerated `internal/dbsqlc/*`, `internal/pgdriver/pgdriver.go`,
`internal/pgdriver/pgdriver_integration_test.go`, `internal/migrate/migrate_integration_test.go`

**Do**: Add the migration that widens the fetch index to the three waiting states and adds the lease
index; widen the `FetchAvailable` predicate to match; add the five new queries; regenerate sqlc and
commit the generated code in this same commit; implement the five driver methods over them.

**Verify**: Due `retryable` and `scheduled` rows are claimed and not-yet-due rows are not; each new
query performs its documented state change and no other; concurrent `FetchExpired` from two
connections dispositions each expired row exactly once; the migrator still applies cleanly from
scratch and is idempotent on an already-migrated database.

**Gate**: Full (Docker is available).
**Commit**: `feat(postgres): claim due retryable jobs and reclaim expired leases`

---

### T5 — Loop disposition: retry, die, cancel, snooze

**Requirements**: RETRY-01, RETRY-02, RETRY-03, SENT-01, SENT-02, SENT-03
**Files**: `loop.go`, `loop_test.go`

**Do**: Replace the dead-on-error branch (AD-003) with the shared classifier from the design: nil →
completed; `Cancel` → cancelled; `Snooze` → snoozed; anything else → retryable at the policy's time,
or dead once attempts are exhausted. Panics and unregistered kinds route through the ordinary
failure path (AD-013, AD-014). Every failure path appends exactly one structured error entry.

**Verify**: Every row of the design's state-transition table is exercised end to end through the
loop against the in-memory driver, including: exactly one error entry appended per failed attempt; a
panic's stack trace retained; `dead` reached only at the attempt ceiling; a job with non-positive
`max_attempts` dying on its first failure; repeated snoozes never reaching `dead` and never driving
`attempt` negative; a finalize write failure logged without stopping the loop.

**Gate**: Quick.
**Commit**: `feat(worker): retry failed jobs with backoff instead of killing them`

---

### T6 — In-flight tracking and the heartbeat

**Requirements**: LEASE-01, LEASE-02
**Files**: `inflight.go` (new), `heartbeat.go` (new), `client.go`, `loop.go`,
`heartbeat_test.go` (new)

**Do**: Track executing job ids in a mutex-guarded set that is correct for many concurrent jobs, and
run a heartbeat that extends every tracked job's lease by a full lease duration on each
`HeartbeatInterval`. Extension failures are logged and survived.

**Verify**: A job executing across several heartbeat intervals has its lease extended on each one,
to a full lease duration ahead of the extension time; extension stops as soon as the job finalizes;
an extension error neither aborts the job nor stops the loop; an extension aimed at an
already-finalized row changes nothing. Cadence assertions use `testing/synctest` with both bounds
stated.

**Gate**: Quick.
**Commit**: `feat(worker): keep running jobs leased with a heartbeat`

---

### T7 — Rescuer sweep

**Requirements**: RESCUE-01, RESCUE-02, RESCUE-03
**Files**: `rescue.go` (new), `rescue_test.go` (new)

**Do**: Add the sweep that re-claims expired-lease rows and routes each through the same disposition
used for a failed attempt, with a lease-expiry error as the cause, and the interval loop that drives
it. `attempt` is never modified by rescue. A sweep drains a backlog rather than handling one batch
per tick.

**Verify**: An expired row returns to `retryable` at the policy's time with `attempt` unchanged and
exactly one lease-expiry error appended; an expired row at the attempt ceiling becomes `dead`; an
unexpired `running` row is untouched; a sweep that errors is logged and the loop keeps running; two
sweeps running concurrently disposition each expired row exactly once; an empty sweep performs no
writes.

**Gate**: Quick.
**Commit**: `feat(worker): return jobs from crashed workers to the queue`

---

### T8 — Supervisor lifecycle and configuration defaults

**Requirements**: LEASE-03, POLICY-02 (config half), plus the config edge cases
**Files**: `client.go`, `loop.go`, `loop_test.go`, `client_test.go`

**Do**: Turn `Start` into the supervisor described in the design — rescuer under a cancellable child
context, heartbeat under a stop channel closed only after the fetch loop returns, both joined before
`Start` returns. Add and default the new `Config` fields, including clamping a heartbeat interval
that is not strictly shorter than the lease.

**Verify**: `Start` returns only after both background goroutines have exited and `goleak` reports
none surviving; cancelling the context mid-job keeps that job's lease extended until it finalizes;
zero, negative and over-long configuration values each resolve to their documented substitute; a
supplied retry policy is the one the loop actually consults.

**Gate**: Quick.
**Commit**: `feat(worker): supervise heartbeat and rescue alongside the fetch loop`

---

### T9 — End-to-end coverage and documentation

**Requirements**: all — the integration-level restatement
**Files**: `e2e_integration_test.go`, `doc.go`, `README.md`, `docs/adr/0003-*.md` (status note only
if warranted)

**Do**: Add the end-to-end scenarios against real Postgres and bring the package documentation in
line with the new behavior — retries, the attempt ceiling, lease and rescue timing, and the two
handler sentinels — replacing the Cycle A text that documents dead-on-first-error and the unenforced
lease.

**Verify**: A worker failing twice and succeeding on the third attempt reaches `completed` with two
recorded errors; a worker that always fails reaches `dead` only after its attempts are exhausted; a
job abandoned in `running` with an expired lease is picked up and re-run by a live client; a job
that runs for several lease durations completes exactly once while a rescuer sweeps; documentation
contains no surviving claim that a failure is immediately fatal or that the lease is unenforced.

**Gate**: Full, plus lint.
**Commit**: `docs(queue): document retry, lease and rescue behavior`

---

## Requirement Coverage

| Requirement | Tasks |
|---|---|
| RETRY-01 | T3, T4, T5, T9 |
| RETRY-02 | T5, T9 |
| RETRY-03 | T3, T4, T5, T9 |
| RETRY-04 | T4, T9 |
| POLICY-01 | T1 |
| POLICY-02 | T1, T8 |
| RESCUE-01 | T3, T4, T7, T9 |
| RESCUE-02 | T3, T7, T9 |
| RESCUE-03 | T4, T7 |
| LEASE-01 | T3, T6, T9 |
| LEASE-02 | T3, T6 |
| LEASE-03 | T8 |
| SENT-01 | T2, T3, T5 |
| SENT-02 | T2, T3, T5 |
| SENT-03 | T2, T5 |

**Coverage:** 15 requirements, 15 mapped, 0 unmapped.
