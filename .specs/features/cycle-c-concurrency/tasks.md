# Concurrency: Worker Pool and Graceful Shutdown — Tasks

## Execution Protocol (MANDATORY — do not skip)

Implement these tasks with the `tlc-spec-driven` skill: activate it by name and follow its Execute flow and Critical Rules. The skill is the source of truth for the per-task cycle, sub-agent delegation, the adequacy review, the Verifier and the discrimination sensor.

---

**Design**: `.specs/features/cycle-c-concurrency/design.md`
**Status**: Approved (auto — ship-cycle autonomy contract)

## Progress

| Task | Commit | Notes |
| --- | --- | --- |
| T1 `Concurrency` setting | `4097bb7` | `client_test.go` 7 → 8 |
| T2 per-job context split | `7eb7b94` | Needed a driver that honours context — `memdriver` ignores it, so the first version of the sensor could not discriminate (lesson L-002 in the wild). Both mutants killed. |
| T3 pool + `Start`/`Stop` | `169a30d` | Root unit tests 58 → 70. Five mutants killed (pool-of-one, over-claim, early heartbeat stop, no watcher, immortal rescuer). |
| T4 escalation + requeue | `366ec21` | Root unit tests 70 → 76. Uncovered a token-accounting deadlock when a driver returns more rows than the limit; fixed by returning the surplus immediately. First cancel-ordering sensor was masked by a `defer`, so it was rewritten to pin the order. |
| — regression fix | `95ade96` | The integration suite caught a real defect from T2: the finalization deadline was anchored at job start, so any job outliving its lease finished successfully and then failed to record it. Now taken at the write. Unit sensor added. |
| T5 integration proof | `f907a3b` | `e2e_integration_test.go` 7 → 10. Also repaired two existing tests and the `runLoop` helper, which had become vacuous under a non-blocking `Start`. |
| — lint | `3a79fd4` | errcheck + errorlint on the shutdown path. |
| T6 example program | delegated | phase worker |
| T7 documentation | delegated | phase worker |

---

## Test Coverage Matrix

> Generated from codebase sampling, project guidelines, and the spec. Guidelines found: `CLAUDE.md` (Tests section: table-driven, `t.Parallel`, `-race` always, testcontainers for Postgres integration, `testing/synctest` for time-dependent tests, goleak on lifecycle tests, unit suite must run without Docker), `.github/workflows/ci.yml` (the authoritative gate commands).

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| Lifecycle & pool (`pool.go`, `loop.go`) — domain logic | unit | All branches; 1:1 to spec ACs; every listed edge case; goleak on every lifecycle test | `./*_test.go` (root, no build tag) | `go test -race ./...` |
| Heartbeat / in-flight tracking (`heartbeat.go`, `inflight.go`) | unit | All branches; deterministic timing via `testing/synctest`; bounded-count assertions, never lower-bound-only (lesson L-001) | `./heartbeat_test.go` | `go test -race ./...` |
| Configuration & defaults (`client.go` `Config`) | unit | Every defaulting branch, including the invalid-input branch | `./client_test.go` | `go test -race ./...` |
| Errors / sentinels (`errors.go`) | unit | Identity and `errors.Is` reachability through wrapping | `./errors_test.go` | `go test -race ./...` |
| End-to-end against real PostgreSQL | integration | Happy path + concurrency + shutdown paths; exactly-once under a real pool | `./*_integration_test.go` (`//go:build integration`) | `go test -race -tags=integration ./...` |
| Example program (`examples/email/`) | unit (stub only) + build gate | The delivery stub's documented failure pattern is asserted; the program itself is covered by build + vet | `examples/email/*_test.go` | `go test -race ./...`, `go build ./... && go vet ./...` |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
| --- | --- | --- | --- |
| Root unit tests | Yes | Each test constructs its own `Client` over a fresh `memdriver.New()`; no shared global state | `newClient(memdriver.New(), Config{...})` per test throughout `loop_test.go`, `rescue_test.go` |
| `testing/synctest` tests | Yes, but must not call `t.Parallel` inside the bubble | `synctest.Test` runs an isolated fake-clock bubble per test | `heartbeat_test.go` synctest usage |
| Integration tests | No | Shared PostgreSQL container per package via `testdb.RunMain`; rows are shared across tests | `TestMain`/`testdb.RunMain` in `client_integration_test.go` |
| Example stub unit test | Yes | Pure function, no state | new file |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After tasks with unit tests only | `go test -race ./...` |
| Full | After tasks with integration tests | `go test -race ./...` && `go test -race -tags=integration ./...` |
| Build | After phase completion or config-only tasks | `go build ./... && go vet ./...` |
| Publish | Once, before opening the PR | Build + Full + `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run` |

**Baseline test counts (before this cycle):** root unit 58 (`client` 7, `errors` 4, `example` 1, `heartbeat` 8, `loop` 17, `rescue` 10, `retry` 7, `worker` 4); `internal/memdriver` 20; integration 35 (`client` 2, `e2e` 7, `migrate` 3, `pgdriver` 23). No task may reduce a count without stating why.

---

## Execution Plan

### Phase 1: Configuration (Sequential)
```
T1
```

### Phase 2: Per-job context split (Sequential)
```
T1 → T2
```

### Phase 3: Pool and lifecycle (Sequential)
```
T2 → T3 → T4
```

### Phase 4: Integration proof (Sequential)
```
T4 → T5
```

### Phase 5: Example and documentation (Parallel OK)
```
        ┌→ T6 [P]
T5 ─────┤
        └→ T7 [P]
```

---

## Task Breakdown

### T1: Add the `Concurrency` setting

**What**: Add `Concurrency int` to `Config`, mirror it onto `Client`, and default it to 10 when unset or invalid.
**Where**: `client.go`, `client_test.go`
**Depends on**: None
**Reuses**: The existing defaulting block in `newClient` (`client.go:114-149`) and its documentation idiom.
**Requirement**: POOL-02

**Done when**:
- [ ] `Config.Concurrency` documented, including why a value above the database pool's connection count is safe (a connection is held only across claim and finalize).
- [ ] `Concurrency <= 0` takes the default of 10 without erroring; a positive value is used verbatim.
- [ ] Unit tests cover both defaulting branches and an explicit value.
- [ ] Gate passes: `go test -race ./...`
- [ ] Test count: `client_test.go` ≥ 8 (was 7)

**Tests**: unit
**Gate**: quick
**Commit**: `feat(client): make worker concurrency configurable`

---

### T2: Separate the handler's context from the finalization context

**What**: Split `runJob`'s single context into a cancellable handler context and a non-cancellable, timeout-bounded context used to record the outcome.
**Where**: `loop.go`, `loop_test.go`
**Depends on**: T1
**Reuses**: `runProtected` (`loop.go:124`), `dispose` (`loop.go:147`) — both unchanged in behaviour.
**Requirement**: SAFE-03

**Done when**:
- [ ] `runJob` takes the handler context; finalization derives its own via `context.WithoutCancel` plus a bounded timeout.
- [ ] The rule is stated in a comment where a future reader will collapse the two if it is not: the handler's context may be cancelled, the context that records an outcome never is.
- [ ] A unit test proves a job whose handler context is already cancelled still records a terminal outcome — the sensor for the D-7 option (c) failure.
- [ ] Behaviour is otherwise unchanged: the existing loop suite passes without assertion edits.
- [ ] Gate passes: `go test -race ./...`
- [ ] Test count: `loop_test.go` ≥ 18 (was 17)

**Tests**: unit
**Gate**: quick
**Commit**: `fix(worker): record a job's outcome even when its context was cancelled`

---

### T3: Replace the single fetch loop with a worker pool and a real lifecycle

**What**: Introduce the `runner` (fetch loop with capacity tokens, `Concurrency` worker goroutines, unbuffered handoff channel), make `Start` non-blocking, and add `Stop(ctx)` performing the ordered clean drain.
**Where**: `pool.go` (new), `loop.go`, `client.go`, `errors.go`, `heartbeat.go` (stop signal only), `loop_test.go`, `heartbeat_test.go`, `example_test.go`
**Depends on**: T2
**Reuses**: `inflightSet` unchanged (`inflight.go`); `heartbeat`/`extendLeases` unchanged in body (`heartbeat.go`); `runJob`, `dispose`, `sleep` from `loop.go`; `rescueLoop` unchanged (`rescue.go`).
**Requirement**: POOL-01, POOL-03, POOL-04, POOL-05, SHUT-01, SHUT-02, SHUT-03, SAFE-01, SAFE-02, SAFE-04

**Done when**:
- [ ] A fixed pool of `Concurrency` workers executes jobs concurrently, fed by one fetch loop over an unbuffered channel.
- [ ] The fetch loop claims no more rows than there are idle workers; `inflight.add` happens at claim, not at execution.
- [ ] `Start` returns once the pool is running; a second `Start` returns `ErrAlreadyStarted`.
- [ ] `Stop` stops claiming — fetch loop **and** rescuer — before it begins waiting, and returns `nil` once every in-flight job has recorded a terminal outcome.
- [ ] `Stop` before `Start` returns `ErrNotStarted`; a second `Stop` returns the first call's result without blocking.
- [ ] Cancelling `Start`'s context performs the whole shutdown, not merely the start of it.
- [ ] The heartbeat stops only after the last worker drains, and renews every in-flight lease in one call at any pool size.
- [ ] Sensors exist for: N concurrent handlers observed; each job executed exactly once; one handler's panic and one handler's block leave the other workers working; an idle pool's fetch count over a fixed span is bounded above and below (lesson L-001); a fetch error backs off without stopping the pool; `goleak.VerifyNone` after a full `Start`/`Stop`.
- [ ] `example_test.go` updated to the new lifecycle.
- [ ] Gate passes: `go test -race ./...`
- [ ] Test count: root unit ≥ 70 total; no existing assertion weakened or deleted without a stated reason.

**Tests**: unit
**Gate**: quick
**Commit**: `feat(worker): run jobs on a worker pool with start and stop`

---

### T4: Bound the drain, escalate, and return unfinished jobs to the queue

**What**: On drain-budget exhaustion, cancel the per-job contexts, requeue every job still in flight and every job the fetch loop claimed but never dispatched, and report the unfinished count.
**Where**: `pool.go`, `errors.go`, `loop_test.go` (or a new `pool_test.go`)
**Depends on**: T3
**Reuses**: `MarkRetryable` (`internal/driver/driver.go:127`) as the requeue; `driver.AttemptError`; `writeFailed`'s takeover-vs-failure logging distinction (`loop.go:222`).
**Requirement**: SHUT-04, SHUT-05

**Done when**:
- [ ] Exhausting `Stop`'s budget cancels the handler contexts, requeues what remains, and returns an error wrapping `ErrDrainIncomplete` that states how many jobs did not finish.
- [ ] A requeued job is claimable immediately, its `attempt` is unchanged by the requeue, and the reason is recorded in its error history.
- [ ] Jobs the fetch loop holds claimed but undispatched at shutdown are requeued by the fetch loop rather than left to lease expiry.
- [ ] `Stop` with an already-done context still stops claiming and requeues before returning its error.
- [ ] A requeue that loses the race with the worker's own finalization is logged as expected, not as a failure.
- [ ] Sensors exist for each of the above, including one proving the requeue does **not** decrement `attempt` — the fence-safety property D-6 turns on.
- [ ] Gate passes: `go test -race ./...`
- [ ] Test count: root unit ≥ 76 total

**Tests**: unit
**Gate**: quick
**Commit**: `feat(worker): return unfinished jobs to the queue when shutdown runs out of time`

---

### T5: Prove the pool against real PostgreSQL

**What**: Integration coverage for concurrent execution and both shutdown paths against a real database.
**Where**: `e2e_integration_test.go`
**Depends on**: T4
**Reuses**: `internal/testdb` harness and the existing `TestEndToEndConcurrentLoopsExecuteEachJobExactlyOnce` pattern (`e2e_integration_test.go:109`).
**Requirement**: POOL-04, SHUT-03, SHUT-04, SAFE-03

**Done when**:
- [ ] A pool with `Concurrency > 1` drains a batch of jobs with each executed exactly once, proven against real `SELECT ... FOR UPDATE SKIP LOCKED` claiming.
- [ ] A clean `Stop` leaves zero rows in `running`.
- [ ] A `Stop` whose budget is exhausted leaves the unfinished rows claimable rather than `running`, and a second client picks them up.
- [ ] Gate passes: `go test -race ./...` and `go test -race -tags=integration ./...`
- [ ] Test count: `e2e_integration_test.go` ≥ 10 (was 7)

**Tests**: integration
**Gate**: full
**Commit**: `test(worker): cover pool execution and shutdown against postgres`

---

### T6: Runnable email pipeline example [P]

**What**: A `package main` program demonstrating enqueue, concurrent execution, retries through a deterministic flaky delivery stub, and graceful shutdown on SIGINT.
**Where**: `examples/email/main.go`, `examples/email/delivery.go`, `examples/email/delivery_test.go`, `examples/email/README.md`
**Depends on**: T5
**Reuses**: The public API only — `Migrate`, `NewWorkers`, `Register`, `NewClient`, `Insert`, `Start`, `Stop`.
**Requirement**: EX-01

**Done when**:
- [ ] The program compiles under `go build ./...` and passes `go vet ./...`.
- [ ] It reads `DATABASE_URL`, migrates, registers a worker, enqueues a batch, runs a pool of more than one worker, and stops on SIGINT with a bounded context, printing the verdict.
- [ ] The delivery stub fails a documented, deterministic subset of recipients — no randomness — so a reader can predict which jobs retry.
- [ ] A unit test asserts the stub's documented pattern.
- [ ] No new module dependency: `go.mod` is unchanged.
- [ ] Gate passes: `go build ./... && go vet ./...` and `go test -race ./...`

**Tests**: unit
**Gate**: build + quick
**Commit**: `docs(examples): add a runnable email pipeline example`

---

### T7: Document the lifecycle and concurrency [P]

**What**: Bring the package documentation and README in line with the new lifecycle contract.
**Where**: `doc.go`, `README.md`
**Depends on**: T5
**Reuses**: Existing documentation voice.
**Requirement**: SHUT-01 (documentation half)

**Done when**:
- [ ] The documented contract states that `Start` returns once the pool is running, that `Stop` drains within the caller's budget, and what a non-nil `Stop` error means.
- [ ] The at-least-once consequence of an incomplete drain is stated plainly: those jobs are returned to the queue and may run twice.
- [ ] `Concurrency` appears in the configuration documentation.
- [ ] No unverifiable performance claims (CLAUDE.md constraint).
- [ ] Gate passes: `go build ./... && go vet ./...`

**Tests**: none (documentation layer — matrix says build gate only)
**Gate**: build
**Commit**: `docs: describe the worker pool and shutdown contract`

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: `Concurrency` setting | 1 config field + defaulting | ✅ Granular |
| T2: context split | 1 function's contract | ✅ Granular |
| T3: pool + lifecycle | 1 new component + its call sites | ⚠️ Large but cohesive — the pool and the lifecycle cannot be split without leaving untestable intermediate state (a pool with no way to stop it) |
| T4: escalation + requeue | 1 code path | ✅ Granular |
| T5: integration proof | 1 test file | ✅ Granular |
| T6: example program | 1 program | ✅ Granular |
| T7: documentation | 2 documentation files | ✅ Granular |

**On T3:** splitting "add the pool" from "add `Stop`" would produce a commit in which the pool cannot be shut down and no lifecycle test can be written — the exact anti-pattern the test co-location rule forbids. It stays one task, and the Verifier's discrimination pass is the counterweight.

## Diagram-Definition Cross-Check

| Task | Depends On (body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | Phase 1 entry | ✅ Match |
| T2 | T1 | T1 → T2 | ✅ Match |
| T3 | T2 | T2 → T3 | ✅ Match |
| T4 | T3 | T3 → T4 | ✅ Match |
| T5 | T4 | T4 → T5 | ✅ Match |
| T6 | T5 | T5 → T6 `[P]` | ✅ Match |
| T7 | T5 | T5 → T7 `[P]` | ✅ Match |

T6 and T7 are `[P]`: they touch disjoint files (`examples/` vs `doc.go`/`README.md`), neither depends on the other, and both test types are parallel-safe.

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1 | Configuration & defaults | unit | unit | ✅ OK |
| T2 | Lifecycle & pool (domain logic) | unit | unit | ✅ OK |
| T3 | Lifecycle & pool + heartbeat + errors | unit | unit | ✅ OK |
| T4 | Lifecycle & pool + errors | unit | unit | ✅ OK |
| T5 | End-to-end against PostgreSQL | integration | integration | ✅ OK |
| T6 | Example program | unit (stub) + build | unit | ✅ OK |
| T7 | Documentation | none (build gate only) | none | ✅ OK |

No violations.

---

## Delegation plan

Five phases exceeds the three-phase threshold at which `tlc-spec-driven` offers one worker per phase. Under the ship-cycle autonomy contract there is no user to accept that offer, so it is decided here and recorded:

- **Phases 1–4 (T1–T5) execute inline.** Every one of them carries a correctness invariant named in the brief — claiming, leases, the ownership fence, shutdown ordering. The cost-discipline rule puts those on the strong model, and they share a single design whose coherence is the thing most likely to break if split across workers with separate mental models.
- **Phase 5 (T6, T7) is delegated to one worker.** It is fully specified, touches files nothing else touches, introduces no invariant, and a failing `go build`/`go vet`/`go test` catches any slip immediately — it passes all four downshift-safe conditions as an independent unit.
- **The Verifier is a fresh agent regardless**, per the always-on rule: author ≠ verifier.
