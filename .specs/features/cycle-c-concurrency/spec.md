# Concurrency: Worker Pool and Graceful Shutdown — Specification

## Problem Statement

Drover executes exactly one job at a time. `Start` runs a fetch loop inline that claims a single row, runs it to completion, and only then claims the next — so throughput is bounded by the slowest handler, and a queue with any latency at all cannot be drained by adding capacity within a process. There is also no way to stop a running client except cancelling its context and hoping: the in-flight job is allowed to finish, but a caller cannot wait for that to happen, cannot bound how long it waits, and gets no answer about whether anything was left running. Every shutdown therefore relies on lease expiry — the crash-path backstop — even when the process exited cleanly.

## Goals

- [ ] A configurable, fixed pool of N worker goroutines executes jobs concurrently, fed by a single fetch loop over a channel, with each claimed job executed by exactly one worker.
- [ ] `Start(ctx)` / `Stop(ctx)` are a real lifecycle pair: shutdown stops claiming, drains in-flight work within a caller-supplied budget, escalates by cancelling per-job contexts, and returns jobs it could not finish to the queue — reporting honestly what it could not do.
- [ ] Every reliability guarantee that held for one job continues to hold for N: leases renewed for all in-flight jobs including during drain, the ownership fence intact, no goroutine leaked across a lifecycle.
- [ ] A runnable example program demonstrates the library end to end on a legible domain.

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| ------- | ------ |
| Named queues, per-queue pool sizing, weighted priorities | Cycle D owns queues and priorities (RFC-0001). This cycle ships one pool on the default queue, shaped so a per-queue split is an extension rather than a rewrite. |
| Per-job execution timeout as a `Config` field | Cycle D ships a `func(Handler) Handler` chain including timeout middleware (RFC-0001). This cycle provides the cancellable per-job context that such middleware needs, and nothing more. |
| Metrics for pool utilisation, queue depth, oldest-job age | Cycle E owns observability. Structured logs only here. |
| Dynamic resizing of the pool while running | No demand, and a fixed pool is what ADR-0003 specifies. Size is fixed at `NewClient`. |
| Batch-claiming beyond idle capacity as a latency optimisation | Prefetching parks claimed rows holding live leases; rejected on reliability grounds (see D-2 in `context.md`). |
| `-race` in CI | Already delivered — `.github/workflows/ci.yml` runs `go test -race ./...` and `go test -race -tags=integration ./...`. Confirmed, no work. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here. Full option sets and rationale live in `context.md`; this table is the summary.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --------------------- | -------------- | --------- | ---------- |
| `Start` blocking vs. non-blocking | `Start(ctx)` returns once the pool is running; `Stop(ctx)` blocks until drained | RFC-0001 names `Start(ctx)`/`Stop(ctx)` as a pair; a blocking `Start` leaves no caller able to wait on shutdown. Pre-1.0, no released version, so the contract change costs nothing outside the repo. | y (auto, D-4) |
| Default `Concurrency` | 10 | A queue library that defaults to serial execution defaults to the bug this cycle exists to fix. Drover holds a database connection only across claim and finalize, not for the job's duration, so concurrency well above the pool's connection count is safe. | y (auto, D-3) |
| How shutdown returns a job to the queue | `MarkRetryable(lease, now, detail)` — existing driver method, no new SQL | It is the only existing transition that returns a running row to a claimable state **without decrementing `attempt`**, which is exactly what the ownership fence requires. See D-6. | y (auto, D-6) |
| Whether a shutdown requeue consumes an attempt | Yes — `attempt` is unchanged by the requeue, so the next claim increments it | Decrementing would let a cancelled-but-still-running handler present the same attempt number a later claim hands out, silently defeating the fence (AD-019). The precedent is AD-012. | y (auto, D-6) |
| Drain budget source | The `ctx` passed to `Stop`; no deadline means wait indefinitely | The caller owns its shutdown budget; inventing a second config knob for it would compete with the caller's own deadline. | y (auto, D-5) |
| Behaviour after the drain deadline passes | Cancel per-job contexts, requeue what is still in flight, return an error — do not wait again | ADR-0003:29 fixes this order. A second grace period would need a budget nobody specified. | y (auto, D-5) |
| Where the example app lives | `examples/email/` — a `package main` program | Conventional Go placement, outside the library's API surface, and not a `cmd/` binary the project ships. | y (auto, D-8) |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Jobs execute concurrently ⭐ MVP

**User Story**: As an application operator, I want a single drover client to work N jobs at once so that throughput is a configuration choice rather than a property of my slowest handler.

**Why P1**: It is the cycle's reason to exist. Everything else here either enables it or keeps it safe.

**Acceptance Criteria**:

1. WHEN `Config.Concurrency` is `n > 0` and at least `n` jobs are due THEN the client SHALL have `n` handlers executing simultaneously.
2. WHEN `Config.Concurrency` is zero or negative THEN the client SHALL use the documented default of 10 and SHALL NOT error.
3. WHEN the fetch loop claims jobs THEN the number it claims in one round SHALL NOT exceed the number of idle workers at that moment, so no claimed job waits behind a busy pool holding a live lease.
4. WHEN `n` jobs are claimed and executed concurrently THEN each job SHALL be executed exactly once by exactly one worker.
5. WHEN a handler panics on one worker THEN the remaining workers SHALL continue claiming and executing jobs, and the panicking job SHALL be recorded as a retryable failure carrying its stack trace.
6. WHEN no jobs are due THEN the client SHALL issue fetches at the configured poll interval and no faster — asserted as a bounded count over a fixed span, not a lower bound.
7. WHEN a handler blocks indefinitely on one worker THEN the other workers SHALL remain free to claim and execute other jobs.

**Independent Test**: register a handler that signals entry and blocks on a release channel; insert `n` jobs; assert `n` concurrent entries are observed before any release, with pool size `n`.

---

### P1: Shutdown is ordered, bounded and honest ⭐ MVP

**User Story**: As an application operator, I want `Stop` to drain in-flight work within a budget I choose and tell me what it could not finish, so that a deploy neither drops jobs nor hangs my process.

**Why P1**: Without it, concurrency multiplies the amount of work a restart strands behind a lease.

**Acceptance Criteria**:

1. WHEN `Start(ctx)` is called on a client that is not running THEN it SHALL start the pool and return `nil` without blocking on job execution.
2. WHEN `Start` is called on a client that is already running THEN it SHALL return an error identifying the misuse and SHALL NOT start a second pool.
3. WHEN `Stop(ctx)` is called THEN the client SHALL cease claiming jobs before it begins waiting, and no job SHALL be claimed from the driver after that point.
4. WHEN `Stop(ctx)` is called and every in-flight job finishes before `ctx` is done THEN `Stop` SHALL return `nil`, and every one of those jobs SHALL have recorded a terminal outcome.
5. WHEN `ctx` passed to `Stop` is done while jobs are still executing THEN `Stop` SHALL cancel the per-job contexts, return the still-unfinished jobs to the queue best-effort, and return a non-`nil` error stating how many jobs did not finish.
6. WHEN the fetch loop has claimed jobs it has not yet handed to a worker at the moment shutdown begins THEN those jobs SHALL be returned to the queue rather than left running until their leases expire.
7. WHEN a job is returned to the queue by shutdown THEN it SHALL become claimable immediately, its `attempt` SHALL NOT be decremented by the requeue, and the reason SHALL be recorded in the job's error history.
8. WHEN `Stop` is called on a client that was never started THEN it SHALL return an error immediately and SHALL NOT block.
9. WHEN `Stop` is called a second time THEN it SHALL return without blocking and SHALL NOT panic or double-close any channel.
10. WHEN the context passed to `Start` is cancelled THEN shutdown SHALL begin on its own, following the same ordering as an explicit `Stop`.

**Independent Test**: start a pool with a handler that sleeps past the drain budget; call `Stop` with a short-deadline context; assert the error names the unfinished count and that the job's row is claimable again rather than still `running`.

---

### P1: Reliability guarantees survive the pool ⭐ MVP

**User Story**: As a maintainer, I want every at-least-once guarantee proven for one job to be proven again for N concurrent jobs, so that concurrency does not quietly buy throughput with duplicates.

**Why P1**: The ownership fence and the heartbeat are the load-bearing invariants; a pool changes who calls them, and that is exactly where they break.

**Acceptance Criteria**:

1. WHEN `n` jobs are in flight THEN each heartbeat tick SHALL renew all `n` leases in one call, not only the most recent claim.
2. WHEN shutdown has begun and workers are still draining THEN the heartbeat SHALL keep renewing their leases until the last worker has finished, and SHALL stop only after that.
3. WHEN a worker's job has been re-claimed by another holder THEN that worker's attempt to record an outcome SHALL be refused with `ErrLeaseLost` and logged as a takeover, not as a failure.
4. WHEN a job's context has been cancelled by shutdown escalation THEN the client SHALL still record that job's outcome — finalization SHALL NOT run on the cancelled context.
5. WHEN a full `Start`/`Stop` cycle completes THEN no goroutine created by the client SHALL still be running.
6. WHEN concurrent claims race against the same rows THEN no row SHALL be executed by two workers of the same client.

**Independent Test**: run the pool against the in-memory driver with `n` jobs and assert the recorded `ExtendLeases` calls carry `n` lease entries; separately, assert `goleak.VerifyNone` after `Stop` returns.

---

### P2: A runnable example program

**User Story**: As someone evaluating drover, I want a small program I can read and run that shows enqueue, concurrent execution, retries and clean shutdown, so that I can judge the library without reading its tests.

**Why P2**: It proves the public API is usable from outside the module and is the RFC's designated Cycle C deliverable, but no library guarantee depends on it.

**Acceptance Criteria**:

1. WHEN the repository is built with `go build ./...` and vetted with `go vet ./...` THEN the example program SHALL compile and pass vet.
2. WHEN the example runs against a reachable PostgreSQL THEN it SHALL migrate, register at least one worker, enqueue a batch of jobs, and work them through a pool of more than one worker.
3. WHEN the example's simulated delivery fails THEN the job SHALL be retried by drover's ordinary retry path, and the retry SHALL be visible in the program's output.
4. WHEN the example receives SIGINT THEN it SHALL call `Stop` with a bounded context and report what drained.
5. The example SHALL introduce no module dependency beyond those drover already requires.

**Independent Test**: `go vet ./examples/...` passes and the program's flaky-delivery stub is unit-tested for its documented failure pattern.

---

## Edge Cases

- WHEN `Concurrency` is set to 1 THEN behaviour SHALL match the previous single-worker semantics: one job in flight at a time.
- WHEN `Stop` is called while the fetch loop is blocked sleeping out a poll interval THEN shutdown SHALL NOT wait for that interval to elapse.
- WHEN the driver returns an error from a fetch THEN the pool SHALL log it, back off one poll interval, and continue — a fetch failure SHALL NOT stop the pool or any worker.
- WHEN the requeue of an unfinished job fails at shutdown THEN the failure SHALL be logged and shutdown SHALL continue, leaving lease expiry as the backstop for that row.
- WHEN a handler returns after its context was cancelled and its job has already been requeued and re-claimed elsewhere THEN its write SHALL be refused by the fence rather than overwriting the new holder's outcome.
- WHEN `Concurrency` exceeds the number of due jobs THEN the idle workers SHALL consume no database capacity — a fetch SHALL be issued for the number of idle workers, not one per worker.
- WHEN `Stop`'s context is already done at the moment it is called THEN `Stop` SHALL still stop fetching and requeue in-flight work before returning its error.

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| -------------- | ----- | ----- | ------ |
| POOL-01 | P1: Jobs execute concurrently | Design | Pending |
| POOL-02 | P1: Jobs execute concurrently | Design | Pending |
| POOL-03 | P1: Jobs execute concurrently | Design | Pending |
| POOL-04 | P1: Jobs execute concurrently | Design | Pending |
| POOL-05 | P1: Jobs execute concurrently | Design | Pending |
| SHUT-01 | P1: Shutdown is ordered, bounded and honest | Design | Pending |
| SHUT-02 | P1: Shutdown is ordered, bounded and honest | Design | Pending |
| SHUT-03 | P1: Shutdown is ordered, bounded and honest | Design | Pending |
| SHUT-04 | P1: Shutdown is ordered, bounded and honest | Design | Pending |
| SHUT-05 | P1: Shutdown is ordered, bounded and honest | Design | Pending |
| SAFE-01 | P1: Reliability guarantees survive the pool | Design | Pending |
| SAFE-02 | P1: Reliability guarantees survive the pool | Design | Pending |
| SAFE-03 | P1: Reliability guarantees survive the pool | Design | Pending |
| SAFE-04 | P1: Reliability guarantees survive the pool | Design | Pending |
| EX-01 | P2: A runnable example program | Design | Pending |

**Requirement definitions**

| ID | Requirement | Covers |
| -- | ----------- | ------ |
| POOL-01 | A fixed pool of `Concurrency` workers executes jobs concurrently, fed by one fetch loop over a channel | P1-Story1 AC1, AC7 |
| POOL-02 | `Concurrency` is configurable and defaults to 10 when unset or invalid | P1-Story1 AC2 |
| POOL-03 | The fetch loop claims no more rows than there are idle workers | P1-Story1 AC3; Edge: idle pool |
| POOL-04 | Each claimed job is executed exactly once, and one worker's panic or block does not affect the others | P1-Story1 AC4, AC5; P1-Story3 AC6 |
| POOL-05 | An idle pool polls at the configured interval; a fetch error backs off and does not stop the pool | P1-Story1 AC6; Edge: fetch error |
| SHUT-01 | `Start` is non-blocking, idempotency-guarded, and cancellation of its context begins shutdown | P1-Story2 AC1, AC2, AC10 |
| SHUT-02 | `Stop` stops claiming before waiting; nothing is claimed after shutdown begins | P1-Story2 AC3; Edge: poll-interval sleep |
| SHUT-03 | `Stop` drains within the caller's budget and returns nil when everything finished | P1-Story2 AC4 |
| SHUT-04 | On budget exhaustion `Stop` cancels per-job contexts, requeues what remains, and reports the unfinished count | P1-Story2 AC5, AC7; Edge: requeue failure, already-done ctx |
| SHUT-05 | Claimed-but-undispatched jobs are requeued; `Stop` is safe when never started and when called twice | P1-Story2 AC6, AC8, AC9 |
| SAFE-01 | The heartbeat renews every in-flight lease, at any pool size | P1-Story3 AC1 |
| SAFE-02 | The heartbeat outlives the fetch loop and stops only after the last worker drains | P1-Story3 AC2 |
| SAFE-03 | The ownership fence holds: a lost lease refuses the write and is logged as a takeover; finalization never runs on a cancelled context | P1-Story3 AC3, AC4; Edge: late handler return |
| SAFE-04 | No goroutine leaks across a `Start`/`Stop` cycle | P1-Story3 AC5 |
| EX-01 | A runnable example program builds, vets, and demonstrates concurrency, retries and graceful shutdown with no new dependencies | P2-Story4 AC1–AC5 |

**Coverage:** 15 total, 15 mapped to tasks, 0 unmapped

---

## Success Criteria

- [ ] With `Concurrency: n`, `n` jobs are demonstrably in flight at once, and each is executed exactly once.
- [ ] `Stop` with a generous budget returns `nil` and leaves zero jobs in `running`.
- [ ] `Stop` with an inadequate budget returns within that budget, names the unfinished count, and leaves those rows claimable rather than stranded.
- [ ] `goleak.VerifyNone` passes after a full `Start`/`Stop` cycle in every lifecycle test.
- [ ] The full gate is green: `go build ./...`, `go vet ./...`, `go test -race ./...`, `go test -race -tags=integration ./...`, and golangci-lint.
