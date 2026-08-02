# Middleware and Scheduling Specification

## Problem Statement

Drover executes a job by looking up its registered worker and calling it. There is no
place to put behaviour that belongs to *every* job — timing out a handler that hangs,
logging an attempt, and later (Cycle E) recording a metric — so each of those would have
to be hard-coded into the execution path or repeated inside every worker. Separately, a
job can only be enqueued for immediate execution into the single queue named `default`:
the `queue` and `scheduled_at` columns have existed since the first migration, but no
caller can set them. That leaves two gaps a queue is expected to close — deferring work to
a later time, and separating urgent work from bulk work so a backlog of newsletters cannot
delay a password reset.

## Goals

- [ ] A `func(Handler) Handler` middleware chain wraps every job execution, in a defined
      order, composed once at client construction.
- [ ] Ship the two middleware the roadmap names: logging (attempt start and outcome with
      duration) and timeout (a per-job deadline on the handler's context, closing the half
      of ADR-0003's "child context with timeout" that the concurrency cycle deferred).
- [ ] A caller can enqueue into a named queue and at a chosen future time.
- [ ] One pool serves several named queues, choosing among them by weight, such that a
      heavily weighted queue is served preferentially and no queue is ever starved.

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| Strict-priority queue ordering | ADR-0003 names it "an optional strict flag"; the RFC-0001 Cycle D row asks only for weighted priorities. Weighted fetch alone is starvation-free, which strict ordering is not — shipping the safe mode first and the footgun never is the smaller surface. |
| Per-queue concurrency limits | Weights allocate *fetch preference* across one shared pool. Splitting the pool per queue would strand workers on an idle queue and is a different feature (it changes the topology AD-021 fixed) with no roadmap row. |
| Pausing or draining an individual queue | Operator action with no configuration surface yet; belongs with the CLI in Cycle F. |
| Per-kind default insert options (`JobArgs.InsertOpts()`) | Convenience layer over what this cycle builds; adds an interface method to every args type for no capability that `*InsertOpts` does not already give. |
| Metrics middleware | Cycle E owns the metric set and the ops port. This cycle only has to make the chain exist so E can hang a middleware on it. |
| Unique jobs, batch insert | Cycles H and G respectively. |
| Retry-policy-per-queue | Not in any roadmap row; the retry policy is a client-level `RetryPolicy` (AD-015) and stays one. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here. All were settled under the ship-cycle
auto-decision rule; the option sets and rationale are in `context.md` as `D-1`..`D-9` and
mirrored into `.specs/STATE.md`.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| What a middleware sees | `Handler func(ctx, *JobRow) error`, over the existing public `JobRow` | `JobRow` is already exported and already carries every field a middleware could want. A second near-identical public job type would be surface for nothing. | auto (D-1) |
| Where the chain is applied | Around dispatch inside `runJob`, not at `Register` | Middleware must also observe the failure modes that are not a handler returning an error — an unregistered kind, a panic. Registration cannot see client config in any case. | auto (D-2) |
| Chain order | Index 0 is outermost | Matches `net/http` and every Go middleware convention; the alternative reads backwards. | auto (D-3) |
| Panic containment | Recovery is innermost (around the registered worker) *and* outermost (around the whole chain) | Innermost so every middleware observes a panic as an ordinary error (AD-013); outermost so a bug in a middleware cannot kill a pool worker goroutine. | auto (D-4) |
| Built-in logging | The client always installs `Logging` as the outermost middleware; `Config.Middleware` nests inside it | Keeps job logs from disappearing the moment a user configures any middleware, while still shipping `Logging` as an exported, reusable piece. | auto (D-5) |
| Insert options shape | `Insert(ctx, args, opts *InsertOpts)`, `nil` meaning defaults | One extensible type beats a growing set of functional-option constructors for a library whose stated value is a small surface. Pre-1.0 with no release, so the call-site change costs nothing outside this repo — the same reasoning as AD-023. | auto (D-6) |
| Which clock decides a delayed job's state | The database | AD-020 already settled that the database clock owns lease deadlines; the same argument applies to whether a job is due, and a fleet with skewed clocks must not disagree about it. | auto (D-7) |
| Queue selection algorithm | Weighted sampling without replacement produces a full ordering each round; queues are tried in that order until capacity is filled | Starvation-free by construction, and an empty high-weight queue costs a query rather than a whole poll interval. | auto (D-8) |
| Invalid configuration (nil middleware, empty queue name, non-positive weight) | Nil middleware and empty queue name panic at construction; a non-positive weight warns and is corrected to 1 | AD-007 already made boot-time programmer errors panic. A weight is a tuning number rather than a structural error, so it follows the `HeartbeatInterval` precedent of warn-and-default instead. | auto (D-9) |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Middleware chain ⭐ MVP

**User Story**: As an application author, I want to wrap every job execution with my own
behaviour, so that concerns shared by all jobs live in one place instead of being copied
into every worker.

**Why P1**: It is the cycle's structural deliverable — the timeout and logging middleware
are instances of it, and Cycle E's metrics hang off it.

**Acceptance Criteria**:

1. WHEN a `Middleware` is configured THEN the system SHALL call it once per job execution,
   with the next handler in the chain as its argument.
2. WHEN `Config.Middleware` holds `[A, B]` THEN the system SHALL run A's pre-handler work
   before B's, and A's post-handler work after B's.
3. WHEN a middleware returns an error without calling the next handler THEN the system
   SHALL NOT invoke the registered worker, and SHALL dispose of the job on that error
   exactly as it disposes of a worker's error.
4. WHEN a middleware modifies the context it passes on THEN the registered worker SHALL
   receive the modified context.
5. WHEN a middleware modifies the context it passes on THEN the context used to record the
   job's outcome SHALL be unaffected by that modification.
6. WHEN no middleware is configured THEN a job SHALL execute exactly as it does today, with
   its outcome recorded identically.
7. WHEN a job's kind has no registered worker THEN the configured middleware SHALL still
   run, and SHALL observe the unregistered-kind error as the chain's return value.
8. WHEN the registered worker panics THEN each configured middleware SHALL observe an
   ordinary non-nil error rather than an unwinding panic.
9. WHEN a middleware itself panics THEN the pool SHALL survive, the job SHALL be disposed
   of as a failed attempt, and the worker goroutine SHALL remain available for more work.
10. WHEN the caller mutates the slice it passed as `Config.Middleware` after construction
    THEN the chain the client executes SHALL be unchanged.
11. WHEN `Config.Middleware` contains a nil entry THEN construction SHALL panic with a
    message naming the offending index.

**Independent Test**: configure a middleware that appends to a recorder, enqueue one job,
assert the recorder shows the middleware ran around the worker and that the job completed.

---

### P1: Timeout middleware ⭐ MVP

**User Story**: As an operator, I want a handler that hangs to be cut off after a bounded
time, so that one stuck job does not occupy a pool worker indefinitely.

**Why P1**: ADR-0003 specifies a per-job child context *with timeout*; the concurrency
cycle shipped the cancellable child and deferred the timeout to this chain. It is a stated
carry-over debt, not a new idea.

**Acceptance Criteria**:

1. WHEN `Timeout(d)` is in the chain and the handler runs longer than `d` THEN the context
   the handler received SHALL be done with `context.DeadlineExceeded`.
2. WHEN the handler returns before `d` elapses THEN the system SHALL record the handler's
   own outcome, unaltered by the middleware.
3. WHEN the handler returns after its deadline expired THEN the system SHALL record the
   error the handler returned, without substituting one of its own.
4. WHEN a handler cut off by the deadline returns an error THEN the job SHALL be disposed
   of as an ordinary failed attempt — retried if attempts remain, dead if they do not.
5. WHEN the deadline expires THEN the context used to record the job's outcome SHALL still
   be usable, so the attempt is finalized rather than left running for the rescuer.
6. WHEN `Timeout(d)` is constructed with `d` less than or equal to zero THEN it SHALL apply
   no deadline and pass the context through unchanged.
7. WHEN the pool is shut down while a job under a timeout is running THEN cancelling the
   job context SHALL still reach the handler.

**Independent Test**: register a worker that blocks on `ctx.Done()` and returns `ctx.Err()`,
configure `Timeout(50ms)`, enqueue one job, assert it finishes in well under a second with
a deadline-exceeded error recorded against the attempt.

---

### P1: Logging middleware ⭐ MVP

**User Story**: As an operator, I want each attempt's start, duration, and error reported
on a consistent line, so that I can follow a job through the log without reading the code.

**Why P1**: Named in the RFC-0001 Cycle D row, and it is the reference implementation
users copy when writing their own middleware.

**Acceptance Criteria**:

1. WHEN a job execution begins THEN `Logging` SHALL emit one record at `INFO` naming the
   job's id, kind, queue, and attempt.
2. WHEN a job execution ends without error THEN `Logging` SHALL emit one record at `INFO`
   carrying the elapsed duration of the execution.
3. WHEN a job execution ends with an error THEN `Logging` SHALL emit one record at `WARN`,
   not `ERROR`, carrying the error and the elapsed duration — a failed attempt is a normal,
   designed outcome that the retry machinery expects, and only its terminal disposition
   decides whether anything is wrong.
4. WHEN no middleware is configured THEN the client SHALL still emit those records, because
   it installs `Logging` itself.
5. WHEN `Config.Middleware` is configured THEN the client's own `Logging` SHALL still run,
   and SHALL be outermost — its duration therefore covering the configured middleware.
6. WHEN a job execution succeeds THEN exactly one record SHALL report that success: the
   middleware's, replacing the line the execution path emitted before this cycle. A
   successful job SHALL NOT produce two success records.
7. WHEN a job is disposed of THEN the disposition records (retry scheduled, dead, cancelled,
   snoozed) SHALL each still be emitted exactly once, at their existing levels, and SHALL
   NOT be duplicated by the logging middleware.

**Independent Test**: run one succeeding and one failing job against a capturing
`slog.Handler`; assert exactly one start record and one end record per attempt, at the
levels above.

---

### P1: Scheduled and queued enqueue ⭐ MVP

**User Story**: As an application author, I want to enqueue a job into a named queue and/or
for a future time, so that I can defer work and separate urgent work from bulk work.

**Why P1**: The columns exist and the fetch predicate already honours them; without an
insert-side API they are unreachable, and the named-queue story below has nothing to fetch.

**Acceptance Criteria**:

1. WHEN `Insert` is called with `nil` options THEN the job SHALL be enqueued into the
   `default` queue, due immediately, exactly as before this cycle.
2. WHEN `InsertOpts.Queue` is set THEN the stored job SHALL carry that queue name.
3. WHEN `InsertOpts.Queue` is empty THEN the stored job SHALL carry the `default` queue.
4. WHEN `InsertOpts.ScheduledAt` is in the future THEN the stored job SHALL carry that
   scheduled time and SHALL be in the `scheduled` state.
5. WHEN `InsertOpts.ScheduledAt` is in the future THEN a fetch occurring before that time
   SHALL NOT claim the job.
6. WHEN a job's scheduled time passes THEN the next fetch of its queue SHALL claim it.
7. WHEN `InsertOpts.ScheduledAt` is the zero time THEN the job SHALL be due immediately and
   SHALL be in the `available` state.
8. WHEN `InsertOpts.ScheduledAt` is in the past THEN the job SHALL be due immediately and
   SHALL NOT be rejected.
9. WHEN whether a job is due is decided THEN it SHALL be decided by the database clock, not
   the enqueuing client's.
10. WHEN `InsertTx` is called with options THEN it SHALL honour them identically to `Insert`,
    and the job SHALL exist only if the caller's transaction commits.

**Independent Test**: insert one job 200ms in the future, assert a fetch immediately after
returns nothing and a fetch after the delay returns it.

---

### P1: Named queues with weighted fetch ⭐ MVP

**User Story**: As an operator, I want to give my queues relative weights, so that urgent
work is served preferentially without bulk work ever being starved.

**Why P1**: The reason to have named queues at all; without weighting, a second queue is
just a label.

**Acceptance Criteria**:

1. WHEN `Config.Queues` is unset THEN the client SHALL work exactly one queue, `default`,
   behaving as it did before this cycle.
2. WHEN `Config.Queues` names several queues THEN the client SHALL claim and execute jobs
   from all of them using one shared pool of `Config.Concurrency` workers.
3. WHEN several queues are configured and all have work THEN over many fetch rounds each
   queue SHALL be tried first with a frequency proportional to its weight.
4. WHEN a queue with a high weight is empty THEN the fetch round SHALL go on to try the
   remaining configured queues before sleeping for the poll interval.
5. WHEN a queue has the lowest possible weight and another queue is permanently saturated
   THEN jobs in the low-weight queue SHALL still be executed — no configuration starves a
   queue.
6. WHEN a fetch round finds work THEN it SHALL claim no more jobs in total than there are
   idle workers, preserving the invariant that a claimed job is one a worker is about to run.
7. WHEN a queue is configured with a weight less than one THEN the client SHALL log a
   warning naming that queue and SHALL treat its weight as one.
8. WHEN `Config.Queues` contains an empty queue name THEN construction SHALL panic.
9. WHEN the client starts THEN it SHALL log the queues it will work and their weights.
10. WHEN a job is enqueued into a queue no running client works THEN it SHALL remain
    claimable indefinitely rather than being rejected or lost.

**Independent Test**: configure `{"high": 9, "low": 1}`, saturate both, run the pool, and
assert both queues drain while `high` is selected first materially more often than `low`.

---

## Edge Cases

- WHEN the same middleware value appears twice in `Config.Middleware` THEN both positions
  SHALL run, once each, in their configured order.
- WHEN a middleware calls the next handler more than once THEN the system SHALL not
  prevent it, and SHALL dispose of the job on the error the chain finally returns.
- WHEN a middleware swallows the handler's error and returns nil THEN the job SHALL be
  marked completed — the chain's return value is the attempt's verdict.
- WHEN `Timeout` is configured with a duration longer than the lease THEN the job SHALL
  still run to its deadline, its lease held open by the heartbeat.
- WHEN a shutdown hands back a job that was running under a timeout THEN the hand-back
  SHALL behave exactly as it does for any other running job.
- WHEN every configured queue is empty THEN the fetch round SHALL sleep one poll interval,
  not spin.
- WHEN only one queue is configured THEN queue selection SHALL issue exactly one fetch per
  round, adding no query the current single-queue client does not already make.
- WHEN `Config.Queues` maps a queue to a very large weight THEN selection SHALL remain
  well-defined and SHALL NOT overflow or starve the others.
- WHEN a scheduled job's time arrives while the client is stopped THEN it SHALL be claimed
  by the next client to fetch that queue.

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| MW-01 | P1: Middleware chain | Implementing | Implemented |
| MW-02 | P1: Middleware chain | Implementing | Implemented |
| MW-03 | P1: Middleware chain | Implementing | Implemented |
| MW-04 | P1: Middleware chain | Implementing | Implemented |
| MW-05 | P1: Middleware chain | Implementing | Implemented |
| TMO-01 | P1: Timeout middleware | Implementing | Implemented |
| TMO-02 | P1: Timeout middleware | Implementing | Implemented |
| LOG-01 | P1: Logging middleware | Implementing | Implemented |
| LOG-02 | P1: Logging middleware | Implementing | Implemented |
| SCHED-01 | P1: Scheduled and queued enqueue | Implementing | Implemented |
| SCHED-02 | P1: Scheduled and queued enqueue | Implementing | Implemented |
| SCHED-03 | P1: Scheduled and queued enqueue | Implementing | Implemented |
| QUEUE-01 | P1: Named queues with weighted fetch | Implementing | Implemented |
| QUEUE-02 | P1: Named queues with weighted fetch | Implementing | Implemented |
| QUEUE-03 | P1: Named queues with weighted fetch | Implementing | Implemented |
| QUEUE-04 | P1: Named queues with weighted fetch | Implementing | Implemented |

**Requirement definitions**

- **MW-01** — `Handler` and `Middleware` types exist and a chain is composed once at
  construction, index 0 outermost, from a defensive copy. (Story 1 AC1, AC2, AC10)
- **MW-02** — The chain is applied around dispatch in the execution path, covering the
  registered-worker call, the unregistered-kind failure, and short-circuiting middleware.
  (Story 1 AC3, AC6, AC7)
- **MW-03** — Context passed down the chain reaches the worker; the finalize context is
  derived from the original job context and is unaffected. (Story 1 AC4, AC5)
- **MW-04** — Panic containment: innermost recovery makes a worker panic an ordinary error
  to middleware; outermost recovery keeps a middleware panic from killing a pool worker.
  (Story 1 AC8, AC9)
- **MW-05** — Configuration validation: a nil middleware entry panics at construction with
  its index. (Story 1 AC11)
- **TMO-01** — `Timeout(d)` applies a deadline to the handler's context and passes the
  handler's own outcome through. (Story 2 AC1, AC2, AC3, AC6)
- **TMO-02** — A timed-out attempt is finalized normally and remains reachable by shutdown
  cancellation. (Story 2 AC4, AC5, AC7)
- **LOG-01** — `Logging(logger)` emits one start record and one end record per execution at
  the specified levels, carrying duration; a failed execution is `WARN`, not `ERROR`.
  (Story 3 AC1, AC2, AC3)
- **LOG-02** — The client installs `Logging` outermost by default; success is reported once
  and the disposition records are neither lost nor duplicated. (Story 3 AC4, AC5, AC6, AC7)
- **SCHED-01** — `InsertOpts` exists; `Insert`/`InsertTx` accept it; `nil` and empty values
  reproduce today's behaviour. (Story 4 AC1, AC2, AC3, AC10)
- **SCHED-02** — A future scheduled time is stored, sets the `scheduled` state, and holds
  the job back from fetch until it passes. (Story 4 AC4, AC5, AC6)
- **SCHED-03** — Zero and past scheduled times mean due-now, and dueness is decided by the
  database clock. (Story 4 AC7, AC8, AC9)
- **QUEUE-01** — `Config.Queues` exists with defaulting and validation; the start log names
  the queues and weights. (Story 5 AC1, AC7, AC8, AC9)
- **QUEUE-02** — Weighted sampling without replacement orders the queues each round with
  first-position frequency proportional to weight. (Story 5 AC3)
- **QUEUE-03** — One shared pool fetches across queues in that order within a round,
  respecting idle-worker capacity and falling through empty queues. (Story 5 AC2, AC4, AC6)
- **QUEUE-04** — No configuration starves a queue, and a queue nobody works keeps its jobs.
  (Story 5 AC5, AC10)

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 16 total, 16 mapped to tasks, 0 unmapped.

---

## Success Criteria

- [ ] A user can add a middleware, a per-job timeout, a delayed job, and a weighted second
      queue without touching drover's internals.
- [ ] The default configuration behaves exactly as it did before this cycle: one queue,
      immediate execution, the same log records per job.
- [ ] Weighted selection is demonstrated by a distributional assertion, not asserted by
      construction.
- [ ] The full gate is green: build, vet, unit `-race`, integration, and lint.
