# Middleware and Scheduling — Tasks

Contract per task: tests derive from the spec's acceptance criteria and assert
spec-defined outcomes; the gate is green before the task is done; one atomic commit per
task. Quick gate = `go test -race ./...`; full gate = quick plus
`go test -race -tags=integration ./...`; build gate = `go build ./... && go vet ./...`.
Lint (`golangci-lint run`) and the full gate run once per phase boundary and before
pushing.

Baseline on `main`: 149 test functions, all gates green.

---

## Phase 1 — Handler and chain

**T1.1 — Introduce `Handler` and apply a chain around dispatch**
Files: `middleware.go` (new), `worker.go`, `loop.go`, `rescue.go`
- `Handler func(ctx, *JobRow) error` and `Middleware func(Handler) Handler`; `wrap`
  applying a slice with index 0 outermost.
- `workFunc` is deleted; the registry and `Register`'s closure use `Handler` over the
  public `*JobRow`.
- `panicError` carrying value and stack; `dispose` recovers the stack with `errors.As` and
  loses its `stack []byte` parameter (`rescue.go` drops the argument at its call).
- `dispatch` (innermost: registry lookup + worker call + recover) and `execute` (outermost
  recover around the chain); `runJob` calls `execute` and derives `finalizeContext` from
  `jobCtx`.
- Chain is empty for now — behaviour must be unchanged.
Covers: MW-02, MW-03, MW-04 · Story 1 AC7, AC8, AC9; Story 2 AC5
Verify: quick gate; the existing suite passes untouched apart from the `dispose` signature.
A test asserts a panicking worker and a panicking middleware each end as a disposed failed
attempt with the pool still able to run a further job, and that a panic's stack survives
into the recorded attempt error.

**T1.2 — `Config.Middleware`, validation, and composition**
Files: `client.go`, `middleware.go`
- `Config.Middleware []Middleware`; copied at construction; nil entry panics naming its
  index; chain composed once in `newClient`.
Covers: MW-01, MW-05 · Story 1 AC1, AC2, AC3, AC4, AC5, AC6, AC10, AC11
Verify: quick gate. Tests assert outermost-first ordering around the worker, a
short-circuiting middleware never reaching the worker while its error is still disposed of,
a context value set by a middleware arriving at the worker, the finalize context being
unaffected by a middleware's derived context, that mutating the caller's slice after
construction changes nothing, and the nil-entry panic.

*Phase boundary: full gate + lint.*

---

## Phase 2 — Timeout and Logging middleware

**T2.1 — `Logging` middleware and record relocation**
Files: `middleware.go`, `loop.go`
- `Logging(logger)`; nil logger falls back to `slog.Default()`; start at `INFO`, success at
  `INFO` with duration, failure at `WARN` with duration and error.
- The client installs `Logging(c.logger)` outermost, ahead of `Config.Middleware`.
- `runJob`'s `"job started"` and `"job completed"` records are removed; `dispose`'s
  disposition records are untouched.
Covers: LOG-01, LOG-02 · Story 3 AC1–AC7
Verify: quick gate. Tests capture records via a `slog.Handler` and assert exactly one start
and one end record per attempt at the stated levels, that a failing attempt logs `WARN` and
not `ERROR`, that success is reported exactly once, and that each disposition record still
appears exactly once.

**T2.2 — `Timeout` middleware**
Files: `middleware.go`
- `Timeout(d)`; `d <= 0` returns the next handler unchanged; otherwise a derived
  `context.WithTimeout` for the next handler, returning its error verbatim.
Covers: TMO-01, TMO-02 · Story 2 AC1–AC4, AC6, AC7
Verify: quick gate. Tests assert the handler's context reports `DeadlineExceeded` after the
duration, that a fast handler's own outcome passes through unaltered, that a handler
returning after its deadline has *its* error recorded, that the attempt is finalized rather
than left running, that `d <= 0` applies no deadline, and — with an upper-bound elapsed
assertion — that a timed-out job finishes promptly rather than running to the poll interval.

*Phase boundary: full gate + lint.*

---

## Phase 3 — Scheduling

**T3.1 — Scheduled time reaches storage**
Files: `internal/driver/driver.go`, `internal/pgdriver/queries.sql`,
`internal/pgdriver/pgdriver.go`, `internal/dbsqlc/*` (regenerated), `internal/memdriver/memdriver.go`
- `InsertParams.ScheduledAt time.Time`, zero meaning now.
- Insert statement sets `scheduled_at` and derives `state` from the database clock
  (`scheduled` when in the future, `available` otherwise); `sqlc generate` run and the
  generated diff committed.
- `memdriver` mirrors the same rule against its own clock.
Covers: SCHED-02, SCHED-03 · Story 4 AC4, AC7, AC8, AC9
Verify: quick gate, plus `go test -race -tags=integration ./internal/pgdriver/...`. Tests
assert a future time stores `scheduled` and is not returned by a fetch before it is due but
is after, that zero and past times store `available` and are immediately claimable, and that
both drivers agree. Integration exercises the state against a real Postgres, but does **not**
prove the decision is the database's: the container and the test share a host clock, so a
client-side decision would pass identically. That property is asserted by the SQL and by
AD-035, and remains unsensed — see the G-5 entry in `validation.md`.

**T3.2 — `InsertOpts` on the public API**
Files: `client.go`, all call sites (`client_test.go`, `client_integration_test.go`,
`e2e_integration_test.go`, `example_test.go`, `doc.go`, `examples/email/*`)
- `InsertOpts{Queue, ScheduledAt}`; `Insert`/`InsertTx` take `*InsertOpts`; `nil` and empty
  values reproduce current behaviour; empty `Queue` becomes `default`.
Covers: SCHED-01 · Story 4 AC1, AC2, AC3, AC10
Verify: quick gate + full gate. Tests assert nil options reproduce today's row, a named
queue is stored, an empty queue name stores `default`, and `InsertTx` honours options and
still only materialises on commit.

*Phase boundary: full gate + lint.*

---

## Phase 4 — Named queues with weighted fetch

**T4.1 — Weighted queue set and selection**
Files: `queues.go` (new), `client.go`
- `Config.Queues map[string]int`; nil → `{"default": 1}`; empty queue name panics; weight
  below one warns naming the queue and is corrected to one; flattened to a slice sorted by
  name.
- `weightedOrder` — weighted sampling without replacement, `math/rand/v2` used directly,
  weights accumulated as `int64`.
Covers: QUEUE-01, QUEUE-02 · Story 5 AC3, AC7, AC8
Verify: quick gate. Tests assert every ordering is a permutation of the configured queues
(the starvation property), that first-position frequency over many samples tracks the
configured weights within a stated tolerance, that a single queue always yields a
one-element ordering, that a very large weight neither overflows nor starves the others,
and the warn-and-correct and panic paths.

**T4.2 — Multi-queue fetch round**
Files: `pool.go`, `loop.go`
- `runner.queues` replaces `runner.queue`; a fetch round walks a fresh ordering, passing the
  remaining capacity as each call's limit and stopping when capacity is filled, the list is
  exhausted, a fetch errors, or shutdown begins; rows already claimed in the round are still
  dispatched.
- `Start`'s log record reports the queues and weights; the stale "a second queue is a second
  runner" comment is corrected.
Covers: QUEUE-03, QUEUE-04 · Story 5 AC1, AC2, AC4, AC5, AC6, AC9, AC10
Verify: quick gate + full gate. Tests assert jobs in every configured queue are executed by
the one shared pool; that a round whose first-sampled queue is empty still claims from a
later queue *within the same round* (bounded elapsed assertion, not merely "eventually");
that the total claimed in a round never exceeds the idle-worker count across queues; that a
fetch error on a later queue does not strand rows already claimed; that a single configured
queue issues exactly one fetch per round; and that a job enqueued to an unworked queue stays
claimable.

*Phase boundary: full gate + lint.*

---

## Phase 5 — Documentation and example

**T5.1 — Package documentation**
Files: `README.md`, `doc.go`
- Document the middleware chain and its order, the two shipped middleware, delayed enqueue,
  and named queues with weights. No unverified performance claims.
Verify: build gate; `go vet`.

**T5.2 — Example application**
Files: `example_test.go`, `examples/email/*`
- Demonstrate a custom middleware, `Timeout`, a delayed job, and a weighted second queue in
  the email pipeline.
Verify: quick gate + full gate + lint.

---

## Traceability

| Requirement | Task(s) |
| --- | --- |
| MW-01 | T1.2 |
| MW-02 | T1.1 |
| MW-03 | T1.1, T1.2 |
| MW-04 | T1.1 |
| MW-05 | T1.2 |
| TMO-01 | T2.2 |
| TMO-02 | T2.2 |
| LOG-01 | T2.1 |
| LOG-02 | T2.1 |
| SCHED-01 | T3.2 |
| SCHED-02 | T3.1 |
| SCHED-03 | T3.1 |
| QUEUE-01 | T4.1 |
| QUEUE-02 | T4.1 |
| QUEUE-03 | T4.2 |
| QUEUE-04 | T4.2 |

**Coverage:** 16 requirements, 16 mapped to tasks, 0 unmapped.
