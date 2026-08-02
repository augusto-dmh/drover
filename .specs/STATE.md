# Drover — Project State

## Decisions (AD-NNN)

Durable, cross-cycle. Architecture-level decisions live in `docs/adr/`; entries here are cycle-scoped picks that later cycles must honor. Mirrored from each cycle's `context.md`.

| ID | Decision | Source |
|---|---|---|
| AD-001 | Migrations: embedded `.sql` via `embed.FS` + minimal internal migrator, `drover_schema_version` table | cycle-a `context.md` D-1 |
| AD-002 | Full 7-state enum created in migration 001; transitions guarded in storage layer | cycle-a D-2 |
| AD-003 | Cycle A: handler error ⇒ `dead` immediately; Cycle B MUST replace this branch with retry scheduling | cycle-a D-3 — **superseded by AD-013/AD-014 and the cycle-b disposition table** |
| AD-004 | Schema is forward-compatible: `attempt`/`max_attempts(25)`/`errors`/`scheduled_at`/`leased_until`/`queue` from day one; fetch predicate already Cycle-D-shaped | cycle-a D-4 |
| AD-005 | `leased_until` written on claim but unenforced until Cycle B rescuer | cycle-a D-5 — **discharged by AD-016** |
| AD-006 | Constant 1s default poll interval; DB errors log + wait one tick | cycle-a D-6 |
| AD-007 | `Register` panics on duplicate kind; empty kind ⇒ `ErrInvalidKind` at Insert | cycle-a D-7 |
| AD-008 | Unexported `driver` interface; `internal/pgdriver` (pgx+sqlc) + `internal/memdriver` (tests only); export deferred | cycle-a D-8 |
| AD-009 | A retrying job waits in `retryable` with `scheduled_at` set; the fetch predicate widens to `available`/`retryable`/`scheduled`, with no promoter component | cycle-b `context.md` D-1 |
| AD-010 | A snoozed job waits in `scheduled`, not `retryable` — a deferral is not a failure | cycle-b D-2 |
| AD-011 | Snooze decrements `attempt` (floored at zero) so it consumes no attempt | cycle-b D-3 |
| AD-012 | Rescue never changes `attempt`: the attempt that stranded the row was really spent | cycle-b D-4 |
| AD-013 | A recovered panic is a retryable failure, with its stack trace retained | cycle-b D-5 |
| AD-014 | An unregistered job kind is a retryable failure, so rolling deploys do not destroy jobs | cycle-b D-6 |
| AD-015 | `RetryPolicy` is a one-method interface `NextRetry(ctx, job *JobRow) time.Time`, defaulting to the exponential policy; a past answer means "claimable now" | cycle-b D-7, context added at review |
| AD-016 | The rescuer re-claims expired rows with `FOR UPDATE SKIP LOCKED` and a fresh lease, then reuses the ordinary `running` → terminal transitions | cycle-b D-8 |
| AD-017 | Jitter uses `math/rand/v2` directly and is not injectable; tests assert the documented bound over many samples | cycle-b D-9 |
| AD-018 | The heartbeat stops only after the fetch loop returns, not at context cancellation, so a draining job never loses its lease | cycle-b D-10 |
| AD-019 | Every state change is guarded on a `Lease{ID, Attempt}`, not on state alone. `attempt` is the fence token: the claim increments it and a rescue deliberately does not, so a stale worker cannot record the outcome of an attempt it no longer holds. A refused write reports `ErrLeaseLost` and is logged as a takeover, not a failure | review of #3, finding 20 |
| AD-020 | Lease deadlines are computed by the database (`now() + interval`), never by the client. Drivers take a duration, not an instant, so a fleet with unsynchronised clocks cannot shorten or stretch the effective lease | review of #3, finding 21 |
| AD-021 | The executor is one fetch loop feeding a fixed pool of N worker goroutines over an unbuffered channel. Rejected: N independent fetch loops (multiplies polling by N and leaves no single point at which claiming stops) and a goroutine per job (no ceiling) | cycle-c `context.md` D-1 |
| AD-022 | The fetch loop claims exactly as many jobs as there are idle workers, tracked by capacity tokens, so a claimed row is always one a worker is about to run. Prefetching into a buffer is rejected: it parks rows that are `running` and leased with nothing executing them | cycle-c D-2 |
| AD-023 | `Config.Concurrency` defaults to 10, not 1 and not `NumCPU`. A connection is held only across claim and finalize, never for the handler's duration, so concurrency above the pool's connection count is safe | cycle-c D-3 |
| AD-024 | `Start(ctx)` returns once the pool is running; `Stop(ctx)` blocks until drained. The lifecycle is single-use: one `Start`, one `Stop`, guarded by `ErrAlreadyStarted`/`ErrNotStarted` | cycle-c D-4 |
| AD-025 | The drain budget is the context passed to `Stop`, never a config field. When it runs out: cancel job contexts, requeue, return — no second grace period | cycle-c D-5 |
| AD-026 | Shutdown returns an unfinished job to the queue with `MarkRetryable` at `now`, leaving `attempt` untouched. `MarkSnoozed` is rejected as fence-unsafe: it decrements `attempt`, so a cancelled-but-still-running handler could present the number a later claim hands out and its stale write would be accepted (AD-019). The attempt cost follows the AD-012 precedent | cycle-c D-6 |
| AD-027 | A job's handler runs on a cancellable context; the context used to record its outcome never is. Collapsing them would make every escalated job fail to finalize and sit `running` until its lease lapsed | cycle-c D-7 |
| AD-028 | The example application lives in `examples/`, not `cmd/` — `cmd/` is for the shipped CLI, and a demo beside a released binary invites being distributed as one | cycle-c D-8 |
| AD-029 | `Handler` is `func(ctx, *JobRow) error` over the existing public row, and the registry's adapter type becomes it — one function type, not two. Rejected a second minimal `JobInfo` type: two public job types differing only by omission taxes every future field | cycle-d `context.md` D-1 |
| AD-030 | The chain is applied around dispatch in the execution path, not at `Register`. Registration cannot see client config, and an unregistered kind never reaches a registered worker — so no middleware could observe the failure most worth logging during a rolling deploy | cycle-d D-2 |
| AD-031 | Index 0 of `Config.Middleware` is outermost, matching every Go middleware convention | cycle-d D-3 |
| AD-032 | Panics are recovered twice: innermost around the registered worker, so middleware sees a panic as an ordinary error (AD-013); outermost around the whole chain, so a panicking *middleware* cannot unwind its pool worker's goroutine and silently shrink the pool | cycle-d D-4 |
| AD-033 | The client always installs `Logging` outermost, ahead of `Config.Middleware`, so configuring middleware never silently removes job logging. The middleware reports the *execution*; `dispose` keeps reporting the *outcome*. A failed execution logs `WARN`, not `ERROR` — a failed attempt is designed behaviour the retry machinery expects | cycle-d D-5 |
| AD-034 | Insert options are `*InsertOpts` (nil = defaults), not variadic and not functional options. A struct keeps every future enqueue-time option a field rather than a new exported identifier; a variadic would permit an arity whose meaning nothing defines | cycle-d D-6 |
| AD-035 | Whether a delayed job is due — and therefore whether it lands `scheduled` or `available` — is decided by the database clock, in the insert statement. The fetch predicate compares against that same clock, so a client running fast or slow would otherwise write a state its own store disagrees with (AD-020's argument, applied to dueness) | cycle-d D-7 |
| AD-036 | Queue selection is weighted sampling without replacement producing a full ordering per round; the round walks it until idle-worker capacity is spoken for. Weight decides how often a queue is tried *first*, never whether it is tried — so nothing starves, and an empty high-weight queue costs one query rather than a poll interval. Rejected: one runner per queue (pins workers to possibly-idle queues, multiplies polling/heartbeat/rescuer), and weighted ordering in SQL (defeats the queue-equality index, needs a migration, and is unexplainable in review) | cycle-d D-8 |
| AD-037 | Structural configuration errors panic at construction (nil middleware entry, empty queue name); tuning values warn and are corrected (weight below one). Follows AD-007 for the first and the `HeartbeatInterval` precedent for the second, and keeps `newClient`'s signature — the seam the whole unit suite is built on. Silently dropping a nil middleware is rejected outright: a timeout that was never installed looks exactly like one that has not fired | cycle-d D-9 |

**Amended by this cycle:** AD-018 is generalised, not superseded — the heartbeat now stops after the last *pool worker* drains rather than after the fetch loop returns. Same principle, wider scope.

**Corrected by cycle D:** a `pool.go` comment from cycle C guessed that "a second queue is a second runner rather than a rewrite of this one". That was a forward guess, not a recorded decision, and AD-036 supersedes it: a second queue is another entry in the weighted set served by the same pool.

## Roadmap progress

| Cycle | Feature | PR | Merged |
|---|---|---|---|
| A — Walking skeleton | `cycle-a-walking-skeleton` | #1 | 2026-07-25 |
| B — Reliability core | `cycle-b-reliability-core` | #3 | 2026-07-26 |
| B — Lease ownership hardening (from #3 review) | — | #4 | 2026-07-26 |
| C — Concurrency | `cycle-c-concurrency` | #6 | 2026-08-02 |

## Handoff

- **Last shipped**: the concurrency cycle (#6) — worker pool, fetcher→workers channels, `Start`/`Stop` graceful shutdown, and the email example app. `main` is green: build, vet, unit `-race`, integration and lint, 149 test functions.
- **How #6 went**: verification PASS on iteration 3 (the last Major finding was an unsensed hand-back of never-dispatched rows, closed by a deterministic sensor). Review by six independent agents produced 21 findings, **none rejected** — 20 fixed in-cycle, 1 deferred. The reasoning behind every verdict is in `features/cycle-c-concurrency/review-triage.md`, which outlives the PR comments.
- **What the review caught that verification missed**: a bounded shutdown logging one `ERROR` per requeued job for its own designed behaviour; a `Stop` landing between `c.mu`'s release and `r.start` draining a runner with zero goroutines and returning `nil`; `drain`'s `select` returning `ErrDrainIncomplete` with `0 job(s)` on a fully drained pool. Common shape: correct behaviour, wrong verdict reported to the caller — a class no acceptance criterion named, so nothing sensed it.
- **Deviations to remember**: phases 1–4 of #6 executed inline in the orchestrator because each carried a correctness invariant; phase 5 (example + docs) was delegated. Verification and review were both independent fresh agents, so author ≠ verifier and author ≠ reviewer held.
- **Known weak sensor** (worth strengthening if the area is touched again): the test asserting lease deadlines come from the database clock cannot prove skew handling, because the client and the test container share a host clock. A real sensor needs a container with a deliberately skewed clock.
- **Follow-up work carried forward** (recorded, not scheduled): batch the shutdown hand-back into one `unnest`-based statement to remove N serial round trips and the per-lease `GetJob` probe — needs a new `.sql` query, sqlc regeneration, and a driver-interface change, so it belongs with the Cycle G fetch/perf work. Separately, revisit the lifecycle tests for `testing/synctest` where handler lifetime allows a bubble.
- **Next**: Cycle D — middleware + scheduling: a `func(Handler) Handler` chain with logging and timeout middleware, `ScheduledAt` for delayed jobs, and named queues with weighted-priority fetch. Two RFC constraints this cycle must honour: the per-job **timeout** half of ADR-0003's "child context with timeout" was deferred out of #6 and belongs to this cycle's middleware chain; and the Cycle C row's shipped-shape note means named queues land here, extending the single pool rather than replacing it. The fetch predicate has been Cycle-D-shaped since AD-004 (`scheduled_at`, `queue` present from migration 001), so scheduling is a predicate widening, not a schema change.
