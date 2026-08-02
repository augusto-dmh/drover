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
| D — Middleware + scheduling | `cycle-d-middleware-scheduling` | #8 | 2026-08-02 |

## Handoff

- **Last shipped**: the middleware and scheduling cycle (#8) — a `func(Handler) Handler` chain with `Timeout` and `Logging`, `InsertOpts{Queue, ScheduledAt}` for delayed and routed enqueue, and named queues served by weighted fetch from one shared pool. `main` is green: build, vet, unit `-race`, integration, lint, vulncheck, no sqlc drift. 202 test functions, up from 149. No migration — the columns and the index covering them were already there.
- **How #8 went**: verification PASS, then a second PASS re-verifying the fixes. Six independent reviewers produced 15 distinct findings; 14 fixed, 1 declined. Every verdict and its reasoning is in `features/cycle-d-middleware-scheduling/review-triage.md`, which outlives the deleted PR comments.
- **What review caught that verification could not**: three findings where the code was correct and the *explanation attached to it* was false — a panic rationale describing a degradation Go does not have (an unrecovered panic ends the process), a README quickstart that still did not compile after a worker reported fixing it, and a "same clock" claim that ignores `now()` being transaction-scoped. None could fail a test, because nothing was wrong with the code. This is the second cycle running where the review's yield was concentrated in claims rather than logic; treat a confident rationale as the thing most needing an outside reader.
- **The one substantive bug review found**: rows claimed early in a multi-queue round were leased but untracked until the round finished, so their leases went unheartbeated across every remaining claim and a mid-round shutdown could not see them. Only the configuration this cycle introduced exposes it. Fixed by tracking at the claim.
- **Deviations to remember**: the first review orchestrator stalled twice without posting and was re-dispatched; the second completed its six passes but died on a session limit before writing its consolidation comment — the findings were all posted, so triage proceeded from those. Verification and review were both independent fresh agents, so author ≠ verifier and author ≠ reviewer held.
- **Known weak sensors** (unchanged and now doubled): neither database-clock property — lease deadlines, nor a delayed job's dueness — can be falsified while the test container and the client share a host clock. One skewed-clock container fixture would close both, and is worth building whenever either area is touched again.
- **Follow-up work carried forward** (recorded, not scheduled): batch the shutdown hand-back into one `unnest`-based statement, removing N serial round trips and the per-lease `GetJob` probe — belongs with the Cycle G fetch/perf work. Revisit the lifecycle tests for `testing/synctest` where handler lifetime allows a bubble. Reconsider the constructor contract as a whole before 1.0 — config validation panics while `NewClient` returns errors, and `Register` panics too; the inconsistency is real and the per-field fix is the wrong shape. `errShutdownRequeued`'s wording is used on hand-back paths where no shutdown occurred, including the surplus trim.
- **Next**: Cycle E — observability: Prometheus via `promhttp` on a dedicated ops port with `/healthz` and `/readyz`, oldest-job-age as the primary alerting metric, plus processed/failed counters and duration histograms. Two things this cycle set up for it: the middleware chain is where the per-job metrics belong, so E should hang a middleware on it rather than instrumenting the execution path, and `Config.Queues` means every metric now needs a queue label. Cycle E closes the v0.1.0 cut line — A through E is the release.
