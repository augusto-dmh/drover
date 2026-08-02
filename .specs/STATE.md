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

**Amended by this cycle:** AD-018 is generalised, not superseded — the heartbeat now stops after the last *pool worker* drains rather than after the fetch loop returns. Same principle, wider scope.

## Roadmap progress

| Cycle | Feature | PR | Merged |
|---|---|---|---|
| A — Walking skeleton | `cycle-a-walking-skeleton` | #1 | 2026-07-25 |
| B — Reliability core | `cycle-b-reliability-core` | #3 | 2026-07-26 |
| B — Lease ownership hardening (from #3 review) | — | #4 | 2026-07-26 |

## Handoff

- **Last shipped**: the reliability core (#3) and its ownership hardening (#4). `main` is green: build, vet, unit, integration and lint, 113 test functions.
- **How #3 went**: verification PASS after one fix iteration; review by six independent agents produced 21 findings, **none rejected** — 19 fixed in-cycle, 2 promoted to #4. The reasoning behind every verdict is in `features/cycle-b-reliability-core/review-triage.md`, which outlives the PR comments.
- **Deviations to remember**: the loop/heartbeat/rescuer/supervisor phase of #3 was written inline by the orchestrator rather than a phase worker, at the user's request — but verification and review were both independent fresh agents, so author ≠ verifier and author ≠ reviewer held. #4 skipped the independent review stage entirely at the user's direction; its only adversarial check was orchestrator-run mutation testing of both fences.
- **Known weak sensor** (worth strengthening if the area is touched again): the test asserting lease deadlines come from the database clock cannot prove skew handling, because the client and the test container share a host clock. A real sensor needs a container with a deliberately skewed clock.
- **In flight**: Cycle C — concurrency, on `feat/worker-pool-and-graceful-shutdown`. Planned as five phases (T1–T7); phases 1–4 execute inline because every one carries a correctness invariant, phase 5 (example + docs) is delegated. Decisions recorded as AD-021…AD-028.
- **Was next**: Cycle C — concurrency (per-queue worker pools, fetcher→workers channels, `Start`/`Stop` graceful shutdown). The in-flight set, heartbeat and supervisor already accept N concurrent jobs unchanged, so the pool changes who calls `add`/`remove`, not what they mean — and the ownership fence from #4 is what makes adding more concurrent workers safe.
