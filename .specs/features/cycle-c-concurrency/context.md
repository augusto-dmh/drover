# Concurrency Cycle — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's auto-decision rule. Each records the options considered with reasons for and against, the choice, and why. They are mirrored into `.specs/STATE.md` as `AD-021`–`AD-028`.

Decisions already binding on this cycle and **not** relitigated here: ADR-0003 (fixed goroutine pool over channels, not `errgroup`; the shutdown ordering), ADR-0004 (layout, stdlib-first), AD-018 (heartbeat outlives the fetch loop), AD-019 (the `attempt` fence), AD-020 (database-computed lease deadlines).

---

## D-1 — Pool topology

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) One fetch loop + `n` worker goroutines over a channel | Matches ADR-0003:28 exactly. One fetch cadence regardless of pool size, so database polling load is independent of concurrency. One place to stop claiming, which makes shutdown ordering expressible. | Requires explicit accounting of idle capacity; the fetcher can be caught holding claimed rows mid-handoff. |
| (b) `n` independent fetch loops, each claiming one job | Trivially simple; no channel, no capacity accounting; each goroutine is the single-worker loop that already exists and is already tested. | Multiplies database polling by `n` — an idle pool of 50 issues 50 queries per interval. Contradicts ADR-0003:28, which names a fetch loop feeding workers over channels. No single point at which claiming stops, so "no new job after shutdown begins" becomes `n` separate races. |
| (c) Unbounded goroutine per claimed job | No pool sizing at all; natural backpressure from the fetch limit. | "Fixed pool" is the ADR's word. Unbounded concurrency has no ceiling on database connections or handler resource use, which is the failure mode pools exist to prevent. |

**Chosen: (a).** It is the ADR's shape, and the two properties that matter for this cycle — a single point where claiming stops, and polling cost independent of pool size — both fall out of it. Option (b)'s appeal is that it reuses tested code, but it buys that by giving up the ordered shutdown that is half of this cycle's scope.

---

## D-2 — How the fetch loop decides how many jobs to claim

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) Demand-driven: claim exactly as many as there are idle workers | A claimed row is always one a worker is about to run, so no row sits claimed-but-unstarted holding a live lease. Batch size adapts to load for free: a busy pool claims few, an idle pool claims a full batch. | Needs a capacity accounting mechanism (a token channel) and a blocking acquire before each fetch round. |
| (b) Fixed batch size with a buffered channel | Fewer, larger queries; classic prefetch throughput win. | Parks claimed jobs in a buffer holding live leases. Every buffered row must be heartbeat-tracked and requeued on shutdown, and if the process dies the whole buffer waits out a full lease before anyone else can run it. Prefetching converts a reliability property into a latency optimisation. |
| (c) Always claim 1 | Simplest; today's behaviour. | Serialises claiming behind one round trip per job. With `n` workers and a fast handler, the fetch loop becomes the bottleneck the pool was meant to remove. |

**Chosen: (a).** The deciding argument is the one against (b): a claimed job that no worker is running is invisible work — it is `running` in the database, it holds a lease, and nothing is executing it. Making that state unrepresentable is worth an internal token channel. Recorded in the spec as `POOL-03`.

**Consequence carried into the design:** the fetch loop can still be caught holding claimed rows it has not yet handed over when shutdown begins (it claims a batch, then dispatches one at a time). Those rows must be requeued by the fetcher itself — spec `SHUT-05`.

---

## D-3 — `Concurrency` configuration and default

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) `Concurrency int`, default 10 | One flat knob matching the one queue this cycle supports. A queue library should not default to serial execution. Drover holds a database connection only across the claim and the finalize — never for the handler's duration — so a concurrency well above `pgxpool`'s connection count costs only brief contention at those two points. | 10 is a judgement call, not a derived number. Changes the effective default behaviour of existing code from serial to concurrent. |
| (b) `Concurrency int`, default 1 | Preserves today's semantics exactly; no behaviour change for any existing caller. | Makes the cycle's entire deliverable opt-in, and leaves the out-of-the-box experience the bottleneck this cycle exists to remove. |
| (c) Default `runtime.NumCPU()` | Self-scaling; conventional for CPU-bound pools. | Job queues are overwhelmingly IO-bound — the handler waits on SMTP, HTTP, another database. Core count is an unrelated quantity, and tying to it makes the default vary between a developer laptop and production for no principled reason. |
| (d) `Queues map[string]QueueConfig` now | Would not need changing when Cycle D lands named queues. | Ships queue configuration this cycle, which the ship-cycle brief explicitly reserves for Cycle D. Configuring a map with exactly one legal key is a worse API than a scalar until the second key exists. |

**Chosen: (a), default 10.** Rejecting (b) is the substantive call: pre-1.0 with no released version, the cost of changing the default is zero outside this repo, and shipping the feature switched off would be a strange way to ship it. `Concurrency <= 0` takes the default rather than erroring, consistent with how every other `Config` field already behaves.

**Forward compatibility with Cycle D:** the pool is constructed with `(queue, concurrency)` internally, so Cycle D adds a second pool rather than restructuring the first.

---

## D-4 — `Start` / `Stop` shape

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) `Start(ctx) error` returns once running; `Stop(ctx) error` blocks until drained | The pair RFC-0001 names. Lets the caller hold the client, serve traffic, and shut down on its own signal — the shape every comparable Go queue uses. `Stop` can take a budget and return a verdict, which a blocking `Start` has nowhere to put. | Changes `Start`'s contract: existing callers that treat `Start(ctx)` as "run until cancelled" would fall through immediately. `example_test.go` is such a caller and must be updated. |
| (b) `Start(ctx) error` keeps blocking; add `Stop(ctx) error` called from elsewhere | No contract change. | The caller must run `Start` in a goroutine to be able to call `Stop`, and then two goroutines both have a shutdown verdict — `Start`'s return and `Stop`'s return — with no defined relationship between them. Awkward, and the drain error has two possible homes. |
| (c) Blocking `Start` only, no `Stop`; shutdown stays context-driven | No new API. | Cannot bound the drain, cannot report what failed to drain, cannot requeue on a schedule the caller controls. Fails four of the cycle's acceptance criteria outright. |

**Chosen: (a).** The contract change is real but confined: the module has no tagged release, and the only in-repo caller is the package example, which is updated as part of the same task. Cancelling the context passed to `Start` still begins shutdown with the same ordering (spec `SHUT-01` AC10), so the ergonomic that existing code relies on survives — what changes is that `Stop` is how you wait for it.

`Start` on an already-running client and `Stop` on a never-started client both return errors rather than silently no-op'ing: a lifecycle misuse is a programming error, and reporting it costs one sentinel each.

---

## D-5 — The drain budget and what happens when it runs out

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) Budget from `Stop`'s `ctx`; on expiry cancel per-job contexts, requeue, return an error naming the unfinished count | The caller already has a shutdown budget — the deadline it was given by its own supervisor — and no second knob competes with it. Matches ADR-0003:29's ordering literally. | A `Stop(context.Background())` waits forever, which a careless caller may not expect. Documented. |
| (b) A `ShutdownTimeout` field on `Config` | Discoverable; one place to set it for every call site. | Competes with the caller's context deadline, and when the two disagree there is no defensible winner. Adds a knob for something the standard library already models. |
| (c) After cancelling per-job contexts, wait a second grace period before requeuing | Gives cancellation-respecting handlers a chance to unwind and record their own outcome, which is tidier than requeuing work that was about to finish. | Requires a second budget nobody specified, and it would be spent after the caller's deadline has already passed — i.e. exactly when the caller asked to stop waiting. |

**Chosen: (a).** Notably, `Stop` still performs the full ordered shutdown even when its context is *already* done on entry — it stops fetching and requeues in-flight work before returning the error (spec edge case). Returning early on a dead context would leave rows `running` with no requeue, which is precisely the outcome the requeue exists to avoid.

---

## D-6 — How shutdown returns an unfinished job to the queue

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) `MarkRetryable(lease, now, detail)` | Already exists — no driver interface change, no new SQL, no `sqlc` regeneration. Returns the row to a claimable state at `now`, leaves `attempt` untouched, and appends the reason to the job's error history where an operator will find it. | Consumes the attempt: a shutdown costs the job one of its 25 tries. Records an infrastructure event in a column named `errors`. |
| (b) A new `MarkRequeued(lease)` driver method | Clean semantics and a dedicated log line; could reset state to `available` with no error entry. | A driver-interface change plus new SQL plus a `memdriver` implementation plus `sqlc` regeneration, for a behaviour option (a) already provides. CI has an `sqlc-drift` job, so the generated diff must be committed too. |
| (c) `MarkSnoozed(lease, now)` | Semantically "a deferral, not a failure" (AD-010), and gives back the attempt the claim consumed (AD-011), so a shutdown costs the job nothing. | **Unsafe here.** `MarkSnoozed` decrements `attempt`. Snooze is safe today only because the handler has already returned when it is called — nobody else is running. At shutdown escalation the handler may still be executing, having ignored its cancelled context. Decrementing `attempt` means the next claim increments it back to the same number the still-running handler holds, so that handler's late write would be **accepted** by the fence. It would silently defeat AD-019 in exactly the scenario the fence exists for. |

**Chosen: (a).** Option (c) is the trap in this decision and is rejected on correctness, not taste. The attempt cost that (a) carries is the safe direction and has a precedent: AD-012 already holds that a rescued job does not get its attempt back because "the attempt that stranded the row was really spent" — an attempt cut short by shutdown was spent the same way. The requeue is written at `now` with no retry backoff, because the job did not fail; it was interrupted, and it should be claimable immediately.

---

## D-7 — Per-job context and where finalization runs

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) Handler runs on a cancellable pool-scoped job context; finalization runs on a separate uncancellable context with a bounded timeout | Cancellation actually reaches handlers, which is what makes escalation meaningful. A cancelled job can still record its own outcome, so a handler that respects cancellation finalizes itself instead of being requeued. | Two contexts in `runJob` instead of one; the distinction must be documented or a later change will collapse them. |
| (b) Keep `context.WithoutCancel` for everything, as today | No change; no risk of a cancelled context breaking a database write. | Escalation becomes a lie: `Stop` would claim to cancel per-job contexts while handlers never observe cancellation. ADR-0003:29 requires the cancel step to be real. |
| (c) One cancellable context for handler and finalization alike | Simplest to write. | **Actively harmful.** Cancelling the job would also cancel the write that records its outcome, so every escalated job would fail to finalize and sit `running` until its lease lapsed — converting a clean shutdown into the crash path for every affected row. |

**Chosen: (a).** Option (c) is the subtle failure this decision exists to prevent, and it is the shape a careless refactor lands on. The rule stated for implementers: **the handler's context may be cancelled; the context used to record an outcome never is.**

---

## D-8 — The example application

**Options**

| Option | For | Against |
| ------ | --- | ------- |
| (a) `examples/email/` — a `package main` program | Conventional Go placement, plainly not part of the library's API. Compiles under `go build ./...` and is vetted by `go vet ./...`, so it cannot silently rot. | Adds a directory the ADR-0004 layout does not name (it names root + `internal/` + `cmd/`). |
| (b) `cmd/drover-email-example/` | Uses a directory the layout already sanctions. | `cmd/` is where the shipped `drover` CLI lives (Cycle F). Putting a demo beside a released binary invites it to be built and distributed as one. |
| (c) A testable `Example` function in package docs only | Zero new directories; appears in godoc. | Cannot demonstrate SIGINT handling, a pool under load, or observable retries — it cannot be run. The RFC asks for an example *app*. |

**Chosen: (a).** ADR-0004's layout rule exists to prevent a `pkg/` sprawl and to keep the library single-module; a non-shipped `examples/` tree contradicts neither. The RFC's locked decision fixes the domain — an email/notification pipeline with a flaky delivery stub, zero heavy dependencies — so the program uses an in-process stub that fails on a documented, deterministic pattern rather than any real SMTP client. The stub's failure pattern is unit-tested so `EX-01`'s retry claim has a sensor.
