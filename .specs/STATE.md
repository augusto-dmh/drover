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
| AD-038 | Per-execution metrics are recorded by a middleware the client installs immediately inside `Logging` and ahead of `Config.Middleware`, so metrics cannot be silently disabled and the chain sees panics and unregistered kinds that never reach a registered worker | cycle-e `context.md` D-1 |
| AD-039 | Database-derived gauges are refreshed by a background goroutine on `Config.StatsInterval`, not queried on scrape — database load is O(1) in scraper count, the RFC row's stated constraint | cycle-e D-2 |
| AD-040 | Gauge queries are two SQL statements behind one `driver.Stats` method — one grouped count by `(queue, state)`, one oldest-claimable age per queue — rather than one clever statement whose row shape has to be explained every review | cycle-e D-3 |
| AD-041 | Queue statistics go through one new `Stats(ctx)` on the unexported `driver.Driver` interface, implemented by both `pgdriver` and `memdriver`, so the unit suite exercises gauge logic without Docker | cycle-e D-4 |
| AD-042 | `Config.MetricsRegistry *prometheus.Registry`; nil creates a fresh private registry per client so two clients in one process cannot collide on registration | cycle-e D-5 |
| AD-043 | The ops listener binds eagerly in `Start` before any goroutine launches; a bind failure returns an error and starts nothing — a worker running unobservably is worse than failing at boot | cycle-e D-6 |
| AD-044 | `/readyz` is ready when the client is started and the last gauge refresh succeeded within twice `StatsInterval`; the check is a memory read, so probe rate adds no database load. The refresher runs once immediately at start | cycle-e D-7 |
| AD-045 | `drover_jobs_executing` is incremented and decremented by the metrics middleware around each execution, not read from `inflightSet` — inflight means leased, a larger set than executing | cycle-e D-8 |
| AD-046 | Metric names use the `drover_` namespace with explicit histogram buckets from 5ms to 10 minutes; job metrics carry a `queue` label only; depth additionally carries `state`; pool gauges are unlabelled | cycle-e D-9 |
| AD-047 | The ops server shuts down last, after workers drain and after the heartbeat and refresher stop, so `/metrics` and `/readyz` remain answerable throughout the drain | cycle-e D-10 |
| AD-048 | `drover_queue_depth` counts only `available`, `scheduled`, `retryable`, `running`, and `dead`; `completed` and `cancelled` are deliberately absent, and migration 003 indexes `dead` so the refresher's cost stays proportional to backlog plus retained dead rows, not completed-job history | cycle-e D-11 |
| AD-049 | CLI uses stdlib `flag` + a small subcommand dispatcher in `cmd/drover/` — not cobra | cycle-f `context.md` D-1 |
| AD-050 | Operator introspection is an exported `Inspector` constructed from `*pgxpool.Pool`, not methods on `Client` | cycle-f D-2 |
| AD-051 | List/Get/OperatorCancel/RedriveDead extend the unexported `driver.Driver` (both adapters), reusing `Stats` and `Insert` | cycle-f D-3 |
| AD-052 | Operator cancel/redrive use state-conditioned UPDATEs without the lease attempt fence; `running` jobs cannot be cancelled this way | cycle-f D-4 |
| AD-053 | Redrive moves `dead` → `available` with `attempt=0`, cleared lease, `scheduled_at=now()`, retaining `errors` | cycle-f D-5 |
| AD-054 | CLI output is human text by default with a global `--json` flag | cycle-f D-6 |
| AD-055 | Database URL from `--database` or `DATABASE_URL`; no YAML config this cycle | cycle-f D-7 |
| AD-056 | CLI enqueue takes `--kind`, optional `--queue`, and `--args` as a JSON object string | cycle-f D-8 |
| AD-057 | GoReleaser ships multi-OS/arch archives + checksums with version ldflags; no Homebrew tap or Docker image this cycle | cycle-f D-9 |
| AD-058 | Batch enqueue is `InsertMany`/`InsertManyTx` taking `[]InsertItem` (per-item `Args` + `*InsertOpts`) | cycle-g `context.md` D-1 |
| AD-059 | Postgres batch insert stages via session-temp table + `COPY FROM` + `INSERT … SELECT … RETURNING`, reusing InsertJob's database-clock CASE | cycle-g D-2 |
| AD-060 | `Config.NotifyWakeup` is opt-in (default false); polling remains the source of truth | cycle-g D-3 |
| AD-061 | When the flag is set, the inserting Client emits one `pg_notify('drover','')` per successful Insert/InsertMany; Tx variants emit inside the caller transaction | cycle-g D-4 |
| AD-062 | Idle fetch is woken by a capacity-1 client channel (local insert) plus an optional pgdriver LISTEN goroutine (other processes). LISTEN is not on `driver.Driver` | cycle-g D-5 |
| AD-063 | LISTEN failure logs and degrades to poll; `Start` still succeeds. PgBouncer transaction pooling is incompatible with the flag | cycle-g D-6 |
| AD-064 | Claim batch size stays idle-worker count (AD-022). Cycle G does not add a prefetch or FetchBatchSize knob | cycle-g D-7 |
| AD-065 | `cmd/drover-bench` measures enqueue throughput and drain latency percentiles and prints methodology; it is not a GoReleaser artifact | cycle-g D-8 |
| AD-066 | Empty InsertMany is success with no write; `memdriver.InsertManyTx` returns `ErrTxUnsupported` | cycle-g D-9 |
| AD-067 | Unique jobs: `InsertOpts.UniqueKey` (empty = not unique, stored NULL). Partial unique index on `(queue, kind, unique_key)` WHERE `unique_key IS NOT NULL` AND `state IN (available, scheduled, retryable, running)` | cycle-h `context.md` D-1, D-2 |
| AD-068 | Duplicate unique insert returns `ErrDuplicateJob` and inserts no row; `InsertMany` fails the whole batch | cycle-h D-3 |
| AD-069 | Cron parser is stdlib-owned 5-field + `@every <duration>` in `internal/cron` (or equivalent); no third-party cron module; fuzz the parser | cycle-h D-4 |
| AD-070 | Periodic jobs are `Config.PeriodicJobs []PeriodicJob` at construction; empty/duplicate ID, nil Args, or bad cron panics; a process with an empty slice does not take the lock | cycle-h D-5 |
| AD-071 | Leadership is session `pg_try_advisory_lock` on a dedicated connection and a documented int64 key, off `driver.Driver` (optional interface). memdriver is always leader. Lock failure logs and retries; `Start` still succeeds | cycle-h D-6, D-7 |
| AD-072 | Periodic enqueue UniqueKey is `id + "/" + fireTime.UTC().Format(time.RFC3339Nano)`; `ErrDuplicateJob` is tick success. RFC3339 (seconds) collapsed sub-second `@every` ticks | cycle-h D-8, amended review of #15 |
| AD-073 | First periodic fire is strictly after leadership gain; ticks missed while not leader are skipped; `@every` is Unix-epoch aligned | cycle-h D-8, amended on review of unique-key catch-up |
| AD-074 | Leader computes next run with `time.Now()` in the job location; Insert still uses the database clock for available vs scheduled (AD-035) | cycle-h D-9 |
| AD-075 | Scheduler shares `fetchCtx` with the rescuer: it stops when claiming stops; Stop must not wait out the next fire | cycle-h D-10 |
| AD-076 | `JobRow.UniqueKey` is exported (empty when unset) so handlers can use it downstream | cycle-h ASM-14 |
| AD-077 | Status UI lives in `cmd/drover` via `embed.FS` and talks to `Inspector`, not `Client` or the ops port | cycle-i `context.md` D-1 |
| AD-078 | `drover web --listen` defaults to `127.0.0.1:7180`; bind failure exits 1 | cycle-i D-2 |
| AD-079 | No application auth on the status page; POST mutations require Origin (else Referer) same-host as `r.Host` | cycle-i D-3 |
| AD-080 | Auto-refresh is `<meta http-equiv="refresh">` (`--refresh`, default 5s, 0 disables, non-zero ≥ 1s), not JavaScript | cycle-i D-4 |
| AD-081 | Retry/cancel are POST + 303 PRG with coded flash; no enqueue from the page | cycle-i D-5 |
| AD-082 | One GET `/` page; omitted `state` lists `dead`; `state=all` is unfiltered; `queue`/`limit` match the CLI | cycle-i D-6 |
| AD-083 | `drover web --json` is a usage error | cycle-i D-7 |

**Amended by cycle C:** AD-018 is generalised, not superseded — the heartbeat now stops after the last *pool worker* drains rather than after the fetch loop returns. Same principle, wider scope.

**Corrected by cycle D:** a `pool.go` comment from cycle C guessed that "a second queue is a second runner rather than a rewrite of this one". That was a forward guess, not a recorded decision, and AD-036 supersedes it: a second queue is another entry in the weighted set served by the same pool.

## Roadmap progress

| Cycle | Feature | PR | Merged |
|---|---|---|---|
| A — Walking skeleton | `cycle-a-walking-skeleton` | #1 | 2026-07-25 |
| B — Reliability core | `cycle-b-reliability-core` | #3 | 2026-07-26 |
| B — Lease ownership hardening (from #3 review) | — | #4 | 2026-07-26 |
| C — Concurrency | `cycle-c-concurrency` | #6 | 2026-08-02 |
| D — Middleware + scheduling | `cycle-d-middleware-scheduling` | #8 | 2026-08-02 |
| E — Observability | `cycle-e-observability` | #9 | 2026-08-08 |
| F — CLI + introspection | `cycle-f-cli-introspection` | #11 | 2026-08-08 |
| G — Benchmark + hardening | `cycle-g-benchmark-hardening` | #13 | 2026-08-28 |
| H — Periodic jobs | `cycle-h-periodic-jobs` | #15 | 2026-09-04 |
| H — Scheduler failover hardening (from #15 review) | — | #16 | 2026-09-04 |
| I — Status page | `cycle-i-status-page` | #18 | 2026-09-04 |

## Handoff

- **Feature**: RFC-0001 roadmap table is complete (A–I)
- **Phase / Task**: none
- **Last shipped**: Status page (`drover web`) #18
- **Completed last cycle**: `drover web` — one server-rendered auto-refreshing page; retry/cancel; loopback default; Origin/Referer CSRF
- **Next step**: no further RFC cycle; pick from follow-up work if continuing
- **Blockers**: none
- **Branch**: `main`
- **What review should look at**: n/a
- **Known weak sensors** (carried forward): neither database-clock property — lease deadlines, nor a delayed job's dueness — can be falsified while the test container and the client share a host clock
- **Follow-up work carried forward** (recorded, not scheduled): terminal-state retention/pruning; list-query index-friendly variants; `testing/synctest` where viable; constructor panic-vs-error consistency before 1.0; deferred observability niceties from cycle E; batch completer; `go test -bench` CI job; UniqueOpts.ByArgs/Period; RunOnStart; CLI `--unique-key`
- **Next after this cycle**: none — RFC-0001 ends at the status page
