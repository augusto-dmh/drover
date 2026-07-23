# RQ01 — Existing Go task-queue systems

Survey of the Go task-queue landscape (July 2026) for drover: what each system does, how it does it, what users think of it, and where a small, educational-but-production-quality queue can be defensibly different.

## 1. System-by-system mechanics

### 1.1 River (riverqueue/river) — Postgres-native

- **Storage / claim:** Single Postgres table (`river_job`). Jobs are claimed with `SELECT ... FOR UPDATE SKIP LOCKED` batch queries. A per-process **producer** fetches and locks batches of jobs on behalf of many in-process goroutine executors, minimizing lock contention and round trips (batch inserts use `COPY FROM`, driver is pgx binary protocol). `LISTEN/NOTIFY` wakes producers so new work starts near-instantly, with polling as fallback (NOTIFY is known to be incompatible with pgbouncer transaction pooling — a limitation Oban hit and that HN commenters flagged).
- **Delivery semantics:** at-least-once; the headline feature is **transactional enqueueing** — a job inserted in the same DB transaction as business data is guaranteed enqueued iff the transaction commits, invisible until commit. This removes the classic "row not committed yet when worker picks up job" and "job enqueued but transaction rolled back" races.
- **In-flight loss:** jobs stuck in `running` past a threshold are recovered by a background **rescuer** service; companion maintenance services include a scheduler (moves scheduled/retryable jobs to available), job cleaner (prunes completed/cancelled/discarded), and reindexer.
- **Retry/DLQ:** configurable retry policy with backoff (default polynomial-ish curve, customizable per worker via `NextRetry`); after max attempts the job goes to a `discarded` state kept in the table (the DLQ is just rows you can query and retry).
- **Uniqueness:** unique jobs by args, period, queue, and state (states considered for uniqueness are configurable); implemented in-database so it is actually race-free.
- **Scheduling:** `ScheduledAt` for future jobs, periodic/cron jobs built in. Pro adds sequences (per-key strict ordering), workflows (DAGs of dependent jobs, with durable signals/timers in 2026's Workflows V2), and concurrency limits.
- **Worker/client API:** Go generics. `type SortArgs struct{...}` implementing `river.JobArgs` (with `Kind() string`), `river.Worker[SortArgs]` with `Work(ctx, *river.Job[SortArgs])`, registered via `river.AddWorker`. No manual JSON unmarshalling; insert and work sides share the args type.
- **Observability:** job rows are queryable SQL; subscription hooks for telemetry; River UI (separate project) and paid Pro tier.
- **Status:** very actively maintained by Blake Gentry & Brandur Leach; MIT-licensed core (launch-era LGPL concern was resolved); commercial Pro. Roughly 4–5k stars (unverified estimate — GitHub star count not directly captured). ~10k trivial jobs/sec on a laptop per author benchmark.

### 1.2 Asynq (hibiken/asynq) — Redis

- **Storage / claim:** Redis. Pending tasks live in Redis **lists**, time-based sets (scheduled, retry) in **sorted sets**. Dequeue is a **Lua script** that checks the queue's paused flag, `RPOPLPUSH`es from pending list to the active list, marks the task active, and writes a **lease** expiration into a lease sorted set. The processor loop effectively polls (short sleep when all queues are empty) across queues honoring weighted or strict priority.
- **Delivery semantics:** at-least-once, explicitly documented.
- **In-flight loss:** lease-based. Server heartbeats periodically extend leases for all tasks its workers hold; a **recoverer** scans for lease-expired tasks (with ~30s clock-skew allowance) and retries or archives them with `ErrLeaseExpired`. This is a visibility-timeout design with active heartbeat extension — closer to Sidekiq Pro's super_fetch than to plain BRPOP.
- **Retry/DLQ:** default 25 retries with exponential backoff (customizable `RetryDelayFunc`); exhausted tasks move to the **archived** state for manual inspection/re-run. `SkipRetry` error to fail fast.
- **Uniqueness:** `TaskID` option (explicit idempotency key — enqueue fails with `ErrTaskIDConflict`) and `Unique(ttl)` option (uniqueness lock derived from queue+type+payload held for TTL). Best-effort relative to River's DB-enforced uniqueness.
- **Scheduling:** `ProcessAt`/`ProcessIn`, plus a separate `Scheduler` for cron-style periodic tasks; task **aggregation/grouping** (batch many small tasks into one), timeouts and deadlines per task, Redis Cluster and Sentinel support (with the caveat that some Lua scripts may not be Cluster-compatible).
- **Worker/client API:** payload is `[]byte` (`asynq.NewTask("email:deliver", payloadBytes)`); handlers implement `ProcessTask(ctx, *asynq.Task) error` and are registered on a `ServeMux` by task type string, mirroring `net/http`. Users must define and unmarshal their own payload structs — typical duplication between enqueue and handler sides.
- **Observability:** best-in-class for OSS Go queues — Asynqmon web UI, `asynq` CLI, `Inspector` API, Prometheus metrics.
- **Status:** 13.5k stars, MIT, v0.26.0 (Feb 2026), 218 open issues. Still v0.x ("public API could change"); maintenance is real but historically bursty — there were visible lulls (~2022–2023) that pushed some users toward River; open bug reports exist about tasks stalling (e.g. issue #907) and pubsub reconnects (#801).

### 1.3 Faktory (contribsys/faktory) — language-agnostic server

- **Storage / claim:** standalone **server** (by Mike Perham, Sidekiq's author) with an embedded Redis as the storage engine (replaced RocksDB at 1.0 — widely documented; treat as verified-by-reputation, primary blog post not fetched here). Workers in any language speak a simple text protocol: `FETCH` (blocking, over queues in priority order), then `ACK` or `FAIL`. Jobs are JSON payloads (`jid`, `jobtype`, `args`).
- **Delivery semantics / in-flight loss:** at-least-once via **reservation timeout** (`reserve_for`, default 30 min): a fetched job stays in WORKING state until ACK/FAIL; if neither arrives before the reservation expires, the server requeues it. This is a classic visibility-timeout model enforced server-side.
- **Retry/DLQ:** default 25 retries with exponential backoff (~21 days), then a Dead set with manual retry via the web UI — Sidekiq semantics, verbatim.
- **Uniqueness:** not in OSS; unique jobs, batches, cron, queue throttling are **Faktory Enterprise** (paid) features.
- **Scheduling:** scheduled (`at`) jobs in OSS; cron in Enterprise.
- **Worker/client API:** per-language worker libs (Go: `faktory_worker_go`) registering `jobtype -> func(ctx, args ...interface{})`; args arrive as decoded JSON `[]interface{}` — the loosest typing of the group.
- **Observability:** built-in web UI (Sidekiq-style dashboard: queues, retries, dead, busy).
- **Status:** 6.1k stars; v1.9.4 (Feb 2026); actively maintained; AGPL-ish dual licensing (LICENSE + COMM-LICENSE) with paid Enterprise.

### 1.4 Machinery (RichardKnop/machinery)

- **Storage / claim:** broker-agnostic, Celery-style: RabbitMQ (AMQP consume/ack), Redis (list ops), SQS (its own visibility timeout), GCP Pub/Sub; separate **result backend** (Redis, MongoDB, Memcache, DynamoDB) to store task states and return values.
- **Delivery semantics / in-flight loss:** delegated to the broker (AMQP acks / SQS visibility timeout / Redis list handling); no unified lease/rescue abstraction of its own.
- **Retry/DLQ:** configurable retry count with **Fibonacci** backoff; task states PENDING→RECEIVED→STARTED→RETRY→SUCCESS/FAILURE tracked in the result backend; error callbacks rather than a first-class DLQ.
- **Uniqueness:** none built in.
- **Scheduling / workflows:** its differentiator — **groups** (parallel), **chains** (sequential, result forwarding), **chords** (group + callback), periodic tasks via cron.
- **Worker/client API:** tasks are registered by name as functions with reflection-based signatures; args are serialized as typed `{Type, Value}` tuples in JSON — verbose, reflection-heavy, `interface{}`-laden; the least idiomatic API of the group by modern standards.
- **Observability:** minimal (no UI; OpenTracing hooks).
- **Status:** ~8k stars; v2 is the recommended line, v2.0.16 (Aug 2025); 220 open issues, many stale — community reviews consistently describe it as drifting into maintenance mode and criticize the single-default-queue limitation (issue #71) and weak priority support. Its main historical role: the Celery-shaped option for Go.

### 1.5 Briefer: gocraft/work, neoq, gue

- **gocraft/work** (Redis, 2.5k stars): `LPUSH` to per-jobtype lists; Lua script atomically moves job to a per-pool **in-progress** list; a **reaper** watches worker-pool heartbeats and requeues in-progress jobs of dead pools. Retries via a retry zset with timestamps, then a dead queue after `MaxFails`. Unique jobs via marker keys. Cron-style periodic jobs. API: custom context struct + middleware chain (very 2016 web-framework flavored). **Effectively unmaintained — last release June 2018.** Historically important; do not imitate.
- **neoq** (acaloiaro, 335 stars): queue-**agnostic** library — in-memory, Postgres, Redis backends behind one handler API (handlers are `func(ctx) error` pulling job from context). Postgres backend grew out of the widely-cited "Choose Postgres queue technology" essay. Retries with exponential backoff + jitter, per-job timeouts/deadlines, cron, uniqueness by payload **fingerprinting**. Pre-1.0, one-maintainer project; repo moving off GitHub. Lesson: the backend-agnostic abstraction costs precision — you can't promise transactional enqueue through an interface that must also fit Redis and memory.
- **gue** (vgarvardt, 315 stars, v6.0.0 Oct 2025): fork of bgentry/que-go (itself a port of Ruby's Que). Postgres-only, claims jobs with transaction-level locks (que heritage is advisory locks; v2 rewrote the schema/logic — exact current locking not verified from source). `WorkMap` of type→handler funcs, worker pools, backoff, exec-frequency polling. Solid, small, maintained-by-one-person; no UI, no uniqueness.

### 1.6 The Sidekiq / Laravel Horizon shadow

Expectations in this space were set by Sidekiq and, for the PHP world, Laravel Horizon:

- **Sidekiq** defined the default semantics everyone copies: 25 retries, exponential backoff `(retry_count^4 + 15 + jitter)` spread over ~20 days, then a capped **Dead set** (10k jobs / 6 months) with manual retry; a polished web dashboard; and the reliability ladder — OSS `BRPOP` (job lost if worker crashes) vs Pro `super_fetch` (`LMOVE` to a working list + recovery, with poison-pill detection after 3 recoveries in 72h). Faktory is literally Sidekiq-as-a-server; asynq's archive and faktory's retry curve are direct copies.
- **Laravel Horizon** set the *operator UX* bar: supervisor-managed worker pools with **auto-balancing** across queues, per-queue wait-time metrics, throughput/runtime charts, failed-job inspection with stack traces and retry buttons, tags for finding a specific job. Multiple HN commenters on the River thread explicitly asked for "a UI like Hangfire/Horizon" — observability is the most-missed feature in Go queues, and Asynqmon's popularity confirms it.

## 2. Comparison table

| | River | Asynq | Faktory | Machinery | gocraft/work | neoq | gue |
|---|---|---|---|---|---|---|---|
| Backend | Postgres | Redis | Server + embedded Redis | RabbitMQ/Redis/SQS/PubSub | Redis | memory/PG/Redis | Postgres |
| Claim | `FOR UPDATE SKIP LOCKED` batch + LISTEN/NOTIFY wake | Lua `RPOPLPUSH` + poll loop | blocking `FETCH` protocol | broker-native consume | Lua pop→in-progress list | backend-dependent | tx-level locks + poll |
| In-flight loss | rescuer sweeps stuck `running` rows | lease + heartbeat + recoverer | reservation timeout (30 min default) | broker acks / visibility timeout | heartbeat reaper | backend-dependent | tx rollback returns job |
| Retry → DLQ | policy fn → `discarded` rows | 25× exp backoff → `archived` | 25× exp backoff → Dead set | Fibonacci backoff, no real DLQ | retry zset → dead queue | exp backoff + jitter | backoff, error hook |
| Unique/idempotency | by args/period/queue/state, DB-enforced | `TaskID` + `Unique(ttl)` | Enterprise only | none | unique-key markers | payload fingerprint | none |
| Transactional enqueue | **yes** (flagship) | no | no | no | no | PG backend only | yes (same tx) |
| Cron/scheduled | yes + Pro workflows/sequences | yes + groups/aggregation | scheduled OSS, cron paid | yes + chains/groups/chords | yes | yes | no cron |
| Typed API | generics `Worker[T]` | `[]byte` + mux | `[]interface{}` args | reflection signatures | context struct + middleware | handler + ctx | `WorkMap` |
| UI / metrics | River UI, Pro | Asynqmon + CLI + Prometheus | built-in web UI | none | none | none | none |
| Health (2026) | very active, commercial | active, v0.x, bursty history | active, commercial | semi-maintained | dead (2018) | one-person, pre-1.0 | one-person, fine |
| Stars (approx) | ~4–5k (unverified) | 13.5k | 6.1k | ~8k | 2.5k | 335 | 315 |

## 3. Client-API verdicts (what users praise/complain about)

- **River's typed-generics API is the consensus best-in-class.** The HN thread and subsequent coverage repeatedly single out `JobArgs` + `Worker[T]`: no hand-rolled JSON unmarshalling, enqueue and handler share one type, compile-time checking. Complaints about River are not about the API: launch-time LGPL licensing ambiguity (since resolved), the Oban-attribution controversy, LISTEN/NOTIFY vs pgbouncer, Postgres bloat/vacuum tuning under heavy churn, missing "await job result" primitive, and the general "don't use your DB as a queue" orthodoxy (which defenders counter with "most systems never outgrow it").
- **Asynq's `[]byte` payload + string-keyed ServeMux** is praised for familiarity (net/http feel) and simplicity, but the costs show up in user code: every task type needs a payload struct, a constructor that marshals, and a handler that unmarshals — with type errors surfacing at runtime. Praise centers on Asynqmon/CLI/Prometheus and the feature set (priorities, groups, uniqueness). Complaints: perpetual v0.x, maintenance lulls, Redis Cluster Lua caveats, occasional stuck-task bug reports, and no transactional coupling with application data.
- **Faktory's dynamic `[]interface{}` args** are the price of language-agnosticism — fine for polyglot shops, unloved by Go users. **Machinery's** reflection-based signatures are the most criticized API of the group. **gocraft/work's** middleware+context style has aged out.
- Takeaway for drover: **typed generics on the enqueue AND worker side is the modern expectation**; a `[]byte` escape hatch underneath is fine (River itself stores JSON), but the public API should be typed.

## 4. Gap analysis — where drover can be defensible

There is no room to out-feature River or out-star Asynq. The defensible position is a **small, transparent, single-backend queue whose every mechanism you can explain and prove** — a project built from first principles, small enough to fully understand, that still holds production-quality invariants.

The concrete gap: River is now a big commercial-grade codebase (plus Pro); asynq is big and Redis-tied; the small Postgres options (gue, neoq) are one-person projects without typed APIs, uniqueness (gue), UIs, or crisp delivery-semantics documentation. **A ~3–5k LOC Postgres queue with River-grade semantics, a typed generics API, and first-class observability docs/tests does not exist.**

Documented, defensible tradeoff statements this enables:

1. "I chose **Postgres + SKIP LOCKED over Redis** because transactional enqueue eliminates the outbox problem asynq users have to solve themselves; I accept lower ceiling throughput, and I can quote why (~10k/s is River's laptop number, far beyond most apps' needs)."
2. "I use **polling with jittered intervals instead of LISTEN/NOTIFY** (or NOTIFY as an optional accelerator) because NOTIFY breaks under pgbouncer transaction pooling — Oban removed it for exactly this reason."
3. "In-flight recovery is a **heartbeat + rescuer sweep**, not per-job visibility timeouts, because with SKIP LOCKED the claim is a DB row-state, not a lock held by a connection — and I can explain the crash matrix for each state."
4. "Retry semantics copy **Sidekiq's proven curve** (exp backoff + capped dead set) rather than inventing new defaults — defaults are a compatibility surface."
5. "Uniqueness is **DB-enforced (unique partial index on args-hash + state)**, not a Redis TTL lock, so it can't race — a correctness difference vs asynq I can demonstrate with a test."

## Options

Design directions that emerged for drover:

- **Option A — Postgres-native, single-binary library, typed generics API** ★ RECOMMENDED. SKIP LOCKED claims, transactional enqueue, heartbeat+rescuer recovery, Sidekiq-style retry/dead-set, DB-enforced uniqueness, Prometheus metrics + a minimal Horizon-inspired dashboard (even read-only) as the differentiator vs gue/neoq. Directly exercises Go generics, database craft, and concurrency, while every design choice has a citable contrast with River/asynq.
- **Option B — Redis-backed asynq-alike with Lua claim scripts.** Teaches Lua/Redis atomicity and lease design, but lands in asynq's shadow with weaker correctness stories (no transactional enqueue) and a harder time being "different, not worse".
- **Option C — Language-agnostic server (mini-Faktory).** Most impressive systems-design scope (protocol design, server state machine), but doubles the surface area (server + client libs), and delivery semantics end up re-implementing Faktory with fewer guarantees. Too large for drover's intended scope.
- **Option D — Backend-agnostic library (neoq path).** Rejected: the abstraction forbids the strongest guarantee (transactional enqueue) and neoq demonstrates the resulting muddiness.

## Sources

- https://brandur.org/river (accessed 2026-07-22) — River design deep-dive: SKIP LOCKED, producer model, generics API, maintenance services, ~10k jobs/s figure.
- https://github.com/riverqueue/river (accessed 2026-07-22)
- https://riverqueue.com/blog/announcing-river (accessed 2026-07-22)
- https://news.ycombinator.com/item?id=38349716 (accessed 2026-07-22) — HN thread: praise/criticism, Oban attribution, pgbouncer/NOTIFY, licensing, Horizon-style UI requests.
- https://riverqueue.com/docs/pro and https://riverqueue.com/docs/pro/workflows and https://riverqueue.com/docs/pro/sequences (accessed 2026-07-22) — Pro features incl. Workflows V2 (2026).
- https://github.com/hibiken/asynq (accessed 2026-07-22) — 13.5k stars, v0.26.0 Feb 2026, feature list, at-least-once, Cluster Lua caveat.
- https://github.com/hibiken/asynq/wiki/Life-of-a-Task (accessed 2026-07-22) — task states.
- https://github.com/hibiken/asynq/wiki/Task-Retry (accessed 2026-07-22, via search snippet) — 25 retries, exponential backoff, archive.
- https://github.com/hibiken/asynq/blob/master/recoverer.go and https://github.com/hibiken/asynq/blob/master/heartbeat.go (accessed 2026-07-22, via search snippets) — lease extension by heartbeat, recoverer with 30s clock-skew allowance.
- https://pandaychen.github.io/2021/08/18/A-GOLANG-ASYNQ-ANALYSIS/ and https://blog.whichxjy.com/asynq-task-queue/ (accessed 2026-07-22, via search snippets) — dequeue Lua script mechanics (paused check, RPOPLPUSH, lease zset).
- https://github.com/hibiken/asynq/issues/907 and https://github.com/hibiken/asynq/issues/612 and https://github.com/hibiken/asynq/issues/801 (accessed 2026-07-22, titles via search) — stuck-task and lease-expiry user reports.
- https://github.com/contribsys/faktory (accessed 2026-07-22) — 6.1k stars, v1.9.4 Feb 2026, FETCH/ACK/FAIL, 30-min reservation, retry workflow, dual license.
- https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md and https://github.com/contribsys/faktory/wiki/Worker-Lifecycle (accessed 2026-07-22, via search snippets) — WORKING state, reservation expiry semantics.
- https://github.com/RichardKnop/machinery (accessed 2026-07-22) — ~8k stars, v2.0.16 Aug 2025, brokers/backends, chains/groups/chords, Fibonacci backoff, 220 open issues.
- https://github.com/RichardKnop/machinery/issues/71 (accessed 2026-07-22, via search) — single-default-queue criticism.
- https://github.com/gocraft/work (accessed 2026-07-22) — Lua claim, reaper, retry zset, dead queue, last release June 2018, 2.5k stars.
- https://github.com/acaloiaro/neoq (accessed 2026-07-22) — queue-agnostic design, fingerprint uniqueness, 335 stars, v0.71.3 Feb 2025.
- https://adriano.fyi/posts/2023-09-24-choose-postgres-queue-technology/ (accessed 2026-07-22, via search) — "choose Postgres" rationale behind neoq.
- https://github.com/vgarvardt/gue (accessed 2026-07-22) — que-go fork, transaction-level locks, WorkMap API, v6.0.0 Oct 2025, 315 stars.
- https://github.com/sidekiq/sidekiq/wiki/Error-Handling and https://github.com/sidekiq/sidekiq/wiki/Reliability (accessed 2026-07-22, via search snippets) — 25 retries over ~20 days, backoff formula, Dead set caps, BRPOP loss mode.
- https://thoughtbot.com/blog/enhancing-job-reliability-with-sidekiq-pro-s-super-fetch-strategy and https://www.bigbinary.com/blog/increase-reliability-of-background-job-processing-using-super_fetch-of-sidekiq-pro (accessed 2026-07-22) — super_fetch LMOVE + poison-pill (3 recoveries/72h).
- https://medium.com/@geisonfgfg/task-queues-in-go-asynq-vs-machinery-vs-work-powering-background-jobs-in-high-throughput-systems-45066a207aa7 (accessed 2026-07-22) — third-party comparison.
- https://medium.com/@ericsssan/golang-background-task-queue-scheduling-a-comparison-with-laravel-queue-46fcb954b4d2 (accessed 2026-07-22) — Laravel-queue-shaped expectations mapped to Go libraries.

**Unverified claims flagged:** River's current GitHub star count (~4–5k) is an estimate, not fetched. Faktory's storage engine being embedded Redis (post-1.0, replacing RocksDB) is well known but the primary announcement was not fetched in this session. gue's exact current locking primitive (advisory locks vs SKIP LOCKED after its v2 rewrite) was not confirmed from source. Asynq maintenance-lull characterization (~2022–2023) is based on community discussion patterns, not a fetched primary source. River license now MIT: reflects current repo metadata as commonly reported; launch-era discussion cited LGPL — verify in-repo before citing.
