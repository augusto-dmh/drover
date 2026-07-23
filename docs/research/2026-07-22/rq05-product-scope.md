# RQ05 — Product surface and staged scope

Research date: 2026-07-22. Question: what makes a finishable, demonstrable v1 of drover (Go + Postgres distributed task queue, solo evening/weekend project) and a compelling staged growth path.

## 1. Minimum credible feature set

### 1.1 What comparable projects shipped at launch

**River (Nov 2023, launch by Brandur Leach).** The initial release already included: transactional enqueue (jobs inserted in the same Postgres transaction as application data), typed job args via Go generics (`river.Job[T]`, "100% reflect-free"), batch insertion via `COPY FROM`, error/panic handlers, retries with backoff, unique jobs, periodic jobs, and subscription hooks for telemetry. The launch post quoted "~10k trivial jobs/second" on modest hardware and named the hardware — an honest-benchmark framing worth copying. Notably, River launched **without** a web UI (River UI came ~5 months later, Apr 2024) and without middleware (added post-launch).

**Asynq (Redis-backed, first release late 2019).** v0.1.x was much smaller than River's launch: `Enqueue`/`EnqueueAt`/`EnqueueIn`, multiple queues with priorities, a `Handler` interface, retries, and a CLI. Everything else accreted over years of releases: `ServeMux` + middleware (v0.6.0, Mar 2020), unique tasks (v0.7.0), pluggable `Logger` (v0.9.0), mandatory timeouts/deadlines (v0.10.0), `Inspector` introspection API (v0.11.0), the canonical state model Pending/Active/Scheduled/Retry/Dead (v0.12.0), periodic tasks via `Scheduler` (v0.13.0), Prometheus `x/metrics` (v0.20.0, Dec 2021), task grouping/aggregation (v0.23.0, 2022). Asynq reached 13.5k stars with this trajectory. Lesson: a small, reliable core plus a visible changelog of deliberate additions *is* the growth story — you do not need River's launch breadth on day one, but you need Asynq v0.12's **state model** clarity early, because retries/DLQ semantics are what people probe first.

### 1.2 What the HN crowd demands of a new queue announcement

From the River launch thread (HN id 38349716, top-ranked, heavily commented):

- **Transactional enqueue is the headline value** of a Postgres queue — commenters repeatedly said this solves "whole classes of distributed systems problems" (job visible before commit, job enqueued then transaction rolls back). A Postgres queue that doesn't lead with this has no reason to exist.
- **Comparisons demanded**: to Oban, Sidekiq, graphile-worker, que/gue, neoq. A new queue without a "why not X" section gets that section written for it in the comments, less charitably.
- **Design mechanics scrutinized**: `SELECT ... FOR UPDATE SKIP LOCKED` vs `LISTEN/NOTIFY` vs polling; Oban's author noted they abandoned DB triggers because of pgbouncer incompatibility. Reviewers of any new queue probe exactly here — the README should state the fetch mechanism and its trade-offs explicitly.
- **Feature asks**: scheduled/future jobs, job completion notification, a web UI ("like Hangfire / Laravel Horizon"), benchmarks with methodology, documented Postgres version requirements.
- **Licensing and attribution matter**: River got hammered for LGPL (later relicensed MPL-2.0) and for uncredited similarity to Oban's schema. For drover: MIT/Apache-2.0, and a CREDITS/prior-art section acknowledging River, Asynq, Oban, Sidekiq. Cheap insurance, reads as maturity.

### 1.3 Minimum credible v1 for drover

A queue "anyone would actually try" needs, in order of credibility weight:

1. Transactional enqueue (`InsertTx`) — the differentiator of the Postgres approach.
2. Typed job args via generics — table stakes for a modern Go library post-River.
3. At-least-once delivery with a written crash story: `FOR UPDATE SKIP LOCKED` fetch, visibility/lease timeout, rescue of stuck jobs.
4. Retries with exponential backoff + jitter, capped attempts, then a dead/discarded state that is queryable and retryable.
5. Graceful shutdown (finish in-flight work, stop fetching, hard deadline).
6. Multiple named queues with per-queue concurrency.
7. Honest README: comparison table, non-goals, benchmark with methodology.

Explicitly **not** needed for credibility at v1: web UI, cron/periodic jobs, unique jobs, batching, rate limiting, multi-language clients. All are roadmap material — and a documented roadmap is itself credibility.

## 2. Client API surface for v1

### 2.1 Essential

- **Typed enqueue**: `type SendEmailArgs struct{...}` implementing `drover.JobArgs` (a `Kind() string` method), `client.Insert(ctx, args, opts...)` and `client.InsertTx(ctx, tx, args, opts...)`. This is River's shape and it demos generics, interfaces, and JSON marshaling in one API.
- **Worker registration**: `drover.Worker[T]` interface with `Work(ctx, *Job[T]) error`; a `Workers` registry mapping kind → worker (River's `AddWorker(workers, &SendEmailWorker{})`). Demonstrates generics + type registry; contrast with Asynq's untyped `ServeMux` + payload unmarshal, which is the more familiar but weaker design.
- **Insert options**: queue name, priority, `ScheduledAt` (future jobs are ~free once the fetch query filters on `scheduled_at <= now()` — and HN explicitly demands them), `MaxAttempts`.
- **Client config**: per-queue worker counts, poll interval, logger injection (`*slog.Logger`).

### 2.2 Middleware — essential in minimal form

Asynq shipped middleware at v0.6 within months because users immediately need logging/recovery/metrics without forking the library. Ship the *mechanism* in v1 with two built-ins:

- `func(next Handler) Handler` chain (idiomatic, mirrors `net/http`, instantly legible to any Go developer).
- Built-in **recovery** (panic → job error → retry path; a panicking worker must not kill the process) and **logging** (structured, job id/kind/attempt/duration/outcome).
- **Metrics middleware** arrives in the metrics cycle; **timeout/deadline enforcement** via `context.WithTimeout` per job can be v1 (Asynq made it mandatory at v0.10 for good reason).

### 2.3 Later

Unique jobs (needs careful keying — Asynq v0.7, River launch-day, but deferrable), batch insert (`COPY FROM`), completion subscription/hooks, `Inspector`-style introspection API (arrives with the CLI cycle), rate limiting, task aggregation (Asynq's most complex feature, v0.23 — skip entirely or mark "non-goal").

## 3. Observability surface

### 3.1 Metrics that matter (Prometheus)

Consensus across Prometheus instrumentation docs, Sidekiq's monitoring guidance, and queue-monitoring guides:

- **Queue latency / age of oldest pending job** (gauge, per queue) — Sidekiq's Mike Perham's position, echoed by Judoscale: *latency, not depth, is the metric that matters*, because it maps to SLA ("jobs wait ≤30s") independent of throughput. This is the alerting metric.
- **Queue depth** (gauge, per queue, per state: pending/running/retryable/dead) — the intuition metric; dead-queue depth > 0 is its own alert.
- **Throughput**: `jobs_processed_total{queue,kind,outcome=succeeded|failed|discarded}` (counter) — failure *ratio* derivable in PromQL; Prometheus best practice is counters-for-attempts + counters-for-failures, never a precomputed rate.
- **Job duration**: histogram by kind (p50/p95/p99).
- **Worker utilization**: active workers vs configured concurrency (gauge) — answers "am I under-provisioned or is work just slow?"
- Nice-to-have: fetch round-trip latency, per-attempt histogram of retry counts.

Implementation note: depth/oldest-age come from one cheap aggregate query executed by a collector on scrape or ticker — a deliberate choice not to hammer Postgres from the metrics path. Expose via `promhttp` on an ops port with `/healthz` (process up) and `/readyz` (DB reachable). Structured logs via `slog` from day one; the logging middleware plus lifecycle events (start, shutdown begin/complete, job rescued) are the log surface.

### 3.2 Web dashboard: worth it or scope trap?

Evidence on what UIs cost their authors (GitHub API, retrieved 2026-07-22):

| Project | Commits | Timespan | Status |
|---|---|---|---|
| hibiken/asynqmon | 355 | Nov 2020 → May 2024 | **last push May 2024, 69 open issues** — effectively unmaintained for 2+ years while asynq itself continued |
| riverqueue/riverui | 514 | Apr 2024 → present | actively maintained — but by a two-person **company** with a commercial Pro tier funding it |

Both are full React+TypeScript SPAs with their own build pipeline (asynqmon requires Node+Yarn to build assets), i.e., a second product. Asynqmon (355 commits, then abandonment) is the cautionary tale: for a solo maintainer, a SPA dashboard steals evenings from the core queue work the project actually stands on, and rots visibly. River launched with **no UI** and topped HN anyway.

**Verdict: scope trap as a SPA; fine as a single-page server-rendered status page.** A final-cycle `drover web` command serving one Go `html/template` page (queue table, depth/latency, recent failures, retry/cancel buttons) delivers 80% of the demo value for ~2 evenings and zero JS toolchain. The CLI (`drover stats`, `drover jobs list --state=dead`, `drover retry <id>`) is the primary inspection surface — cheaper, and terminal output demos well.

## 4. Repo presentation and demo strategy

Engineers evaluating a new OSS project — potential adopters and reviewers alike — give a repo **minutes**, so the README must front-load a one-paragraph problem statement, an architecture diagram, a ≤5-line quickstart, and proof it works (numbers, GIF) before any prose. The durable credibility markers are: clear README, real tests, CI badge, coherent commit/PR history, and documented design decisions that survive being probed.

Concrete package for drover:

- **README order**: tagline → 30-line "hello queue" code sample (River's README leads with code) → mermaid architecture diagram (client → jobs table → producer/fetcher → worker pool → states) → quickstart → feature table vs River/Asynq/machinery → benchmark table with methodology → design docs links → roadmap → non-goals → prior art/credits.
- **Example app** in `examples/`: an email-pipeline (or image-resize) app; `docker-compose up` starts Postgres + API + 2 workers; a seed script enqueues a burst; logs + `drover stats` show drain-down. One command, < 1 minute to "it works". Compose also demos multi-worker competition for jobs (kill a worker mid-run to show rescue — the most convincing 60 seconds of any queue demo).
- **Benchmarks**: a `cmd/drover-bench` load generator (enqueue-throughput and end-to-end drain), not wrk/vegeta against an HTTP facade — drover is a library, so benchmark the library; vegeta only becomes relevant if the demo app's HTTP ingest is benchmarked. Report jobs/sec + p95/p99 latency, name the hardware and Postgres version, publish the harness, and state caveats (trivial no-op jobs, single node) — mirroring River's "~10k trivial jobs/sec on modest hardware" framing. An honest 3–6k jobs/sec with methodology beats an unreproducible big number.
- **Docs that evaluating engineers actually read**: `docs/adr/` (why Postgres not Redis, why SKIP LOCKED, why lease-based rescue), `docs/research/` (these reports), roadmap file. *Documented trade-off reasoning builds more trust than any single feature.*

## 5. Staged roadmap

Sizing: each cycle ≈ one PR, 3–6 evenings. Ship order front-loads the crash-safety story — the part of a queue most worth proving early — and defers everything with UI/ecosystem flavor.

| Cycle | Deliverable | Go concepts | What this cycle proves |
|---|---|---|---|
| **A — Walking skeleton** | `jobs` table + migrations; `Client.Insert`/`InsertTx` with typed args (`JobArgs`, `Worker[T]`, registry); single worker loop polling with `FOR UPDATE SKIP LOCKED`; states pending→running→succeeded; docker-compose Postgres; happy-path integration test | Generics + interfaces for typed API; `database/sql`/pgx; context propagation; table-driven tests | "Why Postgres as a queue, why transactional enqueue kills a class of distributed-systems bugs, how SKIP LOCKED gives lock-free-ish competing consumers" |
| **B — Reliability core: retries, DLQ, rescue** | Error → scheduled retry with exp backoff + jitter; `max_attempts` → dead state; lease/heartbeat timeout + rescuer that re-queues jobs from crashed workers; recovery middleware (panic → error); state machine documented | Error wrapping/`errors.As`; panic/recover; time-based logic + injectable clock for tests; crash-safety testing | "At-least-once semantics: what happens when a worker dies mid-job — walk the lease/rescue design; why idempotent handlers are the consumer's contract" |
| **C — Concurrency: worker pool + graceful shutdown** | Configurable per-queue worker pools; producer/fetcher feeding workers via channels; `Start(ctx)/Stop(ctx)`: stop fetching, drain in-flight, hard-deadline cancel; signal handling in example app; race-detector CI | Goroutines, channels, `sync.WaitGroup`, `errgroup`, context cancellation hierarchies, `-race` | "Draw the goroutine topology; how shutdown avoids both job loss and hung exits — the classic graceful-shutdown problem, answered with working code" |
| **D — Middleware + scheduled jobs** | `func(Handler) Handler` chain; built-in logging (slog) + timeout middleware; `ScheduledAt` insert option; multiple named queues with priorities | Decorator/chain pattern, first-class functions, `log/slog`, functional options pattern | "Extensibility without bloating core — same pattern as net/http; how one fetch query serves immediate and future jobs" |
| **E — Observability** | Prometheus registry: depth + oldest-job-age gauges (per queue/state), processed/failed counters, duration histograms, active-worker gauge; metrics middleware; `/healthz`, `/readyz`; sample Grafana dashboard JSON + alert rules (latency-based, per §3.1) | `promhttp`/collectors, ticker-based aggregate queries, HTTP servers, embedding dashboards in repo | "Which queue metrics matter and why latency beats depth for alerting; how metrics collection avoids loading the primary datastore" |
| **F — CLI + introspection** | `drover` binary (cobra or stdlib flag): `stats`, `jobs list --state --queue`, `retry`, `cancel`, `enqueue`; backed by an exported `Inspector` API; goreleaser or Makefile release | CLI structure, exported vs internal API design, JSON/table output, build tooling | "Operator experience: how you debug a stuck queue at 3am without psql; API layering (core vs inspector vs CLI)" |
| **G — Benchmark + demo hardening** | `cmd/drover-bench`; batch insert (`COPY FROM`); tune fetch (batch claim, poll interval, optional `LISTEN/NOTIFY` wake-up); README benchmark table + methodology; polished `examples/email-pipeline` with kill-a-worker demo script; architecture diagram | pprof profiling, benchmarking discipline, Postgres query tuning/EXPLAIN, batch DB ops | "Here's the number, here's the harness, here's what I tuned and what I'd tune next — honest performance engineering" |
| **H — Periodic jobs** | Cron-style scheduler with leader election via advisory lock (single scheduler across N nodes); unique-job option (partial unique index) | Advisory locks / distributed coordination, cron parsing, idempotent scheduling | "How N processes agree on one scheduler without etcd — coordination with the tools you already have" |
| **I (optional) — Status page** | `drover web`: one server-rendered `html/template` page (auto-refresh) with queue table, recent failures, retry/cancel actions | `html/template`, `embed.FS`, minimal HTTP handlers | "A deliberately scoped UI: server-rendered page in 2 evenings vs asynqmon's 355-commit abandoned SPA — scoping is itself a design decision worth documenting" |

Cut line: A–E is a credible announced v1 (v0.1.0 tag + comparison README); F–G harden the operational story; H–I are visible-roadmap upside. If time compresses, drop I, then H.

## Options

### Option set 1 — Client API style

- **A. River-style typed args + generic `Worker[T]` + registry** — modern, reflect-free, shows generics depth; slightly more design work. **★ RECOMMENDED** — it is the post-2023 ecosystem default and the strongest expression of type-safe API design available here.
- B. Asynq-style `ServeMux` + untyped payload — familiar shape (mirrors `net/http`), faster to build, but reads as pre-generics Go in 2026.
- C. Both (typed core, mux adapter on top) — scope creep for v1; viable later cycle.

### Option set 2 — Dashboard

- A. Full SPA dashboard (asynqmon/riverui class) — high demo shine; evidence says 350–500+ commits and ongoing maintenance, killed asynqmon; a second product.
- **B. CLI-first + optional single server-rendered status page (Cycle I)** — 80% of demo value, ~2 evenings, zero JS toolchain, and the scoping decision itself is worth documenting in an ADR. **★ RECOMMENDED**
- C. No UI ever — defensible (River launched UI-less) but forfeits a cheap wow moment.

### Option set 3 — Demo example app

- **A. Email/notification pipeline** — domain instantly legible to anyone, zero heavy deps, easily shows retries (flaky SMTP stub), priorities, scheduled digests. **★ RECOMMENDED**
- B. Image-resize pipeline — visually satisfying, but adds imaging deps and CPU noise to benchmarks.
- C. Webhook-delivery service — great retry/backoff story, slightly less obvious at first glance; good second example later.

### Option set 4 — Benchmark tooling

- **A. Custom `cmd/drover-bench` (library-level: enqueue throughput + drain latency percentiles) with published methodology** — measures what drover is; reproducible. **★ RECOMMENDED**
- B. vegeta/wrk against the demo app's HTTP endpoint — measures the demo app, not the queue; use only as a supplementary end-to-end number.
- C. `go test -bench` micro-benchmarks only — good for CI regression guarding, not a README headline.

## Sources

- River launch post (features at launch, benchmark framing): https://brandur.org/river (accessed 2026-07-22)
- River HN launch thread (community demands, criticisms): https://news.ycombinator.com/item?id=38349716 (accessed 2026-07-22)
- River repo: https://github.com/riverqueue/river (accessed 2026-07-22)
- Announcing River UI: https://riverqueue.com/blog/announcing-river-ui (accessed 2026-07-22)
- River UI repo (commit count, activity via GitHub API): https://github.com/riverqueue/riverui (accessed 2026-07-22)
- Asynq CHANGELOG (feature timeline v0.1→v0.26): https://github.com/hibiken/asynq/blob/master/CHANGELOG.md (accessed 2026-07-22)
- Asynq repo: https://github.com/hibiken/asynq (accessed 2026-07-22)
- Asynqmon repo (commit count, last-push date, open issues via GitHub API): https://github.com/hibiken/asynqmon (accessed 2026-07-22)
- Prometheus instrumentation best practices (queue metrics, failure counters): https://prometheus.io/docs/practices/instrumentation/ (accessed 2026-07-22)
- Sidekiq monitoring wiki (queue latency definition): https://github.com/sidekiq/sidekiq/wiki/Monitoring (accessed 2026-07-22)
- Sidekiq queue-latency discussion: https://github.com/sidekiq/sidekiq/issues/4079 (accessed 2026-07-22)
- Judoscale — planning Sidekiq queues ("latency is what matters"): https://judoscale.com/blog/planning-sidekiq-queues (accessed 2026-07-22)
- Message-queue Prometheus monitoring guide: https://oneuptime.com/blog/post/2026-02-09-message-queue-prometheus-monitoring/view (accessed 2026-07-22)

Flags on unverified claims:
- Asynq middleware/feature version numbers summarized from CHANGELOG via automated extraction; spot-check before quoting exact versions publicly.
- River's post-launch middleware timing (stated above as "added post-launch") is from memory of release notes and was **not independently verified** in this session.
- HN thread summary is an AI-assisted digest of the thread; individual quotes paraphrased, not verbatim.
