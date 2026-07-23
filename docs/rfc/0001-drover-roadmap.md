# RFC-0001: Drover v0.1 roadmap

- **Status**: Accepted (2026-07-22)
- **Driver**: Augusto de Melo Henriques
- **Approvers**: Augusto de Melo Henriques
- **Impact**: whole project — defines cycle order, the v0.1.0 cut line, and out-of-scope walls

## Background

The founding research fleet (`docs/research/2026-07-22/`, synthesized in `synthesis.md`) settled the architecture: Postgres-only backend behind a narrow storage interface (ADR-0002), at-least-once delivery with lease + heartbeat + rescuer (ADR-0003), single-module root-package layout with a stdlib-first toolchain (ADR-0004). This RFC sequences the build into PR-sized cycles. Each cycle is one `tlc-spec-driven` cycle producing one PR.

## Locked decisions

- Cut line: **cycles A–E constitute v0.1.0**. F–G harden the operational story; H–I are visible-roadmap upside, droppable in that order.
- No SPA dashboard, ever (ADR-0001 positioning + RQ05 evidence: asynqmon abandoned at 355 commits, riverui company-funded). Cycle I is one server-rendered `html/template` page.
- The example app (Cycle C onward) is an email/notification pipeline: legible domain, zero heavy deps, naturally shows retries (flaky SMTP stub), priorities, and scheduled digests.
- Benchmarks ship only with published methodology (`cmd/drover-bench`, enqueue throughput + drain latency percentiles).

## Cycles

| Cycle | Deliverable | Core concepts exercised |
|---|---|---|
| **A — Walking skeleton** | `jobs` table + migrations; `Client.Insert`/`InsertTx` with typed args (`JobArgs`, `Worker[T]`, registry); single worker loop with `FOR UPDATE SKIP LOCKED`; job states | Transactional enqueue, generics API, sqlc/pgx |
| **B — Reliability core** | Retries with `attempt^4` ±10% jitter; `max_attempts` → dead state; lease/heartbeat + rescuer; `Cancel`/`Snooze` sentinels | Crash recovery, error classification |
| **C — Concurrency** | Per-queue worker pools; fetcher→workers via channels; `Start(ctx)`/`Stop(ctx)` graceful shutdown (stop-fetch → drain → cancel → requeue); `-race` CI; example app | Goroutine topology, cancellation hierarchies |
| **D — Middleware + scheduling** | `func(Handler) Handler` chain; logging + timeout middleware; `ScheduledAt`; named queues with weighted priorities | Decorator chain, weighted fetch |
| **E — Observability** | Prometheus: oldest-job-age + depth gauges, processed/failed counters, duration histograms; `/healthz`, `/readyz`; ops port | Metrics that don't hammer the DB |
| **F — CLI + introspection** | `drover` binary: `stats`, `jobs list`, `retry`, `cancel`, `enqueue`; exported `Inspector` API; GoReleaser | CLI-first operability |
| **G — Benchmark + hardening** | `cmd/drover-bench`; batch insert (`COPY FROM`); fetch tuning (batch claim, poll interval, optional `LISTEN/NOTIFY` wake-up); README benchmark table | Measured, reproducible performance claims |
| **H — Periodic jobs** | Cron scheduler with advisory-lock leader election; unique jobs via partial unique index | Distributed coordination |
| **I — Status page (optional)** | `drover web`: one server-rendered auto-refreshing page; retry/cancel actions | `html/template`, `embed.FS` |

## Out of scope (walls, not gaps)

Redis production adapter (ADR-0002) · SPA dashboard · multi-language clients · workflow/DAG orchestration · exactly-once claims of any kind.
