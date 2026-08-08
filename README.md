# drover

A PostgreSQL-backed task queue for Go, built from first principles — small enough to fully audit, honest about its delivery semantics, and documented down to every trade-off.

> **Status**: pre-v0.1, under active development. The architecture is settled ([ADRs](docs/adr/)), the roadmap is public ([RFC-0001](docs/rfc/0001-drover-roadmap.md)), and cycles ship as reviewable PRs.

## Why another queue?

Because the reasoning is the product. Established Go queues are excellent — [River](https://github.com/riverqueue/river) for Postgres-native semantics, [Asynq](https://github.com/hibiken/asynq) for observability — but their cores are large. Drover implements River-grade semantics (transactional enqueue, `FOR UPDATE SKIP LOCKED` claims, lease-based crash recovery, dead-letter retention) in a deliberately small codebase where every mechanism is explainable and provable, with each design decision recorded against the alternatives in [docs/adr/](docs/adr/) and grounded in [published research](docs/research/).

## Design at a glance

```mermaid
flowchart LR
    App[Your app] -->|"InsertTx(tx, args)"| PG[(PostgreSQL)]
    subgraph drover worker
        F[Fetcher<br/>SKIP LOCKED batch claim] --> CH[channel]
        CH --> W1[worker 1]
        CH --> W2[worker N]
        HB[Heartbeat<br/>extends leases] --> PG
        R[Rescuer<br/>requeues expired leases] --> PG
    end
    F --> PG
    W1 -->|complete / retry / dead| PG
```

- **Transactional enqueue** — jobs insert inside your own transaction; no ghost jobs, no outbox needed ([ADR-0002](docs/adr/0002-postgres-only-backend-behind-narrow-storage-interface.md)).
- **At-least-once, stated plainly** — lease + heartbeat + rescuer; every duplicate source is named and bounded; handlers are idempotent by contract ([ADR-0003](docs/adr/0003-at-least-once-delivery-lease-heartbeat-rescuer.md)).
- **A real worker pool** — a fixed, configurable number of goroutines claim and run jobs concurrently over a channel, and `Stop` drains in-flight work within a caller-supplied budget instead of leaving shutdown to lease expiry.
- **Composable middleware** — a `func(Handler) Handler` chain wraps every job, whatever its kind; the built-in `Timeout` bounds a handler's context and `Logging` reports each execution, and both compose with middleware you write yourself ([ADR-0004](docs/adr/0004-single-module-root-package-layout-and-toolchain.md)).
- **Scheduled, prioritized queues** — `InsertOpts` delays a job to a future time and routes it to a named queue; queues are served from one shared worker pool by configurable weight, so a low-priority queue is slower, never starved ([ADR-0003](docs/adr/0003-at-least-once-delivery-lease-heartbeat-rescuer.md)).
- **Typed jobs** — `JobArgs` + `Worker[T]` generics, no `[]byte` payloads, no reflection.
- **Stdlib-first** — pgx + sqlc and the standard library; the dependency list stays short enough to read ([ADR-0004](docs/adr/0004-single-module-root-package-layout-and-toolchain.md)).

## Planned API (v0.1 target)

```go
type SendEmail struct {
    To, Template string
}

func (SendEmail) Kind() string { return "send_email" }

// Worker
drover.Register(workers, &EmailWorker{}) // implements drover.Worker[SendEmail]

// Concurrency sizes the worker pool; it defaults to 10. A running job
// holds no connection, so it need not match the database pool's size.
// Queues share that one pool by weight; Middleware wraps every job,
// Logging installed outermost ahead of whatever you add.
client, err := drover.NewClient(pool, drover.Config{
    Workers:     workers,
    Concurrency: 8,
    Queues:      map[string]int{"default": 1, "bulk": 9},
    Middleware:  []drover.Middleware{drover.Timeout(30 * time.Second)},
})

// Enqueue atomically with your own writes
_, err = client.InsertTx(ctx, tx, SendEmail{To: user.Email, Template: "welcome"}, nil)

// Or delay a job and route it to a named queue; nil opts mean the
// "default" queue, runnable now.
_, err = client.Insert(ctx, SendEmail{To: user.Email, Template: "reminder"}, &drover.InsertOpts{
    Queue:       "bulk",
    ScheduledAt: time.Now().Add(24 * time.Hour),
})

// Start returns once the pool is running. Stop stops claiming, drains
// in-flight jobs within the given budget, and returns nil once every
// one of them has recorded its outcome — or an error naming how many
// did not finish and were returned to the queue instead.
err = client.Start(ctx)
err = client.Stop(shutdownCtx)
```

A full runnable version of this, including the retry path, a custom middleware, and shutdown on SIGINT, lives in [`examples/email`](examples/email).

## Observability

Drover exposes Prometheus metrics on a dedicated ops port, separate from anything your application serves. Per-execution counters and histograms are recorded from the middleware chain; queue depth and oldest-job age are refreshed from the database on an interval, so scrape rate never multiplies database load ([ADR-0005](docs/adr/0005-prometheus-observability-via-ops-port-and-background-gauge-refresh.md)).

```go
client, err := drover.NewClient(pool, drover.Config{
    Workers:         workers,
    Concurrency:     8,
    Queues:          map[string]int{"default": 1, "bulk": 9},
    OpsAddr:         "127.0.0.1:9090", // /metrics, /healthz, /readyz
    StatsInterval:   15 * time.Second, // how often depth/age gauges refresh
    MetricsRegistry: prometheus.NewRegistry(),
})
```

Leave `OpsAddr` empty to record metrics without serving them — pass `MetricsRegistry` to your own `/metrics` handler instead. Bind the ops port to a private interface; TLS and authentication are deployment concerns.

### Ops endpoints

| Path | Meaning |
| --- | --- |
| `GET /metrics` | Prometheus text format from this client's registry |
| `GET /healthz` | Process is alive — always `200` |
| `GET /readyz` | Worker is started and the last gauge refresh succeeded within twice `StatsInterval`; otherwise `503` with a reason. Removes the worker from rotation on database loss without restarting the process. |

### Metrics

| Name | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `drover_jobs_completed_total` | Counter | `queue` | Executions that returned no error. |
| `drover_jobs_failed_total` | Counter | `queue` | Executions that returned an error, including recovered panics. Counts **attempts**, not jobs that reached `dead` — a job that fails four times and succeeds on the fifth increments this four times. Use `drover_queue_depth{state="dead"}` for permanent failures. |
| `drover_job_duration_seconds` | Histogram | `queue` | Wall-clock time one execution took, successful or not. Buckets span 5ms to 10 minutes. |
| `drover_jobs_executing` | Gauge | — | Executions currently inside the middleware chain. |
| `drover_pool_concurrency` | Gauge | — | Configured worker count; compare to `drover_jobs_executing` for saturation. |
| `drover_queue_depth` | Gauge | `queue`, `state` | Jobs held in one state on one queue. States: `available`, `scheduled`, `retryable`, `running`, `dead`. `completed` and `cancelled` are deliberately absent — those rows accumulate forever, and counting them would make this gauge's cost grow with history instead of backlog. |
| `drover_oldest_job_age_seconds` | Gauge | `queue` | Age in seconds of the oldest job claimable *now* (same predicate the fetch loop uses). `0` when no job is waiting — a configured-but-empty queue publishes zero, not a missing series. |

### Alerting

Recommended primary alert — a queue with work that is not moving:

```promql
max by (queue) (max_over_time(drover_oldest_job_age_seconds[5m])) > 300
```

`drover_oldest_job_age_seconds` is the primary alerting metric because it detects a stuck queue regardless of cause: workers dead, fetch broken, database slow, or handlers hanging. Counters tell you jobs failed; depth by state tells you *where* they are; oldest age tells you work is waiting longer than it should. Page on five minutes for most workloads; tune the threshold to your SLA.

Secondary signals: `drover_queue_depth{state="dead"}` for permanent failure accumulation; `drover_jobs_executing / drover_pool_concurrency` near 1.0 for saturation.

## Roadmap

v0.1.0 = cycles A–E of [RFC-0001](docs/rfc/0001-drover-roadmap.md): walking skeleton → retries/DLQ/rescue → worker pools + graceful shutdown → middleware + scheduled jobs → **Prometheus observability (shipped)**. Then: CLI introspection, benchmarks with published methodology, periodic jobs via advisory-lock leader election, and an optional server-rendered status page.

## Documentation

- [Architecture Decision Records](docs/adr/) — what was decided and why, including [observability (ADR-0005)](docs/adr/0005-prometheus-observability-via-ops-port-and-background-gauge-refresh.md)
- [RFC-0001 roadmap](docs/rfc/0001-drover-roadmap.md) — what ships when
- [Research](docs/research/) — the evidence behind the decisions (existing-system survey, storage mechanics, delivery semantics, conventions, scope)

## License

MIT
