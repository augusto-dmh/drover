# ADR-0005: Prometheus observability via a dedicated ops port and background gauge refresh

- **Date**: 2026-08-07
- **Status**: Accepted
- **Deciders**: Augusto de Melo Henriques
- **Tags**: architecture, observability, prometheus

## Context and Problem Statement

A queue that cannot be observed from outside is operable only while someone is tailing its logs. Drover already had the execution machinery — claim, retry, rescue, shutdown — but no way to answer the three questions that decide whether a deployment is healthy: is work piling up, are jobs failing, and can this worker still reach the database?

Observability is also a public API commitment. Metric names, label sets, and endpoint semantics become contracts that operators alert on; changing them after release breaks dashboards and pages.

## Decision Drivers

- **Database load must not track scrape rate.** The RFC row for this cycle is explicit: "metrics that don't hammer the DB." During an incident, scrape frequency rises, federation adds second collectors, and someone curls `/metrics` by hand — all at the moment the database is least able to absorb extra queries.
- **Honest readiness without per-probe queries.** Kubernetes (and similar orchestrators) need a signal that removes a worker from rotation on database loss without restarting the process. That signal must not multiply database round trips by probe frequency.
- **Separate the ops surface from the application.** The embedder owns the application HTTP server; Drover must not bind a port the application did not ask for, and must not require routing application traffic through queue machinery to reach metrics.
- **Small, explainable surface.** Every mechanism — where a counter is incremented, when a gauge is refreshed, what makes `/readyz` return 503 — must be traceable in review. Abstraction layers with one backend, shipped dashboards, and push-gateway adapters are scope traps for a library this size.
- **Stdlib-first where possible; name dependencies where not.** ADR-0004 prefers a short dependency list, but a metrics backend is not something the standard library provides; the choice must be named and defended on merit, not hidden behind an interface.

## Considered Options

- **Metrics backend**: Prometheus pull model · OpenTelemetry push/export · a Drover-local metrics interface with Prometheus behind it
- **Gauge collection**: background refresher on a configured interval · `prometheus.Collector` that queries on every scrape · collect-on-scrape with a TTL cache
- **Ops server placement**: dedicated ops port (`Config.OpsAddr`) · serve from the application's existing HTTP server · unconditionally bind at construction
- **Readiness signal**: refresh freshness (memory read) · database ping on every `/readyz` · process-started only (same as `/healthz`)
- **Out-of-scope items**: OpenTelemetry tracing · shipped Grafana dashboards · Prometheus push gateway · SPA status page

## Decision Outcome

Chosen: **Prometheus via `github.com/prometheus/client_golang`, served on an opt-in dedicated ops port, with database-derived gauges refreshed by a background goroutine and per-execution counters recorded from the middleware chain.**

### Prometheus as the backend

Prometheus pull is the model: the client registers collectors on a per-client `*prometheus.Registry` (`Config.MetricsRegistry`; nil creates a private registry so two clients in one process cannot collide). The ops server serves that registry at `GET /metrics` through `promhttp`. This permanently exposes Prometheus types in Drover's public API — an accepted cost, because a metrics abstraction with exactly one implementation would reinvent label and histogram semantics badly enough that the Prometheus backend becomes lossy, and ADR-0003 already named Prometheus by name.

What this commits to publicly:

- The `drover_` metric namespace, the seven families documented in the README, their label sets, and the explicit histogram bucket boundaries — all treated as stable contracts after v0.1.0.
- A direct dependency on `prometheus/client_golang`, pulled in by every importer whether or not they scrape.

Per-execution counters and the duration histogram are recorded by a middleware the client installs immediately inside `Logging` and ahead of user middleware, so metrics cannot be silently disabled by configuration and the chain observes failures that never reach a registered worker (panics, unregistered kinds).

### Refresher, not collect-on-scrape

Queue depth and oldest-job age are gauges written by a background refresher that calls `driver.Stats` on `Config.StatsInterval` (default 15s). A scrape reads the last written values; no code path from the ops server to the database exists.

Collect-on-scrape was rejected because database load becomes a function of scraper count and frequency — a property the library cannot see or bound. A TTL cache keeps the worst property: the first scrape after expiry blocks on the database, making scrape duration bimodal with a tail equal to the database's tail. Refresher staleness is real but bounded, configurable, and far below the resolution of the primary alerting metric (minutes, not seconds).

### Dedicated ops port

`Config.OpsAddr` binds a separate listener for `/metrics`, `/healthz`, and `/readyz`. An empty address starts no listener and no goroutine — metrics are still recorded on the registry, but not served. This keeps the embedder in control of every bound port and matches the existing convention that a zero config value means "off."

Bind failure fails `Start` and starts nothing. A worker that runs but cannot be scraped is the state this cycle exists to eliminate; failing open would make a misconfigured address indistinguishable from a healthy one.

The ops server shuts down last in the stop sequence, after workers drain, so `/metrics` and `/readyz` remain answerable throughout the drain — the moment observability matters most.

### Readiness from refresh freshness

`/healthz` always returns 200: the process is alive. `/readyz` returns 200 when the client is started **and** the last successful gauge refresh is younger than twice `StatsInterval`; otherwise 503 with a reason in the body. This reuses a query the process already runs, so probe rate adds no database load. A database that accepts connections but cannot serve the jobs table is correctly reported unready because the refresh fails.

The refresher runs once immediately at start so a freshly started worker is not unready for a full interval waiting for the first tick.

### Deliberately excluded

| Excluded | Reason |
| --- | --- |
| OpenTelemetry tracing, spans, exemplars | A distinct instrumentation model deserving its own cycle and dependency justification; not in the RFC row. |
| Shipped Grafana dashboards or alert rule files | Artifacts maintained against specific Prometheus and Grafana versions; the README documents metric names and a recommended alerting expression instead. |
| Prometheus push gateway | Push is for batch jobs that exit; a long-running worker is always pull-scrapeable. |
| SPA dashboard | Permanently out of scope (RFC-0001 positioning); a single server-rendered status page is optional and late in the roadmap. |
| Metrics abstraction over multiple backends | One backend, named and documented, beats an interface with one implementation (ADR-0004 small-surface rule). |
| Authentication or TLS on the ops port | Bind to a private interface; TLS and auth are deployment concerns. |

### Positive Consequences

- Database load from observability is O(1) in scraper count — a provable property, not a hope.
- Metric provenance is visible: one registry per client, one middleware site for execution metrics, one refresher for gauges.
- `/healthz` and `/readyz` diverge under database loss without per-probe queries.

### Negative Consequences

- Gauge values may be up to one refresh interval stale; acceptable for alerting measured in minutes.
- `prometheus/client_golang` is a permanent public dependency and type surface.
- Histogram bucket boundaries are fixed at registration; widening them later invalidates existing series.
- `drover_jobs_failed_total` counts failed **executions**, not jobs that reached `dead`; operators must use `drover_queue_depth{state="dead"}` for permanent failure signal.
- Terminal-state rows (`completed`, `cancelled`) accumulate without bound; excluding them from the depth gauge is load-bearing, and no roadmap row yet owns a retention or pruning story.

## Links

- Pre-recorded direction: ADR-0003 (Prometheus via `promhttp`, oldest-job age as primary alert)
- Layout and dependency discipline: ADR-0004
- Storage seam the refresher uses: ADR-0002
- Cycle evidence: `.specs/features/cycle-e-observability/context.md`
- Operator surface: README observability section
