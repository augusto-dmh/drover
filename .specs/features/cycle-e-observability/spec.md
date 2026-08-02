# Observability Specification

## Problem Statement

Drover can enqueue, claim, execute, retry, rescue, and shut down — and an operator watching
it from outside can see none of that. There is no way to answer the three questions that
decide whether a queue is healthy: is work piling up, are jobs failing, and can this worker
still reach the database? Today the only signal is the process's own log lines, which are
per-job, unaggregatable, and useless as an alert source. Cycle E closes the v0.1.0 cut line,
so this is the last chance to make the release operable before it is released.

## Goals

- [ ] A Prometheus scrape endpoint on a dedicated ops port, separate from anything the
      application serves, exposing every metric below.
- [ ] `oldest_job_age_seconds` per queue as the primary alerting metric — the one number
      that detects a stuck queue regardless of cause.
- [ ] Queue depth by state and queue, so an operator can distinguish a backlog from a
      failure storm.
- [ ] Per-execution counters and a duration histogram, labelled by queue, sourced from the
      middleware chain rather than the execution path.
- [ ] `/healthz` and `/readyz` with distinct, documented meanings.
- [ ] Database-derived gauges refreshed on a bounded schedule, so scrape rate and database
      load are decoupled — a scrape never issues a query.

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| SPA dashboard | Permanently out of scope (ADR-0001 positioning; RFC "walls, not gaps"). |
| Server-rendered status page | Cycle I, and explicitly optional there. |
| `drover` CLI, `stats` command, exported `Inspector` | Cycle F. The CLI is the primary inspection surface, but it is a different cycle's deliverable. |
| OpenTelemetry tracing, spans, exemplars | Not in the RFC row. Tracing is a distinct instrumentation decision that deserves its own cycle and its own dependency justification. |
| Shipped Grafana dashboards or alerting rule files | Artifacts to be maintained against a Prometheus version and a Grafana version; the README documents the alerting metric instead. |
| A metrics abstraction layer over multiple backends | ADR-0004 is stdlib-first with a small surface; one backend chosen and named beats an interface with one implementation. |
| Per-job-kind metric labels | Cardinality is unbounded by construction — kinds are user-defined strings. Queue is bounded by configuration. See ASM-05. |
| Push gateway support | Prometheus pull is the model; a push gateway is for batch jobs that exit, which a worker does not. |
| Authentication or TLS on the ops port | The ops port is documented as bind-to-private-interface. Serving auth is a deployment concern, not a library one. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here — nothing is left silently unclear.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| ASM-01 — Prometheus client library | `github.com/prometheus/client_golang` is added as a direct dependency | ADR-0003 and CLAUDE.md name `promhttp` explicitly. This is a pre-recorded decision, not a new one; the stdlib-first rule's "needs its own justification" clause is already discharged. | y — pre-recorded |
| ASM-02 — Ops server is opt-in | A zero-value ops address means no ops server is started and no goroutine is spawned | A library that unconditionally binds a port surprises an embedder. Opt-in matches the `Config` conventions already in place (a zero value means "off" or "default", never "guess"). | y |
| ASM-03 — Gauge freshness vs. scrape rate | A background refresher polls the database on a configured interval and writes gauge values; a scrape reads the last written values | The RFC row's stated concept is "metrics that don't hammer the DB". Collect-on-scrape would tie database load to the number and rate of scrapers, which the library cannot control. | y |
| ASM-04 — Registry ownership | Metrics are registered on a per-client registry the client owns, defaulting to a newly created one; the ops handler serves that registry | Two clients in one process must not collide on a global registry, and a global registry makes a metric's provenance invisible. See EDGE-07. | y |
| ASM-05 — Label set | Every job metric carries a `queue` label and nothing else | `Config.Queues` makes queue a bounded, operator-declared set. Job kind is a user-supplied string with no bound, and a per-kind label turns one histogram into one per kind. | y |
| ASM-06 — "Failed" means a failed execution | The failure counter counts executions that returned a non-nil error, not jobs that reached `dead` | An execution is what a metric can observe from the middleware chain. A job's terminal disposition is a separate fact, exposed by the state-depth gauge (`dead` is one of its label values). | y |
| ASM-07 — Age of an empty queue | An empty queue reports `oldest_job_age_seconds` of `0` | A missing series breaks `max_over_time` alert expressions and is indistinguishable from a dead exporter. Zero is the honest answer: there is no job waiting, so the oldest wait is zero. | y |
| ASM-08 — Which jobs count toward "oldest" | Jobs that are claimable now — the same predicate the fetch loop uses | The metric answers "how long has the longest-waiting *runnable* job been waiting". A job deliberately scheduled for next Tuesday is not late, and counting it would make every delayed job look like an outage. | y |
| ASM-09 — Age is measured by the database clock | The age is computed in SQL against `now()`, returned as seconds | AD-020 and AD-035 already settled that time comparisons belong to the database clock. A client running fast would report ages that no other client agrees with. | y |
| ASM-10 — Refresher failure behaviour | A failed refresh logs at `WARN` and leaves the previous gauge values in place; readiness is unaffected by refresher failure alone | Zeroing on error would render a database blip indistinguishable from an empty queue — the exact false-negative an alerting metric must not produce. | y |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Scrapeable metrics on a dedicated ops port ⭐ MVP

**User Story**: As an operator, I want a Prometheus endpoint on a port I control, so that my
existing scrape configuration can collect Drover's metrics without me adding an HTTP server
to the application.

**Why P1**: Every other metric in this cycle is unreachable without it. It is the vertical
slice: configure an address, scrape it, see values.

**Acceptance Criteria**:

1. WHEN the ops address is configured and the client starts THEN the system SHALL serve
   Prometheus text-format metrics at `GET /metrics` on that address.
2. WHEN the ops address is left at its zero value THEN the system SHALL start no listener,
   bind no port, and spawn no ops goroutine.
3. WHEN the client is stopped THEN the system SHALL shut the ops listener down, and the
   address SHALL be bindable again afterwards.
4. WHEN the configured ops address cannot be bound THEN `Start` SHALL return an error
   naming the address, and the client SHALL NOT be left partially started.
5. WHEN two clients are constructed in one process with distinct registries THEN neither
   construction SHALL fail on duplicate metric registration.

**Independent Test**: configure an ops address on an otherwise ordinary client, start it,
issue `GET /metrics`, and assert the body parses as Prometheus text format and contains the
metric families this cycle defines.

---

### P1: Execution counters and duration histogram ⭐ MVP

**User Story**: As an operator, I want to know how many jobs succeeded, how many failed,
and how long they took, per queue, so that I can chart throughput and spot a slow or
failing deployment.

**Why P1**: Throughput and failure rate are the two things every queue dashboard opens with.

**Acceptance Criteria**:

1. WHEN a job's execution completes without error THEN the system SHALL increment a
   completed-executions counter labelled with that job's queue, by exactly one.
2. WHEN a job's execution returns any non-nil error THEN the system SHALL increment a
   failed-executions counter labelled with that job's queue, by exactly one, and SHALL NOT
   increment the completed counter.
3. WHEN a job's execution completes, whether it succeeded or failed THEN the system SHALL
   observe its wall-clock duration in a histogram labelled with that job's queue.
4. WHEN a job's registered worker panics THEN the system SHALL count that execution as
   failed, not as completed and not as absent.
5. WHEN metrics are collected THEN the counters and histogram SHALL be produced by a
   middleware in the chain, not by instrumentation embedded in the execution path.
6. WHEN a user supplies their own middleware THEN the metrics middleware SHALL still be
   installed, so that configuring middleware cannot silently disable metrics.

**Independent Test**: run a client with a handler that succeeds, one that errors, and one
that panics; scrape and assert the completed count, the failed count, and a non-zero
histogram sample count on the expected queue label.

---

### P1: Queue depth and oldest-job age gauges ⭐ MVP

**User Story**: As an operator, I want oldest-job age and per-state depth per queue, so that
I can alert on a queue that has stopped draining before users notice.

**Why P1**: The RFC names oldest-job age as *the* primary alerting metric. Counters tell you
what happened; only these tell you what is happening right now.

**Acceptance Criteria**:

1. WHEN the refresher runs and a queue holds claimable jobs THEN the system SHALL set that
   queue's oldest-job-age gauge to the age in seconds of the longest-waiting claimable job,
   computed against the database clock.
2. WHEN the refresher runs and a queue holds no claimable jobs THEN the system SHALL set
   that queue's oldest-job-age gauge to `0`.
3. WHEN the refresher runs THEN the system SHALL set a depth gauge per queue and job state
   to the count of jobs in that state on that queue.
4. WHEN a scrape arrives THEN the system SHALL serve the most recently refreshed values and
   SHALL NOT issue any database query as part of serving it.
5. WHEN N scrapes arrive within one refresh interval THEN the system SHALL have issued the
   same number of database queries as it would have for zero scrapes.
6. WHEN a refresh fails THEN the system SHALL log a warning and leave the previously
   published gauge values unchanged.
7. WHEN the client is stopped THEN the refresher SHALL stop, and no refresher goroutine
   SHALL outlive `Stop`.

**Independent Test**: insert jobs of known ages and states against a real database, wait for
one refresh, scrape, and assert the gauge values match the inserted data; then scrape
repeatedly and assert the query count did not rise.

---

### P1: Liveness and readiness endpoints ⭐ MVP

**User Story**: As an operator deploying to a scheduler, I want `/healthz` and `/readyz` to
mean different things, so that a database outage takes the worker out of rotation without
triggering a restart loop.

**Why P1**: An orchestrator that restarts a process because its database is briefly
unreachable turns a database blip into a fleet-wide outage. The distinction is the whole
point of having two endpoints.

**Acceptance Criteria**:

1. WHEN the ops server is running THEN `GET /healthz` SHALL return `200` whenever the
   process is running, without consulting the database.
2. WHEN the client is started and the database is reachable THEN `GET /readyz` SHALL return
   `200`.
3. WHEN the database is unreachable THEN `GET /readyz` SHALL return `503` and `GET
   /healthz` SHALL still return `200`.
4. WHEN the client has been stopped but the ops server is still serving THEN `GET /readyz`
   SHALL return `503`.
5. WHEN `/readyz` is requested THEN its database check SHALL be bounded by a timeout, so a
   hung database yields a `503` rather than a hung request.

**Independent Test**: point a client at a database, assert both endpoints return `200`;
sever the database, assert `/readyz` becomes `503` while `/healthz` stays `200`.

---

### P2: Pool saturation gauge

**User Story**: As an operator, I want to see how many workers are busy versus configured,
so that I can tell a queue that is backed up because the pool is saturated from one that is
backed up because nothing is fetching.

**Why P2**: Genuinely diagnostic and costs no database access — but oldest-job age already
detects the outage; this only explains it.

**Acceptance Criteria**:

1. WHEN jobs are executing THEN the system SHALL publish a gauge of currently-executing jobs
   that never exceeds the configured concurrency.
2. WHEN no jobs are executing THEN the gauge SHALL read `0`.
3. WHEN metrics are collected THEN the configured concurrency SHALL be published as a gauge,
   so saturation is computable from the scrape alone without out-of-band knowledge.

**Independent Test**: block N handlers on a barrier with concurrency N, scrape, and assert
the executing gauge reads N and never exceeds it.

---

### P2: Documented operator surface

**User Story**: As someone evaluating Drover, I want the README to show the ops port
configuration, the metric names, and the alerting expression, so that I can judge its
operability without reading the source.

**Why P2**: The metrics are useless to someone who cannot discover their names, but the
code ships correct regardless.

**Acceptance Criteria**:

1. WHEN the README documents observability THEN it SHALL list every metric family this
   cycle publishes, with its type and labels.
2. WHEN the README documents alerting THEN it SHALL give a concrete expression over
   oldest-job age and state why that metric is the recommended primary alert.
3. WHEN the README shows configuration THEN the shown code SHALL compile against the
   package as shipped.

**Independent Test**: extract the README's configuration snippet and compile it.

---

## Edge Cases

- **EDGE-01** — WHEN a scrape arrives before the first refresh has completed THEN the system
  SHALL serve the metric families with their zero values rather than omitting them or
  blocking the request.
- **EDGE-02** — WHEN a queue is configured but has never held a job THEN the system SHALL
  still publish that queue's gauges, so a configured-but-empty queue is visibly zero rather
  than missing.
- **EDGE-03** — WHEN a job is found in a queue that is not in the configured queue set
  (enqueued by another deployment) THEN the system SHALL publish its depth under its own
  queue label rather than dropping it.
- **EDGE-04** — WHEN the refresh interval is configured to a non-positive value THEN the
  system SHALL substitute the documented default and log a warning, per the AD-037 tuning
  convention.
- **EDGE-05** — WHEN `Stop` is called while a refresh is in flight THEN `Stop` SHALL not
  block indefinitely on it, and no goroutine SHALL leak.
- **EDGE-06** — WHEN an unknown path is requested on the ops port THEN the system SHALL
  return `404` rather than falling through to the metrics handler.
- **EDGE-07** — WHEN two clients are constructed in one process THEN each SHALL register its
  metrics on its own registry, and neither construction SHALL panic on duplicate
  registration.
- **EDGE-08** — WHEN a queue name cannot be used as a Prometheus label value THEN the system
  SHALL log a warning naming the queue and skip that one series, and SHALL NOT panic and
  SHALL NOT abandon the remaining queues' series. (Recording it under its own name is not
  achievable — that is what "cannot be used as a label value" means. The requirement is that
  an unusable name costs one series, not the worker.)
- **EDGE-09** — WHEN the ops server is enabled but the client is never started THEN no
  refresher SHALL run and no database query SHALL be issued.

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| OBS-01 | P1: Ops port | Design | Pending |
| OBS-02 | P1: Ops port — opt-in and lifecycle | Design | Pending |
| OBS-03 | P1: Execution counters | Design | Pending |
| OBS-04 | P1: Duration histogram | Design | Pending |
| OBS-05 | P1: Metrics sourced from the middleware chain | Design | Pending |
| OBS-06 | P1: Oldest-job-age gauge | Design | Pending |
| OBS-07 | P1: Depth-by-state gauge | Design | Pending |
| OBS-08 | P1: Scrape issues no query | Design | Pending |
| OBS-09 | P1: Refresh failure preserves last values | Design | Pending |
| OBS-10 | P1: `/healthz` liveness | Design | Pending |
| OBS-11 | P1: `/readyz` readiness | Design | Pending |
| OBS-12 | P2: Pool saturation gauges | Design | Pending |
| OBS-13 | P2: README operator surface | Design | Pending |

**ID format:** `OBS-NN`

**Coverage:** 13 total, 0 mapped to tasks yet.

---

## Success Criteria

- [ ] A single `GET /metrics` scrape returns every metric family named in this spec, parsing
      cleanly as Prometheus text format.
- [ ] The number of database queries issued for gauge collection is a function of elapsed
      time only, provably independent of scrape count.
- [ ] A stuck queue is detectable from `oldest_job_age_seconds` alone, with no other metric
      consulted.
- [ ] `/healthz` and `/readyz` diverge under database loss — the property that makes having
      two endpoints worth anything.
- [ ] The unit suite still runs to completion without Docker.
- [ ] No goroutine leaks on lifecycle tests with the ops server enabled.
