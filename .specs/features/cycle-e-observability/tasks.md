# Observability Tasks

## Execution Protocol (MANDATORY — do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path.

**If the skill cannot be activated, STOP and tell the orchestrator — do not proceed without it.**

---

**Spec**: `.specs/features/cycle-e-observability/spec.md`
**Design**: `.specs/features/cycle-e-observability/design.md`
**Context**: `.specs/features/cycle-e-observability/context.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase, project guidelines, and spec. Guidelines found: `CLAUDE.md`
> (tests table-driven, `t.Parallel`, `-race` always, testcontainers for integration,
> goleak on lifecycle tests, unit suite must run without Docker), `.github/workflows/ci.yml`,
> `.golangci.yml`.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| Root-package logic (`metrics.go`, `stats.go`, `ops.go`, `client.go`, `loop.go`, `pool.go`) | unit | All branches; 1:1 to spec ACs; every listed edge case has a test | `*_test.go` (root, no build tag) | `go test -race ./...` |
| Lifecycle behaviour (anything that starts or stops a goroutine) | unit + goleak | `defer goleak.VerifyNone(t, goleak.IgnoreCurrent())` as the first line, per the existing 74-site idiom | `*_test.go` (root) | `go test -race ./...` |
| `internal/memdriver` | unit | Key query paths + error paths, mirroring `memdriver_test.go`'s existing conformance style | `internal/memdriver/*_test.go` | `go test -race ./...` |
| `internal/pgdriver` | integration | Key query paths + error paths, mirroring `pgdriver_integration_test.go` | `internal/pgdriver/*_integration_test.go` | `go test -race -tags=integration ./...` |
| End-to-end against real Postgres | integration | Happy path + each documented failure path for the cycle's operator-facing surface | `*_integration_test.go` (root) | `go test -race -tags=integration ./...` |
| Migrations | integration | Applies cleanly; the object it creates exists afterwards | `internal/migrate/*_integration_test.go` | `go test -race -tags=integration ./...` |
| Generated sqlc output (`internal/dbsqlc`) | none | Build gate + CI drift check only — never hand-edited, never asserted on | — | build gate only |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
| --- | --- | --- | --- |
| Root unit tests | Yes | Every test builds its own client over a fresh `memdriver.New()`; no shared global state | `memdriver.New()` at 100 call sites; `t.Parallel()` throughout `pool_test.go` |
| Metric-set tests | Yes — **only because** each test constructs its own `prometheus.Registry` | A private registry per test; the global default registry is never touched | This is the property D-5/EDGE-07 exists to guarantee; a test that reaches for `prometheus.DefaultRegisterer` breaks it |
| Ops-server tests | Yes | Each test binds `127.0.0.1:0` and reads the assigned port; never a fixed port | New this cycle — a hardcoded port would make the suite fail under `-race` reruns and in CI |
| `internal/memdriver` | Yes | Per-test in-memory store | `internal/memdriver/memdriver_test.go` |
| `internal/pgdriver` + root integration | Yes | `testdb.NewDB(t)` creates `drover_test_<n>` per test off an atomic counter | `internal/testdb/testdb.go:64,73` |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After tasks with unit tests only | `go test -race ./...` |
| Full | After tasks with integration tests | `go test -race ./... && go test -race -tags=integration ./...` |
| Build | Phase completion, or config/generated/docs-only tasks | `go build ./... && go vet ./...` |

**Before the phase-closing commit of every phase**, also run
`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run`.
**If any `.sql` file changed**, run `go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate`
and commit the regenerated `internal/dbsqlc` — CI fails on drift
(`.github/workflows/ci.yml:40-50`).

**Environment**: Docker **is** available in this environment — integration gates can and must
be run, not skipped. Baseline before this cycle: **202** test functions.

---

## Execution Plan

### Phase 1: Storage (Sequential)

```
T1 → T2 → T3 → T4 → T5
```

### Phase 2: Metric set and middleware (Sequential)

```
T6 → T7 → T8
```

### Phase 3: Stats refresher (Sequential)

```
T9 → T10 → T11
```

### Phase 4: Ops server (Sequential)

```
T12 → T13 → T14
```

### Phase 5: Documentation (Parallel OK)

```
     ┌→ T15 [P]
     ├→ T16 [P]
     └→ T17 [P]
```

---

## Task Breakdown

### T1: Add the dead-jobs partial index migration

**What**: Migration `003` creating a partial index on `(queue, id) WHERE state = 'dead'`.
**Where**: `internal/migrate/migrations/003_index_dead_jobs.sql`
**Depends on**: None
**Reuses**: `002_widen_fetch_predicate.sql` for comment style — migrations in this repo
explain *why* the index exists and what breaks without it.
**Requirement**: OBS-07

**Done when**:
- [x] Migration applies cleanly on a fresh database and on one already at version 002
- [x] An integration test asserts the index exists after migration
- [x] The file carries a comment explaining the sequential-scan problem it solves (D-11)
- [x] Gate passes: `go test -race ./... && go test -race -tags=integration ./...`

**Tests**: integration
**Gate**: full
**Commit**: `feat(schema): index dead jobs for per-queue counting`

---

### T2: Add the depth and oldest-claimable queries

**What**: Two named sqlc queries plus the regenerated client.
**Where**: `internal/pgdriver/queries.sql`, `internal/dbsqlc/` (generated)
**Depends on**: T1
**Reuses**: the existing `FetchAvailable` predicate — `OldestClaimable` must use the *same*
claimability predicate, not a re-derived one.
**Requirement**: OBS-06, OBS-07

**Done when**:
- [x] `QueueDepths :many` and `OldestClaimable :many` exist per the design's SQL
- [x] `sqlc generate` run and `internal/dbsqlc` committed; `git diff --exit-code internal/dbsqlc` is clean after a regenerate
- [x] Gate passes: `go build ./... && go vet ./...`

**Tests**: none (generated code is never asserted on directly — T4 covers the behaviour)
**Gate**: build
**Commit**: `feat(storage): add queue depth and oldest-claimable queries`

---

### T3: Implement in-memory queue statistics

**What**: A concrete `Stats` method on `memdriver` plus the shared result types.
**Where**: `internal/driver/driver.go` (types only, **not** the interface yet),
`internal/memdriver/memdriver.go`, `internal/memdriver/memdriver_test.go`
**Depends on**: None (parallel to T1/T2 in principle; sequenced for a clean history)
**Reuses**: `memdriver.selectRows` (`memdriver.go:161`) for predicate handling
**Requirement**: OBS-06, OBS-07

**Done when**:
- [x] `driver.QueueDepth`, `driver.QueueAge`, `driver.Stats` types defined per the design
- [x] `memdriver.Stats` counts only `available`, `scheduled`, `retryable`, `running`, `dead`
- [x] Oldest age uses the claimable predicate — a future-scheduled job does not contribute
- [x] A queue with no claimable jobs yields no age row (the caller seeds zero — spec ASM-07)
- [x] Adding `Stats` here does **not** yet change the `Driver` interface, so the tree builds
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(storage): report queue depth and wait age from the in-memory driver`

---

### T4: Implement Postgres queue statistics

**What**: A concrete `Stats` method on `pgdriver` over the T2 queries.
**Where**: `internal/pgdriver/pgdriver.go`, `internal/pgdriver/pgdriver_integration_test.go`
**Depends on**: T2, T3
**Reuses**: `pgdriver.explain` (`pgdriver.go:185`) for error wrapping
**Requirement**: OBS-06, OBS-07

**Done when**:
- [x] Returns the same shape as `memdriver.Stats` for the same data — this equivalence is the point of D-4
- [x] Ages are computed by the database clock, never in Go (AD-020, ASM-09)
- [x] Integration tests cover: depth per state and queue; a future-scheduled job excluded from age; a `dead` job counted; an empty database yielding empty slices rather than an error
- [x] Gate passes: `go test -race ./... && go test -race -tags=integration ./...`

**Tests**: integration
**Gate**: full
**Commit**: `feat(storage): report queue depth and wait age from Postgres`

---

### T5: Put Stats on the driver interface

**What**: Add the one method to `driver.Driver`; both implementations already satisfy it.
**Where**: `internal/driver/driver.go`
**Depends on**: T3, T4
**Requirement**: OBS-06, OBS-07

**Done when**:
- [x] `Stats(ctx context.Context) (*Stats, error)` on the interface, documented in the
      style of its neighbours
- [x] Whole tree builds; no test changes needed
- [x] Gate passes: `go build ./... && go vet ./...` plus `go test -race ./...`, plus lint (phase close)

**Tests**: none (no new behaviour — both implementations and their tests already exist)
**Gate**: build
**Commit**: `refactor(storage): add queue statistics to the driver interface`

---

### T6: Define the metric set

**What**: Every collector, its name, labels, and buckets, registered on a supplied registry.
**Where**: `metrics.go`, `metrics_test.go`, `go.mod`/`go.sum`
**Depends on**: None
**Requirement**: OBS-03, OBS-04, OBS-06, OBS-07, OBS-12

**Done when**:
- [x] `github.com/prometheus/client_golang` added as a direct dependency
- [x] All seven metric families from the design exist with exactly the documented names, types, and labels
- [x] Histogram buckets are the documented explicit set, not `DefBuckets`
- [x] `drover_pool_concurrency` is set at construction from the configured concurrency
- [x] Every label-taking accessor uses the error-returning `GetMetricWith*` family — a
      test proves an unusable queue name logs and skips rather than panicking (EDGE-08)
- [x] Two metric sets on two distinct registries construct without error (EDGE-07)
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(metrics): define the job and queue metric set`

---

### T7: Record executions from a middleware

**What**: The middleware that increments the counters, observes the histogram, and tracks
in-flight executions.
**Where**: `metrics.go`, `metrics_test.go`
**Depends on**: T6
**Reuses**: `Logging` (`middleware.go:124`) — the two should read as siblings
**Requirement**: OBS-03, OBS-04, OBS-05, OBS-12

**Done when**:
- [x] A nil-returning execution increments completed by exactly one and failed by zero
- [x] An error-returning execution increments failed by exactly one and completed by zero
- [x] Both cases observe exactly one histogram sample on the job's queue label
- [x] A panicking registered worker is counted as failed (it reaches the middleware as an
      ordinary error via the existing inner recover)
- [x] The in-flight gauge returns to zero after execution and never exceeds concurrency
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(metrics): record job outcomes and duration from the chain`

---

### T8: Install the metrics middleware on the client

**What**: `Config.MetricsRegistry`, metric-set construction, and chain insertion.
**Where**: `client.go`, `client_test.go`, `middleware_test.go`
**Depends on**: T7
**Reuses**: the chain composition at `client.go:244-247`
**Requirement**: OBS-05, OBS-12

**Done when**:
- [x] `Config.MetricsRegistry *prometheus.Registry`; nil yields a fresh private registry
- [x] The middleware sits immediately inside `Logging` and ahead of `Config.Middleware`,
      asserted by an ordering test, not by reading the source
- [x] A client configured with user middleware still records metrics (spec OBS-03.6)
- [x] Two clients constructed in one process do not collide (EDGE-07)
- [x] Gate passes: `go test -race ./...`, plus lint (phase close)

**Tests**: unit
**Gate**: quick
**Commit**: `feat(metrics): install job metrics on every client`

---

### T9: Build the stats refresher

**What**: The component that turns one periodic `Stats` call into gauge values and a
freshness timestamp.
**Where**: `stats.go`, `stats_test.go`
**Depends on**: T5, T6
**Reuses**: `rescueLoop` (`rescue.go:34`) as the interval-loop shape
**Requirement**: OBS-06, OBS-07, OBS-09, OBS-11

**Done when**:
- [x] `run` refreshes once immediately before its first tick (D-7 depends on this)
- [x] Configured queues are seeded to zero for every published state and for age, so an
      idle configured queue reads zero rather than missing (EDGE-02)
- [x] A queue present only in the database is published under its own label (EDGE-03)
- [x] A queue that drains away has its series deleted, and a test proves no `Reset()`-style
      window exists where a surviving series is momentarily absent
- [x] A failing refresh logs a warning, leaves previous gauge values untouched, and does
      not advance the freshness timestamp (OBS-09)
- [x] `fresh` is a pure function of recorded state and is tested directly at, inside, and
      beyond the staleness bound
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(metrics): refresh queue gauges on an interval`

---

### T10: Run the refresher under the client lifecycle

**What**: `Config.StatsInterval` validation plus refresher start/stop wiring.
**Where**: `client.go`, `pool.go`, `client_test.go`, `pool_test.go`
**Depends on**: T9
**Reuses**: the rescuer's launch (`pool.go:113`) and `r.background` join
**Requirement**: OBS-08, OBS-09

**Done when**:
- [x] `Config.StatsInterval`: zero takes the default silently; explicitly non-positive warns
      and takes the default (EDGE-04, AD-037)
- [x] The refresher starts with the runner and is joined by `Stop`; a goleak lifecycle test
      proves no goroutine outlives `Stop` (EDGE-05)
- [x] A client that is never started issues no `Stats` call (EDGE-09)
- [x] A test drives many gathers within one interval and asserts the driver's `Stats` call
      count did not rise — this is OBS-08, the cycle's headline property, and it must be
      sensed by counting calls, not by inspecting the code
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(metrics): refresh queue gauges for the running client`

---

### T11: Verify the gauges against a real database

**What**: An integration test asserting published gauge values match inserted data.
**Where**: `client_integration_test.go` (or a new `metrics_integration_test.go`)
**Depends on**: T10
**Reuses**: `testdb.NewDB` (`internal/testdb/testdb.go:64`)
**Requirement**: OBS-06, OBS-07

**Done when**:
- [x] Jobs of known states and known wait ages are inserted, one refresh is awaited, and the
      gathered gauge values match
- [x] A queue with only future-scheduled jobs reports age `0`, not the delay (ASM-08)
- [x] A dead job is reflected in `drover_queue_depth{state="dead"}`
- [x] Gate passes: `go test -race ./... && go test -race -tags=integration ./...`, plus lint (phase close)

**Tests**: integration
**Gate**: full
**Commit**: `test(metrics): verify queue gauges against Postgres`

---

### T12: Build the ops server

**What**: The mux, the three handlers, and a shutdown that genuinely joins the serving
goroutine.
**Where**: `ops.go`, `ops_test.go`
**Depends on**: T6
**Requirement**: OBS-01, OBS-10, OBS-11

**Done when**:
- [x] `/metrics` serves the supplied registry in Prometheus text format
- [x] `/healthz` returns `200` without consulting anything (OBS-10.1)
- [x] `/readyz` returns `200` when the readiness function returns nil and `503` with the
      reason in the body when it does not
- [x] An unknown path returns `404` (EDGE-06)
- [x] `shutdown` returns only after the serving goroutine has returned — a goleak test
      proves it, since `http.Server.Shutdown` alone does not join `Serve`
- [x] Tests bind `127.0.0.1:0`, never a fixed port
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(ops): serve metrics and health endpoints`

---

### T13: Run the ops server under the client lifecycle

**What**: `Config.OpsAddr`, eager bind in `Start`, and shutdown as the final drain step.
**Where**: `client.go`, `loop.go`, `pool.go`, `client_test.go`, `pool_test.go`
**Depends on**: T10, T12
**Requirement**: OBS-01, OBS-02, OBS-11

**Done when**:
- [x] An empty `OpsAddr` binds nothing and starts no goroutine (OBS-01.2)
- [x] An unbindable address makes `Start` return an error naming it, and leaves the client
      startable again afterwards — nothing partially started (OBS-01.4, D-6)
- [x] The address is bindable again after `Stop` (OBS-01.3)
- [x] `/readyz` reports `503` from the instant `Stop` is called, not one staleness bound
      later (OBS-11.4)
- [x] The ops server is shut down after the workers drain, so `/readyz` and `/metrics`
      answer throughout the drain (D-10)
- [x] A goleak lifecycle test with the ops server enabled passes
- [x] Gate passes: `go test -race ./...`

**Tests**: unit
**Gate**: quick
**Commit**: `feat(ops): start and stop the ops server with the client`

---

### T14: Verify health endpoints against a real database

**What**: An integration test proving `/healthz` and `/readyz` diverge under database loss.
**Where**: `client_integration_test.go` (or `ops_integration_test.go`)
**Depends on**: T13
**Reuses**: `testdb.NewDB`
**Requirement**: OBS-10, OBS-11

**Done when**:
- [x] Both endpoints return `200` against a healthy database
- [x] After the database becomes unreachable, `/readyz` returns `503` while `/healthz`
      still returns `200` — the property that justifies having two endpoints at all
- [x] Gate passes: `go test -race ./... && go test -race -tags=integration ./...`, plus lint (phase close)

**Tests**: integration
**Gate**: full
**Commit**: `test(ops): verify health endpoints diverge on database loss`

---

### T15: Record the observability architecture decision [P]

**What**: `ADR-0005`, defending the stack on technical merit.
**Where**: `docs/adr/0005-*.md`
**Depends on**: T14
**Reuses**: ADR-0003's structure
**Requirement**: OBS-13

**Done when**:
- [x] Covers: Prometheus as the backend and what that commits the public API to; the
      refresher-versus-collect-on-scrape choice and why database load must not track scrape
      rate; the ops-port split; readiness derived from refresh freshness; and what is
      deliberately excluded (tracing, dashboards, push gateway)
- [x] Argues on technical merit only — the repository is public
- [x] Linked from the README's Documentation section
- [x] Gate passes: `go build ./... && go vet ./...`

**Tests**: none
**Gate**: build
**Commit**: `docs(adr): record the observability stack decision`

---

### T16: Document the operator surface [P]

**What**: README and package-doc updates.
**Where**: `README.md`, `doc.go`
**Depends on**: T14
**Requirement**: OBS-13

**Done when**:
- [ ] Every metric family listed with type, labels, and meaning
- [ ] `drover_jobs_failed_total` explicitly described as counting attempts, not deaths, with
      `drover_queue_depth{state="dead"}` named as the permanent-failure signal
- [ ] `completed` and `cancelled` documented as deliberately absent from the depth gauge
- [ ] A concrete alerting expression over `drover_oldest_job_age_seconds`, with a sentence
      on why it is the recommended primary alert
- [ ] The configuration snippet compiles against the package as shipped — verify by
      building it, not by reading it (this exact class of error escaped a prior cycle)
- [ ] `doc.go:123`'s "Metrics are not implemented yet" removed
- [ ] The roadmap line at `README.md:80` reflects observability as shipped
- [ ] Gate passes: `go build ./... && go vet ./...`

**Tests**: none
**Gate**: build
**Commit**: `docs: document the metrics and health surface`

---

### T17: Record the cycle's decisions in project state [P]

**What**: `AD-038`–`AD-048` and an updated handoff.
**Where**: `.specs/STATE.md`
**Depends on**: T14
**Requirement**: —

**Done when**:
- [ ] One row per decision D-1…D-11 from `context.md`, in the established one-line style
      with its source
- [ ] `## Handoff` rewritten for this cycle: what shipped, what review should look at, known
      weak sensors, and the next cycle
- [ ] The unowned terminal-row retention gap from the design's risk table is carried forward
      explicitly — no roadmap row owns it
- [ ] `## Roadmap progress` is **not** touched here; it needs the PR number and merge date
- [ ] Gate passes: `go build ./... && go vet ./...`

**Tests**: none
**Gate**: build
**Commit**: `docs(planning): record the observability cycle decisions`

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1 | 1 migration file | ✅ Granular |
| T2 | 2 queries + regenerate | ✅ Granular (one cohesive change; generated output is not hand-written) |
| T3 | 3 types + 1 method + tests | ✅ Granular (types have no meaning apart from the method) |
| T4 | 1 method + tests | ✅ Granular |
| T5 | 1 interface line | ✅ Granular |
| T6 | 1 type + its constructor + accessors | ✅ Granular |
| T7 | 1 middleware | ✅ Granular |
| T8 | 1 config field + 1 wiring site | ✅ Granular |
| T9 | 1 component | ✅ Granular |
| T10 | 1 config field + lifecycle wiring | ✅ Granular |
| T11 | 1 test file | ✅ Granular |
| T12 | 1 component | ✅ Granular |
| T13 | 1 config field + lifecycle wiring | ✅ Granular |
| T14 | 1 test file | ✅ Granular |
| T15 | 1 document | ✅ Granular |
| T16 | 2 documents, one change | ✅ Granular (cohesive: the same facts in both places) |
| T17 | 1 document | ✅ Granular |

## Diagram-Definition Cross-Check

| Task | Depends On (body) | Diagram shows | Status |
| --- | --- | --- | --- |
| T1 | None | phase head | ✅ Match |
| T2 | T1 | T1 → T2 | ✅ Match |
| T3 | None (sequenced) | T2 → T3 | ✅ Match — the arrow is history ordering, not a build dependency; noted in the task body |
| T4 | T2, T3 | T3 → T4 | ✅ Match (T2 dependency is transitive through T3's position) |
| T5 | T3, T4 | T4 → T5 | ✅ Match |
| T6 | None | phase head | ✅ Match |
| T7 | T6 | T6 → T7 | ✅ Match |
| T8 | T7 | T7 → T8 | ✅ Match |
| T9 | T5, T6 | phase head (cross-phase deps satisfied by phase order) | ✅ Match |
| T10 | T9 | T9 → T10 | ✅ Match |
| T11 | T10 | T10 → T11 | ✅ Match |
| T12 | T6 | phase head (cross-phase dep satisfied by phase order) | ✅ Match |
| T13 | T10, T12 | T12 → T13 | ✅ Match |
| T14 | T13 | T13 → T14 | ✅ Match |
| T15–T17 | T14 | three parallel arrows, no arrows between them | ✅ Match — they touch three disjoint files |

## Test Co-location Validation

| Task | Layer created/modified | Matrix requires | Task says | Status |
| --- | --- | --- | --- | --- |
| T1 | Migration | integration | integration | ✅ OK |
| T2 | Generated sqlc output | none | none | ✅ OK |
| T3 | `internal/memdriver` | unit | unit | ✅ OK |
| T4 | `internal/pgdriver` | integration | integration | ✅ OK |
| T5 | Interface declaration only | none | none | ✅ OK — no behaviour; both implementations and their tests already exist and are unchanged |
| T6 | Root-package logic | unit | unit | ✅ OK |
| T7 | Root-package logic | unit | unit | ✅ OK |
| T8 | Root-package logic | unit | unit | ✅ OK |
| T9 | Root-package logic | unit | unit | ✅ OK |
| T10 | Lifecycle | unit + goleak | unit | ✅ OK — goleak required explicitly in Done-when |
| T11 | End-to-end | integration | integration | ✅ OK |
| T12 | Root-package logic + lifecycle | unit + goleak | unit | ✅ OK — goleak required explicitly in Done-when |
| T13 | Lifecycle | unit + goleak | unit | ✅ OK — goleak required explicitly in Done-when |
| T14 | End-to-end | integration | integration | ✅ OK |
| T15–T17 | Docs | none | none | ✅ OK |

---

## Requirement Coverage

| Requirement | Tasks |
| --- | --- |
| OBS-01 | T12, T13 |
| OBS-02 | T13 |
| OBS-03 | T6, T7 |
| OBS-04 | T6, T7 |
| OBS-05 | T7, T8 |
| OBS-06 | T2, T3, T4, T9, T11 |
| OBS-07 | T1, T2, T3, T4, T9, T11 |
| OBS-08 | T10 |
| OBS-09 | T9, T10 |
| OBS-10 | T12, T14 |
| OBS-11 | T9, T12, T13, T14 |
| OBS-12 | T6, T7, T8 |
| OBS-13 | T15, T16 |

13 of 13 requirements mapped.
