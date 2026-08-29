# Periodic Jobs + Unique Jobs Tasks

## Execution Protocol (MANDATORY — do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path.

**If the skill cannot be activated, STOP and tell the orchestrator — do not proceed without it.**

---

**Spec**: `.specs/features/cycle-h-periodic-jobs/spec.md`
**Design**: `.specs/features/cycle-h-periodic-jobs/design.md`
**Context**: `.specs/features/cycle-h-periodic-jobs/context.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase, project guidelines, and spec. Guidelines found: `CLAUDE.md`
> (table-driven, `t.Parallel`, `-race`, testcontainers, goleak on lifecycle, unit suite
> without Docker, `testing/synctest` for time-dependent scheduler, fuzz cron parsing),
> `.github/workflows/ci.yml`, `.golangci.yml`.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| Unique insert (client + memdriver) | unit | Every UNIQ AC memdriver can see; `errors.Is` on `ErrDuplicateJob`; InsertMany all-or-nothing; terminal state frees the key | `client_test.go`, `internal/memdriver/*_test.go` | `go test -race ./...` |
| Unique insert (pgdriver) | integration | Same plus two concurrent inserts; unique index; COPY path still used | `internal/pgdriver/*_integration_test.go`, `client_integration_test.go` | `go test -race -tags=integration ./...` |
| `internal/cron` | unit + fuzz | 5-field Next, `@every` alignment, 0/7 Sunday, parse errors, no panic; FuzzParse | `internal/cron/*_test.go` | `go test -race ./internal/cron/...` |
| Scheduler / leadership | unit | Empty PeriodicJobs starts no scheduler; first tick strictly after Start; duplicate tick is success; Stop does not wait next fire (upper bound); panic on bad config; goleak | `scheduler_test.go` / `pool_test.go`, `client_test.go` | `go test -race ./...` |
| Two-client lock | integration | At most one leader; lock drop lets the other enqueue; Start succeeds when lock fails | `*_integration_test.go` | `go test -race -tags=integration ./...` |
| README / example | Example compile | UniqueKey and PeriodicJobs named | `example_test.go`, `examples/email` | `go test -race ./...` |
| Generated sqlc | none | Build + CI drift check | — | build gate |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
| --- | --- | --- | --- |
| Root / memdriver / cron unit | Yes | Fresh memdriver / Parse per test | Existing suite uses `t.Parallel` |
| pgdriver / client integration | Yes | `testdb.NewDB(t)` per test | `internal/testdb` |
| Two-client lock | Yes | Two clients, one testdb | New, same isolation |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After unit-only tasks | `go test -race ./...` |
| Full | After storage / lock / unique-index paths | `go test -race ./... && go test -race -tags=integration ./...` |
| Build | Phase completion | `go build ./... && go vet ./...` |

**Before each phase-closing commit**, run
`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run`.
**If any `.sql` file changed**, run
`go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate` and commit `internal/dbsqlc`.

**Environment**: Docker is required for pgdriver unique-index and two-client
lock tests. Probe `docker ps` before those tasks; if it is down, stop and tell
the user to start Docker Desktop rather than debugging further.

**Commit hygiene**: scoped Conventional Commit subjects (`feat(pgdriver): …`);
no task/decision/cycle IDs; no AI attribution. After each commit inspect
`git log -1 --format=%B` and strip any `Co-authored-by` / generated-with trailer
before continuing.

---

## Execution Plan

Phase 1 and Phase 2 are independent and may run in parallel.

### Phase 1: Unique storage (Sequential)

```
T1 → T2 → T3 → T4 → T5
```

### Phase 2: Cron parser (Sequential, parallel with Phase 1)

```
T6
```

### Phase 3: Client unique API (Sequential)

```
T7 → T8
```

### Phase 4: Scheduler + leadership (Sequential)

```
T9 → T10 → T11
```

### Phase 5: Docs (Sequential)

```
T12
```

---

## Phase 1: Unique storage

### T1: Migration + Insert SQL + sqlc

**What**: Migration `004` adds `unique_key text` and the partial unique
index. `InsertJob` and `InsertJobsFromStaging` persist `unique_key`.
Staging table DDL (`insertBatchDDL` and `sqlc_schema.sql`) gains the
column. `InsertParams` / `JobRow` in `internal/driver` gain `UniqueKey`.
Regenerate sqlc.
**Where**: `internal/migrate/migrations/004_unique_jobs.sql`,
`internal/pgdriver/queries.sql`, `internal/pgdriver/sqlc_schema.sql`,
`internal/pgdriver/pgdriver.go` (DDL string), `internal/driver/driver.go`,
`internal/dbsqlc/`
**Depends on**: None
**Reuses**: InsertJob CASE (database clock)
**Requirement**: UNIQ-01

**Done when**:

- [x] Migration is applied by the existing migrator (next version)
- [x] Generated sqlc is committed and `sqlc generate` is drift-free
- [x] Staging SELECT/INSERT still uses the database clock for state

**Tests**: none (implementations in T2/T3)
**Gate**: build
**Commit**: `feat(migrate): add unique key column and partial unique index`

### T2: memdriver unique insert + unit tests

**What**: Enforce uniqueness among non-terminal rows on Insert and
InsertMany. Empty UniqueKey does not participate. InsertMany: collision
with existing or in-batch duplicate fails the whole batch with a driver
error the client will map (use `driver` sentinel or a new
`driver.ErrDuplicateJob` that the client wraps). InsertManyTx unchanged
(`ErrTxUnsupported`).
**Where**: `internal/memdriver/memdriver.go`, `internal/memdriver/memdriver_test.go`
**Depends on**: T1
**Reuses**: existing mutex / Insert
**Requirement**: UNIQ-01, UNIQ-02, UNIQ-03

**Done when**:

- [x] Tests: first insert ok; duplicate while available/scheduled/retryable/running fails; completed/cancelled/dead frees the key; empty key allows many rows; InsertMany in-batch duplicate inserts zero
- [x] JobRow.UniqueKey is populated
- [x] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(memdriver): reject duplicate unique keys`

### T3: pgdriver unique insert + integration tests

**What**: Pass UniqueKey through Insert/InsertMany COPY. Translate
Postgres unique_violation on the new index to the driver duplicate
error. Concurrent inserts: at most one row.
**Where**: `internal/pgdriver/pgdriver.go`,
`internal/pgdriver/pgdriver_integration_test.go`
**Depends on**: T1
**Reuses**: `testdb.NewDB`, existing Insert/InsertMany
**Requirement**: UNIQ-01, UNIQ-02, UNIQ-03

**Done when**:

- [x] Integration: persist key, duplicate → duplicate error, terminal frees key, concurrent pair, InsertMany collision rolls back all, COPY still used for N>1
- [x] Full gate green (Docker required)

**Tests**: integration
**Gate**: full
**Commit**: `feat(pgdriver): enforce unique jobs on insert`

### T4: Client-visible driver error type

**What**: If not done in T2/T3, export `driver.ErrDuplicateJob` (or
equivalent) so T7 can wrap it with `drover.ErrDuplicateJob` via
`errors.Is` / `%w`.
**Where**: `internal/driver/driver.go`
**Depends on**: T2, T3
**Requirement**: UNIQ-02

**Done when**:

- [x] Both adapters return an error that `errors.Is` recognizes as the driver duplicate sentinel
- [x] Build green

**Tests**: covered by T2/T3
**Gate**: build
**Commit**: `feat(driver): add duplicate unique-key sentinel`

(Skip the commit if the sentinel already landed in T2 — do not create an
empty commit. Note the skip in the phase summary.)

### T5: Phase 1 lint close

**What**: Full lint; fix issues from T1–T4.
**Depends on**: T2, T3, T4
**Done when**: lint clean; `go build ./... && go vet ./...` ✅
**Tests**: none
**Gate**: build
**Commit**: `fix(pgdriver): satisfy lint on unique insert`

(Skip the commit if lint is already clean.)

---

## Phase 2: Cron parser

### T6: Parse 5-field and @every; fuzz

**What**: `internal/cron` with `Parse`/`ParseIn`, `Schedule.Next` strictly
after `t`. 5-field numeric with `*`, lists, ranges, steps; dow 0 and 7
are Sunday. `@every` positive duration, Unix-epoch aligned. Invalid
specs error without panic. `FuzzParse` with seed corpus.
**Where**: `internal/cron/`
**Depends on**: None
**Requirement**: CRON-01, CRON-02

**Done when**:

- [x] Table tests: hourly `0 * * * *`, `*/5`, `@every 30s` alignment, Sunday 0/7, bad arity, non-positive duration
- [x] Next is strictly after t (never equal)
- [x] FuzzParse exists
- [x] Quick gate green for the package

**Tests**: unit + fuzz (fuzz need not run to completion in the gate;
`go test` compile is enough, CI may skip long fuzz)
**Gate**: quick
**Commit**: `feat(cron): parse five-field specs and every-durations`

---

## Phase 3: Client unique API

### T7: InsertOpts.UniqueKey + ErrDuplicateJob

**What**: Public `ErrDuplicateJob`, `InsertOpts.UniqueKey`,
`JobRow.UniqueKey`. `insertParamsFor` passes the key. Client Insert /
InsertMany / Tx variants wrap driver duplicate as
`fmt.Errorf("…: %w", ErrDuplicateJob)` so `errors.Is` works.
**Where**: `errors.go`, `client.go`, `client_test.go`,
`client_integration_test.go`
**Depends on**: T2, T3, T6 is not required
**Reuses**: `insertParamsFor`, `rowFromDriver`
**Requirement**: UNIQ-01, UNIQ-02, UNIQ-03

**Done when**:

- [x] Unit tests through `newClient`+memdriver for every unique AC except concurrency
- [x] Integration: concurrent Insert unique key
- [x] Full gate if integration added; else quick

**Tests**: unit + integration
**Gate**: full
**Commit**: `feat(client): reject duplicate unique keys at insert`

### T8: Phase 3 lint close

**What**: lint + vet
**Depends on**: T7
**Gate**: build
**Commit**: `fix(client): satisfy lint on unique insert`

(Skip if clean.)

---

## Phase 4: Scheduler + leadership

### T9: PeriodicJob config + construction panics

**What**: `PeriodicJob` type, `Config.PeriodicJobs`. `newClient` panics
on empty/duplicate ID, nil Args, empty kind, unparseable Cron. Empty
slice is fine. Store parsed schedules on the client. Do not start the
loop yet if that keeps the commit atomic — or start a stub that returns
immediately when `fetchCtx` is done. Prefer: validation only in this
task if T10 is the loop; the client must still compile.
**Where**: `client.go`, `client_test.go`, new `periodic.go` as needed
**Depends on**: T6, T7
**Reuses**: AD-037 panic style (nil middleware / empty queue name)
**Requirement**: PER-01

**Done when**:

- [x] Tests: panics on bad ID/cron/args; accepts valid slice; empty slice does not panic
- [x] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(client): validate periodic job registration`

### T10: Scheduler loop + memdriver leadership

**What**: When PeriodicJobs is non-empty, `runner.start` launches a
scheduler on `fetchCtx`. memdriver: always leader. Insert each due tick
with UniqueKey `id/RFC3339UTC`, ScheduledAt = fire, overwrite
`PeriodicJob.Opts.UniqueKey`. First fire strictly after Start.
`ErrDuplicateJob` is success. Stop/fetchCtx must beat a long next-fire
(elapsed upper bound). goleak on Start/Stop with periodic jobs.
**Where**: `scheduler.go` (or similar), `pool.go`, `*_test.go`
**Depends on**: T9
**Reuses**: `Client.Insert`, fetchCtx, background WaitGroup
**Requirement**: PER-02, PER-04

**Done when**:

- [x] Empty PeriodicJobs: no extra goroutine (goleak vs baseline)
- [x] `@every` or a 5-field spec with synctest/fake time: exactly one insert per tick
- [x] Duplicate Insert is not logged as a handler failure
- [x] Stop with a far-future next fire returns well under that interval
- [x] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(client): enqueue periodic jobs from a leader loop`

### T11: pgdriver advisory lock + two-client integration

**What**: `leaderLocker` on pgdriver: dedicated connection,
`pg_try_advisory_lock` with the documented int64, `ReleaseLeader`
closes the conn. Scheduler uses it when the driver implements the
interface. Integration: two clients, one row per tick; closing leader
lets the other continue. Lock acquire failure does not fail Start.
**Where**: `internal/pgdriver/pgdriver.go`, `pool.go`/`scheduler.go`,
`*_integration_test.go`
**Depends on**: T10
**Reuses**: LISTEN dedicated-conn pattern
**Requirement**: PER-03

**Done when**:

- [x] Two-client test: at most one holder; failover enqueue
- [x] Lock is not taken from the pool (session lock would otherwise leak)
- [x] Full gate green
- [x] Phase lint clean

**Tests**: integration
**Gate**: full
**Commit**: `feat(pgdriver): elect a periodic scheduler with an advisory lock`

---

## Phase 5: Docs

### T12: README, Example, email example

**What**: Planned API shows UniqueKey and PeriodicJobs. Roadmap blurb
lists periodic jobs as shipped (status page remains "Then:"). Example
in `example_test.go` names the fields. `examples/email` registers at
least one periodic job. Update `.specs/STATE.md` Handoff / requirement
status as needed (planning files may be in this commit).
**Where**: `README.md`, `example_test.go`, `examples/email/main.go`
**Depends on**: T11
**Requirement**: DOC-01

**Done when**:

- [ ] Examples compile (`go test`)
- [ ] email example builds
- [ ] Quick gate green
- [ ] Lint clean

**Tests**: Example compile
**Gate**: quick
**Commit**: `docs(readme): document unique keys and periodic jobs`

---

## Traceability (tasks mapped)

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| UNIQ-01 | Unique AC1, AC4, AC7 | T1–T3, T7 | In Tasks |
| UNIQ-02 | Unique AC2, AC3 | T2–T3, T7 | In Tasks |
| UNIQ-03 | Unique AC5, AC6 | T2–T3, T7 | In Tasks |
| CRON-01 | Cron AC1, AC4 | T6 | In Tasks |
| CRON-02 | Cron AC2, AC3, AC5 | T6 | In Tasks |
| PER-01 | Leader AC1, AC7 | T9 | Done |
| PER-02 | Leader AC2, AC5, AC6 | T10 | Done |
| PER-03 | Leader AC3, AC4, AC8 | T11 | Done |
| PER-04 | Leader AC9 | T10 | Done |
| DOC-01 | Docs AC1–4 | T12 | In Tasks |
