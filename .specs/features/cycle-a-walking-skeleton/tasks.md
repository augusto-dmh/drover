# Cycle A — Walking Skeleton Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute flow and Critical Rules.** Do not search for skill files by filesystem path. If the skill cannot be activated, STOP.

**Design**: `.specs/features/cycle-a-walking-skeleton/design.md`
**Status**: Done — all tasks committed; validation in `validation.md`

| Task | Commit | Status |
|---|---|---|
| T1 core types | b4572c5 | ✅ |
| T2 driver contract | 96d1223 | ✅ |
| T3 memdriver | 3f9fb88 | ✅ |
| T4 migrations + migrator | dd8a6e4 | ✅ |
| T5 pgdriver (pgx+sqlc) | a974c6a | ✅ |
| T6 workers registry | be8fc45 | ✅ |
| T7 client + enqueue | 375f452 | ✅ |
| T8 worker loop | 0fd5c55 | ✅ |
| T9 e2e + docs | 8edca28 (+ d33519b goleak fix) | ✅ |
| T10 CI + lint | 2212c13 (+ e4440c7 lint fixes) | ✅ |
| Validation fixes (iteration 1) | 063bd2e | ✅ |

## Test Coverage Matrix

> Guidelines found: `CLAUDE.md` (Workflow section: table-driven, `t.Parallel`, `-race` always, testcontainers-go for integration, unit suite Docker-free via memdriver), `docs/adr/0004` — matrix conforms to them. No existing tests (greenfield).

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
|---|---|---|---|---|
| Public API logic (`client.go`, `worker.go`) | unit (memdriver) | All branches; 1:1 to spec ACs; every listed edge case | `*_test.go` (root pkg) | `go test ./...` |
| `internal/memdriver` | unit | All driver methods + transition guards + concurrent use (`-race`) | `internal/memdriver/*_test.go` | `go test ./...` |
| `internal/pgdriver` + `internal/migrate` | integration | Key query paths + error paths + concurrent-claim exactly-once + migrator idempotence | `*_integration_test.go` with `//go:build integration` | `go test -race -tags=integration ./...` |
| End-to-end (enqueue→work→completed) | integration | Happy path + failure paths + 2-loop concurrency | root `*_integration_test.go` | `go test -race -tags=integration ./...` |
| Types/config/schema files, CI config | none | build gate only | — | `go build ./... && go vet ./...` |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
|---|---|---|---|
| unit (root + memdriver) | Yes | per-test in-memory driver instance, no shared state | design: memdriver constructed per test |
| integration | No (sequential per package) | one testcontainer per package via `TestMain`; tests share the DB and truncate `drover_jobs` in setup | greenfield default: sequential until per-schema isolation exists |

## Gate Check Commands

| Gate Level | When to Use | Command |
|---|---|---|
| Quick | after unit-only tasks | `go test -race ./...` |
| Full | after integration-bearing tasks | `go test -race ./... && go test -race -tags=integration ./...` |
| Build | config/types-only tasks, phase close | `go build ./... && go vet ./...` (plus Full at phase close) |

## Execution Plan

### Phase 1: Types and contract (sequential)
```
T1 → T2 → T3
```
### Phase 2: Storage (sequential)
```
T3 → T4 → T5
```
### Phase 3: API, loop, hardening (sequential; T6 [P] with T4–T5 conceptually but executed in order)
```
T5 → T6 → T7 → T8 → T9 → T10
```

## Task Breakdown

### T1: Core types and sentinel errors
**What**: `job.go` (`JobArgs`, `JobState` consts for all 7 states, `Job[T]`), `errors.go` (`ErrInvalidKind`), `doc.go` stub.
**Where**: root package
**Depends on**: None · **Requirement**: CORE-02, CORE-03 (types)
**Done when**: build gate passes; states match AD-002 exactly.
**Tests**: none (types only) · **Gate**: build
**Commit**: `feat(core): add job types, states, and sentinel errors`

### T2: Driver contract
**What**: `internal/driver` — `Driver` interface, `JobRow`, `InsertParams`, `ErrInvalidTransition`, error-detail struct (`AttemptError{Attempt, At, Error, Trace}`).
**Where**: `internal/driver/driver.go`
**Depends on**: T1 · **Requirement**: CORE-01..03 (contract)
**Done when**: build gate passes; interface has exactly the 6 methods from design (AD-008).
**Tests**: none (interface) · **Gate**: build
**Commit**: `feat(driver): define narrow storage contract`

### T3: In-memory driver
**What**: `internal/memdriver` implementing `Driver` with mutex-guarded map, transition guards, FIFO-by-id fetch honoring `scheduled_at`, `InsertTx` returning documented unsupported error.
**Where**: `internal/memdriver/memdriver.go` + tests
**Depends on**: T2 · **Requirement**: CORE-03 (test substrate)
**Done when**: table-driven unit tests cover every method, every transition guard (valid + invalid), concurrent fetch exactly-once under `-race`; quick gate passes.
**Tests**: unit · **Gate**: quick
**Commit**: `feat(memdriver): in-memory driver for the unit suite`

### T4: Migrations and migrator
**What**: `internal/migrate` (embed.FS, transactional apply, `drover_schema_version`), migration `001_create_jobs.sql` per design DDL, root `Migrate(ctx, pool)`; `internal/testdb` helper (testcontainers `TestMain` pattern, truncate-per-test).
**Where**: `internal/migrate/`, `migrate.go`, `internal/testdb/`
**Depends on**: T2 · **Requirement**: CORE-01
**Done when**: integration tests prove CORE-01 AC1–3 (fresh apply, idempotent re-run, exact column set); full gate passes.
**Tests**: integration · **Gate**: full
**Commit**: `feat(migrate): embedded migrations with version tracking`

### T5: Postgres driver (pgx + sqlc)
**What**: `sqlc.yaml`, `internal/pgdriver/queries.sql` (insert, claim with `FOR UPDATE SKIP LOCKED`, complete, dead), committed `internal/dbsqlc` codegen, `internal/pgdriver` implementing `Driver` incl. `InsertTx` on `pgx.Tx`.
**Where**: `internal/pgdriver/`, `internal/dbsqlc/`
**Depends on**: T4 · **Requirement**: CORE-02, CORE-03
**Done when**: integration tests cover each method's happy + error path AND a concurrent-claim test (2 goroutines × 25 jobs, zero double-claims, `-race`); full gate passes.
**Tests**: integration · **Gate**: full
**Commit**: `feat(pgdriver): pgx+sqlc driver with SKIP LOCKED claiming`

### T6: Workers registry and type erasure
**What**: `worker.go` — `Worker[T]`, `WorkerDefaults[T]`, `Workers`, `Register[T]` building type-erased `workFunc`, duplicate-kind panic (AD-007), decode-failure surfacing.
**Where**: root package
**Depends on**: T1 · **Requirement**: CORE-04 AC1, CORE-03 AC1
**Done when**: unit tests cover registration, duplicate panic (message names kind), decode success/failure through the erased func; quick gate passes.
**Tests**: unit · **Gate**: quick
**Commit**: `feat(worker): typed worker registry with type-erased dispatch`

### T7: Client construction and enqueue
**What**: `client.go` — `Config` (defaults), `NewClient` (nil-pool error), internal driver injection, `Insert`/`InsertTx` (kind validation, marshal, wrapped errors).
**Where**: root package
**Depends on**: T3, T6 · **Requirement**: CORE-02
**Done when**: unit tests (memdriver) cover CORE-02 AC1,4,5 + nil-pool edge; integration test covers AC2–3 (commit/rollback visibility); full gate passes.
**Tests**: unit + integration · **Gate**: full
**Commit**: `feat(client): transactional typed enqueue`

### T8: Worker loop
**What**: `Start(ctx)` poll loop — claim via `FetchAvailable(queue,1)`, dispatch, finalize (`completed`/`dead` per AD-003), panic recovery with stack, unregistered-kind handling, ctx-cancel drain, `PollInterval` idle wait, DB-error log-and-retry (AD-006), slog records (CORE-04 AC2–3).
**Where**: root package (`client.go` / `loop.go`)
**Depends on**: T7 · **Requirement**: CORE-03, CORE-04
**Done when**: unit tests (memdriver, short intervals) cover AC1,3–7 + decode-failure and DB-error edges; goleak asserts clean shutdown; quick gate passes.
**Tests**: unit · **Gate**: quick
**Commit**: `feat(loop): claim-and-execute worker loop with panic recovery`

### T9: End-to-end proof and package docs
**What**: root integration test (50 jobs, 2 concurrent `Start` loops, 50 distinct executions, all `completed`; plus failure-path e2e), `example_test.go` (register→insert→work), `doc.go` completed (at-least-once contract, Cycle A limitations per D-5).
**Where**: root package
**Depends on**: T5, T8 · **Requirement**: CORE-03 AC2 (e2e), spec success criteria
**Done when**: full gate passes; example builds as testable example.
**Tests**: integration · **Gate**: full
**Commit**: `test(e2e): prove exactly-once claiming across concurrent loops`

### T10: CI and lint floor
**What**: `.github/workflows/ci.yml` (unit job: `go test -race ./...` + `go vet`; integration job with Docker: `-tags=integration`; sqlc-drift check), `.golangci.yml` (v2, set per ADR-0004).
**Where**: repo root / `.github/`
**Depends on**: T9 · **Requirement**: spec success criteria
**Done when**: `golangci-lint run` passes locally; workflow YAML is valid; build gate passes.
**Tests**: none (config) · **Gate**: build
**Commit**: `ci: add unit, integration, lint, and sqlc-drift gates`

## Diagram-Definition Cross-Check

| Task | Depends On (body) | Diagram | Status |
|---|---|---|---|
| T1 | none | start of Phase 1 | ✅ |
| T2 | T1 | T1→T2 | ✅ |
| T3 | T2 | T2→T3 | ✅ |
| T4 | T2 | T3→T4 (phase order; dep satisfied) | ✅ |
| T5 | T4 | T4→T5 | ✅ |
| T6 | T1 | T5→T6 (phase order; dep satisfied) | ✅ |
| T7 | T3, T6 | T6→T7 | ✅ |
| T8 | T7 | T7→T8 | ✅ |
| T9 | T5, T8 | T8→T9 | ✅ |
| T10 | T9 | T9→T10 | ✅ |

## Test Co-location Validation

| Task | Layer | Matrix Requires | Task Says | Status |
|---|---|---|---|---|
| T1 | types | none | none | ✅ |
| T2 | interface | none | none | ✅ |
| T3 | memdriver | unit | unit | ✅ |
| T4 | migrate (integration layer) | integration | integration | ✅ |
| T5 | pgdriver | integration | integration | ✅ |
| T6 | public API logic | unit | unit | ✅ |
| T7 | public API logic (+tx paths) | unit + integration | unit + integration | ✅ |
| T8 | public API logic | unit | unit | ✅ |
| T9 | e2e | integration | integration | ✅ |
| T10 | CI config | none | none | ✅ |

## Task Granularity Check

All tasks = one package or one cohesive file-set with co-located tests; none spans unrelated components. T4 bundles migrator + testdb helper deliberately (helper is unusable earlier and required by T4's own tests — merge-backward rule). ✅
