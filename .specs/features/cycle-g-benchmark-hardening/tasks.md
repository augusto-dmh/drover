# Benchmark + Hardening Tasks

## Execution Protocol (MANDATORY — do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path.

**If the skill cannot be activated, STOP and tell the orchestrator — do not proceed without it.**

---

**Spec**: `.specs/features/cycle-g-benchmark-hardening/spec.md`
**Design**: `.specs/features/cycle-g-benchmark-hardening/design.md`
**Context**: `.specs/features/cycle-g-benchmark-hardening/context.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase, project guidelines, and spec. Guidelines found: `CLAUDE.md`
> (table-driven, `t.Parallel`, `-race`, testcontainers, goleak on lifecycle, unit suite
> without Docker), `.github/workflows/ci.yml`, `.golangci.yml`.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| Root `InsertMany` / `InsertManyTx` / `NotifyWakeup` | unit | Every BATCH and WAKE AC that memdriver can see; `errors.Is` on sentinels; elapsed-time upper bound on wake (not a lower-bound count) | `client_test.go`, `pool_test.go`, `example_test.go` | `go test -race ./...` |
| `internal/memdriver` InsertMany | unit | Empty batch, mixed queues/schedules, all-or-nothing validation, InsertManyTx unsupported | `internal/memdriver/*_test.go` | `go test -race ./...` |
| `internal/pgdriver` COPY + NOTIFY + LISTEN | integration | Same insert cases against Postgres; two InsertManyTx in one tx; COPY path used; cross-client notify wake; InsertTx notify only on commit | `internal/pgdriver/*_integration_test.go`, `*_integration_test.go` | `go test -race -tags=integration ./...` |
| `cmd/drover-bench` | unit | Flag validation, usage exit codes, methodology keys present; fake runner so no Docker | `cmd/drover-bench/*_test.go` | `go test -race ./cmd/drover-bench/...` |
| Generated sqlc | none | Build + CI drift check only | — | build gate |
| README | Example compile | Every new public Config/API field named in README has an Example | `example_test.go` | `go test -race ./...` |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
| --- | --- | --- | --- |
| Root / memdriver unit | Yes | Fresh `memdriver.New()` / `newClient` per test | Existing suite uses `t.Parallel` |
| pgdriver / client integration | Yes | `testdb.NewDB(t)` per test | `internal/testdb` |
| cmd/drover-bench unit | Yes | Fake runner, buffer writers, no shared globals | New, following `cmd/drover` |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After unit-only tasks | `go test -race ./...` |
| Full | After storage / LISTEN / bench-against-Postgres paths | `go test -race ./... && go test -race -tags=integration ./...` |
| Build | Phase completion | `go build ./... && go vet ./...` |

**Before each phase-closing commit**, run
`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run`.
**If any `.sql` file changed**, run
`go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate` and commit `internal/dbsqlc`.

**Environment**: Docker is required for pgdriver and for filling the README table.
Probe `docker ps` before those tasks; if it is down, stop and tell the user to
start Docker Desktop rather than debugging further.

---

## Execution Plan

### Phase 1: Storage (Sequential)

```
T1 → T2 → T3 → T4
```

### Phase 2: Client batch API (Sequential)

```
T5 → T6
```

### Phase 3: Notify wake-up (Sequential)

```
T7 → T8 → T9
```

### Phase 4: Bench harness (Sequential)

```
T10
```

### Phase 5: Docs (Sequential)

```
T11
```

---

## Phase 1: Storage

### T1: Driver interface + staging SQL + sqlc

**What**: Add `InsertMany` and `InsertManyTx` to `driver.Driver`. Add
`InsertJobsFromStaging` in `queries.sql` (INSERT SELECT from
`drover_insert_batch` with the same CASE as `InsertJob`, `ORDER BY ord`,
`RETURNING *`). Regenerate sqlc.
**Where**: `internal/driver/driver.go`, `internal/pgdriver/queries.sql`,
`internal/dbsqlc/`
**Depends on**: None
**Reuses**: `InsertParams`, `InsertJob` CASE
**Requirement**: BATCH-01, BATCH-06

**Done when**:

- [x] Interface compiles with both new methods
- [x] Generated sqlc is committed and `sqlc generate` is drift-free
- [x] Staging SELECT uses the database clock for state (AD-035)

**Tests**: none (generated/interface — implementations in T2/T3)
**Gate**: build
**Commit**: `feat(driver): add batch insert methods and staging query`

### T2: memdriver InsertMany + unit tests

**What**: Implement `InsertMany` (atomic append, input order, mixed
queue/schedule) and `InsertManyTx` → `ErrTxUnsupported`. Empty batch is a
no-op success.
**Where**: `internal/memdriver/memdriver.go`, `internal/memdriver/memdriver_test.go`
**Depends on**: T1
**Reuses**: existing `Insert` defaults and mutex
**Requirement**: BATCH-01, BATCH-02, BATCH-03, BATCH-04, BATCH-05

**Done when**:

- [x] Tests cover empty batch, mixed queues, future vs due `ScheduledAt`,
      all-or-nothing (if the driver is asked to insert after a bad params
      slice it must not keep a prefix — client validation is T5, but the
      driver still no-ops on empty)
- [x] `InsertManyTx` is `errors.Is(..., ErrTxUnsupported)`
- [x] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(memdriver): implement atomic batch insert`

### T3: pgdriver COPY FROM path + integration tests

**What**: Implement `InsertMany`/`InsertManyTx` with temp table
`IF NOT EXISTS` + `TRUNCATE` + `CopyFrom` + `InsertJobsFromStaging`.
Zero `ScheduledAt` copied as NULL. Two batches in one caller tx must both
persist.
**Where**: `internal/pgdriver/pgdriver.go`,
`internal/pgdriver/pgdriver_integration_test.go`
**Depends on**: T1
**Reuses**: `InsertTx` tx-type assertion, `rowFromDB`, `testdb.NewDB`
**Requirement**: BATCH-01, BATCH-04, BATCH-05, BATCH-06

**Done when**:

- [x] Integration tests: N rows from one call, input order, scheduled vs
      available via DB clock, rollback of `InsertManyTx`, two
      `InsertManyTx` in one tx, empty batch
- [x] The implementation calls `CopyFrom` (not a loop of `InsertJob`)
- [x] Full gate green

**Tests**: integration
**Gate**: full
**Commit**: `feat(pgdriver): batch-insert jobs with COPY FROM`

### T4: Phase 1 lint close

**What**: Full lint; fix issues from T1–T3.
**Depends on**: T2, T3
**Done when**: lint clean; `go build ./... && go vet ./...`
**Tests**: none
**Gate**: build
**Commit**: `fix(pgdriver): satisfy lint on batch insert`

(Skip the commit if lint is already clean — do not create an empty commit.
Note the skip in the phase summary.)

---

## Phase 2: Client batch API

### T5: Client InsertMany / InsertManyTx

**What**: Export `InsertItem`; implement `InsertMany`/`InsertManyTx` by
validating every item through `insertParamsFor` first (nil Args →
`ErrInvalidKind`), then one driver call. Empty slice: empty result, nil
error, no driver write. Wrap driver errors like `Insert` does.
**Where**: `client.go`, `client_test.go`, `client_integration_test.go`
**Depends on**: T2, T3
**Reuses**: `insertParamsFor`, `rowFromDriver`, `InsertTx` tx plumbing
**Requirement**: BATCH-01, BATCH-02, BATCH-03, BATCH-04, BATCH-05

**Done when**:

- [x] Unit tests: happy path, empty, nil Args, empty kind, marshal failure
      inserts zero, opts defaults, mixed queues
- [x] Integration: `InsertManyTx` visibility follows the caller
      transaction (mirror `TestInsertTxVisibilityFollowsCallerTransaction`)
- [x] Full gate green

**Tests**: unit + integration
**Gate**: full
**Commit**: `feat(client): add InsertMany and InsertManyTx`

### T6: Phase 2 lint close

**What**: Lint + vet after T5.
**Depends on**: T5
**Done when**: lint clean, or skipped if already clean (no empty commit)
**Tests**: none
**Gate**: build
**Commit**: `fix(client): satisfy lint on batch insert API`

---

## Phase 3: Notify wake-up

### T7: NotifyWakeup, nudge, interruptible sleep

**What**: Add `Config.NotifyWakeup` (default false). Allocate a cap-1
`wake` channel. `nudge()` after successful non-tx `Insert`/`InsertMany`
when the flag is set. `runner.sleep` also selects on `wake`. Do not nudge
on the Tx path. When the flag is false, inserts must not notify and sleep
must still wait the timer (existing poll tests remain valid).
**Where**: `client.go`, `pool.go`, `pool_test.go`, `client_test.go`
**Depends on**: T5
**Reuses**: `runner.sleep`, Cycle C stop-vs-poll test
**Requirement**: WAKE-01, WAKE-02

**Done when**:

- [x] With a long `PollInterval` and `NotifyWakeup`, a same-client
      `Insert` after `Start` is claimed in well under the interval
      (elapsed-time **upper bound**)
- [x] `Stop` during idle wait still does not wait out `PollInterval`
- [x] Flag false: no wake on insert (job is claimed only after a poll;
      use a fake clock or a long interval plus a short observation window)
- [x] goleak on the new path
- [x] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(client): wake idle fetch on local insert`

### T8: pgdriver NOTIFY + LISTEN + integration tests

**What**: Implement `Notify` / `NotifyTx` (`pg_notify('drover','')`) and
`ListenWakeups` (dedicated acquired conn, `LISTEN drover`,
`WaitForNotification`, reconnect on drop, exit on ctx). Runner starts
the listen goroutine on `fetchCtx` when the flag is set; `Start` still
succeeds if listen fails (log). Type-assert optional interfaces from the
client; do not add them to `driver.Driver`.
**Where**: `internal/pgdriver/pgdriver.go`, `pool.go`, `loop.go` as needed,
`*_integration_test.go`
**Depends on**: T7
**Reuses**: `runner.background` + `fetchCtx`, AD-047 ordering (ops last)
**Requirement**: WAKE-03, WAKE-04, WAKE-05

**Done when**:

- [x] Two-client integration: producer `Insert` with flag, worker `Start`
      with flag and a long poll interval, job runs well under the interval
- [x] `InsertTx` does not wake listeners until commit
- [x] Listen goroutine is joined (goleak clean on Stop)
- [x] Full gate green

**Tests**: integration
**Gate**: full
**Commit**: `feat(pgdriver): optional LISTEN and NOTIFY fetch wake-up`

### T9: Phase 3 lint close

**What**: Lint + vet after T7–T8.
**Depends on**: T8
**Done when**: lint clean, or skipped if already clean
**Tests**: none
**Gate**: build
**Commit**: `fix(client): satisfy lint on notify wake-up`

---

## Phase 4: Bench harness

### T10: cmd/drover-bench

**What**: Stdlib-flag binary: `--database`/`DATABASE_URL`, `--mode
enqueue|drain`, `--jobs`, `--batch`, `--concurrency`, `--queue`,
`--notify`. Enqueue uses `InsertMany` chunks. Drain starts a no-op pool
and waits until all jobs complete. Prints methodology (GOOS, GOARCH,
NumCPU, Postgres version, flags, no-op caveat) then jobs/sec and, for
drain, p50/p95/p99. Invalid flags exit 2. Not added to GoReleaser.
**Where**: `cmd/drover-bench/`
**Depends on**: T5
**Reuses**: `cmd/drover` flag/exit-code style, `drover.Migrate`,
`NewClient`, `InsertMany`
**Requirement**: BENCH-01, BENCH-02

**Done when**:

- [ ] Unit tests: missing DSN, jobs<1, batch<1, bad mode → exit 2 and no
      insert (inject a fake runner / `open` func)
- [ ] Methodology keys appear on a successful fake enqueue run
- [ ] `go build ./cmd/drover-bench`
- [ ] Quick gate green (`go test -race ./...` includes the new package)

**Tests**: unit
**Gate**: quick
**Commit**: `feat(bench): add drover-bench enqueue and drain harness`

---

## Phase 5: Docs

### T11: README, examples, planning close

**What**: Add `## Benchmarks` with a table filled from a **real**
`drover-bench` run against Docker Postgres (record hardware, version,
flags, caveats). Show `InsertMany` in Planned API. Document
`NotifyWakeup` (including PgBouncer session-pooling requirement). Update
the Roadmap blurb. Add compile-checked Examples for new public names.
Update `.specs/STATE.md` Handoff. Mark spec requirements Verified.
**Where**: `README.md`, `example_test.go`, `.specs/STATE.md`,
`.specs/features/cycle-g-benchmark-hardening/spec.md`
**Depends on**: T8, T10
**Reuses**: `ExampleConfig_observability` pattern
**Requirement**: DOC-01

**Done when**:

- [ ] README table numbers match the harness output captured in the cycle
- [ ] `go test` compiles new Examples
- [ ] Quick gate green
- [ ] Spec success criteria checkboxes updated

**Tests**: unit (Examples)
**Gate**: quick
**Commit**: `docs(readme): publish benchmark table and batch-insert API`

---

## Parallel Execution Map

```
Phase 1 (Sequential):
  T1 → T2 → T3 → T4

Phase 2 (Sequential):
  T5 → T6

Phase 3 (Sequential):
  T7 → T8 → T9

Phase 4 (Sequential):
  T10

Phase 5 (Sequential):
  T11
```

No `[P]` tasks: each phase is a single sequence. T10 depends on T5, not on
T7–T9, but phase workers run phases in order so T10 still follows Phase 3.

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1 Driver + SQL | one interface + one query | ✅ |
| T2 memdriver | one adapter + tests | ✅ |
| T3 pgdriver COPY | one adapter + tests | ✅ |
| T4 lint | phase close | ✅ |
| T5 Client API | one exported pair + tests | ✅ |
| T6 lint | phase close | ✅ |
| T7 wake channel | config + sleep + unit tests | ✅ |
| T8 LISTEN/NOTIFY | pg path + integration | ✅ |
| T9 lint | phase close | ✅ |
| T10 bench binary | one cmd | ✅ |
| T11 docs | README + examples | ⚠️ cohesive docs close |

---

## Diagram-Definition Cross-Check

| Task | Depends On (body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | none | Phase 1 start | ✅ |
| T2 | T1 | T1 → T2 | ✅ |
| T3 | T1 | T1 → T3 via T2 sequence (T3 listed after T2; both need T1). Body says T1 only. Diagram is T1→T2→T3 sequential. **Align:** T3 can run after T1 but we serialize after T2 for one worker. Treat Depends on as T1, T2 for the worker order. | ✅ serialized |
| T4 | T2, T3 | after T3 | ✅ |
| T5 | T2, T3 | Phase 2 after Phase 1 | ✅ |
| T6 | T5 | T5 → T6 | ✅ |
| T7 | T5 | Phase 3 after T5 (Phase 2) | ✅ |
| T8 | T7 | T7 → T8 | ✅ |
| T9 | T8 | T8 → T9 | ✅ |
| T10 | T5 | Phase 4 after Phase 3 (superset of T5) | ✅ |
| T11 | T8, T10 | Phase 5 after 3 and 4 | ✅ |

---

## Test Co-location Validation

| Task | Code Layer | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1 | generated sqlc / interface | none | none | ✅ |
| T2 | memdriver | unit | unit | ✅ |
| T3 | pgdriver | integration | integration | ✅ |
| T4 | lint | none | none | ✅ |
| T5 | root client | unit + integration | unit + integration | ✅ |
| T6 | lint | none | none | ✅ |
| T7 | root wake | unit | unit | ✅ |
| T8 | pgdriver LISTEN | integration | integration | ✅ |
| T9 | lint | none | none | ✅ |
| T10 | cmd/drover-bench | unit | unit | ✅ |
| T11 | README / examples | Example compile | unit (Examples) | ✅ |
