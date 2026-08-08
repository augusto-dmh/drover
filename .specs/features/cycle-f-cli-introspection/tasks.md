# CLI + Introspection Tasks

## Execution Protocol (MANDATORY — do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path.

**If the skill cannot be activated, STOP and tell the orchestrator — do not proceed without it.**

---

**Spec**: `.specs/features/cycle-f-cli-introspection/spec.md`
**Design**: `.specs/features/cycle-f-cli-introspection/design.md`
**Context**: `.specs/features/cycle-f-cli-introspection/context.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase, project guidelines, and spec. Guidelines found: `CLAUDE.md`
> (table-driven, `t.Parallel`, `-race`, testcontainers, goleak on lifecycle, unit suite
> without Docker), `.github/workflows/ci.yml`, `.golangci.yml`.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| Root `inspector.go` + errors | unit | Every Inspector AC and edge case; `errors.Is` on sentinels | `inspector_test.go` | `go test -race ./...` |
| `internal/memdriver` new methods | unit | List/Get/Cancel/Redrive happy + refusal paths | `internal/memdriver/*_test.go` | `go test -race ./...` |
| `internal/pgdriver` new methods | integration | Same paths against Postgres | `internal/pgdriver/*_integration_test.go` | `go test -race -tags=integration ./...` |
| `cmd/drover` command functions | unit | Exit codes, JSON vs human, validation errors — prefer testing packages that accept an `Inspector` / fake writer without a live DB | `cmd/drover/*_test.go` | `go test -race ./cmd/drover/...` |
| Generated sqlc | none | Build + CI drift check only | — | build gate |
| GoReleaser config | build/tool | `goreleaser check` when network allows; otherwise YAML present + documented | `.goreleaser.yaml` | build gate |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
| --- | --- | --- | --- |
| Root / memdriver unit | Yes | Fresh `memdriver.New()` per test | Existing suite |
| pgdriver integration | Yes | `testdb.NewDB(t)` per test | `internal/testdb` |
| cmd/drover unit | Yes | Fake Inspector / buffer writers; no shared globals | New |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After unit-only tasks | `go test -race ./...` |
| Full | After storage/CLI Postgres paths | `go test -race ./... && go test -race -tags=integration ./...` |
| Build | Phase completion | `go build ./... && go vet ./...` |

**Before each phase-closing commit**, run
`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run`.
**If any `.sql` file changed**, run
`go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate` and commit `internal/dbsqlc`.

**Environment**: Docker is available. Baseline before this cycle: check
`go test ./... 2>&1 | tail` for function count after Phase 0 if needed.

---

## Execution Plan

### Phase 1: Storage (Sequential)

```
T1 → T2 → T3 → T4
```

### Phase 2: Inspector API (Sequential)

```
T5 → T6 → T7
```

### Phase 3: CLI (Sequential)

```
T8 → T9 → T10 → T11
```

### Phase 4: Release + docs (Sequential)

```
T12 → T13
```

---

## Phase 1: Storage

### T1: Driver interface + SQL + sqlc ✅

**What**: Add `ListJobsParams`, `ListJobs`, `GetJob`, `OperatorCancel`, `RedriveDead` to
`internal/driver.Driver`. Add SQL in `queries.sql`; regenerate sqlc.
**Done when**: Interface compiles; generated code committed; `GetJob` promoted through the
interface (pgdriver may already call the query internally).
**Requires**: —
**Gate**: build
**Commit**: `feat(driver): add list, get, cancel, and redrive operator methods`

### T2: memdriver implementations + unit tests

**What**: Implement the four methods on `memdriver`; table-driven tests for filters,
ordering, limit, cancel allowed states, redrive from dead only, not-found.
**Done when**: memdriver tests cover happy + refusal paths; quick gate green.
**Requires**: T1
**Gate**: quick
**Commit**: `feat(memdriver): implement operator list, cancel, and redrive`

### T3: pgdriver implementations + integration tests

**What**: Wire sqlc queries in `pgdriver`; integration tests mirroring memdriver cases.
**Done when**: integration gate green for the new methods.
**Requires**: T1
**Gate**: full
**Commit**: `feat(pgdriver): implement operator list, cancel, and redrive`

### T4: Phase 1 lint close

**What**: Full lint; fix any issues from T1–T3.
**Done when**: lint clean.
**Requires**: T2, T3
**Gate**: build + lint
**Commit**: only if fixes needed; otherwise skip empty commit and note in summary

---

## Phase 2: Inspector API

### T5: Root sentinels + Inspector core (Stats, List, Get, Enqueue)

**What**: Export `ErrNotFound` / `ErrInvalidTransition` (or document mapping); add
`inspector.go` with `NewInspector`, `newInspector`, Stats/ListJobs/GetJob/Enqueue;
map driver rows to public `JobRow`; default list limit 100; validate kind + JSON.
**Done when**: unit tests for Stats/List/Get/Enqueue ACs pass on memdriver.
**Requires**: T2
**Gate**: quick
**Commit**: `feat: add Inspector for queue stats, list, get, and enqueue`

### T6: Inspector Cancel + Retry

**What**: `CancelJob` / `RetryJob` wrapping operator driver methods; tests for allowed
states, refusals, error wrapping with `errors.Is`.
**Done when**: CLI-04/CLI-05 ACs covered by unit tests.
**Requires**: T5
**Gate**: quick
**Commit**: `feat: add Inspector cancel and dead-job redrive`

### T7: Phase 2 lint close

**What**: lint on Inspector surface.
**Requires**: T6
**Gate**: build + lint
**Commit**: only if fixes needed

---

## Phase 3: CLI

### T8: `cmd/drover` skeleton — DSN, dispatch, version, format

**What**: Create `cmd/drover` with global flags (`--database`, `--json`), version var +
`version`/`--version`, usage, human/JSON helpers, pool open from DSN.
**Done when**: `go build ./cmd/drover` works; version test passes; unknown command → exit 2.
**Requires**: T6
**Gate**: `go test -race ./cmd/drover/...` + build
**Commit**: `feat(cli): add drover binary skeleton with version and output helpers`

### T9: `stats` and `jobs list` commands

**What**: Wire `Inspector.Stats` / `ListJobs` to CLI; human + JSON output.
**Done when**: command-level tests with a fake or mem-backed Inspector cover AC printing
and filters (prefer injecting Inspector over live Postgres in unit tests).
**Requires**: T8
**Gate**: quick (incl. cmd package)
**Commit**: `feat(cli): add stats and jobs list commands`

### T10: `retry`, `cancel`, `enqueue` commands

**What**: Wire remaining verbs; validation and exit codes.
**Done when**: unit tests cover success, not-found, invalid transition, bad JSON, empty kind.
**Requires**: T9
**Gate**: quick
**Commit**: `feat(cli): add retry, cancel, and enqueue commands`

### T11: Phase 3 full gate + lint

**What**: Full unit + integration + lint; fix fallout.
**Requires**: T10
**Gate**: full + lint
**Commit**: only if fixes needed

---

## Phase 4: Release + docs

### T12: GoReleaser config

**What**: Add `.goreleaser.yaml` per design; document tag-based release in README CLI
section; run `goreleaser check` if feasible.
**Done when**: file present; README documents CLI + release; version ldflags match `main.version`.
**Requires**: T8
**Gate**: build
**Commit**: `build: add GoReleaser config for the drover binary`

### T13: README CLI section + planning handoff prep

**What**: README operator docs for the five commands and `DATABASE_URL`; update
`.specs/STATE.md` Decisions (AD-049+) and Handoff for Cycle F in-progress / ready-for-PR
as appropriate during finalize — during Execute, at least append AD rows and point Handoff
at this feature.
**Done when**: README has a CLI section; STATE mirrors context decisions.
**Requires**: T10, T12
**Gate**: build
**Commit**: `docs: document the drover CLI and Inspector`

---

## Requirement Traceability

| Requirement ID | Task(s) | Status |
| --- | --- | --- |
| CLI-01 | T5 | Pending |
| CLI-02 | T5 | Pending |
| CLI-03 | T1–T3, T5 | Pending |
| CLI-04 | T1–T3, T6 | Pending |
| CLI-05 | T1–T3, T6 | Pending |
| CLI-06 | T5 | Pending |
| CLI-07 | T9 | Pending |
| CLI-08 | T9 | Pending |
| CLI-09 | T10 | Pending |
| CLI-10 | T10 | Pending |
| CLI-11 | T8, T12 | Pending |

**Coverage:** 11 total, 11 mapped, 0 unmapped.
