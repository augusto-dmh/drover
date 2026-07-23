# RQ04 — Idiomatic Go conventions, tooling, testing

Research date: 2026-07-22. Target: drover, a Postgres-backed distributed task queue shipped as a **library first, binary second**, written in idiomatic, modern Go. Go 1.26 is current (released 2026-02-10); Go 1.25 is the oldest supported release.

---

## 1. Module layout for a library with an optional binary

### 1.1 What the official guidance says (go.dev/doc/modules/layout)

The official layout doc prescribes exactly the shape drover needs — "packages and commands in the same repository":

```
drover/                      # module github.com/augusto-dmh/drover
  go.mod
  drover.go                  # package drover — doc.go-style package comment
  client.go                  # drover.Client
  worker.go                  # drover.Worker / drover.AddWorker
  job.go
  drovertype/                # (optional) shared leaf types, like river's rivertype
  internal/
    dbadapter/               # everything you don't want to support forever
    leadership/
    maintenance/
    dbsqlc/                  # sqlc-generated code (see §2)
  cmd/
    drover/
      main.go                # CLI/server binary: migrate, serve, dashboard
```

Key points from the doc, verbatim in spirit:

- A basic library is just `go.mod` + `modname.go` at the **root**; supporting private code goes in `internal/` — "since other projects cannot import code from our `internal` directory, we're free to refactor its API and generally move things around without breaking external users."
- "A common convention is placing all commands in a repository into a `cmd` directory; while this isn't strictly necessary in a repository that consists only of commands, it's very useful in a mixed repository." Users then `import "github.com/you/drover"` and `go install github.com/you/drover/cmd/drover@latest`.
- **The doc never mentions `pkg/`.** It recommends root-level importable packages plus `internal/`.

### 1.2 What River and Asynq actually do (verified against GitHub)

**River (`riverqueue/river`)** — the strongest layout model for drover:

- Main package **at repo root**: `client.go`, `job.go`, `worker.go`, plus many `example_*_test.go` files at root (runnable examples double as docs on pkg.go.dev — a touch worth copying).
- Top-level sibling packages, several of which are **separate Go modules** stitched together with `go.work`: `rivertype/` (leaf types, breaks import cycles), `riverdriver/` (driver interface), `riverdriver/riverpgxv5/` and `riverdriver/riverdatabasesql/` (each with own `go.mod` so the core module doesn't force pgx onto database/sql users, and vice versa), `rivershared/`, `rivermigrate/`, `rivertest/`, `riverlog/`, `riverdbtest/`.
- CLI at `cmd/river/`.
- No `pkg/` directory anywhere.

**Asynq (`hibiken/asynq`)** — same core idea, simpler:

- Main package at repo root (`asynq.go`, `client.go`, `server.go`, `handler.go`).
- `internal/` for implementation; `tools/` (separate module) holding the `tools/asynq/` CLI; `x/` for extensions.

### 1.3 Anti-patterns to avoid

- **Cargo-cult `pkg/`**: absent from the official layout doc, absent from River and Asynq. In a library it actively hurts: import paths become `github.com/you/drover/pkg/drover`. Widely called out as a cargo-culted holdover from `golang-standards/project-layout`, which is *not* an official standard (the Go team has explicitly disavowed it — see rakyll's "This is not the Go standard library layout" and issue #117 on that repo).
- **Premature deep nesting / one-type-per-package**: start with one root package; split only when a package boundary earns its keep (independent versioning, import-cycle break, genuinely separate audience). River's `rivertype` exists specifically to break cycles between core and drivers — copy the *reason*, not the shape, on day one.
- **internal/-heavy "service layout" for a library**: `internal/app`, `internal/domain`, `internal/ports` style layouts are fine for closed applications but wrong for a library, where the root package *is* the product. Use `internal/` generously for machinery, but the public API lives at root.

### Options — layout

- **Option A: root-package library + `internal/` + `cmd/drover/`, single module to start** — River/Asynq shape without multi-module overhead. `import "github.com/.../drover"` gives `drover.Client`, `drover.NewClient`. Split driver sub-modules (à la `riverpgxv5`) only if/when a second driver exists. **★ RECOMMENDED**
- Option B: multi-module workspace from day one (core + driver modules + `go.work`, like River). Correct at River's maturity; premature complexity and release-tagging pain (`riverdriver/riverpgxv5/v0.x.y` tags) for a new project.
- Option C: service-style `internal/`-everything with a thin root facade. Hides the API surface pkg.go.dev should showcase; wrong emphasis for a library.

---

## 2. Library choices

### 2.1 Database access: pgx v5, with sqlc-generated code in `internal/`

- **Can a library ship sqlc-generated code? Yes — River proves it.** Verified: `riverdriver/riverpgxv5/internal/dbsqlc/` contains sqlc-generated, checked-in code; RiverUI does the same. Generated code lives under `internal/`, so it's invisible to importers and freely regenerable. Check the generated code into git (sqlc is a build-time tool, not a runtime dependency — adopters never need sqlc installed).
- **pgx v5 directly vs database/sql**: pgx v5 native gives batch support (`pgx.Batch`), `LISTEN/NOTIFY` (essential for a low-latency queue), binary protocol, and proper context cancellation. River's core is decoupled behind a `riverdriver` interface with `riverpgxv5` as the flagship driver; `riverdatabasesql` came later (initially only for migrations, later expanded — River Pro added fuller database/sql support in 2025).
- **database/sql compatibility for adopters**: the honest v0 answer is a small internal driver interface (`internal/dbadapter` or an exported minimal `droverdriver`) implemented first for pgx v5 only, with database/sql documented as future work. Trying to support both on day one doubles the test matrix for near-zero payoff at this stage.

**Options — DB layer**
- **Option A: pgx v5 native + sqlc (queries in `.sql`, generated code checked in under `internal/dbsqlc/`), thin driver interface like River's** — type-safe SQL, zero runtime codegen deps, exactly the pattern the reference project uses. **★ RECOMMENDED**
- Option B: hand-written pgx queries, no sqlc. Fine, but you lose compile-time query/type sync and write boilerplate scanning; sqlc-in-a-library is itself a pattern worth demonstrating.
- Option C: database/sql + lib/pq or stdlib-only. Loses LISTEN/NOTIFY ergonomics and batching; lib/pq is in maintenance mode.

### 2.2 HTTP: net/http 1.22+ mux vs chi

Go 1.22's `http.ServeMux` supports methods and wildcards: `mux.HandleFunc("GET /api/jobs/{id}", h)`, `r.PathValue("id")`. For a small dashboard/API in the binary this covers everything; middleware is just `func(http.Handler) http.Handler` chaining. chi is the right call only when you want route groups, sub-routers, and its middleware collection.

- **Option A: stdlib `net/http` ServeMux (1.22+ patterns), hand-rolled 3-line middleware chain** — zero deps in the binary; the 1.22+ mux covers everything the dashboard needs, so not reaching for a framework is itself a defensible choice. **★ RECOMMENDED**
- Option B: chi v5 — justifiable if the dashboard grows (nested resource routes, many middlewares); 100% `net/http`-compatible so migration later is cheap.
- Option C: gin/echo — wrong dialect for an idiomatic-Go showcase (non-stdlib handler signatures).

### 2.3 Logging in a library: slog injection

Consensus pattern (matches River, which takes `Logger *slog.Logger` in `river.Config`):

- The library **never** logs via a global; accept an optional `*slog.Logger` in config: `Config{Logger: *slog.Logger}`; default to `slog.Default()` or a no-op when nil.
- Accept `*slog.Logger` (the frontend), not `slog.Handler` — callers who have a custom handler wrap it with `slog.New(h)` in one line; accepting the Logger lets callers pre-attach attrs (`logger.With("component", "drover")`).
- Namespace your logs (`logger = logger.With(slog.String("lib", "drover"))` or a group) and use `LogAttrs`/lazy values on hot paths.
- Never require zap/zerolog in a library's public API; if you must bridge, `slog.Handler` is the interop point.

### 2.4 Config for the binary

Library: **no config files, ever** — a `Config` struct passed to `NewClient`. Binary: flags + env + optional file. Idiomatic minimal stack: stdlib `flag` + env parsing via `github.com/caarlos0/env/v11` (or kelseyhightower/envconfig), optional YAML via `gopkg.in/yaml.v3` into the same struct, precedence flags > env > file > defaults. Viper is the heavyweight option most reviewers now consider over-tooled for a single binary; koanf is the lighter alternative if you want a config library at all.

- **Option A: `flag` + env struct tags + optional yaml.v3 file, ~80 lines of your own precedence code** — small, testable, dependency-light. **★ RECOMMENDED**
- Option B: koanf — clean if config sources multiply.
- Option C: viper — heavy transitive deps; widely considered over-tooled for a small binary.

---

## 3. Testing

- **Table-driven tests**: the `tests := []struct{ name string; ... }` + `for _, tt := range tests { t.Run(tt.name, ...) }` form remains the canonical convention (Go wiki TableDrivenTests, Google Go Style). Since Go 1.22 loop-var semantics, no `tt := tt` copy is needed — leaving one in is now a *staleness* signal; `copyloopvar` linter catches it.
- **t.Parallel()**: call in both parent and subtests where safe; enforceable with `tparallel`/`paralleltest` linters. For DB tests, parallelism pairs with per-test schemas or transactions-with-rollback.
- **-race in CI**: non-negotiable for a concurrency library: `go test -race ./...` on every push. Also run a non-race job (race skews timing) and consider `GOFLAGS=-count=1` for integration jobs.
- **Postgres integration tests**: **testcontainers-go with the `postgres` module** (`golang.testcontainers.org/modules/postgres`) beats docker-compose: containers are started/owned by the test process, work identically on laptop and CI, get cleaned up by Ryuk, and support snapshot/restore for fast per-test resets. Common 2025/2026 pattern: one container per test package (`TestMain`), one migrated database or schema per test, guarded by a build tag or `testing.Short()`. docker-compose remains fine as a *dev convenience* (`docker compose up db`) but shouldn't be the test harness. ★ RECOMMENDED: testcontainers-go; keep a small compose file only for manual dev.
- **testing/synctest — status**: experimental in Go 1.24 (`GOEXPERIMENT=synctest`, `synctest.Run`), **graduated to stable in Go 1.25** with `synctest.Test(t, func(t *testing.T){...})` + `synctest.Wait()`; `Run` is deprecated, and in Go 1.26 the old GOEXPERIMENT path is deprecated too. Inside the "bubble," `time` uses a fake clock and `Wait` blocks until goroutines quiesce — purpose-built for testing schedulers, heartbeats, visibility timeouts, and retry backoff without `time.Sleep` flakiness. Using synctest for drover's scheduler/reaper tests is the current idiomatic approach.
- **goleak**: `go.uber.org/goleak` — `defer goleak.VerifyNone(t)` in lifecycle tests and `goleak.VerifyTestMain(m)` per package to prove `Client.Stop` leaks nothing. Note Go 1.26 also added runtime goroutine-leak detection tooling; goleak remains the standard test-level assertion.
- **Benchmarks**: `func BenchmarkEnqueue(b *testing.B)` using the Go 1.24+ `for b.Loop()` form (replaces `for i := 0; i < b.N; i++`); `b.ReportAllocs()`; compare with `benchstat`. A throughput benchmark (jobs/sec enqueue+work) is a natural drover headline number.
- **Fuzzing**: native `go test -fuzz` where there's parsing: job args JSON (de)serialization, cron/schedule expression parsing, queue-name validation. One or two `FuzzXxx` functions with seed corpora checked in — low effort, and it hardens exactly the surfaces that take untrusted input.

---

## 4. Tooling

### 4.1 golangci-lint v2

v2 (released March 2025) restructured config: `version: "2"`, `linters.default: standard|all|none|fast` replacing enable-all/disable-all, and **formatters split into their own `formatters:` section**. Concrete recommended set for drover:

```yaml
version: "2"
linters:
  default: standard        # errcheck, govet, ineffassign, staticcheck, unused
  enable:
    - bodyclose
    - copyloopvar
    - errorlint            # wrapping/%w correctness
    - exhaustive
    - gocritic
    - gosec
    - intrange
    - misspell
    - nilerr
    - noctx
    - paralleltest
    - revive
    - sloglint             # slog usage consistency
    - testifylint
    - thelper
    - tparallel
    - unparam
    - usestdlibvars
formatters:
  enable: [gofumpt, goimports]
```

(The maratori "golden config" gist is the community reference for a maximal set; the above is a curated library-appropriate subset. `modernize` analyzers are also available and worth enabling once stable in your version.)

### 4.2 The rest

- **govulncheck** (`golang.org/x/vuln/cmd/govulncheck ./...`): run in CI on a schedule + on PRs; call-graph-aware so low-noise. Cheap credibility.
- **GitHub Actions matrix**: convention for libraries is the two newest Go releases (the officially supported window): `go-version: ['1.25.x', '1.26.x']` via `actions/setup-go@v5` with `check-latest: true`; OS matrix `ubuntu-latest` (+ `macos-latest`/`windows-latest` only for the binary build, not DB tests). Separate jobs: lint (golangci-lint-action), test `-race`, integration (testcontainers, Linux only), govulncheck. Set `go` directive in go.mod to the *minimum* you support (1.25), not the newest.
- **GoReleaser** for `cmd/drover`: `.goreleaser.yaml` with builds for linux/darwin/windows × amd64/arm64, `CGO_ENABLED=0`, `-trimpath`, `ldflags: -s -w -X main.version={{.Version}}`, archives, checksums, Homebrew tap optional, `ko`/Docker image optional; triggered by tag push. This is the de facto standard release tool for Go binaries.
- **Versioning**: tag `v0.x.y`. Under semver + the Go modules convention, **v0 makes no compatibility promise** — but the mature move (River did exactly this for years pre-1.0) is to *behave* as if minor bumps may break and document changes in a CHANGELOG (Keep a Changelog format). Never reach v2+ without the `/v2` module path suffix; mention the Go 1 compatibility-promise ethos ("import compatibility rule": a new major version = new import path) in the README to show you know it.

---

## 5. API-design idioms reviewers notice

- **Functional options vs config struct**: current mainstream taste (and Google Go Style "options structure" guidance) has shifted toward **config structs for required-plus-many fields** and functional options only for truly optional, open-ended extension. River uses `river.NewClient(driver, &river.Config{...})` — a plain struct. Recommended for drover: `drover.NewClient(ctx?, driver, &drover.Config{Queues: ..., Logger: ...})`; reserve `...Option` funcs for peripheral knobs (e.g., `InsertOpts` overrides). The case *against* functional options is worth documenting explicitly (they hide the option surface, complicate docs and zero-values; cf. Dave Cheney's original post and its later critiques).
- **Consumer-defined small interfaces**: "accept interfaces, return structs." Define interfaces where they're *consumed* (the driver interface lives in core, implemented by the pgx adapter), keep them 1–3 methods, return concrete `*Client`. Never export an interface just to mock it — adopters can define their own.
- **Generics taste — typed job args like River**: the flagship pattern to copy: `type Worker[T JobArgs] interface { Work(ctx context.Context, job *Job[T]) error }` with `river.AddWorker(workers, &SortWorker{})` and `Client.Insert(ctx, SortArgs{...}, nil)`. Generics at the API boundary for type-safe payloads; **no** generics in internals where `any`/concrete types suffice. Restraint is the point.
- **Context-first**: every blocking/IO exported method takes `ctx context.Context` as the first parameter; never store a context in a struct (except the documented request-scoped exception); honor cancellation in worker shutdown (`Stop(ctx)` vs `StopAndCancel(ctx)` like River).
- **Errors**: wrap with `%w` and add context at each layer; export a small set of sentinels (`var ErrJobNotFound = errors.New("drover: job not found")`, prefix messages with the package name) and typed errors where callers need data (`*JobCancelError`); document which errors are API (`errors.Is/As`-able). Snooze/cancel-via-error like River's `river.JobCancel(err)` is a neat idiom to emulate. `errorlint` enforces mechanics.
- **Avoiding interface pollution**: no `DroverInterface`, no premature abstraction over Postgres ("we might support Redis later" is how bad queues start), no exported types that exist only for tests. Small exported surface, rich `internal/`.
- Extra polish worth shipping: `example_test.go` runnable examples for every major API, `doc.go` package comment, `errors.Join` for multi-close paths, `iter.Seq` for any listing APIs (Go 1.23+ range-over-func), zero-value-useful types where possible.

---

## Sources

- https://go.dev/doc/modules/layout — official module layout doc (accessed 2026-07-22)
- https://github.com/riverqueue/river — repo layout, root package, sub-modules, cmd/river (accessed 2026-07-22)
- https://github.com/riverqueue/river/tree/master/riverdriver/riverpgxv5 — confirms own go.mod + internal/dbsqlc sqlc-generated code (accessed 2026-07-22)
- https://github.com/hibiken/asynq — root package, internal/, tools/asynq CLI, x/ (accessed 2026-07-22)
- https://deepwiki.com/riverqueue/riverui/3.3-database-access-layer — sqlc/dbsqlc usage in River ecosystem (accessed 2026-07-22; third-party wiki, corroborated by repo tree above)
- https://riverqueue.com/blog/sqlite-and-pro-dbsql-durable-periodic-jobs-performance-boosts — database/sql + SQLite driver expansion (accessed 2026-07-22)
- https://pkg.go.dev/github.com/riverqueue/river/riverdriver — driver decoupling rationale (accessed 2026-07-22)
- https://go.dev/blog/synctest and https://go.dev/blog/testing-time — synctest design/usage (accessed 2026-07-22)
- https://antonz.org/go-1-25/ and https://appliedgo.net/spotlight/go-1.25-the-synctest-package/ — synctest stable in 1.25, Run deprecated for Test (accessed 2026-07-22)
- https://go.dev/blog/go1.26 and https://go.dev/doc/go1.26 — Go 1.26 released 2026-02-10; goroutine leak detection; GOEXPERIMENT synctest path deprecated (accessed 2026-07-22)
- https://ldez.github.io/blog/2025/03/23/golangci-lint-v2/ and https://golangci-lint.run/docs/configuration/file/ — v2 config structure, linters.default, formatters section (accessed 2026-07-22)
- https://gist.github.com/maratori/47a4d00457a92aa426dbd48a18776322 — community "golden config" reference (accessed 2026-07-22)
- https://golang.testcontainers.org/modules/postgres/ and https://testcontainers.com/guides/getting-started-with-testcontainers-for-go/ — testcontainers-go Postgres module (accessed 2026-07-22)
- https://www.dash0.com/guides/logging-in-go-with-slog and https://betterstack.com/community/guides/logging/logging-in-go/ — slog frontend/handler split, injection patterns (accessed 2026-07-22)

Unverified / partially verified claims (flagged):
- Exact current golangci-lint "standard" default linter membership and availability of `modernize` as a standalone linter in the installed version — verify with `golangci-lint help linters` at setup time.
- River's exact `river.Config` field set (Logger field) and `StopAndCancel` naming cited from training knowledge of River's docs, not re-fetched today — spot-check pkg.go.dev/github.com/riverqueue/river before mirroring names.
- "Two newest releases" CI matrix is the Go support policy convention; individual libraries vary (some test back to their go.mod minimum).
- Claim that per-package container + per-test database is "the 2026 setup" comes from a search summary of practitioner blogs, not an official source.
