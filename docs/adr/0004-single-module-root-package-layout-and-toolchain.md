# ADR-0004: Single-module root-package layout, pgx+sqlc, stdlib-first toolchain

- **Date**: 2026-07-22
- **Status**: Accepted
- **Deciders**: Augusto de Melo Henriques
- **Tags**: conventions, tooling, testing

## Context and Problem Statement

Drover is a library with an optional binary. Go has no enforced project structure, and the ecosystem's cargo-culted layouts (`pkg/`, premature multi-module splits) actively hurt libraries. Conventions must be locked before the first line of code.

## Decision Drivers

- Import ergonomics: `drover.Client`, `drover.NewClient` — the package name is the API.
- Minimal dependency surface; the standard library is production-grade for everything the binary needs.
- The full unit suite must run without Docker; integration tests must own their database lifecycle.

## Decision Outcome

Following `go.dev/doc/modules/layout` and the verified structure of River and Asynq:

- **Layout**: root-package library (`client.go`, `job.go`, `worker.go` at repo root, with runnable `example_*_test.go` files), private code in `internal/`, binary in `cmd/drover/`, runnable demonstrations in `examples/<name>/` (`main` packages only, and never a source of new module dependencies — added for the example app RFC-0001 locks to Cycle C). Single module; no `pkg/` (absent from the official layout guidance and from River/Asynq alike); multi-module splits deferred until a real consumer needs them.
- **Data access**: pgx v5 native + sqlc — queries in `.sql` files, generated code committed under `internal/dbsqlc/` (no runtime codegen deps), behind a thin driver interface. `database/sql` support is documented future work.
- **HTTP**: stdlib `net/http` ServeMux (1.22+ patterns) with a hand-rolled `func(Handler) Handler` middleware chain.
- **Logging/config**: `*slog.Logger` injected via a config struct (config struct over functional options, River-style); binary config via `flag` + env + optional YAML with explicit precedence.
- **API idioms**: context-first parameters, consumer-defined small interfaces, generics only for typed job args (`JobArgs` / `Worker[T]`), `%w` wrapping with package-prefixed sentinel errors.
- **Testing**: table-driven + `t.Parallel` + `-race` always; testcontainers-go (postgres module) for integration; `testing/synctest` for time-dependent scheduler/retry logic; goleak on lifecycle tests; `b.Loop()` benchmarks; fuzzing for JSON/cron parsing.
- **Tooling**: golangci-lint v2 (errorlint, sloglint, tparallel, gosec among the enabled set), govulncheck in CI, CI matrix on the two latest Go versions, GoReleaser for the binary, v0 semver with the module compatibility promise in view.

### Consequences

- Positive: near-zero dependency README story; tests are fast, deterministic, and honest about what they prove.
- Negative: sqlc's committed generated code must be kept in sync via CI check; stdlib-first means occasionally writing ~80 lines where a framework would give one import.

## Links

- Evidence: `docs/research/2026-07-22/rq04-go-conventions.md`
