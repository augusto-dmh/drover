# Drover — Project Context

Drover is a PostgreSQL-backed distributed task queue for Go: a library (`github.com/augusto-dmh/drover`) plus a `drover` CLI. Built from first principles with a deliberately small surface — every mechanism (job claiming, redelivery, retries, shutdown) is meant to be fully explainable and provable, backed by documented trade-offs.

## Current Status

Pre-implementation. Founding research fleet complete (`docs/research/2026-07-22/`), decisions being promoted into ADRs and the RFC-0001 roadmap. No application code yet.

## Durable Decisions

Never re-litigate these without a superseding ADR:

- **Postgres is the only production backend** (ADR-0002). Job claiming via `SELECT ... FOR UPDATE SKIP LOCKED`; transactional enqueue is the flagship feature. Storage sits behind a narrow interface with an in-memory adapter for tests; a Redis adapter is analyzed in the ADR but explicitly not shipped.
- **At-least-once delivery** (ADR-0003). Lease + heartbeat + recoverer sweep for crashed workers; handlers documented as required-idempotent. Retries: `attempt^4` seconds with ±10% jitter, max attempts configurable (default 25), `Cancel`/`Snooze` sentinel errors for classification, retained `dead` state with scoped redrive.
- **Layout and stack** (ADR-0004): root-package library + `internal/` + `cmd/drover/`, single module, no `pkg/`. pgx v5 + sqlc (generated code committed under `internal/dbsqlc`), stdlib `net/http` ServeMux, `*slog.Logger` injected via config struct, generics only for typed job args, context-first APIs, `%w` wrapping with package-prefixed sentinel errors.
- **Worker pool is a fixed goroutine pool** feeding from a fetcher via channels — not `errgroup` (cancel-on-first-error is wrong for a queue). Per-job child contexts with timeout; panic recovery at the job boundary.
- **Shutdown sequence**: stop fetching → drain in-flight with deadline → cancel per-job contexts → best-effort requeue; lease expiry is the crash-path backstop (SIGKILL, node death).
- **Observability**: Prometheus via `promhttp` on an ops port with `/healthz` + `/readyz`; oldest-job age is the primary alerting metric, plus processed/failed counters and duration histograms. CLI is the primary inspection surface; a single server-rendered status page comes late in the roadmap; **no SPA dashboard, ever** (documented scope trap).
- **Scheduling**: delayed jobs via `scheduled_at` fetch predicate; periodic jobs via advisory-lock leader election deduped by unique jobs; weighted-fetch queue priorities over strict ordering.

## Workflow

`RESEARCH → RFC/ADR → tlc-spec-driven cycle → IMPLEMENT → PR`

- Research fleets write dated reports to `docs/research/YYYY-MM-DD/` (per-question files + `synthesis.md`); durable conclusions are always promoted into ADRs/RFCs — the research archive is evidence, read only when recovering it.
- One roadmap cycle = one PR = one `tlc-spec-driven` cycle. The skill creates `.specs/` per feature; do not create `.specs/` structure upfront.
- To ship a roadmap cycle end-to-end (plan → build → PR → review → triage → fix → cleanup → gated merge), use `drover-ship-cycle`; check `.specs/.ship-status` first and resume a mid-stage cycle instead of starting fresh. For standalone PR reviews on explicit request, use `pr-review`.
- Conventional Commits; one atomic commit per spec task; branch names describe the change, not planning labels. Load `drover-finalize` for all commit/PR publishing: it enforces self-contained history — no internal artifact names (task IDs, requirement/decision IDs, cycle letters, `.specs/` paths) in commit messages, PR titles, or PR bodies, and no AI/tooling attribution anywhere in git history or PRs.
- Tests: table-driven, `t.Parallel`, `-race` in CI always; testcontainers-go for Postgres integration tests; `testing/synctest` for time-dependent scheduler/retry tests; goleak on lifecycle tests. The unit suite must run without Docker via the in-memory adapter.
- Lint: golangci-lint v2; vulnerability scanning via govulncheck in CI.

## Progressive Documentation Loading

Do not load everything. Start here, then:

- Roadmap and cycle status → `docs/rfc/0001-drover-roadmap.md`
- Why a decision was made → `docs/adr/`
- Evidence behind a decision → `docs/research/` (only when explicitly recovering evidence)

## Current Constraints

- Solo maintainer, evening/weekend cadence — every cycle must stay PR-sized; resist scope growth (the RFC's cut line after Cycle E is deliberate).
- Public repo: all committed docs defend decisions on technical merit only.
- Benchmarks must publish methodology alongside numbers; no unverifiable claims in the README.
