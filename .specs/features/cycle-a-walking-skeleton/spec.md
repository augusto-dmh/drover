# Cycle A — Walking Skeleton Specification

## Problem Statement

Drover has architecture and roadmap but no code. Cycle A delivers the smallest end-to-end path — typed job enqueued inside a caller's transaction, claimed with `FOR UPDATE SKIP LOCKED`, executed by a registered worker, terminal state recorded — proving the core loop that every later cycle builds on (RFC-0001, Cycle A).

## Goals

- [ ] A caller can enqueue a typed job transactionally and see it executed by a worker loop against real PostgreSQL.
- [ ] The unit suite passes without Docker (in-memory adapter); the integration suite passes under `-race` via testcontainers-go.
- [ ] Two concurrent claimers never execute the same job twice (SKIP LOCKED proven by test).

## Out of Scope

| Feature | Reason |
|---|---|
| Retries, backoff, rescuer, leases enforcement | Cycle B (ADR-0003 machinery lands there) |
| Worker pools, graceful drain (`Stop(ctx)` semantics) | Cycle C |
| Middleware, `ScheduledAt`, multiple queues, priorities | Cycle D |
| Metrics, health endpoints | Cycle E |
| CLI binary | Cycle F |
| Unique jobs, periodic jobs | Cycle H |

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
|---|---|---|---|
| Migration mechanism | Embedded `.sql` files (`embed.FS`) applied by a minimal internal migrator with a `drover_schema_version` table | Stdlib-first (ADR-0004); no external migration tool dependency for library adopters; full decision log in `context.md` D-1 | auto (ADR-settled) |
| Job state enum | Full enum created upfront (`available`, `scheduled`, `running`, `retryable`, `completed`, `cancelled`, `dead`); Cycle A implements only `available → running → completed\|dead` | Avoids enum-migration churn in B/D; transitions guarded in storage layer | auto (D-2) |
| Failed job handling in A | Handler error ⇒ append error detail, state `dead` immediately | Placeholder until Cycle B introduces `retryable`; never silently drops failures | auto (D-3) |
| Schema breadth | Columns needed by B/D (`attempt`, `max_attempts`, `errors`, `scheduled_at`, `leased_until`, `queue`) exist now; unenforced where machinery is missing | One migration file per cycle stays possible without rewriting the core table | auto (D-4) |
| Worker crash mid-job | Job stays `running` until Cycle B's rescuer; documented limitation | Lease enforcement is B's deliverable | auto (D-5) |
| DB unreachable during poll | Log at `ERROR`, keep polling at the same interval | Backoff tuning belongs to Cycle G; constant interval is predictable | auto (D-6) |
| Duplicate worker registration | `Register` panics (setup-time programmer error, `http.Handle` precedent) | Misconfiguration must fail loud at boot, not at runtime | auto (D-7) |

**Open questions:** none — all resolved or logged above.

## User Stories

### P1: Schema and migrator ⭐ MVP

**User Story**: As an adopter, I want drover to create and version its own schema so that setup is one call with no external tooling.

**Acceptance Criteria**:

1. WHEN `Migrate(ctx, pool)` runs against an empty database THEN system SHALL create the `drover_jobs` table, state enum, and `drover_schema_version` table, recording version 1.
2. WHEN `Migrate` runs a second time THEN system SHALL make no schema changes and return nil (idempotent).
3. WHEN `drover_jobs` exists THEN it SHALL contain columns: `id`, `kind`, `queue` (default `'default'`), `args` (jsonb), `state`, `attempt`, `max_attempts`, `errors` (jsonb), `scheduled_at`, `leased_until`, `created_at`, `finalized_at`.

**Independent Test**: run migrator against a fresh testcontainer; assert schema; re-run; assert no error and unchanged version.

### P1: Transactional typed enqueue ⭐ MVP

**User Story**: As an application developer, I want to enqueue a typed job inside my own transaction so that a job exists if and only if my domain write commits.

**Acceptance Criteria**:

1. WHEN `client.Insert(ctx, args)` is called with `args` implementing `JobArgs` THEN system SHALL persist a row with `kind = args.Kind()`, JSON-encoded args, state `available`, and return the job's id and state.
2. WHEN `client.InsertTx(ctx, tx, args)` is called and the surrounding tx commits THEN the job row SHALL be visible and `available`.
3. WHEN `client.InsertTx(ctx, tx, args)` is called and the surrounding tx rolls back THEN no job row SHALL exist.
4. WHEN args fail to marshal to JSON THEN `Insert`/`InsertTx` SHALL return a wrapped error and persist nothing.
5. WHEN `args.Kind()` returns an empty string THEN `Insert`/`InsertTx` SHALL return `ErrInvalidKind` and persist nothing.

**Independent Test**: insert inside an explicitly rolled-back tx → count 0; inside committed tx → count 1 with expected row values.

### P1: Claim-and-execute worker loop ⭐ MVP

**User Story**: As an application developer, I want a worker loop that claims available jobs safely under concurrency and runs my registered `Worker[T]` so that enqueued work actually happens.

**Acceptance Criteria**:

1. WHEN a `Worker[T]` is registered for kind K and an available job of kind K exists THEN the loop SHALL claim it (state `running`), decode args into T, call `Work(ctx, job)`, and on nil error mark it `completed` with `finalized_at` set.
2. WHEN two claimers poll concurrently against N available jobs THEN each job SHALL be executed exactly once (no double-claim; SKIP LOCKED).
3. WHEN `Work` returns a non-nil error THEN system SHALL record the error message and attempt number in `errors` and mark the job `dead` (Cycle A placeholder, per D-3).
4. WHEN `Work` panics THEN system SHALL recover at the job boundary, record the panic value and stack, and mark the job `dead` — the loop SHALL continue.
5. WHEN a claimed job's kind has no registered worker THEN system SHALL mark it `dead` with an "unregistered kind" error.
6. WHEN the loop's context is cancelled THEN the loop SHALL stop polling, wait for the in-flight job (if any) to finish, and return — leaving no goroutines (goleak-clean).
7. WHEN no jobs are available THEN the loop SHALL sleep for the configured poll interval before the next fetch.

**Independent Test**: end-to-end against testcontainer — enqueue 50 jobs, run two client loops, assert 50 distinct executions, all rows `completed`.

### P2: Registry ergonomics and observability floor

**User Story**: As an application developer, I want typed registration and structured logs so that setup errors surface at boot and job outcomes are visible.

**Acceptance Criteria**:

1. WHEN `Register[T]` is called twice for the same kind THEN system SHALL panic with a message naming the kind (D-7).
2. WHEN a job starts, completes, or fails THEN system SHALL emit one `slog` record each with `job_id`, `kind`, `attempt`, duration (on finish), and outcome.
3. WHEN no `*slog.Logger` is configured THEN system SHALL default to `slog.Default()`.

## Edge Cases

- WHEN the jobs table is empty THEN a fetch SHALL return no job and no error (idle path).
- WHEN args JSON decodes fail at execution (schema drift) THEN the job SHALL be marked `dead` with the decode error, loop continues.
- WHEN the database is unreachable at poll time THEN the loop SHALL log at ERROR and retry next tick (D-6).
- WHEN `Client` is constructed with a nil pool/driver THEN the constructor SHALL return an error, not panic.

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
|---|---|---|---|
| CORE-01 | P1: Schema and migrator | Done | Verified |
| CORE-02 | P1: Transactional typed enqueue | Done | Verified |
| CORE-03 | P1: Claim-and-execute worker loop | Done | Verified |
| CORE-04 | P2: Registry ergonomics and observability floor | Done | Verified |

**Coverage:** 4 total, 4 mapped to tasks (T1–T10), 0 unmapped — validation PASS, see `validation.md`

## Success Criteria

- [ ] `go test ./...` passes with no Docker available (in-memory adapter covers unit paths).
- [ ] `go test -race -tags=integration ./...` (testcontainers) passes, including the concurrent-claim test.
- [ ] A runnable `example_test.go` demonstrates register → insert → work end-to-end.
- [ ] goleak reports no leaked goroutines after loop shutdown.

## Dimensions Sweep (Large scope — all dimensions resolved)

| Dimension | Resolution |
|---|---|
| Input validation & bounds | CORE-02 AC4–5 (marshal failure, empty kind); nil-pool edge case |
| Failure / partial-failure | CORE-02 AC3 (rollback), CORE-03 AC3–5; crash-mid-job documented limitation (D-5) |
| Idempotency / retry / duplicates | N/A because retries land in Cycle B and uniqueness in Cycle H (ADR-0003, RFC-0001); at-least-once contract documented in README of the package |
| Auth boundaries & rate limits | N/A because drover is an embedded library; no network surface in Cycle A |
| Concurrency / ordering | CORE-03 AC2 (SKIP LOCKED exactly-once-claim); FIFO by `id` within a queue documented as best-effort |
| Data lifecycle / expiry | N/A because completed-job cleanup is deferred (RFC-0001 Cycle G tuning); rows retained |
| Observability | CORE-04 AC2–3 (slog floor); metrics N/A until Cycle E |
| External-dependency failure | DB-unreachable edge case (D-6) |
| State-transition integrity | D-2: transitions guarded in storage layer; invalid transition returns error |
