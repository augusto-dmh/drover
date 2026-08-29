# Benchmark + Hardening Specification

## Problem Statement

Cycles A–F make Drover correct and operable, but enqueue is one round-trip
per job, an idle worker always waits out `PollInterval`, and the README
still promises numbers it does not publish. Cycle G is the performance
and honesty cycle: a public batch insert that uses `COPY FROM`, an optional
`LISTEN/NOTIFY` wake so idle workers do not sit on a timer, a `drover-bench`
harness, and a README table that states methodology next to any figure.

## Goals

- [x] `Client.InsertMany` / `InsertManyTx` persist a batch atomically, with
      Postgres using `COPY FROM`, returning rows in input order.
- [x] Optional `Config.NotifyWakeup` interrupts idle poll sleep when new
      work is committed, without replacing polling as the source of truth.
- [x] `cmd/drover-bench` measures enqueue throughput and drain latency
      with printed methodology (hardware, Postgres version, flags).
- [x] README publishes a benchmark table with methodology and a command
      that reproduces the run.
- [x] Unit coverage of batch insert and same-process wake-up via
      `memdriver` (no Docker required for the unit suite).

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| Prefetch / claim more jobs than idle workers | AD-022: a claimed row must have a worker about to run it. Cycle C already batches `FetchAvailable` up to idle capacity. |
| New fetch-size config knob | Claim batch size *is* idle-worker count. A second knob would re-open prefetch. |
| Poll-interval jitter | AD-006 keeps a constant interval; `NotifyWakeup` is the latency path. |
| Always-on `LISTEN/NOTIFY` | ADR-0002: commit-serialization lock and PgBouncer transaction-pooling incompatibility. Optional, default off. |
| Database trigger `NOTIFY` | Per-row notifies recreate the Recall.ai outage shape; coalesced notify from the inserter is enough. |
| `Inspector` / CLI emitting notify | Operator enqueue is rare; they stay poll-only this cycle. |
| Batch completer / batched finalize | RFC names insert + fetch tuning, not completion batching. |
| Polished kill-a-worker demo script | In the research row, cut from the RFC table. |
| Homebrew / Docker image for the bench binary | Harness, not a released operator tool; GoReleaser stays `cmd/drover` only. |
| `go test -bench` microbenchmarks as README headline | Research: useful for CI regression, not the published claim. Deferred. |
| Terminal-state retention / pruning | Carried-forward follow-up, not this cycle. |
| Periodic jobs / status page | Cycles H and I. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here — nothing is left silently unclear.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| ASM-01 — Batch public API | `InsertMany` / `InsertManyTx` taking `[]InsertItem` (`Args` + `*InsertOpts` per item) | Mixed kinds and queues in one producer flush; shared-opts would force homogeneous batches. | y |
| ASM-02 — COPY + RETURNING | Unlogged/session temp table + `CopyFrom` + `INSERT … SELECT … RETURNING` | `COPY` has no `RETURNING`; IDs and DB-clock state (AD-035) must come back. | y |
| ASM-03 — Batch atomicity | Validate every item first; one transaction; all rows or none | Partial batches are unexplainable to callers. | y |
| ASM-04 — Empty batch | Return `nil, nil` (or empty slice) without a round trip | No jobs is success, not an error. | y |
| ASM-05 — Single `Insert` path | Unchanged `INSERT … RETURNING`; does not go through COPY | Flagship one-job / `InsertTx` path stays the cheapest single-row write. | y |
| ASM-06 — Notify default | `Config.NotifyWakeup` bool, default false | ADR-0002 caution; RFC says optional. Poll remains correct alone. | y |
| ASM-07 — Who notifies | The inserting `Client`, once per successful `Insert`/`InsertMany` (and once inside `InsertTx`, same transaction), only when the flag is set | Coalesced; producers that never `Start` still wake listeners. | y |
| ASM-08 — Listen failure | Log and keep polling; `Start` still succeeds | Wake-up is an optimization; an unobservable worker is not. Contrast AD-043. | y |
| ASM-09 — Channel name | Postgres channel `drover`, empty payload | Enough to wake; fetch still applies its own queue predicate. | y |
| ASM-10 — Same-process wake | Buffered wake channel on the `Client`, signalled after a successful local insert when the flag is set | Makes the unit suite Docker-free; `LISTEN` covers other processes. | y |
| ASM-11 — Batch claim RFC bullet | Already delivered by Cycle C (`FetchAvailable` limit = idle slots). This cycle documents it, does not change it. | AD-022 forbids prefetch. | y |
| ASM-12 — Bench binary | `cmd/drover-bench`, stdlib `flag`, modes `enqueue` and `drain` | RFC names the binary; ADR-0004 forbids cobra. | y |
| ASM-13 — README numbers | Filled from one documented run of the harness, with hardware / Postgres / flags / caveats | RFC: no unverifiable claims. | y |
| ASM-14 — Two `InsertManyTx` in one tx | Reuse one `ON COMMIT DROP` temp table (`IF NOT EXISTS` + `TRUNCATE`) | A second `CREATE TEMP` in the same session would fail. | y |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Atomic batch insert ⭐ MVP

**User Story**: As a producer, I want to enqueue many jobs in one call so
that a flush is one transaction and one `COPY`, not N inserts.

**Why P1**: This is the RFC's storage deliverable and the bench's enqueue
path. Without it, published throughput would measure the slow API.

**Acceptance Criteria**:

1. WHEN `InsertMany` is called with a non-empty `[]InsertItem` whose every
   `Args` has a non-empty kind and marshals THEN the system SHALL persist
   every item and return a `[]*JobRow` of equal length, in the same order
   as the input, each with a distinct positive `ID`.
2. WHEN any item has a nil `Args`, an empty kind, or a marshal error THEN
   the system SHALL return that error, insert zero rows, and leave the
   table unchanged.
3. WHEN `InsertMany` is called with a nil or empty slice THEN the system
   SHALL return an empty result and nil error without writing.
4. WHEN an item's `Opts` is nil THEN the system SHALL enqueue it on
   queue `"default"`, due immediately, matching single `Insert`.
5. WHEN an item sets `ScheduledAt` in the future THEN the system SHALL
   store that job as `scheduled`; WHEN `ScheduledAt` is zero or in the
   past THEN the system SHALL store it as `available`. Dueness SHALL be
   decided by the database clock (AD-035), not the client.
6. WHEN `InsertManyTx` is used inside a caller transaction that later
   rolls back THEN the system SHALL make none of those jobs visible to
   `FetchAvailable`.
7. WHEN `InsertMany` runs on Postgres THEN the system SHALL load the
   batch through `COPY FROM` (via a session-temp staging table) rather
   than N single-row `INSERT` statements.
8. WHEN two `InsertManyTx` calls share one transaction THEN the system
   SHALL persist both batches (no "temp table already exists" failure).

**Independent Test**: memdriver unit tests for 1–6 and 8; pgdriver
integration tests for 1, 5–8, asserting the COPY path with a statement
counter or equivalent (query that would fail if N `InsertJob` calls were
used for N>1 is acceptable: a single-transaction row count plus a test
that N jobs appear after one call).

---

### P1: Optional notify wake-up ⭐ MVP

**User Story**: As a worker operator, I want idle fetch to resume when a
job is committed rather than when a timer fires, so enqueue-to-start
latency is not bound to `PollInterval`.

**Why P1**: RFC fetch-tuning deliverable. Polling stays the source of
truth (notifications are not durable).

**Acceptance Criteria**:

1. WHEN `NotifyWakeup` is unset or false THEN the system SHALL behave as
   today: idle fetch waits `PollInterval`, and inserts do not emit
   `NOTIFY`.
2. WHEN `NotifyWakeup` is true and `Insert` or `InsertMany` succeeds on
   that same client THEN idle fetch SHALL resume without waiting out the
   remaining poll interval. A test SHALL assert elapsed wait is well
   under the configured `PollInterval` (upper bound, not a lower-bound
   count).
3. WHEN `NotifyWakeup` is true and another process commits an `Insert`
   with the flag set THEN a listening worker SHALL wake via Postgres
   `LISTEN`/`NOTIFY` on channel `drover` (empty payload) and claim the
   job without waiting out `PollInterval`.
4. WHEN `InsertTx` / `InsertManyTx` runs with the flag set THEN the
   system SHALL emit `NOTIFY` in the caller's transaction, so listeners
   wake only if that transaction commits.
5. WHEN `LISTEN` cannot be established at `Start` THEN the system SHALL
   still return nil from `Start`, log the failure, and keep polling.
6. WHEN the listen connection drops after `Start` THEN the system SHALL
   log, reconnect, and keep polling in the meantime — it SHALL NOT stop
   the worker pool.
7. WHEN `Stop` is called during an idle wait THEN the system SHALL not
   wait out `PollInterval` (existing Cycle C edge; wake channel must not
   regress it).
8. WHEN `NotifyWakeup` is true, `Start` SHALL acquire a dedicated
   connection for `LISTEN` (session-scoped). Documentation SHALL state
   that PgBouncer transaction pooling is incompatible with this flag.

**Independent Test**: (2)(7) against memdriver with a long `PollInterval`
and an insert from the same client; (3)(4) pgdriver integration with two
clients; (5) can be a unit test with a stub listener or a documented
pgdriver test that closes the listen path.

---

### P1: Benchmark harness ⭐ MVP

**User Story**: As a maintainer, I want a runnable harness that prints
jobs/sec and drain percentiles next to the machine and Postgres it ran
on, so a README figure is reproducible.

**Why P1**: RFC names `cmd/drover-bench` and forbids unverifiable claims.

**Acceptance Criteria**:

1. WHEN `go run ./cmd/drover-bench` is invoked with `--database` (or
   `DATABASE_URL`) and `--mode enqueue` THEN the system SHALL insert
   `--jobs` no-op jobs via `InsertMany` in chunks of `--batch` and print
   jobs/sec plus a methodology block naming GOOS/GOARCH/NumCPU, Postgres
   version (`SELECT version()`), `--jobs`, `--batch`, `--concurrency`,
   and that handlers are no-ops.
2. WHEN `--mode drain` THEN the system SHALL insert the jobs, start a
   worker pool of `--concurrency` no-op handlers, wait until every job
   has completed, and print drain jobs/sec plus p50/p95/p99 enqueue-to-
   completion latency.
3. WHEN `--database` is missing and `DATABASE_URL` is empty THEN the
   process SHALL exit non-zero with a usage error and insert nothing.
4. WHEN `--jobs` or `--batch` is less than 1 THEN the process SHALL exit
   non-zero with a usage error.
5. WHEN `--mode` is neither `enqueue` nor `drain` THEN the process SHALL
   exit non-zero with a usage error.
6. The binary SHALL live under `cmd/drover-bench/` and SHALL NOT be added
   to GoReleaser.

**Independent Test**: unit tests of flag validation and output shape with
a fake runner; optional integration that runs enqueue mode against
`testdb` when Docker is up.

---

### P1: README benchmark table ⭐ MVP

**User Story**: As a reader, I want the headline numbers, the exact
command, the hardware, and the caveats in one place, so I can tell a
measurement from a marketing figure.

**Why P1**: Project constraint: benchmarks publish methodology alongside
numbers.

**Acceptance Criteria**:

1. WHEN the README is opened THEN it SHALL contain a `## Benchmarks`
   section with a table of enqueue jobs/sec and drain p50/p95/p99 from a
   real `drover-bench` run, plus GOOS/GOARCH, CPU, Postgres version, job
   count, batch size, concurrency, and the caveat that jobs are no-ops on
   a single node.
2. WHEN the section names a Config field (`NotifyWakeup`, `InsertMany`)
   THEN `example_test.go` SHALL contain a compile-checked Example that
   names those fields (so a rename fails `go test`).
3. WHEN the Planned API sample is read THEN it SHALL show `InsertMany`
   alongside `Insert` / `InsertTx`.
4. WHEN the Roadmap blurb is read THEN Cycle G's bench work SHALL be
   described as shipped, not as "then: benchmarks…".

**Independent Test**: `go test` compiles Examples; human review of the
table against the harness output recorded in the cycle.

---

## Edge Cases

- WHEN a batch mixes queues THEN the system SHALL persist each job on
  the queue its `Opts` named (or `"default"`).
- WHEN `InsertMany` fails after validation (COPY or INSERT error) THEN
  the system SHALL return a wrapped error and insert zero rows.
- WHEN `NotifyWakeup` is true and the inserted jobs are all `scheduled`
  in the future THEN fetch MAY wake and claim nothing — that is harmless.
- WHEN many notifies arrive while fetch is already running a round THEN
  extra wakes SHALL coalesce (wake channel capacity 1) rather than queue
  unbounded.
- WHEN `memdriver.InsertTx` is called THEN it SHALL still return
  `ErrTxUnsupported`; `InsertManyTx` SHALL do the same.

## Implicit-requirement dimensions

| Dimension | Resolution |
| --- | --- |
| Input validation & bounds | Empty batch OK; nil/empty kind/marshal fail the whole batch; bench flags bounded (`jobs`/`batch` ≥ 1). |
| Failure / partial-failure | Batch is all-or-nothing. LISTEN failure degrades to poll. |
| Idempotency / retry / duplicate | InsertMany is not idempotent; each call creates new rows (existing Insert contract). |
| Auth boundaries & rate limits | N/A — database URL is the only credential, as Cycle F. |
| Concurrency / ordering | COPY in one transaction; claim still `FOR UPDATE SKIP LOCKED`; returned rows match input order. |
| Data lifecycle / expiry | N/A — no retention this cycle. |
| Observability | Bench prints methodology; LISTEN errors logged at error level; no new Prometheus series. |
| External-dependency failure | Dropped LISTEN connection: reconnect + poll. |
| State-transition integrity | Batch insert uses the same available/scheduled CASE as `InsertJob` (AD-035). Claim batch size unchanged (AD-022). |

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| BATCH-01 | P1: Atomic batch insert AC1 | Tasks T1–T5 | In Tasks |
| BATCH-02 | P1: Atomic batch insert AC2 | Tasks T2, T5 | In Tasks |
| BATCH-03 | P1: Atomic batch insert AC3 | Tasks T2, T5 | In Tasks |
| BATCH-04 | P1: Atomic batch insert AC4–5 | Tasks T2, T3, T5 | In Tasks |
| BATCH-05 | P1: Atomic batch insert AC6, AC8 | Tasks T2, T3, T5 | In Tasks |
| BATCH-06 | P1: Atomic batch insert AC7 | Tasks T1, T3 | In Tasks |
| WAKE-01 | P1: Optional notify wake-up AC1 | Tasks T7 | In Tasks |
| WAKE-02 | P1: Optional notify wake-up AC2, AC7 | Tasks T7 | In Tasks |
| WAKE-03 | P1: Optional notify wake-up AC3, AC8 | Tasks T8 | In Tasks |
| WAKE-04 | P1: Optional notify wake-up AC4 | Tasks T8 | In Tasks |
| WAKE-05 | P1: Optional notify wake-up AC5–6 | Tasks T8 | In Tasks |
| BENCH-01 | P1: Benchmark harness AC1–2 | Tasks T10 | In Tasks |
| BENCH-02 | P1: Benchmark harness AC3–6 | Tasks T10 | In Tasks |
| DOC-01 | P1: README table AC1–4 | Tasks T11 | In Tasks |

**ID format:** `BATCH-NN`, `WAKE-NN`, `BENCH-NN`, `DOC-NN`

**Coverage:** 14 total, 0 mapped to tasks, 14 unmapped ⚠️ (Tasks phase fills this)

---

## Success Criteria

- [x] A producer can flush N jobs in one `InsertMany` / `InsertManyTx` call
- [x] A worker with `NotifyWakeup` starts a just-inserted job without waiting a full poll interval
- [x] `go run ./cmd/drover-bench --mode drain …` prints jobs/sec and percentiles with methodology
- [x] README table matches a real harness run and Examples compile
- [x] `go test -race ./...` stays Docker-free; integration covers COPY and cross-client NOTIFY
