# Periodic Jobs + Unique Jobs Specification

## Problem Statement

N worker processes that each ran a cron loop would enqueue N copies of
every tick. Cycle H is the coordination cycle: one elected leader
enqueues periodic jobs, and a partial unique index makes a double-fire
harmless. Unique jobs are also a public enqueue option so producer
retries do not create duplicate rows.

## Goals

- [ ] `InsertOpts.UniqueKey` deduplicates among non-terminal jobs of the
      same kind and queue, enforced by a partial unique index.
- [ ] A duplicate insert returns `ErrDuplicateJob` and inserts no row.
- [ ] `Config.PeriodicJobs` registers cron/`@every` schedules in code;
      only an advisory-lock leader enqueues them.
- [ ] Periodic enqueue sets a per-tick unique key so a split-brain
      leader cannot create two rows for the same firing.
- [ ] Unit coverage of unique insert and in-process scheduling via
      `memdriver` (no Docker required for the unit suite).

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| `robfig/cron` or any cron dependency | ADR-0004 stdlib-first; ADR-0004 already names fuzzing our own parser |
| Quartz 6/7-field cron, named months/days, `?`, seconds field | Unix 5-field + `@every` is the RFC's "cron-style" |
| `RunOnStart` | First tick is the next instant after Start; River extra, not in the RFC row |
| Runtime add/remove of periodic jobs | Construction-time `Config` only (AD-037); Asynq PeriodicTaskManager is a later product |
| Unique-by-args-hash helper / time-window `UniqueOpts.Period` | Caller supplies `UniqueKey`; periodic jobs embed the fire instant themselves |
| Unique jobs on the CLI (`--unique-key`) | RFC row is library coordination, not operator flags |
| Execution-time idempotency middleware (`ExecutedOnce`) | Research layer (b); handlers stay required-idempotent (ADR-0003) |
| Leader election for the rescuer/cleaner | Those already run on every client; only the periodic enqueuer must be single |
| Dynamic timezone database loading | `*time.Location` on the periodic job; nil means UTC |
| New Prometheus series for periodic enqueue | Cycle E metrics stay as they are; leadership is a log line |
| Redis / etcd / extra consensus | Postgres advisory locks are the RFC's coordination tool |
| Status page | Cycle I |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here — nothing is left silently unclear.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| ASM-01 — Unique key shape | `InsertOpts.UniqueKey string`; empty means not unique; stored NULL | One field; callers who want an args hash compute it | y |
| ASM-02 — Unique index | Partial unique on `(queue, kind, unique_key)` where `unique_key IS NOT NULL` and `state IN (available, scheduled, retryable, running)` | Completed/cancelled/dead must not block a legitimate re-enqueue | y |
| ASM-03 — Duplicate API | Sentinel `ErrDuplicateJob`; no row inserted; `InsertMany` fails the whole batch | Matches InsertMany all-or-nothing (AD-058/ASM-03); scheduler uses `errors.Is` | y |
| ASM-04 — Cron grammar | 5-field (`min hour dom month dow`) plus `@every <ParseDuration>`; `0` and `7` are Sunday; numbers only | ADR-0004 fuzzing note; no new module | y |
| ASM-05 — Registration | `Config.PeriodicJobs []PeriodicJob` at construction; duplicate or empty IDs panic; invalid cron panics | AD-037 structural errors panic; every potential leader must carry the same list | y |
| ASM-06 — Leader lock | Session `pg_try_advisory_lock` on a dedicated connection; documented int64 key; only clients with a non-empty `PeriodicJobs` participate | Session lock dies with the connection (crash path); LISTEN already taught us not to take session state from the pool | y |
| ASM-07 — memdriver leadership | Always leader when `PeriodicJobs` is non-empty | Unit suite stays Docker-free | y |
| ASM-08 — Periodic unique key | `id + "/" + fireTime.UTC().Format(time.RFC3339Nano)` | Belt-and-suspenders with the lock; fire time is the period bucket. Seconds-only RFC3339 collapsed sub-second `@every` ticks | y |
| ASM-09 — First tick | Strictly after this client becomes leader (Start, for the first leader); no `RunOnStart`; ticks missed while not leader are skipped | Smaller surface; failover does not stampede completed ticks | y |
| ASM-10 — Next-run clock | Leader uses `time.Now()` in the job's location; `Insert` still uses the database clock for available vs scheduled (AD-035) | Cron next-time in SQL is unexplainable; skew is bounded by poll | y |
| ASM-11 — `@every` alignment | Next instant after `t` aligned to Unix-epoch multiples of the duration (`Truncate` then add) | Failover-stable buckets; unique key still collapses doubles | y |
| ASM-12 — Lock/connect failure | Log and retry; `Start` still succeeds; workers keep processing | Contrast AD-043: a worker that cannot elect is still a correct worker | y |
| ASM-13 — Shutdown | Scheduler shares `fetchCtx` with the rescuer: it stops when claiming stops | No new periodic rows during drain; ops server still last (AD-047) | y |
| ASM-14 — `JobRow.UniqueKey` | Exported; empty when the row is not unique | Research: handlers may use it as a downstream idempotency key | y |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Unique jobs at enqueue ⭐ MVP

**User Story**: As a producer, I want an insert with a unique key to
create at most one non-terminal row of that kind and queue, so a retried
producer call does not double-enqueue.

**Why P1**: RFC names the partial unique index; periodic jobs depend on
it as the split-brain backstop.

**Acceptance Criteria**:

1. WHEN `Insert` is called with a non-empty `UniqueKey` and no
   non-terminal row exists for that `(queue, kind, unique_key)` THEN the
   system SHALL persist the job with that key and return it.
2. WHEN `Insert` is called with the same `(queue, kind, UniqueKey)` as an
   existing `available`, `scheduled`, `retryable`, or `running` job THEN
   the system SHALL insert no row and return an error for which
   `errors.Is(err, ErrDuplicateJob)` is true.
3. WHEN a job with that unique key is `completed`, `cancelled`, or
   `dead` THEN a later `Insert` with the same key SHALL succeed and
   create a new row.
4. WHEN `UniqueKey` is empty THEN the system SHALL persist `unique_key`
   as NULL and SHALL NOT participate in uniqueness.
5. WHEN `InsertMany` includes any item whose unique key collides with an
   existing non-terminal row, or with another item in the same batch,
   THEN the system SHALL insert zero rows of that batch and return
   `ErrDuplicateJob`.
6. WHEN two concurrent `Insert` calls share a unique key THEN at most
   one row SHALL exist afterwards (the other returns `ErrDuplicateJob`).
7. WHEN `JobRow` is returned for a unique insert THEN `UniqueKey` SHALL
   equal the key that was stored.

**Independent Test**: memdriver unit tests for 1–5 and 7; pgdriver
integration for 1–6 including two concurrent goroutines against one
database (AC6).

---

### P1: Cron grammar ⭐ MVP

**User Story**: As a developer, I want to write `"0 * * * *"` or
`"@every 30s"` and know the next fire time, so schedules are reviewable
without a third-party parser.

**Why P1**: RFC's "cron-style scheduler"; ADR-0004 requires a fuzzable
parser we own.

**Acceptance Criteria**:

1. WHEN a 5-field expression is valid THEN `Next(t)` SHALL return the
   earliest instant strictly after `t` that matches, interpreted in the
   provided location (UTC if nil).
2. WHEN the expression is `@every d` and `d` is a positive
   `time.ParseDuration` THEN `Next(t)` SHALL return the next Unix-epoch
   aligned instant strictly after `t` (`t.Truncate(d).Add(d)` in UTC).
3. WHEN the expression has the wrong arity, an out-of-range field, an
   empty `@every` duration, a non-positive duration, or unknown tokens
   THEN parse SHALL return an error and SHALL NOT panic.
4. WHEN `dow` is `0` or `7` THEN both SHALL mean Sunday.
5. Fuzzing SHALL exist for the parser: no corpus input panics.

**Independent Test**: table-driven unit tests in `internal/cron` (or the
chosen package) plus `FuzzParse`; no Docker.

---

### P1: Periodic registration and leader enqueue ⭐ MVP

**User Story**: As an operator of N processes, I want exactly one of
them to enqueue each cron tick, so a fleet does not N-copy every
schedule.

**Why P1**: RFC core: advisory-lock leader election + periodic enqueue.

**Acceptance Criteria**:

1. WHEN `Config.PeriodicJobs` is empty THEN `Start` SHALL NOT take the
   advisory lock and SHALL NOT run a scheduler goroutine.
2. WHEN `PeriodicJobs` is non-empty and this client holds the lock THEN
   the system SHALL insert a job for a schedule whose next fire has
   arrived, with `ScheduledAt` equal to that fire time, `Args`/`Queue`
   from the `PeriodicJob`, and `UniqueKey` equal to
   `id + "/" + fireTime.UTC().Format(time.RFC3339Nano)`.
3. WHEN a second client is configured with the same `PeriodicJobs` THEN
   at most one of the two SHALL hold the lock at a time, and a tick
   SHALL produce one row even if both attempt to enqueue (unique key).
4. WHEN the leader's lock connection drops THEN another client SHALL be
   able to acquire the lock and continue enqueueing subsequent ticks.
5. WHEN `Insert` for a tick returns `ErrDuplicateJob` THEN the scheduler
   SHALL treat that tick as done (not a failure) and SHALL NOT retry it
   as a handler failure.
6. WHEN a client becomes leader THEN the first enqueue for each job SHALL be
   the first fire strictly after leadership, not an immediate run and not a
   replay of ticks from before the lock was held.
7. WHEN a `PeriodicJob` has an empty `ID`, a duplicate `ID` in the
   slice, empty `Args`, or an unparseable `Cron` THEN constructing the
   client SHALL panic.
8. WHEN acquiring or holding the lock fails THEN `Start` SHALL still
   return nil and the worker pool SHALL run; the failure SHALL be
   logged.
9. WHEN `Stop` is called THEN the scheduler goroutine SHALL return
   without waiting for the next fire time (upper-bound elapsed
   assertion, not a lower-bound count), and goleak SHALL report no extra
   goroutines.

**Independent Test**: (1)(5)(6)(7)(9) against memdriver with
`testing/synctest` or a fake clock/sleep where the suite already does;
(2)(3)(4)(8) pgdriver integration with two clients. AC9 must assert
elapsed wait is well under a long next-fire (L-004).

---

### P1: Docs and example ⭐ MVP

**User Story**: As a reader, I want unique jobs and periodic
registration shown in the README and a compile-checked Example, so the
new API cannot silently rot.

**Why P1**: Lesson L-009; public API without an Example is how Cycle E
regressed.

**Acceptance Criteria**:

1. WHEN the README Planned API / usage section is read THEN it SHALL
   show `InsertOpts.UniqueKey` and `Config.PeriodicJobs`.
2. WHEN those names appear THEN `example_test.go` SHALL contain a
   compile-checked Example that names them.
3. WHEN the Roadmap blurb is read THEN Cycle H SHALL be described as
   shipped, not as "Then: periodic jobs…".
4. WHEN the email example is read THEN it SHALL register at least one
   periodic job (so the example still exercises the cycle's surface).

**Independent Test**: `go test` compiles Examples; example package
builds.

---

## Edge Cases

- WHEN two items in one `InsertMany` share a unique key THEN the batch
  SHALL fail with `ErrDuplicateJob` and insert nothing.
- WHEN `UniqueKey` is set on a delayed job (`ScheduledAt` in the future)
  THEN uniqueness SHALL still hold against other non-terminal rows
  (including `scheduled`).
- WHEN the leader is the only client and its lock loop is retrying THEN
  ticks may be late; they SHALL still enqueue once leadership is gained
  if the fire time is in the past (`ScheduledAt` in the past is due —
  AD-035).
- WHEN `Location` is non-nil THEN cron fields SHALL be interpreted in
  that location; `@every` stays Unix-epoch/UTC aligned.
- WHEN `NotifyWakeup` is true and the leader inserts a due periodic job
  THEN idle fetch SHALL wake as for any other insert.
- WHEN `memdriver` is used THEN unique constraints SHALL be enforced
  under the existing mutex; `InsertTx` remains `ErrTxUnsupported`.

## Implicit-requirement dimensions

| Dimension | Resolution |
| --- | --- |
| Input validation & bounds | Empty UniqueKey = not unique; cron parse errors at construction panic; `@every` duration must be > 0. |
| Failure / partial-failure | Unique conflict is all-or-nothing on InsertMany. Lock failure degrades to non-leader; Start succeeds. |
| Idempotency / retry / duplicate | Enqueue dedup via unique key. Handlers remain required-idempotent. Duplicate periodic tick is success. |
| Auth boundaries & rate limits | N/A — database URL is the only credential. |
| Concurrency / ordering | Partial unique index is the race winner; advisory lock is best-effort single enqueuer. |
| Data lifecycle / expiry | Unique index ignores terminal states; no retention/pruning this cycle. |
| Observability | Leadership gain/loss and lock errors logged; no new Prometheus series. |
| External-dependency failure | Dead lock connection: log, retry, other client may win. |
| State-transition integrity | Unique insert uses existing available/scheduled CASE (AD-035). Claim path unchanged. |

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| UNIQ-01 | P1: Unique jobs AC1, AC4, AC7 | Design | Pending |
| UNIQ-02 | P1: Unique jobs AC2, AC3 | Design | Pending |
| UNIQ-03 | P1: Unique jobs AC5, AC6 | Design | Pending |
| CRON-01 | P1: Cron grammar AC1, AC4 | Design | Pending |
| CRON-02 | P1: Cron grammar AC2, AC3, AC5 | Design | Pending |
| PER-01 | P1: Leader enqueue AC1, AC7 | Design | Pending |
| PER-02 | P1: Leader enqueue AC2, AC5, AC6 | Design | Pending |
| PER-03 | P1: Leader enqueue AC3, AC4, AC8 | Design | Pending |
| PER-04 | P1: Leader enqueue AC9 | Design | Pending |
| DOC-01 | P1: Docs AC1–4 | Design | Pending |

**ID format:** `UNIQ-NN`, `CRON-NN`, `PER-NN`, `DOC-NN`

**Coverage:** 10 total, 0 mapped to tasks, 10 unmapped ⚠️ (Tasks phase fills this)

---

## Success Criteria

- [ ] A retried `Insert` with the same unique key does not create a
      second non-terminal row
- [ ] Two started clients with the same `PeriodicJobs` produce one row
      per tick
- [ ] Killing the leader lets the other client enqueue the next tick
- [ ] `go test -race ./...` stays Docker-free; integration covers the
      unique index and two-client lock
- [ ] README + Example name `UniqueKey` and `PeriodicJobs`
