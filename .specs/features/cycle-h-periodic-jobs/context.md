# Periodic Jobs + Unique Jobs — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's
auto-decision rule. Each records the options considered with reasons for and against,
the choice, and why. They are mirrored into `.specs/STATE.md` as `AD-067`–`AD-076`.

Decisions already binding on this cycle and **not** relitigated here: ADR-0002
(Postgres-only; advisory locks are in-database coordination), ADR-0003
(at-least-once; handlers required-idempotent), ADR-0004 (stdlib-first, fuzz
cron parsing, no `pkg/`), AD-008 (unexported driver), AD-020/AD-035 (database
clock for leases and dueness), AD-022 (claim ≤ idle workers), AD-024
(single-use Start/Stop), AD-034 (`*InsertOpts` struct), AD-037 (structural
errors panic; tuning values warn), AD-043 (ops bind failure fails Start —
contrast: lock failure does not), AD-047 (ops server shuts down last),
AD-058 (InsertMany all-or-nothing).

---

## D-1 — Unique key public API

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `InsertOpts.UniqueKey string`; empty = not unique | One field; AD-034 keeps future unique options as fields; callers who want an args hash compute it | No built-in args-hash or period window |
| (b) `UniqueOpts{ByArgs, Key, Period, States}` River-style | Matches research §3.1 full surface | Too much API for a PR-sized cycle; state-set configurability is a second product |
| (c) Unique only as an internal periodic mechanism; no public Insert option | Smaller | RFC names "unique-job option"; producer retries are the other half of research C |

**Chosen: (a).** Period bucketing for periodic jobs is done by putting the fire time in the key (D-8). Args-hash is a helper a later cycle can add without a new exported type.

---

## D-2 — Partial unique index predicate

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Unique among `available`/`scheduled`/`retryable`/`running` | Terminal rows do not block a real re-enqueue; running is included so a live execution still dedups | A long-running job blocks the next producer retry until it finishes — which is the point |
| (b) Unique among all states forever | Simplest index (`WHERE unique_key IS NOT NULL`) | A completed job would forbid the next legitimate run of a periodic kind |
| (c) Unique including `dead` | Stops redrive+insert races | Operator redrive (AD-053) plus a producer insert should be allowed; dead is retained on purpose |

**Chosen: (a).** Matches research: "a completed job doesn't block a legitimate re-enqueue".

---

## D-3 — Duplicate insert result

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `ErrDuplicateJob` sentinel; no row; InsertMany aborts the batch | Explicit; `errors.Is` in the scheduler; consistent with AD-058 | Caller who wanted "return existing" must Get |
| (b) Return the existing row with nil error | Producer retries look like success | Cannot tell insert from dedup; surprising |
| (c) `(row, bool, error)` or a result struct | Precise | New public type for a rare path |

**Chosen: (a).** The scheduler treats `errors.Is(..., ErrDuplicateJob)` as tick success.

---

## D-4 — Cron parser dependency

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Small stdlib parser: 5-field + `@every`; fuzz it; `internal/cron` | ADR-0004 stdlib-first and its explicit "fuzz cron parsing"; explainable | DST edge cases we must document |
| (b) `robfig/cron/v3` | DST/name fields battle-tested | External dep the stdlib-first ADR says to justify; grammar we do not own |
| (c) `@every` only, no cron fields | Tiny | RFC says "cron-style"; operators expect `0 * * * *` |

**Chosen: (a).** Not an escalation: the ADR names owning the parser. Location is `*time.Location` on the job (nil = UTC); `@every` is Unix-epoch aligned and timezone-agnostic. Named months/days and 6-field are out of scope.

---

## D-5 — How periodic jobs are registered

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `Config.PeriodicJobs []PeriodicJob` at construction; same list on every potential leader | Research River pattern; AD-037 validates at construction; any client can win the lock | Changing a schedule is a deploy |
| (b) Rows in a `drover_periodic` table, edited via CLI | Dynamic | Schema+CLI product; not in the RFC row; two sources of truth with code workers |
| (c) Separate `Scheduler` process (Asynq) | Clear duty | Extra binary; RFC is library+CLI, not a third command |

**Chosen: (a).** Empty ID, duplicate ID, nil Args, or bad cron panics. A process with an empty slice does not take the lock.

---

## D-6 — Advisory lock mechanics

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Session `pg_try_advisory_lock(int64)` on a dedicated connection, documented constant key; not on `driver.Driver` (optional interface like `wakeupListener`) | Crash releases the lock; pool connections must not carry session state (LISTEN lesson) | One extra connection while `PeriodicJobs` is set |
| (b) Transaction-level lock held open | Auto-release on commit | Would pin a transaction for the leader's lifetime |
| (c) Put `TryLeaderLock` on `driver.Driver` | Symmetric adapters | Forces memdriver to fake a PG primitive; LISTEN already stayed off the interface |

**Chosen: (a).** memdriver: always leader (D-7). Lock key is a documented int64, not `hashtext()` (hashtext is not a compatibility promise across Postgres versions).

---

## D-7 — memdriver and Start-failure policy

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) memdriver always leader; lock/connect failure on Postgres logs and retries; Start succeeds | Unit suite Docker-free; a worker that cannot elect is still a correct worker (contrast AD-043) | Silent "no ticks" until someone reads the log |
| (b) Fail Start if PeriodicJobs is set and the lock conn cannot be acquired | Loud | A replica that cannot open a second connection could not work jobs either |

**Chosen: (a).** Leadership gain/loss is logged at INFO; acquire failures at ERROR.

---

## D-8 — Periodic unique key and first tick

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `UniqueKey = id + "/" + fireRFC3339NanoUTC`; first fire strictly after leadership; `@every` aligned to Unix epoch | Lock can split-brain briefly; uniqueness makes that harmless. No RunOnStart stampede on failover. Nanosecond keys keep sub-second `@every` ticks distinct | A deploy or lock-retry gap just after a tick waits a full period |
| (b) RunOnStart plus unique-by-kind (at most one non-terminal instance) | Fast feedback | Skip-next-tick while a long job runs; RFC does not ask for it |
| (c) Unique by kind only, no fire time in the key | Simpler | A still-running tick blocks the next hour's enqueue |

**Chosen: (a).** Research: "unique by period" so a long run does not skip the next firing.

---

## D-9 — Next-run clock

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Leader computes `Next(now)` in process; Insert still uses the database clock for state | Cron in SQL is unexplainable; AD-035 already governs dueness | Leader/DB skew can make a job available slightly early or late |
| (b) Compute next fire in SQL | One clock | Unmaintainable cron in plpgsql |

**Chosen: (a).** Same trade-off as "we cannot falsify database-clock properties while test and DB share a host" (known weak sensor).

---

## D-10 — Scheduler lifecycle

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Scheduler goroutine shares `fetchCtx` with rescuer/refresher; Stop does not wait out the next fire | No enqueue after claiming stops; L-004 upper-bound; goleak | A tick that falls during drain is left to the next Start |
| (b) Keep scheduling through drain | Fewer missed ticks | Enqueues work nobody in this process will claim, against the spirit of stop-fetch |

**Chosen: (a).** Ops server remains last (AD-047). Heartbeat still outlives fetch (AD-018 as generalised by Cycle C).

---

## Agent's Discretion

- Exact int64 lock key and its comment.
- Whether `internal/cron` is that package name or `internal/schedule`.
- How pgdriver proves uniqueness (unique_violation `23505` translation vs `ON CONFLICT`).
- Leader retry interval (may reuse `PollInterval`).
- Example app's periodic kind name and cron string.

## Declined / Undiscussed Gray Areas → Assumptions

All gray areas above were auto-decided. None deferred silently.

---

## Specific References

- Research `rq03` §3.1 unique jobs (partial unique, state-scoped) and §5.2 periodic (advisory-lock leader + unique by period).
- Research `rq05` Cycle H row: advisory locks, cron parsing, idempotent scheduling.
- ADR-0004: fuzz cron parsing; stdlib-first.
- `wakeupListener` in `client.go` — precedent for pg-only session surfaces that stay off `driver.Driver`.

---

## Deferred Ideas

- `UniqueOpts.ByArgs` / period window / configurable state set (River parity).
- `RunOnStart`.
- CLI `--unique-key`.
- `ExecutedOnce` middleware.
- Leadership for rescuer/cleaner.
- Named cron fields (`MON`, `JAN`) and 6-field cron.
