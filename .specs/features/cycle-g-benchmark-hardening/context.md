# Benchmark + Hardening — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's
auto-decision rule. Each records the options considered with reasons for and against,
the choice, and why. They are mirrored into `.specs/STATE.md` as `AD-058`–`AD-066`.

Decisions already binding on this cycle and **not** relitigated here: ADR-0002
(LISTEN deferred as optional tuning; NOTIFY serialization and PgBouncer caution),
ADR-0003 (at-least-once), ADR-0004 (`cmd/` binaries, stdlib `flag`, no `pkg/`),
AD-006 (constant default poll interval), AD-008 (unexported driver),
AD-020/AD-035 (database clock for leases and dueness), AD-022 (claim ≤ idle
workers; no prefetch buffer), AD-024 (single-use Start/Stop), AD-034 (`*InsertOpts`
struct), AD-037 (structural errors panic; tuning values warn), AD-043 (ops bind
failure fails Start — contrast: notify is optional).

---

## D-1 — Batch insert public API

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `InsertMany(ctx, []InsertItem)` with per-item `Args` + `*InsertOpts`, plus `InsertManyTx` | Mixed kinds/queues/delays in one flush; matches how producers actually buffer. | One more exported type. |
| (b) `InsertMany(ctx, []JobArgs, opts *InsertOpts)` — one opts for the whole batch | Smaller API. | Cannot mix queues or schedules without N calls, defeating the feature. |
| (c) No public API; COPY only inside `drover-bench` | Smaller surface. | RFC lists batch insert as a library deliverable, not a hidden harness trick. |

**Chosen: (a).** A batch that cannot mix queues is not what a producer flush looks like.

---

## D-2 — How COPY returns rows

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Session temp table + `CopyFrom` + `INSERT … SELECT … RETURNING` (same CASE as `InsertJob`) | COPY speed; IDs and AD-035 state come back; all-or-nothing in one transaction. | Extra round trips to create/truncate the staging table. |
| (b) `pgx.Batch` of `INSERT … RETURNING` | Native RETURNING; simpler. | RFC names `COPY FROM`; not the write pattern River uses for bulk. |
| (c) COPY directly into `drover_jobs` without RETURNING | Fastest. | Callers lose IDs and cannot tell scheduled from available. |

**Chosen: (a).** pgx `CopyFrom` has no RETURNING; the staging-table pattern is the documented way to combine them.

---

## D-3 — `LISTEN/NOTIFY` default

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `Config.NotifyWakeup bool`, default false | RFC says optional; ADR-0002 records production outages; PgBouncer transaction pooling breaks LISTEN. Poll is already correct. | Idle latency stays `PollInterval` until operators opt in. |
| (b) Default true | Snappier out of the box. | Surprises anyone behind transaction pooling; pays commit-serialization on every insert. |
| (c) Always on, no flag | One code path. | Contradicts ADR-0002. |

**Chosen: (a).** Opt-in is the only default that stays correct on PgBouncer and does not relitigate ADR-0002.

---

## D-4 — Who emits NOTIFY, and how often

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Inserting `Client` emits one `pg_notify('drover','')` per successful `Insert`/`InsertMany` when the flag is set; `InsertTx`/`InsertManyTx` emit inside the caller transaction | Coalesced per call, not per row; listeners wake only on commit; producers that never `Start` still wake workers. | Inspector/CLI enqueue does not wake (poll). |
| (b) `AFTER INSERT` trigger | Wakes even for raw SQL. | One notify per row; the Recall.ai serialization shape. |
| (c) Always notify from `pgdriver.Insert`, flag only controls LISTEN | Workers opt in, producers always pay. | High-volume producers without listeners still serialize commits. |

**Chosen: (a).** The flag is the producer's "I have listeners" switch as well as the worker's "I listen" switch. Set it on both.

---

## D-5 — Same-process wake vs LISTEN

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Capacity-1 `wake` channel on the Client, signalled after local insert when the flag is set; a pgdriver LISTEN goroutine signals the same channel for other processes | Unit suite covers the latency property without Docker; LISTEN is the cross-process path. | Two signalling sources to keep in mind. |
| (b) LISTEN only, even for the inserting process | One path. | Unit tests would need Postgres; self-notify still requires a session LISTEN. |
| (c) Driver method `WaitForWake` replacing `sleep` | Puts wait in the store. | Holds a connection for the whole idle interval if acquired naively; memdriver would need a cond the Client already has. |

**Chosen: (a).** Fetch `sleep` gains a third select arm; LISTEN is pg-only and is **not** added to `driver.Driver`.

---

## D-6 — LISTEN failure vs Start

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Log, continue; `Start` succeeds; reconnect on drop | Wake-up is an optimization. A running worker that polls is correct. | A misconfigured flag looks like a slow poll until someone reads the log. |
| (b) Fail `Start` (AD-043 style) | Loud. | Equates an optional accelerator with "the worker is unobservable". Operators could not run behind transaction pooling even if they accepted poll. |

**Chosen: (a).** Document PgBouncer incompatibility on the Config field.

---

## D-7 — Batch claim (RFC bullet)

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Do nothing to claim size; document that Cycle C already batches `FetchAvailable` to idle workers | Honours AD-022; the RFC was written before Cycle C. | Looks like the bullet was skipped if the README does not say so. |
| (b) Add `FetchBatchSize` that may exceed idle workers | Matches "grab 100" research soundbite. | Prefetch: rows `running` and leased with nobody executing them — the state AD-022 exists to forbid. |
| (c) Raise default concurrency so batches are larger | Indirect. | Conflates pool size with claim size; not fetch tuning. |

**Chosen: (a).** README Design section already says "SKIP LOCKED batch claim"; Benchmarks/docs will state the batch equals idle workers.

---

## D-8 — Bench modes and published numbers

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `cmd/drover-bench` with `--mode enqueue\|drain`, methodology printed every run, README table filled from one real run | RFC + research (River-style honesty). | Numbers age; the command next to them is the antidote. |
| (b) README template with no numbers ("run the harness") | Never stale. | Not a table of results; the RFC asks for one. |
| (c) `go test -bench` numbers in README | CI-reproducible. | Research: microbenchmarks are not the headline; they omit Postgres. |

**Chosen: (a).** Caveats are mandatory: trivial no-op jobs, single node, named hardware.

---

## D-9 — Empty batch and memdriver `InsertManyTx`

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Empty/`nil` slice → empty result, nil error, no write; `memdriver.InsertManyTx` returns `ErrTxUnsupported` like `InsertTx` | Matches "nothing to do"; keeps the memdriver tx story consistent. | Callers must still use Postgres for transactional batches. |
| (b) Empty slice is `ErrInvalidKind` | Forces callers to check length. | Punishes a flush that happened to be empty. |

**Chosen: (a).**

---

## Agent's Discretion

- Temp table name, exact SQL, and whether `UNLOGGED` is specified (temp tables are already unlogged).
- Wake channel placement (`Client` vs `runner`) as long as Stop still beats `PollInterval` and wakes coalesce.
- Bench default flag values, as long as invalid values are rejected and methodology is printed.
- How the pgdriver test proves COPY was used (statement spy, `pg_stat_statements`, or structural: one call produces N rows and the code path is `CopyFrom`).

## Declined / Undiscussed Gray Areas → Assumptions

All gray areas above were auto-decided. None deferred silently.

---

## Specific References

- ADR-0002: LISTEN/NOTIFY as deferred tuning; skip-locked batched claims + polling as the baseline.
- Research `rq02` §1.2: NOTIFY is not durable, serializes commits, breaks PgBouncer transaction pooling; correct design is poll + optional coalesced notify.
- Research `rq05`: `cmd/drover-bench`, enqueue throughput + drain percentiles, name the hardware, trivial jobs caveat.
- pgx: `CopyFrom` has no RETURNING — staging table then `INSERT SELECT`.

---

## Deferred Ideas

- Batch completer (River amalgamates finalizes) — not in the RFC row.
- `go test -bench` CI regression job.
- Inspector/CLI `--notify` for operator enqueue.
- Example-app kill-a-worker demo (research only).
