# RQ02 — Storage backend: PostgreSQL vs Redis

Research date: 2026-07-22. Scope: which storage backend should be drover's backbone, answered at mechanism level so the reasoning is auditable.

---

## 1. PostgreSQL as a queue

### 1.1 The `FOR UPDATE SKIP LOCKED` claim pattern

The canonical claim query (this is essentially what River, Que, pgmq-style libraries, and Good Job do, modulo syntax):

```sql
WITH claimed AS (
  SELECT id
  FROM jobs
  WHERE state = 'available'
    AND scheduled_at <= now()
  ORDER BY priority, id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET state = 'running', attempted_at = now(), attempted_by = $2, attempt = attempt + 1
FROM claimed
WHERE j.id = claimed.id
RETURNING j.*;
```

**Exact locking behavior.** `FOR UPDATE` takes an *exclusive row-level lock* on each returned tuple. Postgres row locks are not held in the shared lock-manager table — they are written into the tuple header itself (`xmax` is set to the locker's transaction ID plus infomask bits saying "this is a lock, not a delete"). That is why you can lock millions of rows without exhausting lock memory. The lock is released only at COMMIT/ABORT — there is no explicit unlock.

**Lock queue interaction.** When transaction B hits a row already `FOR UPDATE`-locked by transaction A, B does not spin on the row; it enqueues in the lock manager *waiting on A's transaction-ID lock* (every transaction holds an exclusive lock on its own XID; waiting on a row means waiting to acquire a share lock on that XID). With plain `FOR UPDATE`, N workers all chasing the head of the queue form a convoy: everyone queues behind whoever locked the front row, so workers dequeue effectively single-file. `SKIP LOCKED` (Postgres 9.5+) changes the executor behavior: instead of enqueueing on a locked tuple, the scan tests the lock and *skips the row*, moving on to the next index entry. Result: N workers each grab a disjoint batch concurrently.

Two sharp edges worth being able to defend:

- **Anti-scaling at high worker counts.** Each worker's index scan must step over every row currently locked by the other workers before finding a free one, so claim cost grows roughly with the number of concurrently locked rows — benchmarks show per-claim latency rising and aggregate throughput *falling* as workers are added past a point (DBOS "Making Postgres Queues Scale"; the hardbyte benchmark harness observed the same "anti-scaling" in pgmq). Mitigations: batch claims (grab 100 jobs per query, not 1), fewer claimer connections than workers (one producer feeding a goroutine pool — River's model), or sharded sub-queues.
- **`SKIP LOCKED` weakens ordering guarantees by design** — a skipped row means "someone else has it," so consumers get an inconsistent view of the head of the queue. Fine for a job queue (you want *some* available job), wrong for strict-FIFO requirements. Que historically used advisory locks (`pg_advisory_xact_lock`) instead, which live in shared memory, cost no row writes, and release on disconnect — a good compare/contrast to know.

**Crash semantics fall out of transactions for free.** If the claiming transaction is still open when the worker dies, the connection drops, Postgres aborts the transaction, the row lock evaporates, and the `state='running'` update rolls back — the job is instantly available again with no janitor. (Section 3 covers why most real systems nonetheless *commit* the claim and use a rescuer.)

### 1.2 Wakeups: LISTEN/NOTIFY vs polling

`LISTEN chan` / `NOTIFY chan, payload` is Postgres' built-in pub/sub. Mechanism: notifications are staged in the transaction and appended to a global SLRU-backed notification queue *at commit*; listeners on dedicated connections are signaled after commit. Properties that matter:

- **Not durable**: notifications are not persisted across a server restart, and a listener that is disconnected misses everything. So NOTIFY can only ever be a *latency optimization*, never the source of truth — you must still poll.
- **Global commit serialization**: any commit containing a NOTIFY takes an exclusive lock over the shared notification queue so entries appear in commit order. Under many concurrent notifying writers this serializes commits — Recall.ai traced three production outages (March 2025) to exactly this: DB load spiked while CPU and I/O dropped, the signature of lock contention. Widely discussed as "Postgres LISTEN/NOTIFY does not scale."
- **8000-byte payload cap**, and LISTEN requires a real session — it does not work through transaction-pooling PgBouncer.

Correct design (and what River does): jobs live in the table; workers poll with an interval + jitter as the reliable path; LISTEN/NOTIFY is layered on top so an idle worker wakes in ~1 ms instead of waiting out the poll interval, and for control-plane signals (queue pause/resume). Notify from an `AFTER INSERT` trigger or from the inserter, ideally coalesced (notify once per batch, not per job) to bound the commit-serialization cost.

### 1.3 MVCC implications: dead tuples, bloat, autovacuum

This is *the* operational cost of a Postgres queue and the first thing reviewers of any Postgres-backed queue scrutinize. Postgres MVCC never updates in place: every UPDATE writes a new tuple version and marks the old one dead (`xmax` set); every DELETE just marks dead. A job row's lifecycle `insert → claim (update) → complete (update) → delete` creates ~3 dead tuples per job. A queue table has a tiny live set and enormous churn, so dead tuples quickly outnumber live rows by orders of magnitude, and index entries keep pointing at dead heap tuples (index bloat). Consequences: the claim query's index scan wades through dead entries, latency climbs, and "empty" queues get slow.

Aggravators:

- **HOT updates don't apply**: an update is Heap-Only-Tuple (no new index entries) only if no indexed column changed — but the claim updates `state`, which is exactly what you index. So every state transition writes new index entries.
- **The xmin horizon**: VACUUM can only reclaim tuples no snapshot can see. One long-running transaction *anywhere in the database* (an analytics query, a stuck migration) pins the horizon and vacuum silently stops reclaiming your queue table. PlanetScale's "Keeping a Postgres queue healthy" post is the standard write-up: the queue degrades not because of queue load but because of unrelated long transactions.
- **Default autovacuum is tuned for big, slow-moving tables**: it triggers at `50 + 20% of live rows` dead — useless for a hot 1k-row queue table generating millions of dead tuples/hour.

Standard tuning (per-table storage parameters on the jobs table):

```sql
ALTER TABLE jobs SET (
  autovacuum_vacuum_scale_factor = 0.01,   -- trigger at ~1% dead, not 20%
  autovacuum_vacuum_threshold   = 500,
  autovacuum_vacuum_cost_delay  = 0,       -- don't throttle vacuum on this table
  autovacuum_analyze_scale_factor = 0.01,
  fillfactor = 90                          -- leave page slack for HOT where possible
);
```

Plus: partial index on the hot path only (`CREATE INDEX ... WHERE state = 'available'` — keeps the claim index small and skips index entries for completed rows entirely), delete finished rows promptly rather than letting them accumulate, and keep transactions short.

**What River does about each of these** (its "maintenance services" are basically an ops checklist turned into code):

- **Batching everywhere**: a *batch completer* amalgamates job completions into batched updates, and bulk inserts use `COPY FROM` — fewer round trips and a friendlier write pattern.
- **Job cleaner**: periodically deletes completed/cancelled/discarded jobs past a retention window, keeping the table small.
- **Reindexer**: periodically runs `REINDEX INDEX CONCURRENTLY` on key job indexes to recover from B-tree bloat after a glut.
- **Rescuer**: reassigns jobs stuck in `running` (see §3), i.e., it commits claims instead of holding transactions open — deliberately avoiding long transactions that would pin the xmin horizon.
- **LISTEN/NOTIFY only as an optimization** over its poll loop, per §1.2.

### 1.4 Throughput ceilings people report

All numbers are workload- and hardware-dependent; treat as order-of-magnitude (River's own docs warn against using theirs for comparisons):

- **River**: ~46k trivial jobs/sec on an 8-core M2 MacBook Air with 2,000 worker goroutines (their published benchmark); early versions did ~10k/sec unoptimized.
- **Que** (advisory-lock design): the classic chanks gist "turning PostgreSQL into a queue serving 10,000 jobs per second."
- **hardbyte's postgresql-job-queue-benchmarking harness** (2026): peak clean throughput ~39.9k jobs/s for pgque in single-consumer mode, ~14.2k for the fastest full job queue, ~11.3k for pgmq — with anti-scaling at higher worker counts.
- Practical folk ceiling: low tens of thousands of jobs/sec on one Postgres node before you're doing serious tuning/sharding; comfortably thousands/sec with no tuning at all. For drover's target workloads this is orders of magnitude more than needed.

---

## 2. Redis as a queue

### 2.1 Lists: BRPOPLPUSH / LMOVE "reliable queue" pattern

Naive queue: `LPUSH queue job` + `BRPOP queue` (blocking pop). Fatal flaw: the pop *removes* the job from Redis before the worker finishes it — between pop and completion the job exists only in worker memory; a crash loses it (at-most-once).

Reliable pattern: `BRPOPLPUSH source processing` — deprecated since Redis 6.2 in favor of `BLMOVE source processing RIGHT LEFT` — *atomically* pops from the pending list and pushes onto a per-worker/per-process *processing list* in one command. On success the worker `LREM`s its entry from the processing list. On crash the job is still in Redis, parked in the processing list. The catch: a list entry carries no metadata — no claim timestamp, no delivery count — so recovery ("which processing-list entries are orphaned?") requires machinery you build yourself: a janitor that scans processing lists, plus a heartbeat/lease structure to know the difference between "slow worker" and "dead worker."

### 2.2 Streams + consumer groups: XACK, XAUTOCLAIM, PEL

Redis Streams (5.0+) are an append-only log; consumer groups add queue bookkeeping natively:

- `XADD stream * field value` appends an entry with a monotonic ID.
- A consumer group tracks one `last-delivered-id` cursor for the group; `XREADGROUP GROUP g consumer c COUNT n BLOCK ms STREAMS s >` delivers new entries to a named consumer **and records each in that consumer's Pending Entries List (PEL)**: entry ID, owning consumer, delivery count, last-delivery timestamp.
- `XACK stream group id` removes the entry from the PEL — the explicit "job done" acknowledgment.
- `XPENDING` inspects the PEL (per-consumer counts, idle times); `XCLAIM`/`XAUTOCLAIM stream group consumer min-idle-time cursor` transfers ownership of entries idle longer than `min-idle-time` to a healthy consumer, incrementing the delivery counter — the delivery count is your poison-pill/dead-letter signal. `XAUTOCLAIM` iterates cursor-style so a periodic sweep is cheap. (Redis 8.4 even folded this into `XREADGROUP ... CLAIM` so one call reclaims idle entries and reads new ones.)

So Streams give you at-least-once with redelivery bookkeeping *built into the datastore*, whereas Lists give you an atomic move and nothing else. Costs of Streams: entries aren't deleted on XACK (the stream grows until `XTRIM`/`MAXLEN ~`), per-entry IDs don't support priority reordering or scheduled/delayed delivery natively (you still need a ZSET for delayed jobs), and the API surface is much larger.

### 2.3 Persistence caveats (why "Redis is fast" has an asterisk)

Redis acknowledges writes after mutating memory, not after fsync. Durability windows by configuration:

- **RDB snapshots**: point-in-time forks every N minutes — a crash loses everything since the last snapshot (minutes of jobs).
- **AOF `appendfsync everysec`** (default AOF policy): fsync once per second — up to ~1 s of acknowledged enqueues lost on power loss/kernel crash. This is the practical production setting.
- **AOF `appendfsync always`**: fsync per command — closest to Postgres semantics, drastically slower, and rarely used.
- **Replication is asynchronous**: a failover can lose writes the primary acknowledged even with AOF on; `WAIT` gives replica-ack counts but is not synchronous durability.

The bottom line: Postgres gives you fsync-at-commit durability by default; Redis gives you a configurable data-loss window that most deployments leave at ~1 s. For a job queue that means *acknowledged enqueues can vanish* — acceptable for cache-warming jobs, not for "charge the customer" jobs.

### 2.4 What Asynq and Sidekiq actually use

- **Asynq (Go)**: Lists, not Streams. Per-queue `pending` and `active` lists moved between with `BRPOPLPUSH`/`BLMOVE`, wrapped in Lua scripts for multi-key atomicity, **plus a lease sorted-set** whose score is the worker's last lease-renewal time; workers heartbeat to extend their lease, and a *recoverer* goroutine periodically finds lease-expired tasks and moves them back to pending (or to the archive after max retries). Delayed/scheduled tasks live in separate ZSETs scored by run-at time, moved to pending by a forwarder. In other words: Asynq hand-builds on Lists exactly the bookkeeping (ownership, deadline, redelivery) that Streams' PEL provides natively.
- **Sidekiq (Ruby) OSS**: plain `BRPOP` — a hard crash loses in-flight jobs (only graceful shutdown re-pushes them). **Sidekiq Pro's `super_fetch`**: each process registers a private queue; jobs are moved `LMOVE` (historically RPOPLPUSH) from the public queue to the private queue atomically, `LREM`d on completion; on restart, orphaned private queues are detected and their jobs re-queued. I.e., the paid "reliability" feature *is* the reliable-queue List pattern plus orphan recovery.

---

## 3. Failure semantics: worker crash mid-job

The universal truth first: **every one of these designs is at-least-once**; after any crash-recovery path the job may run twice, so handlers must be idempotent. The designs differ in *how* a dead worker is detected and *how fast* redelivery happens.

| Design | Where the in-flight job lives | Crash detection | Redelivery latency | Extra machinery |
|---|---|---|---|---|
| PG, transaction-scoped lock (claim tx held open) | Locked row, uncommitted `running` | TCP disconnect → tx abort | Seconds (connection teardown/keepalive) | None — automatic | 
| PG, committed claim + rescuer (River) | Committed `running` row with `attempted_at` | Rescuer sweep: `running` older than threshold | Rescue threshold (River default ~1 h, tunable) | Rescuer service |
| PG, visibility timeout (pgmq style) | Row with `visible_at = now()+timeout` | Row simply reappears when `visible_at` passes | Visibility timeout | None, but timeout tuning |
| Redis Lists (Asynq/Sidekiq Pro) | Entry in processing list / active list | Heartbeat stops → lease ZSET score goes stale | Lease duration | Heartbeat + recoverer/janitor, Lua for atomicity |
| Redis Lists (Sidekiq OSS BRPOP) | Worker memory only | — | **Never — job lost** | — |
| Redis Streams | PEL entry (consumer, delivery count, idle time) | Idle time exceeds `min-idle-time` | XAUTOCLAIM sweep period + min-idle | Periodic XAUTOCLAIM sweep |

The three approaches, conceptually:

- **Visibility timeout** (SQS, pgmq, Streams' min-idle-time): claim = "invisible for T." Trade-off is fixed: T too short → duplicate execution of *slow but healthy* jobs; T too long → slow recovery of *dead* workers. No liveness signal at all.
- **Heartbeat/lease** (Asynq, most serious systems): worker actively renews a short lease; detection latency ≈ lease length (e.g., 30–60 s) independent of job duration — slow jobs just keep renewing. Costs: renewal traffic, clock-skew care, and a recoverer process.
- **Transaction/connection-scoped lock** (Postgres-only): the database *is* the failure detector — lock lifetime is exactly session lifetime. Fastest, zero extra code, and impossible to express in Redis (Redis has no notion of a client-owned lock that auto-releases with row rollback). Its cost is the long-open transaction, which pins the xmin horizon (§1.3) and holds a backend slot per in-flight job — which is precisely why River commits the claim and takes the rescuer trade instead. That trade-off is a design decision worth documenting in an ADR.

A hybrid worth noting for drover: committed claim + short heartbeat column (`heartbeat_at` updated every N seconds) + rescuer sweeping `state='running' AND heartbeat_at < now() - lease`. That is the lease model implemented on Postgres — fast detection without long transactions.

---

## 4. Which backend for drover?

**What each path forces the design to confront.** The Postgres path forces exactly the mechanisms the project charter says it must force: row-level locking and lock queues, MVCC tuple versioning, vacuum/bloat operations, snapshot visibility, transactional atomicity, and the poll-vs-notify trade-off. These are transferable mechanisms — they apply to any relational-database system, not just this queue — and they support a sharp, concrete comparison: Laravel's `database` queue driver is a naive `FOR UPDATE` design, so drover's docs can show exactly what that design does wrong under concurrency, the SKIP LOCKED fix, and the vacuum bill you pay for it. The Redis path teaches a smaller, more product-specific surface (command semantics, Lua atomicity, lease bookkeeping) and its hardest lessons (persistence windows, hand-rolled recovery) are cautionary rather than constructive.

**Defensibility.** A Postgres-backed queue whose docs can explain why River batches completions, why autovacuum defaults starve a queue table, and why LISTEN/NOTIFY melted Recall.ai stands on firmer ground than one that says "BLMOVE, like Sidekiq Pro." Redis knowledge shines brightest as the *comparison* — which a research-backed design doc (this file) already provides without shipping the adapter.

**Pluggable storage: strength or scope trap?** Both, depending on what "pluggable" means:

- A **narrow storage interface** (`Enqueue`, `Claim(n)`, `Complete`, `Fail`, `Rescue`, `Subscribe`) with the Postgres adapter *and an in-memory adapter used by the test suite* is a genuine strength: it shows Go interface design, enables fast unit tests, and costs little.
- **Shipping two production adapters is a scope trap**, and not mainly because of the code volume — because *semantics leak through the interface*. Transactional enqueue (§5) is expressible only in the Postgres adapter; crash recovery is connection-scoped in one and lease-scoped in the other; durability guarantees differ. The interface either degrades to the weakest common denominator (throwing away Postgres' killer feature) or sprouts capability flags and backend-specific extensions — the classic leaky-abstraction lesson, doubled test matrix included. River, notably, is unapologetically Postgres-only; Asynq is unapologetically Redis-only. The pros don't hedge.

Resolution: design the interface, ship Postgres + in-memory, and write an ADR sketching the Redis adapter (what the lease ZSET would look like, which interface methods it can't honor) as documented future work. That converts the scope trap into a documented design artifact about abstraction boundaries.

---

## 5. Transactional enqueue — the outbox killer feature

**The problem it solves (dual-write).** "Create the order, then enqueue send-confirmation-email" touches two systems. With a Redis queue there are only two orderings, both broken:

1. *Commit DB, then enqueue*: crash between the two → order exists, email job lost forever.
2. *Enqueue, then commit DB*: rollback/crash after enqueue → ghost job for an order that doesn't exist (worker races the commit and finds nothing).

Redis `MULTI/EXEC` doesn't help — it batches Redis commands atomically *within Redis*; there is no transaction manager spanning Redis and Postgres (no two-phase commit protocol between them), so no ordering of two independent network calls to two independent logs can be atomic.

**The mechanism in Postgres.** When the queue is a table in the *same database* as the domain data, enqueue is just an INSERT inside the app's open transaction:

```sql
BEGIN;
INSERT INTO orders (...) VALUES (...);
INSERT INTO jobs (kind, args) VALUES ('send_confirmation', '{"order_id": ...}');
COMMIT;
```

Atomicity is physical, not protocol-level: both inserts ride the same transaction, so the COMMIT is a single WAL commit record — both rows become durable together or neither does. Visibility is handled by MVCC snapshots: the job row's `xmin` is the inserting transaction, so no worker's snapshot can see the job until that transaction commits. There is no window where the job is visible but the order isn't, or vice versa. Rollback costs nothing extra — the job tuple is simply never visible. This also gives *exactly-once enqueue* (not exactly-once execution) with zero additional machinery.

**Why this is called the "outbox killer."** The transactional-outbox pattern exists precisely to fake this on top of an external broker: write an `outbox` table row in the domain transaction, then have a relay process poll the outbox and forward to Redis/Kafka, with dedup on the consumer side because the relay is itself at-least-once. But "a table that a poller drains with SKIP LOCKED" *is a Postgres job queue* — the outbox pattern is an admission that you needed one anyway. Putting the queue in Postgres deletes the relay, the second failure domain, and the dedup logic. River's API makes this first-class (`InsertTx(tx, ...)`), and it is the single strongest argument in every "choose Postgres queue technology" essay.

For drover this should be a headline API: `Enqueue(ctx, tx, job)` accepting a `pgx.Tx`, with the README demo showing a rollback that atomically un-enqueues.

---

## Options

**Option A — PostgreSQL-only backbone.**
`FOR UPDATE SKIP LOCKED` claims (batched), committed-claim + heartbeat-lease + rescuer recovery, poll-with-jitter + coalesced LISTEN/NOTIFY wakeups, per-table autovacuum tuning + partial index + job cleaner, transactional enqueue as the flagship feature. Maximum mechanism density, one operational surface, matches River's proven shape. Weakness: no exposure to Redis-style design in the codebase itself.

**Option B — Redis-only backbone (Streams + consumer groups).**
XADD/XREADGROUP/XACK with an XAUTOCLAIM recoverer, ZSET for delayed jobs, AOF-everysec documented loss window. Teaches Streams well and performs effortlessly, but forfeits transactional enqueue, teaches fewer transferable database mechanisms, and its durability story is a liability to defend rather than an asset.

**Option C ★ RECOMMENDED — Postgres backbone behind a narrow storage interface; in-memory adapter for tests; Redis adapter as a documented ADR (stretch goal only).**
All of Option A's mechanism depth, plus demonstrated Go interface design, plus fast unit tests against the in-memory adapter, plus an honest written analysis of why the Redis adapter can't honor transactional enqueue (capability leakage through abstractions — itself worth documenting). Redis adapter is explicitly out of scope for v1 to avoid the doubled test matrix and lowest-common-denominator interface.

**Option D — Two full production adapters (Postgres + Redis) from day one.**
Rejected: semantics (transactions, crash recovery, durability) leak through the interface; test matrix doubles; neither adapter gets deep enough to defend at mechanism level. This is the scope trap.

---

## Sources

- https://brandur.org/river — River design essay: batch completer, COPY FROM, transactional enqueue rationale (accessed 2026-07-22)
- https://riverqueue.com/docs/benchmarks — River ~46k jobs/s on M2 Air, caveats about comparing numbers (accessed 2026-07-22)
- https://riverqueue.com/docs/maintenance-services — job cleaner, reindexer, rescuer (accessed 2026-07-22)
- https://www.recall.ai/blog/postgres-listen-notify-does-not-scale — NOTIFY global commit-serialization lock, production outages (accessed 2026-07-22)
- https://www.dbos.dev/blog/making-postgres-queues-scale — SKIP LOCKED anti-scaling with worker count (accessed 2026-07-22)
- https://planetscale.com/blog/keeping-a-postgres-queue-healthy — xmin horizon / long transactions stalling queue-table vacuum (accessed 2026-07-22)
- https://github.com/hardbyte/postgresql-job-queue-benchmarking — comparative PG queue benchmarks (~11–40k jobs/s, anti-scaling) (accessed 2026-07-22)
- https://gist.github.com/chanks/7585810 — "Postgres as a queue serving 10,000 jobs per second" (Que, advisory locks) (accessed 2026-07-22)
- https://terrislinenbach.medium.com/why-for-update-skip-locked-isnt-enough-using-pg-advisory-xact-lock-to-build-a-correct-postgresql-d3eb9db46473 — advisory-lock alternative to SKIP LOCKED (accessed 2026-07-22)
- https://www.netdata.cloud/academy/update-skip-locked/ — SKIP LOCKED convoy-vs-parallel behavior (accessed 2026-07-22)
- https://redis.io/docs/latest/commands/brpoplpush/ — BRPOPLPUSH deprecated in favor of BLMOVE (6.2+) (accessed 2026-07-22)
- https://oneuptime.com/blog/post/2026-03-31-redis-reliable-queue-rpoplpush/view — reliable-queue List pattern (accessed 2026-07-22)
- https://oneuptime.com/blog/post/2026-01-30-redis-streams-consumer-groups/view — consumer groups, PEL, XAUTOCLAIM sweep (accessed 2026-07-22)
- https://redis.io/blog/single-shot-reliable-consumers-with-xreadgroup-claim-in-redis-84/ — XREADGROUP CLAIM (Redis 8.4) (accessed 2026-07-22)
- https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/ — RDB/AOF, appendfsync everysec ~1 s loss window (accessed 2026-07-22)
- https://github.com/hibiken/asynq/issues/764 and https://pkg.go.dev/github.com/hibiken/asynq — Asynq lists + lease ZSET + heartbeat + recoverer architecture (accessed 2026-07-22)
- https://www.bigbinary.com/blog/increase-reliability-of-background-job-processing-using-super_fetch-of-sidekiq-pro — super_fetch private-queue mechanism (accessed 2026-07-22)
- https://github.com/sidekiq/sidekiq/wiki/Reliability — BRPOP job-loss window in OSS Sidekiq (accessed 2026-07-22)
- https://github.com/sidekiq/sidekiq/issues/6782 — super_fetch now uses LMOVE, not RPOPLPUSH (accessed 2026-07-22)
- https://oneuptime.com/blog/post/2026-01-25-postgresql-autovacuum-tuning-prevent-table-bloat/view and https://medium.com/@philmcc/postgresql-autovacuum-tuning-a-practical-guide-71847badc9d3 — per-table autovacuum settings for high-churn tables (accessed 2026-07-22)

**Unverified / lower-confidence claims:** all throughput figures are workload-dependent single-source benchmarks, not gospel; River's exact rescuer default threshold (~1 h) and Asynq's default lease duration were not independently verified against current source code; the row-lock internals description (xmax/infomask, XID wait queue) is standard Postgres internals knowledge but was not re-verified against pg docs during this session.
