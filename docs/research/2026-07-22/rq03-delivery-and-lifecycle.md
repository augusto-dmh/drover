# RQ03 — Delivery semantics, retries, and worker lifecycle

Research date: 2026-07-22. Scope: mechanism-level answers for drover's delivery guarantees, retry/DLQ design, idempotency, Go worker lifecycle, and scheduling extras — written so the reasoning behind each recommendation is auditable.

---

## 1. Delivery semantics: at-most-once, at-least-once, and the "exactly-once" myth

### 1.1 The three regimes, defined by ack placement

The delivery guarantee of a queue is determined by **when the consumer acknowledges relative to when it does the work**:

- **At-most-once**: ack (or delete) the message *before* processing. If the worker crashes mid-work, the job is lost forever, but it can never run twice. Mechanically: `BRPOP` from Redis and immediately discard, or SQS `DeleteMessage` before handling.
- **At-least-once**: ack *after* processing completes. If the worker crashes between finishing the work and sending the ack, the broker cannot distinguish "crashed before work" from "crashed after work" — so it redelivers, and the work runs again. This ambiguity is the core of the Two Generals Problem: no finite number of acknowledgment round-trips over a lossy channel lets both sides *know* the other knows.
- **"Exactly-once delivery"**: impossible end-to-end across a network. A sender that gets no ack must choose: resend (risking duplicate) or not (risking loss). There is no third option. This is a well-known result popularized by the "You Cannot Have Exactly-Once Delivery" argument (bravenewgeek) and echoed by every serious queue's documentation.

### 1.2 Effectively-once = at-least-once delivery + idempotent processing

What systems that claim "exactly-once" (Kafka transactions, SQS FIFO dedup) actually provide is **exactly-once *processing within a bounded scope*** — a dedup window, a single partition, a transactional boundary. The moment a side effect leaves that boundary (an HTTP call, an email, a non-transactional DB write), the guarantee evaporates. SQS FIFO's 5-minute deduplication window, for example, dedups *enqueues*, not *executions*, and only within 5 minutes.

The industry-standard answer, and drover's: **at-least-once delivery, idempotent consumers**. The queue promises "your job will run one or more times"; the handler is written so that running twice has the same observable effect as running once (see §4).

### 1.3 How duplicate execution arises, concretely

These are the mechanisms reviewers of any new queue probe first. Each one is a specific interleaving:

1. **Visibility timeout / lease expiry while work is still running.** Worker A fetches job J; the broker hides J for a lease of T seconds. A's handler is slow (GC pause, slow downstream, big payload). At T+ε the broker considers A dead and makes J visible; worker B fetches it. Now J runs *concurrently* on two workers. AWS documents this exact failure and recommends visibility timeout ≥ 6× the consumer's own timeout, plus heartbeat-based `ChangeMessageVisibility` extension. Asynq models the same thing as a **lease with heartbeats**: a worker extends its lease periodically; if heartbeats stop, a "recoverer" loop re-enqueues the task with `ErrLeaseExpired` (checking for leases that expired ≥30s ago, to tolerate clock skew).
2. **Crash after work, before ack.** Handler completes its side effect (charged the card, sent the email), process dies before it can ack/delete. Broker redelivers → side effect happens again. This is unfixable at the transport layer; only idempotency at the effect layer fixes it.
3. **Ack lost in transit.** Worker sends the ack; the TCP connection drops or the broker restarts before persisting it. Same outcome as (2) from the broker's point of view.
4. **Deliberate redelivery on requeue/rescue.** Graceful-shutdown requeue (§5.6), River's "rescuer" reclaiming stuck jobs after a hard crash, and operator DLQ-redrive (§2.4) all intentionally re-run jobs that may have partially executed.
5. **Producer-side duplicates.** The enqueuer times out on its insert and retries; two copies of the job now exist. Dedup at enqueue (§4.2) addresses this one, and *only* this one.

The principle worth stating plainly in the docs: *duplicates are not a bug of a badly built queue; they are the price of not losing work. The queue's job is to make the duplicate window small and observable; the handler's job is to make duplicates harmless.*

### Options — drover's delivery guarantee

- **A. At-most-once (ack-on-fetch).** Trivial to build, loses jobs on any crash. Only acceptable for fire-and-forget metrics-type work. Offer as a per-queue opt-in at most.
- **B. At-least-once with lease + heartbeat (Asynq/SQS model).** Job moves to an "active/in-flight" state with a lease deadline; worker heartbeats extend it; a recoverer loop requeues expired leases. Teaches the most mechanisms (lease, heartbeat, recoverer, clock-skew tolerance) and matches how SQS, Asynq, and Faktory work.
- **C. "Exactly-once" via storage transaction (River model).** If jobs live in Postgres and the *work's side effects are also in the same Postgres*, completing the job in the same transaction as the work gives true exactly-once *for that transaction's scope*. Elegant, but only when side effects stay in the DB — worth documenting as a special case, not a general promise.

**★ RECOMMENDED: B — at-least-once with lease + heartbeat + recoverer**, with C called out in docs as achievable when handlers keep side effects inside the job store's transaction. Advertise semantics honestly: "at-least-once; write idempotent handlers."

---

## 2. Retry design

### 2.1 Backoff formulas actually shipped in production queues

- **Sidekiq** (Ruby, the reference design for most modern queues):

  ```
  delay = (retry_count ** 4) + 15 + (rand(10) * (retry_count + 1))   # seconds
  ```

  ≈ 15s, 16s, 31s, 96s, 271s, … with 25 retries by default spread over **~20 days**. The polynomial (`count^4`) grows slower than a true exponential early on but still spreads late retries over days — the design intent is "transient failures resolve in minutes; systemic ones get human time to deploy a fix before retries exhaust."
- **River** (Go + Postgres):

  ```
  delay = attempt ^ 4 seconds, ± 10% random jitter
  ```

  → 1s, 16s, 81s, 256s, … Default max attempts 25 (client-configurable; `MaxAttempts` per job kind). The jitter exists explicitly to prevent a stampede of simultaneously-failed jobs retrying in lockstep. (Amusing documented edge: at attempt ≥ 310 the policy saturates and adds ~292 years, `time.Duration` max.)
- **AWS Architecture Blog, "Exponential Backoff and Jitter" (Marc Brooker).** The canonical analysis. Key results from its simulations:
  - Plain capped exponential backoff (`sleep = min(cap, base * 2^attempt)`) is *not enough*: all clients that failed together retry together, producing synchronized contention spikes.
  - **Full Jitter** — `sleep = random_between(0, min(cap, base * 2^attempt))` — is the recommended default: near-optimal total work and completion time.
  - **Equal Jitter** — `temp = min(cap, base*2^attempt); sleep = temp/2 + random_between(0, temp/2)` — keeps a minimum spacing; slightly worse than Full Jitter in the simulations.
  - **Decorrelated Jitter** — `sleep = min(cap, random_between(base, prev_sleep * 3))` — competitive alternative that bases the next delay on the previous one.

For drover, a good documented, defensible default:

```go
// attempt is 1-based. Polynomial base like Sidekiq/River, full-jitter-style randomization.
func backoff(attempt int, rnd *rand.Rand) time.Duration {
    base := time.Duration(math.Pow(float64(attempt), 4)) * time.Second
    jitter := time.Duration(rnd.Int63n(int64(base)/5+1)) - base/10 // ±10%
    return base + jitter
}
```

Make the policy an interface (`RetryPolicy interface { NextRetry(job *Job) time.Time }`) exactly as River does, so per-worker overrides are trivial.

### 2.2 Max attempts

Every mature queue bounds retries: Sidekiq 25 (~20 days), River 25, Asynq 25 by default. Unbounded retries turn a poison job into a permanent CPU/queue tax. Store `attempt` and `max_attempts` on the job row; the transition on final failure is to the dead/discarded state, not deletion (§2.4).

### 2.3 Error classification: retryable vs terminal

Not every error deserves a retry. The classification mechanism used by River is instructive because it is *return-value-based*, not exception-taxonomy-based:

- `return nil` → success, job completed.
- `return err` → counted failure; retried per policy until `MaxAttempts`, then discarded.
- `return river.JobCancel(err)` → **terminal**: cancel now, never retry, regardless of remaining attempts. For validation errors, "user deleted," 4xx-class failures.
- `return river.JobSnooze(d)` → **not an error at all**: reschedule after `d` without consuming an attempt. For "dependency not ready yet."
- Panic → treated like an error (retried), surfaced via a `PanicHandler`.

The general rule to encode: **retry on ambiguous/transient failures (timeouts, 5xx, connection reset, lease expired); discard/cancel on deterministic failures (bad args, 4xx, business-rule rejection)**. A retry of a deterministic failure is pure waste and delays the DLQ signal. Drover should expose sentinel wrappers: `drover.Cancel(err)`, `drover.Snooze(d)`, and default everything else to retryable.

### 2.4 Dead-letter queue semantics and operator workflow

**Semantics.** When `attempt == max_attempts` (or the handler cancels), the job transitions to a `dead`/`discarded` state — kept, not deleted, with its error history (Sidekiq keeps the last N dead jobs ~6 months; SQS moves the message to a separate DLQ after `maxReceiveCount`; River sets state `discarded` and retains the row per its job-retention cleaner). The DLQ is a *quarantine with provenance*: payload + all attempt errors + timestamps.

**Operator workflow (redrive), distilled from AWS/industry practice:**

1. **Alert on DLQ depth > 0** (or rate). A DLQ nobody watches is a silent data-loss buffer.
2. **Triage with ownership**: inspect payload and error chain; classify — poison message (bad data), downstream outage, or code bug.
3. **Fix first, replay second.** Redrive only after the root cause is addressed; otherwise the job loops straight back to the DLQ (or worse, re-executes a partial side effect).
4. **Scoped redrive, never "send everything back" blindly**: filter by error class, job kind, time range. Redrive resets `attempt` to 0 (SQS redrive behavior) or re-inserts as a fresh job.
5. **Explicit discard** as a first-class operator action for jobs that should never run (with audit note).

For drover: a `dead` state in the job table plus CLI/API verbs `drover dlq list|show|retry|discard` with filters is the whole feature, and it demos extremely well.

### Options — retry policy shape

- **A. Pure exponential `base * 2^n` + Full Jitter (AWS style).** Best theoretical contention behavior; grows very fast (attempt 20 ≈ 12 days even with cap issues).
- **B. Polynomial `n^4` + ±10% jitter (Sidekiq/River style).** Gentler early curve (multiple quick retries in the first minute), naturally spans ~3 weeks by attempt 25 without needing a cap. Battle-tested in the two most-copied queues.
- **C. Fixed intervals table (`[1m, 5m, 30m, 2h, ...]`).** Simple to explain, zero math, but inflexible and un-jittered unless you add it.

**★ RECOMMENDED: B — `attempt^4` seconds with ±10% jitter, max 25 attempts, pluggable `RetryPolicy` interface**, plus `Cancel`/`Snooze` sentinel errors for classification and a retained `dead` state with scoped redrive tooling.

---

## 3. Idempotency and uniqueness

Two different problems, often conflated; drover should implement them as separate mechanisms:

### 3.1 Enqueue-time dedup (unique jobs)

Prevents *duplicate rows* from entering the queue. Mechanisms in the wild:

- **Asynq `Unique(ttl)`**: computes a lock key from queue + task type + payload; `SET NX` in Redis with TTL. Enqueue of a duplicate within the TTL returns `ErrDuplicateTask`. Uniqueness is best-effort and released when the task completes or the TTL expires.
- **River unique jobs**: unique by any combination of **args (hash), period (time window), queue, and state set** — e.g. "unique per hour by args, considering pending/scheduled/running states." Implemented against Postgres with a unique index / advisory-lock-backed check, so it is transactional with the insert.
- **Client-supplied ID** (Asynq `TaskID`, SQS FIFO `MessageDeduplicationId`): caller provides the natural key; broker rejects duplicates.

Design notes: hash of *canonicalized* args (sorted JSON keys) + kind + queue; store the hash in an indexed column; unique constraint scoped by a state predicate (`WHERE state IN ('available','scheduled','running')`) so a completed job doesn't block a legitimate re-enqueue — unless a time window ("at most once per hour") is requested, in which case bucket the timestamp into the hash.

### 3.2 Execution-time idempotency (the handler's job)

Enqueue dedup cannot stop duplicate *execution* (§1.3 cases 1–4). The handler-side patterns, in order of robustness:

1. **Idempotency key + recorded result (Stripe model).** Client attaches a unique key; the server persists the *result* (status + body) of the first execution keyed by it, and replays the stored result on retries. Stripe compares incoming params against the original request and errors on mismatch; keys are pruned after ~24h. Brandur's "Implementing Stripe-like Idempotency Keys in Postgres" shows the general recipe: an `idempotency_keys` row created/checked in the same transaction as each atomic phase of the work.
2. **Upsert / conditional write.** `INSERT ... ON CONFLICT DO NOTHING`, compare-and-set on a version column. Duplicate execution becomes a no-op at the storage layer.
3. **Transactional guard.** If the side effect is in the same DB as the job store, mark-complete and side effect commit atomically (River's headline trick with transactional enqueue *and* completion).
4. **Natural idempotence.** "Set user.email = X" is safe to repeat; "increment balance" is not — rewrite increments as ledger inserts with unique entry IDs.

For drover: ship (a) unique-jobs at enqueue (args-hash + optional window + state scope) and (b) a documented handler contract — "handlers MUST be idempotent; drover provides `job.ID` and `job.UniqueKey` for use as idempotency keys downstream." An optional `ExecutedOnce` middleware that records completion keyed by job ID in the job store demonstrates the execution-guard pattern.

### Options — where dedup lives

- **A. Enqueue-time only.** Cheap, transactional with insert, but leaves execution duplicates unhandled.
- **B. Execution-time only.** Correct but pushes all cost onto every handler.
- **C. Both, explicitly layered.** Enqueue dedup for producer retries and periodic-job overlap; documented handler idempotency (plus helper middleware) for delivery duplicates.

**★ RECOMMENDED: C** — with docs that articulate *which duplicate source each layer kills*.

---

## 4. Worker lifecycle in Go

### 4.1 Bounded concurrency: the three idioms

```go
// (1) errgroup with SetLimit — fan-out with error propagation and cancellation.
g, ctx := errgroup.WithContext(ctx)
g.SetLimit(maxWorkers)          // g.Go blocks when limit reached → backpressure
for _, j := range jobs {
    j := j
    g.Go(func() error { return work(ctx, j) })
}
err := g.Wait()                  // first error cancels ctx for siblings

// (2) Buffered-channel semaphore — zero deps, pattern visible in code.
sem := make(chan struct{}, maxWorkers)
for j := range jobsCh {
    sem <- struct{}{}
    go func(j Job) { defer func() { <-sem }(); work(ctx, j) }(j)
}

// (3) Hand-rolled fixed pool — N long-lived goroutines ranging over a channel.
for i := 0; i < maxWorkers; i++ {
    wg.Add(1)
    go func() { defer wg.Done(); for j := range jobsCh { work(ctx, j) } }()
}
```

Trade-offs (consensus across Go community writeups): `errgroup.SetLimit` is the default for *batch fan-out that returns errors* — cancel-on-first-failure semantics, though, are wrong for a queue (one failed job must not cancel its siblings). `x/sync/semaphore.Weighted` adds context-aware `Acquire` and weighted slots (a "heavy" job can take 3 units). The **fixed long-lived pool (3)** is what actual queue servers use (Asynq, Sidekiq's processor threads, River's producer/executor): worker count = pool size, jobs flow through a channel, no per-job goroutine churn, and shutdown is "close the channel, wait the WaitGroup."

**★ For drover's executor: pattern (3), a fixed pool of N executor goroutines fed by a fetch loop**, with a `semaphore.Weighted` variant noted as the extension point for weighted/priority concurrency. Use `errgroup` internally for subsystem lifecycle (fetcher, heartbeater, recoverer, scheduler as group members).

### 4.2 Per-job context, timeout, cancellation propagation

Each job execution gets a derived context:

```go
func (e *executor) run(base context.Context, j *Job) error {
    ctx := base                                    // worker/server lifecycle ctx
    if j.Timeout > 0 {
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(ctx, j.Timeout)
        defer cancel()
    }
    ctx = withJobMeta(ctx, j)                      // job ID, attempt, deadline
    return e.handler.ProcessTask(ctx, j)
}
```

Key mechanics: the per-job ctx is a *child* of the server ctx, so server shutdown propagates automatically; handlers must honor `ctx.Done()` (Go cannot kill a goroutine — cancellation is cooperative); remote cancellation (operator cancels a running job, as River supports) is implemented by the server tracking `jobID → cancelFunc` and invoking it when a cancel command arrives via LISTEN/NOTIFY or polling. Timeout expiry is recorded as a retryable error (the job may still be running past its deadline if the handler ignores ctx — document that the lease, not the timeout, is the true upper bound).

### 4.3 Panic recovery

A panic in one job must not take down the whole worker process (a naked panic in a goroutine kills the program). Recover at the job boundary, convert to error, keep the stack:

```go
func safeRun(ctx context.Context, h Handler, j *Job) (err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
        }
    }()
    return h.ProcessTask(ctx, j)
}
```

River routes panics through a `PanicHandler` and treats them as retryable failures by default; Asynq does the same via its recoverer of the goroutine. Drover should: recover, capture `debug.Stack()`, record it in the job's error list, count it as an attempt, and expose a hook so operators can page on panics specifically (a panic is a bug signal, unlike a timeout).

### 4.4 Graceful shutdown sequence

The canonical SIGTERM sequence for a queue worker (composite of Asynq/River/Sidekiq behavior and Go graceful-shutdown practice):

1. **Trap signal**: `signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)`.
2. **Stop fetching** immediately — the fetch loop exits (or the producer stops issuing `FETCH`); no new jobs enter the pool. Health endpoint flips to not-ready so orchestrators stop routing.
3. **Drain with a deadline**: wait on the executor WaitGroup, bounded by a drain timeout (must be < the platform's kill grace, e.g. Kubernetes `terminationGracePeriodSeconds`; Sidekiq's `-t 25` vs Heroku's 30s is the classic example):

   ```go
   done := make(chan struct{})
   go func() { wg.Wait(); close(done) }()
   select {
   case <-done:                       // clean drain
   case <-time.After(drainTimeout):   // deadline exceeded
       cancelJobs()                   // cancel per-job contexts (cooperative)
   }
   ```
4. **Handle unfinished jobs** at the deadline — two valid mechanisms:
   - **Requeue** them explicitly (Sidekiq pushes unfinished work back to Redis on quiet/terminate) — fast redelivery, but re-execution of partial work.
   - **Let the lease expire** — do nothing; the recoverer/rescuer on another node reclaims them after lease expiry (Asynq's model). Simpler, slower to recover, no special shutdown path.
   A third refinement: **stop extending leases** at step 2 so in-flight jobs' leases lapse naturally right after the drain deadline.
5. **Close infrastructure last** (DB pools, Redis conns) — reverse order of initialization, only after workers are done, or the drain itself fails with connection errors.
6. Second SIGTERM/SIGINT = hard stop (common CLI convention).

**★ For drover: stop-fetch → drain(WaitGroup, deadline) → cancel per-job ctx → requeue-still-running (best effort) → lease expiry as the crash-path backstop.** The explicit requeue makes clean shutdowns fast; the lease/recoverer covers SIGKILL and node death where no shutdown code runs at all — you need both paths, and the docs should say so explicitly.

### 4.5 Goroutine-leak prevention and detection

Prevention rules for the worker codebase:

- Every goroutine has a documented owner and exit condition; spawn with a `select` on `ctx.Done()` in every blocking receive/send.
- Never send on an unbuffered channel a receiver might abandon (use `select` + `default`/ctx, or a buffered result channel of size 1 for "first result wins" patterns).
- Tickers/timers: `defer ticker.Stop()`.
- `Client.Stop()` must be idempotent and must `Wait()` on everything it started — the test for this is the leak detector.

Detection with **uber-go/goleak**:

```go
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }      // whole package
func TestWorkerDrain(t *testing.T) { defer goleak.VerifyNone(t); ... } // per test
```

`VerifyTestMain` checks once after all tests (cheap, but doesn't finger the culprit test); `VerifyNone` per test pinpoints the leaker at the cost of boilerplate. Options like `goleak.IgnoreTopFunction` whitelist known-benign background goroutines (e.g., database/sql pool openers). Brandur ("Habitually testing for goroutine leaks") argues for wiring this in from day one on any long-lived-goroutine codebase — for a queue, `start → enqueue → work → stop → VerifyNone` is the single most valuable lifecycle test drover can have.

---

## 5. Scheduling extras

### 5.1 Delayed jobs

Job carries `scheduled_at`; it is invisible to fetch until due.

- **Redis model (Asynq/Sidekiq)**: a sorted set scored by run-at epoch; a "forwarder" loop atomically (`ZRANGEBYSCORE` + move, in Lua) moves due members to the ready list. Polling interval = scheduling granularity.
- **SQL model (River)**: simply `WHERE state='scheduled' AND scheduled_at <= now()` in the fetch query (plus a maintenance "scheduler" that flips states in batches). Free with a DB-backed queue — retries reuse the same mechanism (`scheduled_at = now() + backoff`).

### 5.2 Periodic / cron jobs — the duplicate-enqueuer problem

Naive design: every worker process runs a cron loop → N processes enqueue N copies every tick. Two production solutions:

- **River: leader election via Postgres advisory locks.** All clients participate in an election; only the elected **leader** runs the periodic enqueuer (and other maintenance services: rescuer, cleaner). Leadership is held/renewed via advisory lock; on leader death the lock releases and another client wins. Periodic jobs are defined in code on *every* client (so any of them can become leader), optionally with `RunOnStart`. Belt-and-suspenders: pair periodic jobs with **unique jobs** ("unique by period 1h"), so even a double-fire enqueues once.
- **Asynq: separate `Scheduler` process + registration.** A distinct `Scheduler` component enqueues on cron specs (`"*/5 * * * *"`, `"@every 30s"`); running it as a single instance avoids duplication, and multi-instance overlap is mitigated with `TaskID` (client-chosen dedup ID) + `Unique`/retention options. Asynq also ships `PeriodicTaskManager` for dynamic (config-driven) schedules.

The pattern worth naming in the design docs: **exactly-one-enqueuer via lock-based leader election, made harmless-if-violated via unique-job dedup.** Neither layer alone is sufficient: leases/locks can split-brain briefly; uniqueness has TTL edges.

### 5.3 Priority queues

Three shipped designs:

- **Strict ordering across named queues** (Sidekiq ordered mode, Asynq `StrictPriority`): always drain `critical` before `default` before `low`. Simple; risks starvation of low queues.
- **Weighted sampling** (Sidekiq default `queues: [critical, 5], [default, 3], [low, 1]`, Asynq `Queues: map[string]int`): fetch probabilistically by weight; no starvation, approximate priority.
- **Priority column within a queue** (River: `priority 1–4`, part of the fetch query's `ORDER BY priority, scheduled_at`): fine-grained, but a flood of high-priority jobs still starves lower ones within the queue.

**★ For drover: named queues with weighted fetch (starvation-free) + an optional strict flag**, mirroring Asynq — it exercises an interesting fetch-loop mechanism (weighted random queue selection) without complicating the storage schema.

---

## 6. Summary of recommended drover semantics (one paragraph for the README)

Drover is **at-least-once**: a job is leased to a worker with a heartbeat-extended lease; jobs are acked only after the handler returns. Crashes, lost acks, and expired leases cause redelivery — handlers must be idempotent, and drover provides enqueue-time unique jobs (args-hash, optional time window, state-scoped) plus job IDs usable as idempotency keys. Failures retry on an `attempt^4 ± 10%`-jitter schedule up to 25 attempts (pluggable policy; `Cancel`/`Snooze` sentinels classify errors), then land in a retained dead-letter state with scoped `retry`/`discard` operator commands. Workers are a fixed goroutine pool with per-job context timeouts, panic recovery at the job boundary, and a SIGTERM sequence of stop-fetch → drain-with-deadline → cancel → requeue, backstopped by lease-expiry recovery; `goleak` guards the lifecycle in tests. Delayed jobs use `scheduled_at` fetch predicates; periodic jobs are enqueued by an advisory-lock-elected leader and deduped by unique jobs; queues are prioritized by weighted fetch.

---

## Sources

All accessed 2026-07-22.

**Delivery semantics / duplicates**
- You Cannot Have Exactly-Once Delivery — https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/ (accessed 2026-07-22)
- Amazon SQS visibility timeout — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html (accessed 2026-07-22)
- SQS redelivery during Lambda processing (visibility < function timeout) — https://dev.to/siddharth_pandey_27/your-sqs-queue-is-redelivering-messages-your-lambda-is-still-processing-40k2 (accessed 2026-07-22)
- Deduplication in Distributed Systems: Myths, Realities — https://www.architecture-weekly.com/p/deduplication-in-distributed-systems (accessed 2026-07-22)
- Asynq releases (worker lease, ErrLeaseExpired, IsOrphaned) — https://github.com/hibiken/asynq/releases (accessed 2026-07-22)
- Asynq recoverer source (30s clock-skew allowance) — https://github.com/hibiken/asynq/blob/master/recoverer.go (accessed 2026-07-22)

**Retries / backoff / DLQ**
- Sidekiq Error Handling wiki (retry formula, 25 retries / ~20 days) — https://github.com/sidekiq/sidekiq/wiki/Error-Handling (accessed 2026-07-22)
- River job retries (attempt^4, ±10% jitter, MaxAttempts) — https://riverqueue.com/docs/job-retries (accessed 2026-07-22)
- River retry_policy.go — https://github.com/riverqueue/river/blob/master/retry_policy.go (accessed 2026-07-22)
- Exponential Backoff And Jitter (Marc Brooker, AWS Architecture Blog) — https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ (accessed 2026-07-22)
- River error and panic handling (JobCancel, JobSnooze, PanicHandler) — https://riverqueue.com/docs/error-handling (accessed 2026-07-22)
- River maintenance services (rescuer for stuck jobs) — https://riverqueue.com/docs/maintenance-services (accessed 2026-07-22)
- SQS DLQ redrive/replay practices — https://pilotcore.io/blog/how-to-process-dead-letter-queue-messages-in-aws (accessed 2026-07-22)
- DLQ pattern: poison messages, scoped replay — https://www.abstractalgorithms.dev/dead-letter-queue-pattern-poison-message-recovery (accessed 2026-07-22)

**Idempotency / uniqueness**
- Stripe: Designing robust and predictable APIs with idempotency — https://stripe.com/blog/idempotency (accessed 2026-07-22)
- Stripe idempotent requests reference — https://docs.stripe.com/api/idempotent_requests (accessed 2026-07-22)
- Implementing Stripe-like Idempotency Keys in Postgres — https://brandur.org/idempotency-keys (accessed 2026-07-22)
- Asynq unique tasks / TaskID (README + discussion) — https://github.com/hibiken/asynq and https://github.com/hibiken/asynq/discussions/376 (accessed 2026-07-22)
- River unique jobs (via pkg.go.dev/river and brandur.org/river) — https://brandur.org/river (accessed 2026-07-22)

**Worker lifecycle (Go)**
- Bounded Concurrency in Go: worker pools, semaphores, errgroup — https://levelup.gitconnected.com/bounded-concurrency-in-go-worker-pools-semaphores-errgroup-and-the-pitfalls-that-hurt-in-5192eff95e86 (accessed 2026-07-22)
- Goroutine Pool Patterns: errgroup & backpressure — https://tanhdev.com/posts/golang-goroutine-pool-errgroup-worker/ (accessed 2026-07-22)
- Graceful shutdown of worker goroutines — https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/ (accessed 2026-07-22)
- Graceful Shutdown in Go: production patterns (ordering, drain deadline) — https://dev.to/young_gao/graceful-shutdown-in-go-patterns-every-production-service-needs-3l9c (accessed 2026-07-22)
- uber-go/goleak README (VerifyTestMain vs VerifyNone) — https://github.com/uber-go/goleak (accessed 2026-07-22)
- Habitually testing for goroutine leaks — https://brandur.org/fragments/goroutine-leaks (accessed 2026-07-22)

**Scheduling**
- River periodic and cron jobs (leader-only enqueue) — https://riverqueue.com/docs/periodic-jobs (accessed 2026-07-22)
- River leader election discussion (advisory locks) — https://github.com/riverqueue/river/discussions/343 (accessed 2026-07-22)
- Asynq Periodic Tasks wiki (Scheduler, PeriodicTaskManager) — https://github.com/hibiken/asynq/wiki/Periodic-Tasks (accessed 2026-07-22)

**Unverified / lower-confidence claims (flagged):**
- Sidekiq dead-set retention "~6 months / last N jobs" and `-t 25` vs Heroku 30s grace: from general Sidekiq community knowledge, not re-verified against current Sidekiq wiki this session.
- River rescuer *discarding* at max attempts and the ≥310-attempt `time.Duration` saturation detail: sourced from search summaries of River docs/source, not read directly from `retry_policy.go` this session.
- Asynq default 25 max retries and exact unique-lock key composition (queue+type+payload): from Asynq README knowledge; README fetched only via search summary.
- Sidekiq weighted-queue syntax and Asynq `StrictPriority` behavior: well-known documented features, but the exact docs pages were not fetched this session.
