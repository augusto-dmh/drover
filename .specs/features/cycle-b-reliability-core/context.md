# Cycle B — Decision Log

Auto-selected per the ship-cycle autonomy contract: at each gray area the options were formulated
with both a why-recommend and a why-not, the recommended option was taken, and the reasoning is
recorded here so it is auditable without the conversation. Grounded in ADR-0002/0003/0004 and the
Cycle A decisions AD-001..AD-008 before inventing new analysis.

## D-1 (AD-009): Where a retrying job waits

**Options considered:**

- **A — `state = 'available'`, `scheduled_at = retryAt`.** For: zero schema change; the Cycle A fetch
  predicate and its partial index already work untouched. Against: erases the difference between "has
  never run" and "failed and is backing off", which is exactly the distinction the Cycle E depth
  gauges and the Cycle F `drover jobs list` need; the `retryable` state created in migration 001
  (AD-002) would stay permanently unreachable.
- **B — `state = 'retryable'`, `scheduled_at = retryAt`, fetch predicate widened to the non-terminal
  waiting states ★.** For: keeps a single fetch path with no extra moving part; `retryable` becomes
  a real, queryable state; the due-time gate the predicate already applies (AD-004) does all the
  work. Against: costs a migration to widen the partial index, and three states now share one
  predicate, so the index is slightly less selective.
- **C — `retryable` plus a scheduler component that promotes due rows to `available`.** For: what
  River does; keeps `available` as the single fetchable state and makes the fetch index maximally
  selective. Against: introduces a third background sweep this cycle, on top of the heartbeat and the
  rescuer, for a benefit that is invisible until the queue is large; the RFC does not scope a
  promoter in any cycle.

**Pick: B.** Migration 002 widens `drover_jobs_fetch_idx` to
`WHERE state IN ('available', 'retryable', 'scheduled')`. The predicate stays
`state IN (...) AND queue = $1 AND scheduled_at <= now() ORDER BY id ... FOR UPDATE SKIP LOCKED`,
which is still the Cycle D shape. C is the right answer at a scale drover has not yet earned; it can
be adopted later without changing any public API.

## D-2 (AD-010): Where a snoozed job waits

**Pick:** `scheduled`, not `retryable`. A snooze is a handler saying "not yet", not a failure. Filing
it under `retryable` would inflate every future count of failing jobs and make the retry backlog
metric lie. Both states are covered by the D-1 predicate, so this costs nothing.

## D-3 (AD-011): A snooze consumes no attempt

**Options considered:**

- **A — snooze consumes an attempt.** For: trivially simple; no compensating write. Against: a
  handler that snoozes while waiting on an external precondition would walk itself into `dead` after
  25 deferrals, which is the opposite of what the caller asked for.
- **B — decrement `attempt` when snoozing, floored at zero ★.** For: makes snooze exactly
  attempt-neutral given that the claim already incremented; the floor keeps the column's non-negative
  invariant. Against: the compensating decrement is a little surprising to read in the driver.

**Pick: B.** A rescue, by contrast, deliberately does *not* touch `attempt` (D-4): the attempt that
stranded the row was really consumed, and counting it twice would halve every crashed job's effective
`max_attempts`.

## D-4 (AD-012): Rescue does not change `attempt`

**Pick:** the rescuer restores state and schedules a retry but leaves `attempt` exactly as the dead
worker left it. The claim that stranded the row consumed one attempt and that consumption is real —
the work may well have half-run. The alternative (decrementing, treating a crash as "didn't count")
lets a job that reliably crashes its worker retry forever.

## D-5 (AD-013): A recovered panic is a retryable failure

**Pick:** classify a recovered panic exactly like a returned error — retry if attempts remain, die if
not — with the stack trace preserved in the appended error entry. Cycle A sent panics straight to
`dead`, but that was part of the AD-003 placeholder, not a considered stance. A panic is no more
inherently permanent than an error: a nil-map dereference on one malformed payload is transient with
respect to the *next* job, and the retry curve costs little. The stack trace is what makes the
failure diagnosable, and it is retained either way.

## D-6 (AD-014): An unregistered kind is a retryable failure

**Options considered:**

- **A — dead immediately (Cycle A behavior).** For: honest about a configuration error; fails fast
  and loudly. Against: during a rolling deploy, workers running the old binary legitimately see kinds
  only the new binary registers — this option destroys those jobs, and the enqueuer did nothing
  wrong.
- **B — treat as a normal retryable failure ★.** For: the deploy window heals itself; `attempt^4`
  backoff means a genuinely unregistered kind costs a handful of cheap fetches before the intervals
  grow to hours. Against: a real typo in a `Kind()` string now takes 25 attempts to surface instead
  of one, and it surfaces as retry noise rather than an obvious death.

**Pick: B.** The rolling-deploy case is routine and the typo case is caught at the first log line
either way; the difference is only whether the job survives. Losing accepted work to a deploy
ordering accident contradicts the first decision driver in ADR-0003.

## D-7 (AD-015): Retry policy surface

**Options considered:**

- **A — config knobs (`BaseDelay`, `Multiplier`, `MaxDelay`, `JitterFraction`).** For: no interface
  to learn. Against: four fields that only express one curve family; anything outside it (a fixed
  schedule, a per-kind curve, an error-dependent delay) still requires a fork.
- **B — `RetryPolicy` interface, one method `NextRetry(job *JobRow) time.Time` ★.** For: what
  ADR-0003 committed to; one method; the whole row is in scope so a policy can branch on kind,
  attempt, queue or accumulated errors; absolute time lets a policy express "retry at 03:00" that no
  duration can. Against: a trivial "make it 5 seconds" adopter has to write a type.
- **C — func type `func(attempt int, err error) time.Duration`.** For: the lightest possible thing to
  supply inline. Against: throws away the rest of the row, and a func-typed public API is awkward to
  extend without breaking every implementation.

**Pick: B**, with `Config.RetryPolicy` defaulting to the exported exponential policy when nil. A past
or zero time from a policy means "claimable now" rather than an error — the policy is the adopter's
code and drover should not reject an aggressive schedule.

## D-8 (AD-016): The rescuer re-claims before disposing

**Options considered:**

- **A — read expired rows, then finalize them with lease-guarded writes.** For: one round trip to
  find work. Against: needs a second, lease-guarded copy of every finalizer (`running AND
  leased_until < now()` instead of just `running`), roughly doubling the driver's transition surface,
  and two rescuers can still both read the same row and race on the guard.
- **B — re-claim expired rows with `FOR UPDATE SKIP LOCKED` and a fresh lease, then dispose of them
  through the ordinary `running` → terminal transitions ★.** For: `SKIP LOCKED` gives exactly-once
  disposition across concurrent rescuers for free; rescue reuses the finalizers the failure path
  already needs; the fresh lease means a rescuer that itself dies leaves the row rescuable again by
  the next sweep. Against: the sweep is two statements per job instead of one.
- **C — a single `UPDATE ... SET state = CASE WHEN attempt >= max_attempts THEN 'dead' ELSE
  'retryable' END` with the backoff computed in SQL.** For: one statement, no round trips. Against:
  the backoff would have to be expressed in SQL, which discards the pluggable `RetryPolicy` from D-7
  for exactly the jobs — crashed ones — where the operator most wants control.

**Pick: B.** Note `FetchExpired` does **not** increment `attempt`, unlike `FetchAvailable`; that is
the D-4 decision expressed in the query.

## D-9 (AD-017): Jitter randomness is not injectable

**Pick:** use `math/rand/v2` top-level functions directly. The specification of the default policy is
a *bound* (`0.9·N⁴ ≤ delay ≤ 1.1·N⁴`) plus non-determinism, and both are testable by sampling: draw
many values, assert every one is in range and that at least two differ. An injected `rand.Source`
would exist only to let a test assert a number nobody promised, and would leak a seam into
`Config` that no adopter has a use for.

## D-10 (AD-018): The heartbeat outlives context cancellation

**Pick:** the heartbeat goroutine stops only after the fetch loop has returned — that is, after the
last in-flight job has finalized — not when the loop's context is cancelled. The loop deliberately
runs the in-flight job on `context.WithoutCancel` so shutdown drains rather than kills it; a
heartbeat that stopped at cancellation would let that draining job's lease lapse and invite the
rescuer to hand a duplicate to another node. The rescuer goroutine, which owns no in-flight work,
does stop at cancellation. Both are joined before `Start` returns so `goleak` sees a clean exit.

## D-11: Lease, heartbeat and rescue defaults

**Pick:** `LeaseDuration` 1 minute (the existing `driver.DefaultLeaseDuration`, now moved into
`Config`), `HeartbeatInterval` `LeaseDuration / 3`, `RescueInterval` 1 minute. A third of the lease
gives two missed heartbeats of slack before a live worker is falsely rescued. Zero or negative values
fall back to these defaults; a heartbeat interval configured at or above the lease duration is
clamped below it, because such a configuration can only ever produce false rescues.
