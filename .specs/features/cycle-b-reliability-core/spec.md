# Reliability Core Specification

## Problem Statement

Drover currently loses reliability in two ways. A handler that returns an error kills the job
immediately — the row goes straight to `dead` on the first failure, so a transient SMTP timeout or a
brief database blip destroys work that would have succeeded on a second try (AD-003 accepted this as
a deliberate placeholder). And a worker that dies mid-job strands its row in `running` forever: the
`leased_until` column is written on claim but nothing ever reads it (AD-005), so a SIGKILL leaks the
job permanently. ADR-0003 committed to at-least-once delivery backed by retries, leases, heartbeats
and a rescuer; this cycle builds that machinery.

## Goals

- [x] A failing job retries on an `attempt^4`-seconds schedule with ±10% jitter and only becomes
      `dead` once its attempts are exhausted.
- [x] A job whose worker dies is returned to the queue by a rescuer sweep within a bounded,
      configurable time — no row stays `running` indefinitely.
- [x] A long-running job is not falsely rescued: its lease is extended by a heartbeat while it runs.
- [x] A handler can classify its own outcome: `Cancel` ends a job permanently without retrying,
      `Snooze` defers it without consuming an attempt.
- [x] Retry timing is pluggable, so an adopter can replace the schedule without forking.

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| Worker pools / concurrent execution | Cycle C. This cycle keeps the single-job loop; every mechanism is built to work for N in-flight jobs but only one is ever in flight. |
| `Stop(ctx)` and the full graceful-shutdown sequence | Cycle C. This cycle keeps the existing "cancel context, drain the in-flight job" behavior. |
| Caller-supplied `ScheduledAt` at insert time | Cycle D. The `scheduled` state and the due-time fetch predicate land here because retry and snooze need them, but no public insert option exposes them. |
| Named queues / weighted priorities | Cycle D. Everything still runs on the `default` queue. |
| Redrive of dead jobs (`drover retry`) | Cycle F, which owns the CLI. `dead` is retained and inspectable; nothing moves rows out of it. |
| Metrics for retry/rescue counts | Cycle E owns the Prometheus surface. Structured log lines are the observability contract for this cycle. |
| Backoff configuration via anything other than a policy implementation | A tuning-knob config struct is a worse surface than a one-method interface; adopters who need a different curve implement the interface. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here — nothing is left silently unclear. Decisions carrying
cross-cycle weight are also recorded as `AD-NNN` rows in `.specs/STATE.md`; the full option sets and
rationale live in `context.md`.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Where a retrying job waits | `retryable` state with `scheduled_at` set to the retry time; the fetch predicate widens to the three non-terminal waiting states | Keeps one fetch path and no promoter component, while leaving `retryable` distinguishable from a never-run job for the Cycle E/F inspection surfaces | y (auto, D-1) |
| Where a snoozed job waits | `scheduled` state with `scheduled_at` set to the wake time | A snooze is not a failure; reusing `retryable` would misreport it in every future count of retrying jobs | y (auto, D-2) |
| Whether a snooze consumes an attempt | No — `attempt` is decremented to offset the increment the claim already applied, floored at zero | A handler snoozing on "not ready yet" must not be able to exhaust `max_attempts` and land in `dead` | y (auto, D-3) |
| Whether a rescue consumes an attempt | No — the claim that stranded the row already consumed one | Counting it twice would halve the effective `max_attempts` for every crash | y (auto, D-4) |
| Whether a panic is retryable | Yes — a recovered panic is classified exactly like a returned error | A panic is as likely to be transient (a nil map on one malformed payload) as a returned error; the old dead-on-panic branch was part of the AD-003 placeholder | y (auto, D-5) |
| Whether an unregistered job kind is retryable | Yes — it fails and retries like any other error | During a rolling deploy old workers legitimately see kinds they do not know yet; the backoff curve makes the wait cheap and the job survives to be run by a new worker | y (auto, D-6) |
| Retry policy surface | `RetryPolicy` interface with a single `NextRetry(ctx, job *JobRow) time.Time` method, defaulting to the exponential policy | One method, full row context, absolute time — matches the pluggable-`RetryPolicy` commitment in ADR-0003 without a knob-bag config struct | y (auto, D-7) |
| How the rescuer avoids racing another rescuer or a live worker | It re-claims expired rows with `FOR UPDATE SKIP LOCKED` and a fresh lease before disposing of them | Re-claiming makes rescue reuse the ordinary `running` → terminal transitions instead of needing a second set of lease-guarded finalizers | y (auto, D-8) |
| Jitter randomness source | `math/rand/v2` top-level functions; not injectable | Tests assert the documented bound over many samples rather than an exact value, so a seam would exist only for the test | y (auto, D-9) |
| Heartbeat lifetime relative to shutdown | The heartbeat outlives context cancellation and stops only after the last in-flight job has finalized | The loop deliberately runs the in-flight job on an uncancelled context; a heartbeat that stopped at cancellation would let that job's lease expire mid-drain and invite a duplicate | y (auto, D-10) |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Transient failures retry instead of dying ⭐ MVP

**User Story**: As an application author, I want a job that fails for a transient reason to be
retried on a backoff schedule so that a momentary outage does not destroy accepted work.

**Why P1**: This is the reason the queue is durable at all. Without it, `dead` is the outcome of
every hiccup, and AD-003 explicitly requires this cycle to replace that branch.

**Acceptance Criteria**:

1. WHEN a registered worker returns a non-nil error and the job's `attempt` is less than its
   `max_attempts` THEN the system SHALL leave the job in state `retryable`, with `finalized_at`
   unset and `scheduled_at` set to the time returned by the configured retry policy.
2. WHEN a job is moved to `retryable` after a failed attempt THEN the system SHALL append exactly one
   entry to the job's `errors` array recording that attempt's number, the failure time, and the
   error's message.
3. WHEN a registered worker returns a non-nil error and the job's `attempt` has reached
   `max_attempts` THEN the system SHALL move the job to state `dead` with `finalized_at` set, and
   SHALL append the final attempt's error entry.
4. WHEN a job in state `retryable` has a `scheduled_at` at or before the current time THEN the
   system SHALL claim it on the next fetch, in the same way an `available` job is claimed.
5. WHEN a job in state `retryable` has a `scheduled_at` in the future THEN the system SHALL NOT
   claim it.
6. WHEN a worker panics THEN the system SHALL treat the recovered panic as a failed attempt subject
   to the same retry-or-die rule as a returned error, and SHALL record the panic's stack trace in
   the appended error entry.
7. WHEN a job's kind has no registered worker THEN the system SHALL treat the attempt as a failure
   subject to the same retry-or-die rule.
8. WHEN a job is retried and later succeeds THEN the system SHALL move it to `completed` with its
   accumulated `errors` array preserved.

**Independent Test**: Register a worker that fails its first two attempts and succeeds on the third;
run the loop and observe the job reach `completed` with `attempt` = 3 and two recorded errors.

---

### P1: The default backoff is exponential with jitter ⭐ MVP

**User Story**: As an operator, I want retries spaced by a widening, jittered interval so that a
failing dependency is not hammered and a thundering herd of same-second retries cannot form.

**Why P1**: ADR-0003 fixes the curve; a retry feature with a fixed or zero delay is a busy loop
wearing a retry costume.

**Acceptance Criteria**:

1. WHEN the default retry policy computes the wait after attempt N THEN the system SHALL return a
   delay of at least `0.9 × N⁴` seconds and at most `1.1 × N⁴` seconds.
2. WHEN the default retry policy is invoked repeatedly for the same attempt number THEN the system
   SHALL NOT return the same delay every time.
3. WHEN the default retry policy computes waits for two attempt numbers M and N where `M + 1 < N`
   THEN the system SHALL return a strictly larger delay for N than for M.
4. WHEN a `Config.RetryPolicy` is supplied THEN the system SHALL schedule every retry at the time
   that policy returns, and SHALL NOT consult the default policy.
5. WHEN `Config.RetryPolicy` is nil THEN the system SHALL use the default exponential policy.
6. WHEN a retry policy returns a time at or before the current time THEN the system SHALL schedule
   the job to be claimable immediately rather than rejecting the policy's answer.

**Independent Test**: Call the default policy 1000 times for attempt 3 and assert every result falls
in `[72.9s, 89.1s]` and that at least two distinct values appear.

---

### P1: A crashed worker's job is rescued ⭐ MVP

**User Story**: As an operator, I want a job whose worker died to return to the queue automatically
so that a SIGKILL or a node loss costs at most one lease duration of delay, not the job.

**Why P1**: This is the crash-path half of at-least-once delivery in ADR-0003 and the limitation
AD-005 deliberately left open.

**Acceptance Criteria**:

1. WHEN a job is in state `running` and its `leased_until` is in the past THEN the rescuer SHALL
   return it to state `retryable` with `scheduled_at` set by the retry policy, provided its
   `attempt` is below `max_attempts`.
2. WHEN a rescued job's `attempt` has reached `max_attempts` THEN the rescuer SHALL move it to
   `dead` with `finalized_at` set.
3. WHEN the rescuer returns a job to the queue THEN the system SHALL append exactly one entry to its
   `errors` array identifying the cause as an expired lease.
4. WHEN the rescuer processes a job THEN the system SHALL NOT change that job's `attempt`.
5. WHEN a job is in state `running` and its `leased_until` is in the future THEN the rescuer SHALL
   leave it untouched.
6. WHEN the loop is started THEN the system SHALL run a rescue sweep every `Config.RescueInterval`
   until its context is cancelled, and SHALL NOT exit the loop when a sweep returns an error.
7. WHEN two rescuers sweep concurrently THEN each expired job SHALL be dispositioned exactly once.

**Independent Test**: Insert a job, claim it, force its `leased_until` into the past, run one sweep,
and observe state `retryable` with an unchanged `attempt` and one lease-expiry error appended.

---

### P1: A long-running job keeps its lease ⭐ MVP

**User Story**: As an application author, I want a job that legitimately runs longer than the lease
duration to keep running so that slow work is not duplicated by the rescuer.

**Why P1**: Without the heartbeat, the rescuer is not a safety net but a duplicator: any job slower
than the lease is re-queued while still running.

**Acceptance Criteria**:

1. WHEN a job is executing THEN the system SHALL extend that job's `leased_until` at every
   `Config.HeartbeatInterval` to `Config.LeaseDuration` beyond the current time.
2. WHEN a job runs for longer than `Config.LeaseDuration` while heartbeats are being delivered THEN
   the rescuer SHALL NOT rescue it.
3. WHEN a job finishes THEN the system SHALL stop extending its lease.
4. WHEN a heartbeat extension fails THEN the system SHALL log the failure and continue running,
   and SHALL NOT abort the in-flight job.
5. WHEN the loop's context is cancelled while a job is still draining THEN the system SHALL continue
   heartbeating that job until it finalizes.
6. WHEN the loop returns THEN no heartbeat or rescuer goroutine SHALL still be running.

**Independent Test**: With a lease of 100ms and a heartbeat of 25ms, run a job for 400ms alongside a
rescue sweep and assert the job completes normally and was never rescued.

---

### P2: Handlers classify their own outcome

**User Story**: As an application author, I want to tell drover "this job can never succeed" or "ask
me again later" so that permanent failures do not burn 25 retries and deferrals do not count as
failures.

**Why P2**: Retries and rescue are correct without it, but the queue is much harder to use well
without a way out of the retry curve.

**Acceptance Criteria**:

1. WHEN a worker returns an error wrapping `Cancel` THEN the system SHALL move the job to state
   `cancelled` with `finalized_at` set, and SHALL NOT schedule a retry.
2. WHEN a job is cancelled by a handler THEN the system SHALL append exactly one entry to its
   `errors` array recording the cancellation reason.
3. WHEN a worker returns an error wrapping `Snooze(d)` THEN the system SHALL move the job to state
   `scheduled` with `scheduled_at` set to `d` beyond the current time, and `finalized_at` unset.
4. WHEN a job is snoozed THEN the system SHALL restore its `attempt` to the value it had before the
   claim that snoozed it, so the snooze consumes no attempt.
5. WHEN a job whose `attempt` is zero is snoozed THEN the system SHALL leave `attempt` at zero
   rather than making it negative.
6. WHEN a job is snoozed THEN the system SHALL NOT append an entry to its `errors` array.
7. WHEN a job in state `scheduled` reaches its `scheduled_at` THEN the system SHALL claim it on the
   next fetch.
8. WHEN a `Cancel` or `Snooze` sentinel is wrapped in additional context with `%w` THEN the system
   SHALL still classify it as cancellation or snooze respectively.
9. WHEN a job is snoozed repeatedly THEN the system SHALL never move it to `dead` on account of
   those snoozes.

**Independent Test**: Return `fmt.Errorf("nope: %w", drover.Cancel(errors.New("bad input")))` from a
worker and observe state `cancelled` with one recorded error and no retry scheduled.

---

## Edge Cases

- WHEN the retry policy panics THEN the system SHALL recover, log, and fall back to the default
  policy rather than crashing the loop or leaking the job in `running`.
- WHEN a job's `max_attempts` is zero or negative THEN the system SHALL treat the first failure as
  exhausting it and move the job to `dead`.
- WHEN a rescue sweep finds no expired jobs THEN the system SHALL make no writes.
- WHEN the driver returns an error while finalizing a failed job THEN the system SHALL log it and
  continue the loop; the job's lease expiry is the backstop that eventually rescues it.
- WHEN `Config.LeaseDuration`, `Config.HeartbeatInterval`, or `Config.RescueInterval` is zero or
  negative THEN the system SHALL substitute its documented default.
- WHEN `Config.HeartbeatInterval` is greater than or equal to `Config.LeaseDuration` THEN the system
  SHALL substitute an interval strictly less than the lease duration, so a configured heartbeat can
  never be slower than the lease it renews.
- WHEN `Snooze` is given a zero or negative duration THEN the system SHALL make the job claimable
  immediately rather than scheduling it in the past.
- WHEN a job is claimed, finalized, and then a stale heartbeat extension for it arrives THEN the
  extension SHALL NOT resurrect or modify the finalized row.

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| RETRY-01 | P1: Transient failures retry | Verified | Done |
| RETRY-02 | P1: Transient failures retry | Verified | Done |
| RETRY-03 | P1: Transient failures retry | Verified | Done |
| RETRY-04 | P1: Transient failures retry | Verified | Done |
| POLICY-01 | P1: Default backoff | Verified | Done |
| POLICY-02 | P1: Default backoff | Verified | Done |
| RESCUE-01 | P1: Crashed worker rescued | Verified | Done |
| RESCUE-02 | P1: Crashed worker rescued | Verified | Done |
| RESCUE-03 | P1: Crashed worker rescued | Verified | Done |
| LEASE-01 | P1: Long-running job keeps lease | Verified | Done |
| LEASE-02 | P1: Long-running job keeps lease | Verified | Done |
| LEASE-03 | P1: Long-running job keeps lease | Verified | Done |
| SENT-01 | P2: Handlers classify outcome | Verified | Done |
| SENT-02 | P2: Handlers classify outcome | Verified | Done |
| SENT-03 | P2: Handlers classify outcome | Verified | Done |

**Requirement detail:**

- **RETRY-01** — A failed attempt below the attempt ceiling moves the job to `retryable` at the
  policy's time, unfinalized. (P1 AC1, AC5)
- **RETRY-02** — Every failed attempt appends exactly one structured error entry carrying attempt
  number, time, message, and — for panics — a stack trace. (P1 AC2, AC6)
- **RETRY-03** — A failed attempt at the attempt ceiling moves the job to `dead`, finalized. (P1 AC3)
- **RETRY-04** — Due `retryable` and `scheduled` jobs are claimable by the same fetch path that
  claims `available` jobs; jobs not yet due are not. (P1 AC4, AC5, AC8; P2 AC7)
- **POLICY-01** — The default policy returns `N⁴` seconds ±10%, varies between calls, and grows with
  the attempt number. (P2-story AC1–AC3)
- **POLICY-02** — A configured policy fully replaces the default; a nil one selects the default; a
  past answer means "claimable now". (AC4–AC6)
- **RESCUE-01** — Expired-lease `running` jobs return to `retryable` at the policy's time, or to
  `dead` at the attempt ceiling, without changing `attempt`. (AC1, AC2, AC4)
- **RESCUE-02** — Each rescue appends exactly one lease-expiry error entry, and unexpired jobs are
  untouched. (AC3, AC5)
- **RESCUE-03** — The sweep runs on an interval, survives its own errors, and dispositions each
  expired job exactly once under concurrency. (AC6, AC7)
- **LEASE-01** — Executing jobs have their lease extended every heartbeat interval to a full lease
  duration ahead. (AC1, AC2)
- **LEASE-02** — Heartbeats stop when the job finalizes, tolerate their own failures, and never
  touch a finalized row. (AC3, AC4; Edge case 8)
- **LEASE-03** — Heartbeats persist through context cancellation until the drain completes, and no
  background goroutine outlives the loop. (AC5, AC6)
- **SENT-01** — `Cancel` terminates the job as `cancelled` with one recorded reason and no retry.
  (AC1, AC2)
- **SENT-02** — `Snooze(d)` defers the job to `scheduled` at `now+d` without appending an error and
  without consuming an attempt, floored at zero and never leading to `dead`. (AC3–AC6, AC9)
- **SENT-03** — Both sentinels are recognized through `%w` wrapping. (AC8)

**Coverage:** 15 total, 15 to be mapped to tasks, 0 unmapped.

---

## Success Criteria

- [x] A worker failing twice then succeeding reaches `completed` with `attempt` = 3 and two recorded
      errors, with no manual intervention.
- [x] A job abandoned in `running` with an expired lease is back in `retryable` after one sweep, with
      its `attempt` unchanged.
- [x] A job running four times longer than its lease duration completes without ever being rescued.
- [x] `dead` is reachable only by exhausting `max_attempts` — no single failure produces it.
- [x] The unit suite still runs without Docker; lifecycle tests report no leaked goroutines.
