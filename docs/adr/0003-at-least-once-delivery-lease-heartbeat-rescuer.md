# ADR-0003: At-least-once delivery via lease + heartbeat + rescuer; explicit retry and shutdown semantics

- **Date**: 2026-07-22
- **Status**: Accepted
- **Deciders**: Augusto de Melo Henriques
- **Tags**: architecture, reliability, concurrency

## Context and Problem Statement

Every queue must choose what happens when a worker dies mid-job. Exactly-once delivery is impossible end-to-end (Two Generals); the real choice is between losing work (at-most-once) and duplicating work (at-least-once), and how honestly the duplicate window is documented.

## Decision Drivers

- Never lose an accepted job, even under SIGKILL or node death where no shutdown code runs.
- Make every duplicate source (lease expiry, crash-before-ack, rescue, producer retry) explicit and small.
- The executor must not stop on individual job failure — job errors are data, not control flow.

## Considered Options

- At-most-once (ack-on-fetch) · **at-least-once with lease + heartbeat + rescuer** · effectively-once (side effects inside the job's transaction, documented as achievable for DB-only handlers)

## Decision Outcome

Chosen: **at-least-once with lease + heartbeat + rescuer**, with handlers documented as required-idempotent.

- **Claim**: committed claim with a lease (`leased_until`); running workers heartbeat to extend it; a rescuer sweep re-queues jobs whose lease expired. Rationale over connection-scoped locks: long-open transactions pin the xmin horizon and hold a backend slot per in-flight job.
- **Retries**: `attempt^4` seconds with ±10% jitter (Sidekiq/River lineage), default max 25 attempts, pluggable `RetryPolicy`. `Cancel` and `Snooze` sentinel errors classify non-retryable and deferred outcomes. Exhausted jobs land in a retained `dead` state with scoped redrive (`drover retry`).
- **Executor**: fixed pool of N goroutines fed by a fetch loop over channels — not `errgroup` (cancel-on-first-error is the wrong semantic for a queue). Per-job child context with timeout; panic recovery at the job boundary with `debug.Stack()`. (The cancellable child context ships with the worker pool; the per-job *timeout* is deferred to the middleware chain, where RFC-0001 places timeout middleware.)
- **Shutdown**: stop fetching → drain in-flight with deadline (`WaitGroup`) → cancel per-job contexts → best-effort requeue of still-running jobs. Lease expiry is the crash-path backstop; both paths are required.
- **Queues**: named queues with weighted random fetch (starvation-free) and an optional strict flag.

### Positive Consequences

- Honest, testable semantics: each mitigation layer maps to a named duplicate source.
- Clean shutdowns are fast; dirty deaths are bounded by lease duration.

### Negative Consequences

- Handlers must be idempotent; this is a documented API contract, not an implementation detail.
- Heartbeat + rescuer machinery must itself be tested under crash interleavings (synctest + goleak).

## Links

- Evidence: `docs/research/2026-07-22/rq03-delivery-and-lifecycle.md`
- Storage primitives: ADR-0002
