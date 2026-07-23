# ADR-0002: PostgreSQL-only production backend behind a narrow storage interface

- **Date**: 2026-07-22
- **Status**: Accepted
- **Deciders**: Augusto de Melo Henriques
- **Tags**: architecture, database, storage

## Context and Problem Statement

A task queue's storage backend determines its delivery guarantees, failure model, and operational profile. The two realistic candidates are PostgreSQL (River, gue, neoq lineage) and Redis (Sidekiq, Asynq lineage). Supporting both from day one was also on the table.

## Decision Drivers

- **Transactional enqueue is the flagship feature**: a job INSERT that rides the application's own transaction (one WAL commit, MVCC hides the job until commit) eliminates the ghost-job/lost-job window entirely. Redis physically cannot offer this — its workaround (the outbox pattern) is itself a Postgres queue.
- Durability by default: Postgres fsyncs at commit; Redis AOF `everysec` leaves a ~1s loss window for acknowledged writes.
- Failure detection: Postgres offers connection-scoped locks and committed-claim + rescuer patterns; Redis always requires hand-built lease/janitor machinery.
- Two production adapters is a scope trap: transactional semantics leak through any common interface, forcing a lowest-common-denominator API and doubling the test matrix.

## Considered Options

- **A — Postgres only, direct coupling**
- **B — Postgres + Redis adapters from day one**
- **C — Postgres behind a narrow storage interface; in-memory adapter for tests; Redis adapter analyzed but not shipped**

## Decision Outcome

Chosen option: **C**. Job claiming uses `SELECT ... FOR UPDATE SKIP LOCKED` with batched claims and polling (a `LISTEN/NOTIFY` wake-up is deferred tuning — NOTIFY's commit-serialization lock has caused production outages and breaks under pgbouncer transaction pooling). The interface exists for testability, not backend portability: the in-memory adapter keeps the unit suite Docker-free.

### Positive Consequences

- Transactional enqueue, DB-enforced uniqueness, and instant crash recovery come from Postgres primitives rather than bespoke machinery.
- Fast, deterministic unit tests against the in-memory adapter.

### Negative Consequences

- MVCC churn (~3 dead tuples per job lifecycle) makes autovacuum tuning, prompt deletion, and batch completion mandatory operational work.
- A future Redis adapter cannot honor transactional enqueue; that capability leakage is documented here deliberately, and the adapter stays unshipped.

## Links

- Evidence: `docs/research/2026-07-22/rq02-storage-backend.md`
- Delivery semantics built on this choice: ADR-0003
