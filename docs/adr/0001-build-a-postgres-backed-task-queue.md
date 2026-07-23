# ADR-0001: Build drover as a small Postgres-backed task queue, differentiated by transparency

- **Date**: 2026-07-22
- **Status**: Accepted
- **Deciders**: Augusto de Melo Henriques
- **Tags**: scope, positioning

## Context and Problem Statement

The Go ecosystem already has mature task queues: River (Postgres, best-in-class typed API), Asynq (Redis, best-in-class observability), Faktory (language-agnostic server), plus smaller Postgres options (gue, neoq). A new queue cannot win by out-featuring them. The question is what drover should be so that building it is justified.

## Decision Drivers

- Every shipped mechanism must be small enough to fully understand, explain, and prove correct.
- One maintainer, PR-sized increments; scope discipline is existential.
- Design choices must be defensible against the established projects ("why not just use River?") with citable trade-offs.

## Considered Options

- **A — Postgres-native single-module library with typed generics API** (River-grade semantics at ~3–5k LOC, operability-first)
- **B — Redis-backed queue** (re-treads Asynq with fewer guarantees)
- **C — Language-agnostic server, mini-Faktory** (doubles surface area: server + client protocol)

## Decision Outcome

Chosen option: **A**. Drover is a deliberately small Postgres-backed queue whose value is transparency: River-style semantics (transactional enqueue, SKIP LOCKED claims, lease-based crash recovery, dead-letter retention) implemented in a codebase small enough to audit in one sitting, with every trade-off recorded in an ADR and grounded in the founding research (`docs/research/2026-07-22/`).

### Positive Consequences

- A defensible answer to "why not River": drover trades feature breadth for a fully auditable core and documented reasoning.
- Scope has a built-in brake: any feature that can't be fully explained doesn't ship.

### Negative Consequences

- Drover will always trail River/Asynq in features; the README must position it honestly (educational-but-production-quality, with prior-art credit).

## Links

- Evidence: `docs/research/2026-07-22/rq01-existing-go-queues.md`, `rq05-product-scope.md`
- Roadmap: RFC-0001
