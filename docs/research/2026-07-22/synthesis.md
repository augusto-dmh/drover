# Synthesis — Founding research fleet

**Date**: 2026-07-22 · **Inputs**: `rq01-existing-go-queues.md`, `rq02-storage-backend.md`, `rq03-delivery-and-lifecycle.md`, `rq04-go-conventions.md`, `rq05-product-scope.md` · **Output**: ADR-0001..0004, RFC-0001

## Completeness critique (run inline)

- **Convergence**: all five reports independently converge on the same backbone — Postgres-native, `FOR UPDATE SKIP LOCKED` claims, transactional enqueue as the flagship capability, at-least-once semantics made explicit, River as the semantics benchmark and Asynq as the observability benchmark.
- **Tensions resolved**:
  - RQ01's "Horizon-inspired dashboard as differentiator" vs RQ05's "SPA dashboard is a scope trap" — resolved as RQ05's Option B: CLI-first inspection, with a single server-rendered `html/template` status page as optional Cycle I. Both asynqmon (abandoned) and riverui (company-funded) support the scope-trap reading.
  - RQ05's Cycle C concept list mentions `errgroup` while RQ03 recommends a fixed goroutine pool — resolved in RQ03's favor: cancel-on-first-error is the wrong semantic for a job executor; `errgroup`/`semaphore.Weighted` remain documented contrast points.
- **Deferred decisions** (tracked, not decided): Redis storage adapter (analyzed in ADR-0002, not shipped); `LISTEN/NOTIFY` wake-up (deferred to Cycle G tuning — polling first, with the NOTIFY commit-serialization outage as documented caution); `database/sql` driver support beyond pgx (documented future work).
- **Biggest risk**: MVCC churn operations (autovacuum tuning, bloat, batch completion) are the price of the Postgres choice and land on a solo maintainer — mitigated by copying River's proven mitigations (batch completer, cleaner, prompt deletion) and keeping the cut line after Cycle E firm.
- **Re-check triggers**: revisit backend choice only if target workloads exceed ~10k jobs/s sustained; revisit the no-SPA rule never (superseding ADR required); re-verify the four flagged unverified claims in RQ01 before citing them anywhere public.

## Recommendations → promotion

| RQ | Recommendation (★) | Promoted to |
|---|---|---|
| RQ01 | Postgres-native single-module library, typed generics API; differentiate on transparency + operability, not features | ADR-0001 |
| RQ02 | Postgres backbone behind narrow storage interface; in-memory test adapter; Redis adapter documented, not shipped | ADR-0002 |
| RQ03 | At-least-once via lease + heartbeat + recoverer; `attempt^4` ±10% jitter, max 25, `Cancel`/`Snooze` sentinels, dead state + redrive; fixed worker pool; stop-fetch → drain → cancel → requeue shutdown; weighted queue fetch | ADR-0003 |
| RQ04 | Root-package + `internal/` + `cmd/drover/`; pgx v5 + sqlc; stdlib ServeMux; slog injection; config struct; testcontainers-go; synctest; golangci-lint v2 | ADR-0004 |
| RQ05 | v1 = cycles A–E (skeleton, reliability, concurrency, middleware/scheduling, observability); CLI-first; email-pipeline example; `drover-bench` with published methodology | RFC-0001 |
