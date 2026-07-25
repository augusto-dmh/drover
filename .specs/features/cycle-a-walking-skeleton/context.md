# Cycle A — Decision Log

Auto-selected per run instructions: where ADRs/research settle a question, the recommended option is taken and recorded here. Sources: ADR-0001..0004, `docs/research/2026-07-22/`.

## D-1 (AD-001): Migration mechanism

**Options considered:**
- A — golang-migrate/goose external tool: mature, but adds a dependency and an external workflow for library adopters. Against: ADR-0004 stdlib-first; adopters shouldn't need a second tool.
- B — Embedded `.sql` via `embed.FS` + minimal internal migrator (version table, transactional apply) ★: ~100 lines, no deps, `drover migrate` CLI can wrap it in Cycle F. River ships its own migrator for the same reason.
- C — sqlc-managed schema only (no runtime migrator): breaks the adopter story (they'd hand-apply SQL).

**Pick: B.** Migrations numbered `NNN_name.sql` under `internal/migrate/migrations/`, applied in a transaction each, versions recorded in `drover_schema_version`.

## D-2 (AD-002): Full state enum upfront

Pick: create all seven states in migration 001 even though Cycle A uses three. Postgres enum alteration is awkward inside transactions; paying the churn later is worse. Transition guards live in the storage layer, so unused states are unreachable, not latent bugs.

## D-3 (AD-003): Handler error → `dead` in Cycle A

Pick: no silent drops, no fake retry. `dead` is the honest terminal state until Cycle B replaces this branch with the retry scheduler. The Cycle B spec MUST change this exact branch (noted in RFC-0001 Cycle B scope).

## D-4 (AD-004): Forward-compatible schema

Pick: `attempt`, `max_attempts` (default 25 per ADR-0003), `errors` jsonb array, `scheduled_at` (default `now()`, used in fetch predicate from day one), `leased_until` (written on claim, not yet enforced), `queue` (default `'default'`). Fetch predicate: `state = 'available' AND scheduled_at <= now() ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED` — already the Cycle D-ready shape.

## D-5 (AD-005): Crash-mid-job is a documented limitation in A

Pick: `leased_until` is set on claim so Cycle B's rescuer has data to act on retroactively, but nothing enforces it in A. Documented in package docs.

## D-6 (AD-006): Constant poll interval, default 1s

Pick: configurable `PollInterval` in `Config`, default 1s; DB errors log and wait one interval. No jitter/backoff in A (Cycle G tunes fetch).

## D-7 (AD-007): `Register` panics on duplicate kind

Pick: mirrors `http.Handle` and River's `AddWorker`; registration happens at boot where panics are the correct loudness. `Kind()` validity (non-empty) checked at Insert instead, returning `ErrInvalidKind`.

## D-8 (AD-008): Storage interface shape

Pick: single narrow interface `driver` (unexported initially) with `Migrate`, `Insert`, `InsertTx`, `FetchAvailable`, `MarkCompleted`, `MarkDead` — implemented by `internal/pgdriver` (pgx v5 + sqlc) and `internal/memdriver` (mutex-guarded map, for unit tests). Exporting a driver API is deferred until a second real driver exists (ADR-0002: the interface exists for testability).
