# Drover — Project State

## Decisions (AD-NNN)

Durable, cross-cycle. Architecture-level decisions live in `docs/adr/`; entries here are cycle-scoped picks that later cycles must honor. Mirrored from each cycle's `context.md`.

| ID | Decision | Source |
|---|---|---|
| AD-001 | Migrations: embedded `.sql` via `embed.FS` + minimal internal migrator, `drover_schema_version` table | cycle-a `context.md` D-1 |
| AD-002 | Full 7-state enum created in migration 001; transitions guarded in storage layer | cycle-a D-2 |
| AD-003 | Cycle A: handler error ⇒ `dead` immediately; Cycle B MUST replace this branch with retry scheduling | cycle-a D-3 |
| AD-004 | Schema is forward-compatible: `attempt`/`max_attempts(25)`/`errors`/`scheduled_at`/`leased_until`/`queue` from day one; fetch predicate already Cycle-D-shaped | cycle-a D-4 |
| AD-005 | `leased_until` written on claim but unenforced until Cycle B rescuer | cycle-a D-5 |
| AD-006 | Constant 1s default poll interval; DB errors log + wait one tick | cycle-a D-6 |
| AD-007 | `Register` panics on duplicate kind; empty kind ⇒ `ErrInvalidKind` at Insert | cycle-a D-7 |
| AD-008 | Unexported `driver` interface; `internal/pgdriver` (pgx+sqlc) + `internal/memdriver` (tests only); export deferred | cycle-a D-8 |

## Handoff

- **Active feature**: `cycle-a-walking-skeleton` — **COMPLETE, validation PASS** (iteration 1: busy-poll mutant fixed in 063bd2e; sensor 5/5)
- **Branch**: `feat/cycle-a-walking-skeleton`, commits e79d281..063bd2e + .specs bundle; no remote configured yet
- **Next**: user decision — create GitHub remote + publish PR (drover-finalize skill to be created), then Cycle B (reliability core: retries, DLQ, rescuer — must replace the AD-003 dead-on-error branch)
