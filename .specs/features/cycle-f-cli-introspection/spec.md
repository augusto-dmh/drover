# CLI + Introspection Specification

## Problem Statement

Cycles A–E make Drover a working queue — enqueue, claim, retry, rescue, pool,
metrics — but an operator still needs `psql` to answer "what is stuck?" or to
redrive a dead job. Prometheus answers aggregate health; it does not list a
poison payload or flip a row back to runnable. Cycle F ships the primary
inspection surface the roadmap promised: a `drover` binary backed by an
exported `Inspector` API, plus GoReleaser so the binary can ship.

## Goals

- [ ] An exported `Inspector` that reads queue stats, lists and fetches jobs,
      cancels waiting jobs, redrives dead jobs, and enqueues raw jobs — usable
      from the CLI and from other Go programs without starting a worker.
- [ ] A `drover` binary under `cmd/drover/` exposing `stats`, `jobs list`,
      `retry`, `cancel`, and `enqueue`.
- [ ] Human-readable default output with a `--json` escape hatch for scripts.
- [ ] GoReleaser configuration that builds portable, versioned binaries.
- [ ] Unit coverage of Inspector behaviour via `memdriver` (no Docker required
      for the unit suite).

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| Server-rendered status page (`drover web`) | Cycle I, optional there. |
| SPA dashboard | Permanently out of scope. |
| Discard/delete of dead rows | RFC lists `retry`/`cancel`, not discard; cancel covers quarantine without deletion. |
| Cancelling a `running` job mid-execution | Race with the lease holder (AD-019); operator waits for completion, rescue, or shutdown requeue. |
| Batch redrive / "retry all dead" | ADR-0003 and research call for *scoped* redrive; per-id is the scoped unit for this cycle. |
| Typed enqueue from the CLI | The CLI cannot know application `JobArgs` types; raw JSON + kind is the operator contract. |
| YAML config file for the CLI | ADR-0004 allows optional YAML later; this cycle uses `flag` + env only. |
| Cobra / urfave/cli | ADR-0004 names stdlib `flag`; research listed cobra as an alternative, not the choice. |
| Authentication beyond the database connection string | Whoever can open Postgres can operate the queue; no second auth layer. |
| `LISTEN/NOTIFY`, batch claim, COPY FROM | Cycle G. |
| Homebrew tap / Docker image via GoReleaser | Optional extras in research; binary archives + checksums are enough. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here — nothing is left silently unclear.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| ASM-01 — CLI framework | Stdlib `flag` + a small subcommand dispatcher in `cmd/drover` | ADR-0004: "binary config via flag + env"; stdlib-first. | y — pre-recorded |
| ASM-02 — Inspector vs Client | Separate exported `Inspector` type, constructed from a `*pgxpool.Pool`, not methods on `Client` | CLI and operators must not start a worker pool; Asynq's Inspector precedent; keeps `Client` lifecycle (AD-024) orthogonal. | y |
| ASM-03 — Storage seam | New driver methods for list/get/operator-cancel/redrive; reuse `Stats` for `stats` | Same ADR-0002 / AD-041 pattern that let Cycle E unit-test gauges without Docker. | y |
| ASM-04 — Redrive semantics | `dead` → `available`, `attempt` reset to 0, `scheduled_at = now()`, lease cleared; error history retained | ADR-0003 scoped redrive; research §2.4 SQS-style attempt reset; keeping `errors` preserves triage provenance. | y |
| ASM-05 — Operator cancel | Allowed for `available`, `scheduled`, `retryable`, `dead` → `cancelled`; refused for `running`, `completed`, `cancelled` | Avoids fighting the lease fence (AD-019). Terminal completed stays completed. | y |
| ASM-06 — Output format | Human table (or labelled lines for single-object commands) by default; `--json` for machine-readable | Terminal demos well; scripts need JSON. | y |
| ASM-07 — Database URL | `--database` flag, falling back to `DATABASE_URL` | Common operator convention; no YAML this cycle. | y |
| ASM-08 — Enqueue args | `--kind`, optional `--queue`, optional `--args` JSON object string (default `{}`) | Untyped operator path; invalid JSON is a usage error. | y |
| ASM-09 — List defaults | Default limit 100; optional `--state` and `--queue` filters; newest-first by `id` descending | Bounded output; id order is stable without a new index. | y |
| ASM-10 — GoReleaser | Ship `.goreleaser.yaml` for linux/darwin/windows × amd64/arm64, `CGO_ENABLED=0`, version ldflags; tag-triggered docs in README | ADR-0004 and research name GoReleaser; archives + checksums only. | y |
| ASM-11 — Version string | `main.version` set via ldflags; unset builds report `dev` | Standard GoReleaser pattern from research. | y |
| ASM-12 — Migrate subcommand | Not in this cycle | RFC row does not list it; `drover.Migrate` already exists for embedders. | y |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Exported Inspector API ⭐ MVP

**User Story**: As a Go embedder or CLI author, I want an `Inspector` that talks to
the same Postgres schema without starting workers, so that I can inspect and mutate
jobs from tools that are not the worker process.

**Why P1**: Every CLI verb is a thin wrapper over this API; without it the binary
has nowhere honest to live and the unit suite cannot cover operator actions.

**Acceptance Criteria**:

1. WHEN `NewInspector(pool)` is called with a usable pool THEN the system SHALL
   return an `Inspector` ready for use without calling `Start`.
2. WHEN `Inspector.Stats(ctx)` is called THEN the system SHALL return per-queue
   depth counts for the published states and oldest-claimable ages, matching the
   Cycle E `Driver.Stats` semantics (ASM from AD-048).
3. WHEN `Inspector.ListJobs(ctx, opts)` is called THEN the system SHALL return jobs
   matching the optional state and queue filters, capped by limit, ordered by `id`
   descending.
4. WHEN `Inspector.GetJob(ctx, id)` is called for an existing id THEN the system
   SHALL return that job's current row.
5. WHEN `Inspector.GetJob(ctx, id)` is called for an unknown id THEN the system
   SHALL return an error wrapping a not-found sentinel.
6. WHEN `Inspector.CancelJob(ctx, id)` is called on a cancellable state THEN the
   system SHALL transition the job to `cancelled` and set `finalized_at`.
7. WHEN `Inspector.CancelJob(ctx, id)` is called on a non-cancellable state THEN
   the system SHALL leave the row unchanged and return an error naming the refusal.
8. WHEN `Inspector.RetryJob(ctx, id)` is called on a `dead` job THEN the system
   SHALL move it to `available` with `attempt == 0`, a cleared lease, and
   `scheduled_at` at or before "now" on the database clock, retaining prior errors.
9. WHEN `Inspector.RetryJob(ctx, id)` is called on a non-dead job THEN the system
   SHALL leave the row unchanged and return an error.
10. WHEN `Inspector.Enqueue(ctx, kind, argsJSON, opts)` is called with a non-empty
    kind and valid JSON THEN the system SHALL insert a job row and return it.
11. WHEN `Inspector.Enqueue` is called with an empty kind or invalid JSON THEN the
    system SHALL return an error without inserting.

**Independent Test**: drive an `Inspector` over `memdriver` (via the unexported
test constructor), seed rows, exercise Stats/List/Get/Cancel/Retry/Enqueue, and
assert state transitions without Docker.

---

### P1: `drover stats` ⭐ MVP

**User Story**: As an operator, I want `drover stats` so that I can see queue depth
and oldest-job age without scraping Prometheus or opening `psql`.

**Why P1**: First 3am question; reuses the Cycle E stats path.

**Acceptance Criteria**:

1. WHEN `drover stats` runs against a migrated database THEN the system SHALL print
   per-queue depth and oldest-claimable age in human-readable form and exit 0.
2. WHEN `--json` is set THEN the system SHALL print a single JSON document of the
   same facts and exit 0.
3. WHEN the database is unreachable THEN the system SHALL print an error to stderr
   and exit non-zero.

**Independent Test**: CLI test (or Inspector-backed command function) against
memdriver/fake printing; integration optional for the real binary path.

---

### P1: `drover jobs list` ⭐ MVP

**User Story**: As an operator, I want to list jobs filtered by state and queue so
that I can find dead or stuck work without writing SQL.

**Why P1**: Triage before redrive; the RFC names this verb explicitly.

**Acceptance Criteria**:

1. WHEN `drover jobs list` runs THEN the system SHALL print jobs up to the default
   limit, newest `id` first.
2. WHEN `--state` and/or `--queue` are set THEN the system SHALL print only matching
   jobs.
3. WHEN `--limit N` is set THEN the system SHALL return at most N jobs.
4. WHEN `--json` is set THEN each listed job SHALL be represented in JSON (array).

**Independent Test**: seed known rows via Inspector, list with filters, assert
membership and ordering.

---

### P1: `drover retry` and `drover cancel` ⭐ MVP

**User Story**: As an operator, I want to redrive a dead job and cancel a waiting
job by id so that I can recover or quarantine work without hand-editing rows.

**Why P1**: ADR-0003's scoped redrive and the RFC verbs.

**Acceptance Criteria**:

1. WHEN `drover retry <id>` targets a `dead` job THEN the system SHALL redrive it
   per ASM-04 and print a confirmation (human or JSON) of the resulting row.
2. WHEN `drover cancel <id>` targets a cancellable job THEN the system SHALL
   cancel it per ASM-05 and print a confirmation.
3. WHEN either command targets a missing or ineligible id THEN the system SHALL
   exit non-zero with an error on stderr and leave the database unchanged for
   ineligible (non-missing) cases.

**Independent Test**: Inspector unit tests cover transitions; CLI wiring tests
assert exit codes for success and refusal.

---

### P1: `drover enqueue` ⭐ MVP

**User Story**: As an operator, I want to enqueue a job from the CLI with a kind
and JSON args so that I can inject test or recovery work without writing a Go
program.

**Why P1**: RFC verb; closes the "can I kick the queue from the terminal" loop.

**Acceptance Criteria**:

1. WHEN `drover enqueue --kind K [--queue Q] [--args '{...}']` runs with valid
   input THEN the system SHALL insert the job and print the new id (and row under
   `--json`).
2. WHEN kind is missing or empty THEN the system SHALL exit non-zero without
   inserting.
3. WHEN `--args` is not valid JSON THEN the system SHALL exit non-zero without
   inserting.

**Independent Test**: enqueue via command function / Inspector; get job and assert
kind, queue, args.

---

### P2: GoReleaser + version

**User Story**: As a maintainer, I want GoReleaser config and a version flag so
that tagged releases produce portable binaries operators can download.

**Why P2**: Named in the RFC; not required to demo CLI behaviour locally.

**Acceptance Criteria**:

1. WHEN `.goreleaser.yaml` is present THEN it SHALL build `cmd/drover` for
   linux/darwin/windows on amd64 and arm64 with `CGO_ENABLED=0` and version
   ldflags.
2. WHEN `drover version` (or `--version`) runs THEN the system SHALL print the
   embedded version string (defaulting to `dev` for local builds).

**Independent Test**: `goreleaser check` (or parse-level validation) and a unit
assertion that the version command prints the `version` variable.

---

## Edge Cases

- WHEN list filters match zero jobs THEN the system SHALL print an empty table /
  empty JSON array and exit 0.
- WHEN cancel/retry races with a worker claim (row becomes `running` between read
  and write) THEN the system SHALL return a refusal/conflict error and leave the
  worker's claim intact.
- WHEN enqueue `--args` is omitted THEN the system SHALL store `{}`.
- WHEN `--limit` is zero or negative THEN the system SHALL treat it as the default
  limit (100), not as "unbounded".
- WHEN an unknown subcommand is invoked THEN the system SHALL print usage and exit
  non-zero.

---

## Implicit-Requirement Dimensions

| Dimension | Resolution |
| --- | --- |
| Input validation & bounds | Non-empty kind; valid JSON args; positive effective list limit; id must parse as int64. |
| Failure / partial-failure states | DB errors → stderr + non-zero exit; no partial CLI "success" after a failed write. |
| Idempotency / retry / duplicate handling | Cancel on already-cancelled → error (not silent success); retry only from `dead`. |
| Auth boundaries & rate limits | N/A — DB credentials are the auth boundary. |
| Concurrency / ordering | Operator writes use state-conditioned UPDATEs (not lease fences); conflict with claim → error. |
| Data lifecycle / expiry | N/A — no pruning this cycle (handoff: terminal rows accumulate). |
| Observability | CLI logs errors to stderr; no new Prometheus metrics for CLI actions. |
| External-dependency failure | Unreachable Postgres → non-zero exit. |
| State-transition integrity | Cancel and retry are explicit allowed-state sets; others refuse. |

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| CLI-01 | P1: Inspector API | Design | Pending |
| CLI-02 | P1: Inspector Stats | Design | Pending |
| CLI-03 | P1: Inspector List/Get | Design | Pending |
| CLI-04 | P1: Inspector Cancel | Design | Pending |
| CLI-05 | P1: Inspector Retry | Design | Pending |
| CLI-06 | P1: Inspector Enqueue | Design | Pending |
| CLI-07 | P1: `drover stats` | Design | Pending |
| CLI-08 | P1: `drover jobs list` | Design | Pending |
| CLI-09 | P1: `drover retry` / `cancel` | Design | Pending |
| CLI-10 | P1: `drover enqueue` | Design | Pending |
| CLI-11 | P2: GoReleaser + version | Design | Pending |

**Coverage:** 11 total, 0 mapped to tasks, 11 unmapped ⚠️

---

## Success Criteria

- [ ] An operator can inspect depths, list dead jobs, redrive one, cancel one, and
      enqueue a test job using only the `drover` binary and a database URL.
- [ ] The unit suite covers Inspector transitions via `memdriver` without Docker.
- [ ] `go test -race ./...` and lint stay green; integration tags still pass.
- [ ] `.goreleaser.yaml` validates and documents how to cut a release.
