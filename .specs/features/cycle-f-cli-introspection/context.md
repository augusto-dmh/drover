# CLI + Introspection — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's
auto-decision rule. Each records the options considered with reasons for and against,
the choice, and why. They are mirrored into `.specs/STATE.md` as `AD-049`–`AD-057`.

Decisions already binding on this cycle and **not** relitigated here: ADR-0002 (narrow
storage interface + in-memory adapter), ADR-0003 (dead state + scoped redrive via
`drover retry`), ADR-0004 (`cmd/drover/`, stdlib `flag`, GoReleaser, no `pkg/`),
AD-019 (lease+attempt fence for worker writes), AD-020/AD-035 (database clock),
AD-024 (`Client` Start/Stop lifecycle), AD-041/AD-048 (`Driver.Stats` and published
depth states), AD-008 (unexported driver; export deferred).

---

## D-1 — CLI framework

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Stdlib `flag` + small subcommand switch | ADR-0004 names it; zero new deps; five commands fit a switch. | Manual help text; no nested command trees for free. |
| (b) Cobra | Nested commands ergonomic; research listed it as an alternative. | New dependency for a thin operator tool; contradicts stdlib-first without a justifying gap. |
| (c) urfave/cli | Similar to cobra. | Same dependency objection. |

**Chosen: (a).** ADR-0004 already decided. Five top-level verbs do not need a framework.

---

## D-2 — Inspector vs methods on Client

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Separate exported `Inspector` from `*pgxpool.Pool` | CLI needs no worker lifecycle; Asynq precedent; AD-024 stays about workers. | Second public constructor beside `NewClient`. |
| (b) Methods on `Client` (`c.Stats`, `c.CancelJob`, …) | One type to learn. | Forces constructing a full client (workers registry, middleware, …) for `psql`-replacement work; couples ops to AD-024 lifecycle. |
| (c) Methods on `Client` that work before `Start` | Avoids a second type. | Still requires `NewClient` + `Workers`; muddies "what is a Client for". |

**Chosen: (a).** Operator tools and the worker process are different programs that share a schema.

---

## D-3 — Where list/get/cancel/redrive live in storage

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Extend unexported `driver.Driver` with List/Get/OperatorCancel/Redrive; both adapters implement | Unit suite without Docker (AD-041 precedent); one seam. | Widens the interface by four methods. |
| (b) Inspector talks to `*pgxpool.Pool` / sqlc directly | Keeps `Driver` narrow. | Breaks the no-Docker unit-suite constraint for the cycle's core behaviour. |
| (c) A second unexported `inspectorDriver` interface | Leaves worker `Driver` untouched. | Two seams over the same adapters; Cycle E already rejected this for Stats. |

**Chosen: (a).** Same argument as AD-041.

---

## D-4 — Operator writes vs the lease fence

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) State-conditioned UPDATEs (no attempt fence) for cancel/redrive; refuse `running` | Does not invent a fake lease; cannot stomp a live worker's attempt (AD-019). | Cannot cancel in-flight work. |
| (b) Reuse `MarkCancelled` / `MarkRetryable` with a fabricated lease | Reuses code. | Those methods require `state=running` and the current attempt — wrong for waiting/dead rows, and fabricating a lease is how you create stale-write bugs. |
| (c) Allow cancelling `running` by id alone | Max operator power. | Silent fight with heartbeat/dispose; AD-019 exists specifically to prevent this class of write. |

**Chosen: (a).** Spec ASM-05. Running jobs are left to complete, fail, or be rescued.

---

## D-5 — Redrive attempt and error history

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `dead` → `available`, `attempt=0`, clear lease, keep `errors`, `scheduled_at=now()` | Matches research SQS redrive reset; provenance survives triage. | A handler that keys on attempt number sees a reset. |
| (b) Reset attempt and clear errors | Clean slate. | Destroys the triage record the DLQ exists to preserve. |
| (c) Re-insert as a new job id | Fresh row; old dead remains. | Two rows for one logical job; list noise; not what ADR-0003's "scoped redrive" reads as. |

**Chosen: (a).**

---

## D-6 — Output format

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Human text default + global `--json` | Terminal demos; scripts get JSON. | Two formatters to maintain. |
| (b) JSON only | One formatter. | Hostile at 3am in a terminal. |
| (c) Third format (YAML / table library) | Pretty tables. | Dependency or complexity for little gain. |

**Chosen: (a).** Hand-rolled columns; no table dependency.

---

## D-7 — Database connection configuration

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `--database` flag, else `DATABASE_URL` | Ubiquitous; ADR-0004 flag+env. | None material. |
| (b) YAML config file this cycle | ADR-0004 allows optional YAML. | Scope; five commands do not need a file format yet. |
| (c) Individual host/port/user flags | Familiar from `psql`. | URL is enough; duplicates pgx's parser. |

**Chosen: (a).**

---

## D-8 — Enqueue from the CLI

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `--kind` + optional `--queue` + `--args` JSON string via Inspector | Honest untyped operator path; no fake `JobArgs`. | Bypasses typed registry (intentional — CLI is not the app). |
| (b) Require a plugin/shared library of kinds | Type-safe CLI. | Impossible for a generic binary. |
| (c) Only enqueue empty `{}` payloads | Simpler. | Useless for recovery of real jobs. |

**Chosen: (a).** Empty kind and invalid JSON are hard errors (mirrors `ErrInvalidKind` spirit).

---

## D-9 — GoReleaser breadth

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `.goreleaser.yaml` for 3 OS × 2 arch, CGO=0, ldflags version, archives + checksums; document tag release | ADR-0004 / research minimum. | No automated GitHub Actions workflow unless already trivial. |
| (b) Full CI release workflow + Homebrew + Docker | Polished. | Scope beyond "GoReleaser" as named; tap/image are research optionals. |
| (c) Makefile `go build` only | Simpler. | RFC names GoReleaser. |

**Chosen: (a).** A working config file and `drover version` / `--version` satisfy the cycle; wiring `release` CI can follow the first tag.

---

## Feature Boundary

Ship an exported `Inspector`, a `cmd/drover` binary with `stats`, `jobs list`,
`retry`, `cancel`, `enqueue` (plus version), driver methods for operator reads/writes,
and `.goreleaser.yaml`. Do not ship a web UI, batch redrive, running-job cancel,
YAML config, or discard/delete.

---

## Agent's Discretion

- Exact human-output column order and spacing.
- Whether `jobs list` is nested (`drover jobs list`) via a two-token parse or a
  single `jobs-list` — prefer nested tokens matching the RFC wording.
- Whether Inspector integration tests share the existing e2e Postgres helper or add a
  thin sibling — prefer reuse.

---

## Deferred Ideas

- `drover jobs show <id>` (GetJob is on the API; CLI show can wait).
- `drover discard` / prune of terminal rows (retention story still unowned).
- YAML config; Homebrew tap; Docker image.
- Cancelling `running` via cooperative context cancel (needs a signal path the
  worker understands — not a row flip).
