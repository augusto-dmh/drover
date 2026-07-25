---
name: drover-ship-cycle
description: 'End-to-end orchestrator for one drover roadmap PR: pick the next cycle from the roadmap RFC, run a tlc-spec-driven cycle auto-selecting recommended options, publish the PR with drover-finalize, run pr-review in a fresh-context subagent, triage every review finding against the code, apply accepted fixes, delete all PR comments, and merge after a single user approval. Use when asked to "ship the next PR", "run the ship cycle", "do the next roadmap cycle end-to-end", or to resume a partially shipped cycle. Not for ad-hoc edits, standalone reviews (use pr-review), or publishing-only work (use drover-finalize).'
license: CC-BY-4.0
metadata:
  author: Drover contributors
  version: 1.0.0
---

# Drover Ship Cycle — Orchestration Protocol

Runs one roadmap cycle from "what's the next PR?" to merged, as one orchestrated pipeline. This skill owns only the glue; the work itself is delegated to `tlc-spec-driven`, `drover-finalize`, and `pr-review` unchanged.

**Autonomy contract:** the pipeline runs without user prompts except at exactly one gate — merge approval (Stage 7) — plus the escalation rule in Stage 1. Everything else proceeds on the recommended option, logged for audit.

**Continuation contract (binding):** never end a turn "standing by", "awaiting your call", or asking "want me to proceed?" between stages — finish the stage, update the heartbeat, and start the next stage in the same turn. Anything that needs the user's eyes is deferred to the Stage 7 report, not raised as a mid-cycle pause. The only allowed stops are: the Stage 7 gate, a Verifier FAIL, and a hard blocker only the user can clear (state which one when stopping).

**Heartbeat (`.specs/.ship-status`):** at every stage transition, overwrite `.specs/.ship-status` with a single line: `<cycle> | Stage <N> <name> | <branch or PR #> | <timestamp> | <one-line status>`. This is how the user (or a parallel session) answers "what is going on?" without archaeology. Also update it when parking on an outage or limit. Delete the file at Stage 8. The file is local working state — never commit it (it lives under `.specs/` but is transient; add it to the commit only never).

**Resume contract:** on any session start or post-interruption message (a bare "continue" suffices — do not ask what to do), read `.specs/.ship-status`; if it shows a mid-stage cycle, run Stage Detection and resume immediately in that same turn.

## Run modes (optional skill argument)

- `once` (default) — stop at the Stage 7 merge gate for the single approval.
- `auto` — this invocation is the user's standing merge approval: when the Stage 7 report is clean (Verifier PASS, gates green, comments cleaned), merge without asking, run Stage 8, and start the next cycle. Still stop for a Verifier FAIL, an escalation-rule decision, or a dirty report.
- `until <cycle letter>` — like `auto`, but stop after the named roadmap cycle merges.

The mode holds for the whole invocation; do not re-ask it mid-run.

## Stage Detection (always run first)

The pipeline is resumable. Check `.specs/.ship-status` first — if it shows an in-flight cycle (possibly from another session), resume that cycle rather than starting a new one; two concurrent cycles on one repo is always a mistake. Then determine the current stage:

| Observation | Resume at |
|---|---|
| Clean `main`, next roadmap cycle not started (per `.specs/STATE.md` Roadmap progress) | Stage 0 |
| Cycle branch exists, tlc Execute/Verifier incomplete (`.specs/features/<feature>/`) | Stage 1 (tlc resume) |
| Verifier PASS on branch, no open PR | Stage 2 |
| PR open, no `<!-- drover-review:` comments on it | Stage 3 |
| PR has review comments, no `.specs/features/<feature>/review-triage.md` | Stage 4 |
| `review-triage.md` exists, accepted fixes not yet pushed | Stage 5 |
| Fixes pushed, PR comments still present | Stage 6 |
| PR comment-free, unmerged | Stage 7 |

Announce the detected stage and the cycle/PR it applies to before proceeding.

## Stage 0 — Preflight

1. Require a clean working tree. If dirty, stop and report — never stash or discard.
2. `git checkout main && git pull`.
3. Read the cycles table in `docs/rfc/0001-drover-roadmap.md` and the `## Roadmap progress` + `## Handoff` sections of `.specs/STATE.md`. The next cycle is the first RFC row not recorded as merged in Roadmap progress.
4. State the chosen cycle, its scope from the RFC row, and the intended slice in one short paragraph, then continue — no approval needed. Note any RFC constraint the cycle must honor (e.g. a placeholder from an earlier cycle this one is required to replace).

## Stage 1 — Plan & Build (tlc-spec-driven)

Invoke `tlc-spec-driven` for the cycle (Specify → Design → Tasks → Execute per its auto-sizing).

**Auto-decision rule** (replaces the human answering Discuss questions): at every decision point, formulate the options — each with why-recommend AND why-not — pick the recommended one, and record option set, choice, and rationale in the feature's `context.md` and as an `AD-NNN` row in `.specs/STATE.md`. The decision must be auditable later without the conversation. Ground picks in the founding research (`docs/research/`) and ADRs before inventing new analysis.

**Escalation rule** — ask the user (AskUserQuestion) instead of auto-deciding only when:
- the decision changes product direction or roadmap scope beyond the cycle,
- it adds an external dependency that the stdlib-first decision says needs its own justification, and no clear recommendation exists, or
- no option is defensible as recommended.

Execute honors the full tlc contract (tests from acceptance criteria, gate per task, atomic commits, mandatory fresh Verifier). A Verifier FAIL stops the pipeline with the report — do not continue to Stage 2.

**Gates for this repo:** quick = `go test -race ./...`; full = quick plus `go test -race -tags=integration ./...` (requires Docker — probe `docker ps` and tell the user to start Docker Desktop rather than debugging further); build = `go build ./... && go vet ./...`; before publishing also run `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run`, and regenerate sqlc if `.sql` files changed.

## Stage 2 — Publish (drover-finalize)

Invoke `drover-finalize` for branch, commit hygiene, verification notes, and the PR. Include the cycle's planning artifacts (`.specs/features/<feature>/*`, `.specs/STATE.md`) in the PR. Capture the PR number for all later stages. Wait for CI with `gh pr checks <N> --watch` as a background task — never `sleep N && gh …`.

## Stage 3 — Review (fresh context, author ≠ reviewer)

Spawn ONE subagent via the Agent tool (`general-purpose`, fresh context) with a prompt containing only: the repo, the PR number, and the instruction to invoke the project-local `pr-review` skill for that PR and follow it exactly.

**Do not** pass implementation context, spec content, or this session's reasoning into the subagent — the reviewer's independence is the point of the fresh context. Wait for it to finish; its deliverable is comments on the PR, not text returned to you.

## Stage 4 — Triage

1. Fetch every comment: inline `gh api repos/{repo}/pulls/{N}/comments --paginate` and PR-level `gh api repos/{repo}/issues/{N}/comments --paginate`.
2. For each finding, check it against the actual code: is it **real or not**? If real, would you **act on it or not, and why**? Judge on the code as it exists, not on the reviewer's authority; findings that misread the code, duplicate an accepted decision (ADR/AD-NNN), or trade against recorded scope decisions are rejected with the reason.
3. Persist the triage to `.specs/features/<feature>/review-triage.md` before touching anything: one row per finding — source comment, file:line, verdict (real/false), action (fix/won't-fix), rationale. Comments get deleted in Stage 6, so this file is the only surviving record of the review reasoning.

## Stage 5 — Fix

Apply every "fix" finding. Group into atomic Conventional Commits per `drover-finalize` rules (plain-language messages, no internal IDs, no AI attribution). Re-run the cycle's gates (unit `-race`, integration when storage/loop behavior changed, lint) before pushing. Push to the PR branch.

## Stage 6 — Clean Comments

Invoking this skill constitutes the user's standing instruction to delete review comments after triage — the triage record in `review-triage.md` (Stage 4) is the surviving artifact; the comments are scaffolding. Delete ALL comments from the PR:

- inline: each id from `repos/{repo}/pulls/{N}/comments --paginate` via `gh api -X DELETE repos/{repo}/pulls/comments/{id}`
- PR-level: each id from `repos/{repo}/issues/{N}/comments --paginate` via `gh api -X DELETE repos/{repo}/issues/comments/{id}`

Re-fetch both endpoints and verify zero remain. If a submitted *review* (not a comment) exists, it cannot be deleted via the API — report it as a leftover artifact instead of retrying.

## Stage 7 — Merge Gate (the one user prompt)

Present a compact ship report: cycle, PR number, Verifier result, triage counts (real/false, fixed/won't-fix), fix commits, gate and CI results, comment cleanup status. In `once` mode, ask the user (AskUserQuestion): merge now or hold. In `auto`/`until` mode with a clean report, merge without asking.

On approval: `gh pr merge {N} --merge` (merge commit — keeps the atomic commit history visible), then `git checkout main && git pull` and delete the local feature branch. If `gh pr merge` or `gh pr edit` fails with a GraphQL Projects-classic deprecation error, fall back to the REST API (`gh api -X PATCH/PUT repos/{repo}/pulls/{N}...`).

## Stage 8 — Wrap

1. Update the `## Roadmap progress` section of `.specs/STATE.md` (create it if absent): one row per shipped cycle — cycle letter, feature slug, PR number, merge date. Update `## Handoff` to point at the next cycle. Commit these as a tiny `docs` change via a follow-up PR only if they did not ship inside the cycle's PR; otherwise they are already merged.
2. Delete `.specs/.ship-status`.
3. End the wrap report with, in order: (a) the cycle closed + PR merged; (b) the **next roadmap cycle** and its scope in one line; (c) a model recommendation for that cycle per Cost discipline, with a one-line rationale. Do not start the next cycle automatically in `once` mode — the next run of this skill picks it up.

## Delegation resilience (Stages 1 and 3)

**Idle protocol (bounded):** an idle notification from a worker/reviewer that arrives WITHOUT a completion summary is a stall, not completion. Check observable progress first (Stage 3: inline + issue comment counts via `gh api`; Stage 1: the worker's reported commits/task state). If below expectation, send exactly ONE nudge naming what is missing (e.g. "0 comments posted — continue the review and consolidate"). If a second idle arrives with no new progress, TaskStop the agent and re-dispatch a fresh one with the same brief. Do not babysit beyond this protocol — no ScheduleWakeup loops whose only purpose is re-nudging.

**Limit-death degradation:** a `failed` notification citing a session/usage limit → re-dispatch the agent once. If the re-dispatch also fails, execute that stage's remaining work inline in this session, in the same turn, and record the deviation (e.g. "author = verifier this cycle") in `.specs/STATE.md`. If this session is itself rate-limited, write the heartbeat with exact resume instructions before stopping.

**Outages:** on 2+ consecutive provider 5xx/529 or `gh` connection errors, check the status page once. If a real outage is confirmed: write the heartbeat, schedule ONE long wakeup (15–30 min), and park with a one-line "waiting on <provider>, resuming ~HH:MM" message. Never blocking poll loops, never blind retries.

## Cost discipline — model selection (applies across stages)

Default model is the session's strong model. Downshift a delegated unit to a small/fast model only when the task guarantees a low slip-probability: a wrong cheap worker is not free — it costs the bad output *plus* the Verifier catching it *plus* a fix task *plus* re-verification.

**Downshift-safe test — ALL four must hold:**
1. **Fully specified** — exact files, signatures, and steps already in spec/design; no design decision or trade-off left to the worker.
2. **No correctness-critical invariant** — nothing touching claiming, leases, transactions, migrations, concurrency, ordering, or delivery semantics. A weak model fails *quietly* there.
3. **A fast local gate catches a slip** — a failing `go test`/`go vet`/lint run surfaces the mistake immediately.
4. **Small blast radius** — few files, no ripple into shared schema/state.

Fail any one → strong model. **When unsure → strong model.**

**Per-stage default:**

| Unit | Model |
|---|---|
| Stage 0 preflight · Stage 6 comment cleanup · Stage 8 wrap (pure git/gh/file ops) | small/fast |
| Stage 1 doc-only chores dispatched as their own unit | small/fast |
| Stage 1 phase workers carrying any design decision or correctness invariant (schema/migrations, claiming, leases, retries, shutdown) | strong |
| Stage 1 Verifier | strong — **never downshift**; a weak verifier that misses a surviving mutant defeats the pipeline |
| Stage 3 review | governed by `pr-review` — do not override |
| Stage 4 triage (real-vs-false against the code) | strong — adversarial reasoning |
| Stage 5 fixes | strong by default; small/fast only for a truly local, fully-specified fix that passes the four-condition test |

A phase is one worker even when it mixes scaffolding with a hard kernel: the kernel sets the model, so keep the whole phase on the strong model. Do **not** treat the Verifier as license to downshift hard work — its sensor only catches faults it mutates; sensor-blind gaps slip.

## Worker briefs — state the goal, not the steps

A delegated worker's brief must give it **what must be true when it finishes**, and leave *how* to the worker. An ordered recipe caps the worker at what the orchestrator already thought of, which is the wrong ceiling: the expensive defects in a queue are the ones nobody enumerated — they live on branches no test executes, and a worker reasoning about what the code *can* do finds them; a worker transcribing a checklist does not.

**Always give (these are context, not prescription):**
- The **seams** — signatures and `file:line` refs from an Explore survey, not file bodies.
- The **binding decisions** — the ADRs and `AD-NNN` rows the phase must conform to, and any accepted assumption it must not relitigate.
- The **invariants that must hold**, named as invariants: "a claimed job is executed by exactly one worker", "clean shutdown never drops an in-flight job". Require a sensor for each; do **not** dictate the test's shape or name.
- **Environment facts** that cost time to rediscover — Docker availability for integration tests, the baseline test counts, gate commands.
- The **non-negotiable contract** — tests derive from acceptance criteria, gate green before done, one atomic commit per task, no attribution, no internal IDs.
- The **report contract** — what the closing summary must contain, including that deviations be stated plainly rather than buried.

**Don't give:** an ordered list of edits; an enumerated list of tests to write; a solution the worker is meant to transcribe. If you find yourself writing the implementation into the brief, either the unit is a fully-specified chore (where a step list is correct) or you are doing the worker's thinking and should hand it the constraint instead.

**Traps are the exception worth naming.** A known landmine — a lease that is written but unenforced, a state transition with no guard the caller must supply — belongs in the brief, because it is knowledge the worker cannot derive from the seams. State the trap and the consequence, then require a sensor.

**Gate scoping:** run the affected test package per intermediate task commit; run the full suite once per phase boundary and once before pushing fixes. The Verifier's discrimination mutations run only the target test package, not the whole suite per mutation.

**Context hygiene:** delegate the opening codebase survey to an `Explore` agent that returns seams — signatures + `file:line` refs, not bodies. Never dump generated code (`internal/dbsqlc`) into any context.

## Hygiene (applies to every stage)

- No AI/tooling attribution anywhere public (commits, PR, comments).
- No internal IDs (task/AD/requirement/cycle/Gate labels) in commits, PR bodies, or PR comments — they live only under `.specs/`. Validate with `drover-finalize`'s `validate_metadata.py`.
- Multiline `gh` bodies go through `--body-file`/`-F body=@file`, never `-f body=@file`.
- Never post PR-level content as a review (`gh pr review`) — reviews cannot be deleted.
- Wait on CI with `gh pr checks <N> --watch` as a background task — never `sleep N && gh …`.
- Bash cwd can reset between calls: use absolute paths, and run git from the repo root. Read any existing file before Edit/Write. Pass both rules into every worker/reviewer brief — subagents hit these errors most.
- Subagents return compact final text (aim well under 10k tokens) — never report files; the orchestrator must be able to Read what comes back.
