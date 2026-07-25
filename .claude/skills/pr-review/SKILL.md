---
name: pr-review
description: 'Multi-agent PR reviewer for drover. Use when — and only when — explicitly asked to review a pull request: "review PR #N", "review this PR", "code review this PR", "check this pull request". Not for automatic use during coding, feature implementation, or finalizing/publishing (use drover-finalize), nor for general questions.'
license: CC-BY-4.0
metadata:
  author: Drover contributors
  version: 1.0.0
---

# PR Review — Orchestration Protocol

Coordinates 6 specialized subagents (via the Agent tool) then consolidates findings into a unified summary. Each subagent loads the relevant existing drover docs (`CLAUDE.md`, `docs/adr/`, `.specs/features/`) — this skill does not duplicate them.

Drover stack (ADR-0004): a Go library (root package `drover`) plus a future `cmd/drover` binary; PostgreSQL via pgx v5 + sqlc (generated code committed under `internal/dbsqlc`); storage behind the narrow `internal/driver.Driver` interface with a Postgres driver and an in-memory test driver; stdlib-first (no web framework, no ORM); `log/slog` for logging. Delivery semantics are at-least-once with lease-based recovery (ADR-0003); transactional enqueue is the flagship capability (ADR-0002).

## Step 1: Initialize

1. Get PR number from context or ask the user.
2. Identify repo: `gh repo view --json nameWithOwner -q .nameWithOwner`
3. Fetch diff: `gh pr diff {PR_NUMBER}` — save it to a scratchpad file for the subagents. If it exceeds ~200KB, split it per-file or per-directory and hand subagents the chunk list: a Read of a 256KB+ file fails, and six subagents each burning that failed Read is the observed cost.
4. Load existing inline comments: `gh api repos/{REPO}/pulls/{PR_NUMBER}/comments` — build a set of `{path, line}` pairs to avoid reposting.
5. Read PR intent: `gh pr view {PR_NUMBER} --json title,body,headRefName`
6. Derive the feature slug from the branch name: strip the Conventional Commit prefix (`feat/`, `fix/`, `docs/`, …) and any leading issue number, leaving a kebab summary. This slug is the fuzzy key for locating the matching spec under `.specs/features/`.

## Step 2: Launch Subagents in Parallel

Send **one message** with **six Agent tool calls** — all launched simultaneously. Pass REPO, PR_NUMBER, the diff, existing comment locations, the PR intent, and the feature slug to each subagent prompt. After all complete, run Step 3.

---

## Severity Labels (all subagents use these)

- 🚨 Critical — bugs or logic errors that will cause failures
- 🔒 Security — security vulnerabilities or data exposure
- ⚡ Performance — significant performance concerns
- ⚠️ Warning — code smells or maintainability issues
- 💡 Suggestion — optional improvements

---

## Universal Rules (every subagent must follow)

1. **Comment allowlist:** Only post inline comments on lines in the diff starting with `+` (excluding `+++`).
2. **Skip duplicates:** If `{path, line}` within ±3 lines already has a comment, skip.
3. **Mark resolved:** Reply `[RESOLVED] This appears resolved by the recent changes.` on existing comments where the issue is fixed.
4. **False positive guard:** Only report findings with ≥80% confidence. Skip when uncertain.
5. **Positive highlight:** Include at least one well-done aspect of the change before listing issues.
6. **Tone:** Specific, actionable, collegial. Explain WHY something is a problem, and cite the ADR / CLAUDE.md durable decision / Go convention that grounds it.
7. **Never** approve, request-changes, or modify files. Comments only.
8. **Marker:** Start every inline comment body with `<!-- drover-review:{type} -->` (invisible in rendered view, used by the consolidation subagent).
9. **No AI attribution:** Never add tooling/authorship attribution (`Co-Authored-By`, "Generated with", model names) to any comment body — consistent with `drover-finalize` hygiene rules.
10. **Multiline bodies from a file:** Write the comment body to a temp file, then post with `gh pr comment --body-file <file>` or `gh api ... -F body=@<file>`. Never use `gh api -f body=@<file>` — lowercase `-f` does not expand `@`, so the comment is published as the literal file path instead of its content.
11. **PR-level comments must be issue comments, never reviews.** Post every PR-level comment (requirements summary, consolidation summary) with `gh pr comment` / `gh api .../issues/comments`, not `gh pr review`. Submitted reviews cannot be deleted, dismissed, or blanked via the API, so they leave permanent artifacts that break teardown (Step 4).
12. **Oversized inputs:** never Read the shared diff file whole when it may exceed the 256KB cap — use the per-file chunks from Step 1, or `offset`/`limit`/grep.
13. **Completion contract:** a subagent's work ends only when its findings are posted to the PR (or its summary comment updated) and a compact final text summary is returned. Never go idle without posting — an idle with nothing posted is treated as a stall: the orchestrator checks comment counts, nudges once, then re-dispatches a fresh subagent.

---

## Structural Compliance Checklist

Ground each item in CLAUDE.md's Durable Decisions and ADR-0001..0004:

- [ ] **Public API surface (ADR-0004)** — new exported identifiers live in the root package, are deliberate, documented, and context-first; no `pkg/` directory; generics only for typed job args.
- [ ] **Driver narrowness (storage decision)** — the storage interface stays narrow; new storage capabilities extend it only when both drivers can honor the semantics, and capability leakage (e.g. transactions the in-memory driver cannot express) is documented, not papered over.
- [ ] **Stdlib-first (ADR-0004)** — no new dependency without clear justification; no web framework, no ORM; `log/slog` for logging via injected logger.
- [ ] **Generated code (ADR-0004)** — `internal/dbsqlc` is never hand-edited; query changes go through `.sql` files + regeneration.
- [ ] **State-transition integrity** — job state changes go through guarded storage-layer transitions; no direct state writes that bypass the guards.
- [ ] **Delivery semantics (ADR-0003)** — changes touching claiming, leases, retries, or finalization preserve at-least-once semantics and the documented duplicate-source analysis; handler idempotency requirements stay documented.
- [ ] **Scope walls (roadmap)** — no Redis production adapter, no SPA dashboard, no exactly-once claims of any kind.

---

## Subagent 1: Correctness & Concurrency

**Marker:** `<!-- drover-review:concurrency -->`

Load `CLAUDE.md` (Durable Decisions) and skim ADR-0003 (delivery semantics). Review the PR diff for: goroutines with no explicit exit path or leak potential (every `go func` needs a termination story); missing or wrong `context.Context` propagation (context-first params, cancellation actually honored, `context.WithoutCancel` used only where drain semantics require it); race conditions on shared state (maps, counters, slices accessed from multiple goroutines without synchronization — reason as if `-race` were watching); channel misuse (unbuffered sends that can block forever, closed-channel sends, nil channels); missing panic recovery at job-execution boundaries; lease/heartbeat logic that can lose or double-run jobs outside the documented duplicate sources; state transitions that bypass the storage-layer guards; time handling that breaks under clock skew or makes tests flaky; and shutdown paths that can hang (no deadline) or drop in-flight work.

**Second pass:** Re-read the full diff from top to bottom. List every file or hunk you did not comment on. For each uncovered file, ask: "Does this file spawn, synchronize, or cancel anything — and is every path accounted for?" Only skip a file when you can explicitly state why it is clean.

**Comment format:**
```
<!-- drover-review:concurrency -->
[🚨/⚠️/💡] — [Short title]
[What the issue is; the interleaving or input that triggers it]
**Recommendation:** [Specific fix]
```

---

## Subagent 2: Requirements & Definition of Done

**Marker:** `<!-- drover-review:requirements -->`
**Posts:** One PR-level summary comment only — no inline comments.

Use a two-track approach to find requirements. Run both tracks; use whichever yields content.

### Track A — Feature Spec (`.specs/features/`)

1. Use the feature slug derived in Step 1. Look for `.specs/features/{slug}/` — if an exact match is absent, fuzzy-match the slug against directory stems under `.specs/features/`, and also check the PR title/body for an explicit spec path or markdown link.
2. For the matched feature, read `spec.md`, `tasks.md`, and `validation.md`.
3. Extract: requirement IDs, acceptance criteria, the task checklist, and stated goals / out-of-scope items.

### Track B — Accepted Decisions (ADR / RFC)

1. Scan the matched spec and the changed files for decisions they implement under `docs/adr/` and `docs/rfc/`.
2. Read each relevant doc and extract the constraints the PR must honor (e.g. storage backend rules, delivery semantics, layout/toolchain conventions).

### Resolution Logic

| Tracks with content | Action |
|---|---|
| Both A and B | Merge requirements from both; note the source of each item |
| A only | Use the feature spec requirements |
| B only | Use the ADR/RFC constraints |
| Neither | Post: "⚠️ No matching `.specs/features/` spec or applicable ADR/RFC found — requirements verification skipped." and stop |

Compare the merged requirements against the PR diff and post the summary **idempotently as an issue comment** (never a review — see Step 3.8). Look for an existing PR comment containing `<!-- drover-review:requirements -->`: if one exists, update it in place with `gh api -X PATCH repos/{REPO}/issues/comments/{COMMENT_ID} -F body=@<tempfile>`; otherwise create it with `gh pr comment {PR_NUMBER} --body-file <tempfile>`.

**Second pass:** After drafting the summary, re-read the requirements list one item at a time and ask: "Did I evaluate this criterion against the diff?" For any item not yet assessed, find the relevant section of the diff and explicitly mark it ✅, ❌, or 🔲.

**Summary format:**
```markdown
<!-- drover-review:requirements -->
## 📋 Requirements Review

**Sources:** {e.g. ".specs/features/{slug}" · "docs/adr/0003" · "Both"}

### ✅ Implemented
### ❌ Missing or Incomplete
### 🔲 Definition of Done
- [x] covered  - [ ] not covered
### 💬 Notes
```

---

## Subagent 3: Test Coverage

**Marker:** `<!-- drover-review:tests -->`

Load the Workflow section of `CLAUDE.md`. Drover testing: table-driven tests with `t.Parallel`, `-race` always; the unit suite must run without Docker (in-memory driver); integration tests use testcontainers-go behind the `integration` build tag; goleak guards loop/lifecycle tests; `testing/synctest` for time-dependent logic.

Review the PR diff for: new or changed exported API, storage methods, or loop behavior with no covering test (🚨 Critical for new exported functions and state transitions); unit tests that secretly require Docker (breaking the Docker-free contract); missing `-race`-meaningful coverage on newly shared state; assertion-quality issues (asserting only that no error occurred when a value/state is the contract, asserting mock/spy calls instead of resulting state, missing error-path and edge cases the spec lists, timing assertions with only a lower bound — a busy loop passes those); tests that would pass under a plausibly wrong implementation; and weakened or deleted assertions.

**Second pass:** Re-read the full diff from top to bottom. List every new or modified exported function, storage method, and goroutine-bearing path you did not comment on. For each, ask: "Is there a test covering the happy path, at least one failure path, and the concurrency dimension if it has one?" Only skip when you can explicitly state why coverage exists or is not applicable.

**Comment format:**
```
<!-- drover-review:tests -->
[🚨/⚠️/💡] — [Short title]
[Description of the gap or anti-pattern]
**Recommendation:** [Table-driven / integration / goleak / synctest pattern to add]
```

---

## Subagent 4: Architecture & Go Idioms

**Marker:** `<!-- drover-review:architecture -->`

### Phase 0 — Load all reference documents

Load every document below before touching the diff. Do not skip any.

1. `CLAUDE.md` (Durable Decisions + Workflow)
2. `docs/adr/0001-build-a-postgres-backed-task-queue.md`
3. `docs/adr/0002-postgres-only-backend-behind-narrow-storage-interface.md`
4. `docs/adr/0003-at-least-once-delivery-lease-heartbeat-rescuer.md`
5. `docs/adr/0004-single-module-root-package-layout-and-toolchain.md`

Then scan the diff for directory structure: note which layers (root package, `internal/driver`, `internal/pgdriver`, `internal/memdriver`, `internal/migrate`, `cmd/`) the changed paths touch.

### Phase 1 — Extract the rule list from the loaded documents

Do not use a hardcoded list. After loading Phase 0, scan each document and extract every explicit rule into a single numbered checklist: layout rules, dependency rules, API idioms (config struct, context-first, sentinel errors with `%w`, consumer-defined interfaces, generics restraint), storage-interface rules, delivery-semantics constraints, and testing/tooling conventions. Number the combined list sequentially from 1. This is your evaluation matrix for Phase 2. Do not add rules absent from the documents, and do not omit any you find.

Additionally apply standard Go review judgment where the docs are silent: exported identifiers need doc comments; errors are wrapped with context, never swallowed; no naked `interface{}`/`any` where a concrete type or small interface fits; zero values made useful; no premature abstraction.

### Phase 2 — Evaluate the matrix

Work through the diff **one file at a time**. For each changed file:

- For each rule, decide **PASS** / **VIOLATION** / **N/A**.
- N/A is only valid when the rule is structurally inapplicable to the file type (e.g. a `.sql` file cannot violate a goroutine rule; generated code under `internal/dbsqlc` is exempt from style rules but NOT from the never-hand-edited rule).
- For every VIOLATION: post an inline comment on the exact `+` line that is the evidence. Include the rule number and source document.

**Second pass:** After completing the matrix for all files, re-read the full diff top to bottom. List every file or hunk you did not evaluate. For any uncovered file, run the matrix again. Only skip a file when you can explicitly state which rules are N/A and why.

**Comment format:**
```
<!-- drover-review:architecture -->
[🚨/⚠️/💡] — [Short title]
Rule: [Rule number + source, e.g. "Rule 6 — layout/toolchain ADR (no hand-edits to generated code)"]
[What in the diff violates it — quote the offending line]
**Recommendation:** [Exact fix, code snippet if < 6 lines]
```

---

## Subagent 5: Regression & Hallucination Detection

**Marker:** `<!-- drover-review:regression -->`

Review the PR diff for changes unrelated to the PR's stated purpose, or signs of AI-generated artifacts. Look for: deleted code unrelated to the change (🚨 Critical), phantom imports referencing non-existent packages/symbols (🚨 Critical), function/method calls with wrong signatures (🚨 Critical), `TODO`/`FIXME`/empty-body stubs left in production code, `//nolint` directives hiding real issues, duplicate logic that already exists in the module, weakened validation (input checks or state-transition guards removed), silently swallowed errors (`_ = err`, empty error branches) in the loop or storage layer, weakened test assertions, and dead code that is never called.

**Second pass:** Re-read the full diff from top to bottom. List every file or hunk you did not comment on. For each uncovered file, ask: "Does this file contain any unrelated deletions, phantom imports, duplicate logic, or weakened assertions?" Only skip a file when you can explicitly state why none of those categories apply.

**Comment format:**
```
<!-- drover-review:regression -->
[🚨/⚠️/💡] — [Short title]
Type: [unrelated-deletion | phantom-import | hallucination | duplicate | regression | dead-code]
[Specific description with quoted evidence from the diff]
**Recommendation:** [Exact fix]
```

---

## Subagent 6: SQL & Performance

**Marker:** `<!-- drover-review:sql-performance -->`

Only flag issues **clearly visible in the diff** — no speculation. Drover persistence is pgx v5 + sqlc over PostgreSQL; the queue's hot path is the claim query. Load ADR-0002 for the locking/MVCC context. Look for: claim-path correctness (`FOR UPDATE SKIP LOCKED` scope, subquery alias correctness, predicates that stop matching the partial index — the fetch index is on `(queue, scheduled_at, id) WHERE state = 'available'`); unbounded queries with no `LIMIT`; queries inside loops that batch operations should replace; missing transaction boundaries around multi-statement writes; long-open transactions that pin the xmin horizon; unbounded growth of the `errors` jsonb array or job-row retention with no cleanup story; string-concatenated SQL instead of parameterized sqlc queries (🔒); secrets or credentials in code, config defaults, or logs (🔒); connection-pool misuse (per-call pool creation, missing Close, pool exhaustion in worker fan-out); and allocation-heavy patterns on the hot path where a cheap fix is visible.

**Second pass:** Re-read the full diff from top to bottom. List every query, transaction, loop, and pool interaction you did not comment on. For each uncovered block, ask: "Does this contain a clearly visible SQL-correctness, data-safety, or performance issue?" Only skip a block when you can explicitly state why none of the patterns above apply.

**Comment format:**
```
<!-- drover-review:sql-performance -->
[⚡/🔒/🚨/⚠️] — [Short title]
[Description with estimated impact, e.g. "claim query stops using the partial index"]
**Recommendation:** [Fix with short code sketch if < 6 lines]
```

---

## Step 3: Consolidation

After all 6 subagents complete, spawn one more subagent via the Agent tool to consolidate:

1. `gh api repos/{REPO}/pulls/{PR_NUMBER}/comments` — fetch all inline comments.
2. Filter to those starting with `<!-- drover-review:` and parse the type from the marker.
3. Fetch PR-level comments for the `<!-- drover-review:requirements -->` summary.
4. Group by severity: 🔒 Security → 🚨 Critical → ⚡ Performance → ⚠️ Warning → 💡 Suggestion.
5. Deduplicate findings at the same `{path, line}` (±3 lines) — note both agents in the entry.
6. Collect one positive highlight per agent.
7. **Gap detection:** Run `gh pr diff {PR_NUMBER} --name-only` to get the full list of changed files. Cross-reference against all collected inline comment paths. For any file with zero inline comments from any subagent, add it to a `### 🔍 Files With No Inline Comments` section. Omit a file from this section only if it is a config/lock file (`*.json`, `*.yaml`, `*.yml`, `*.mod`, `*.sum`, `.golangci.yml`), generated code under `internal/dbsqlc/`, or a pure declaration file with no logic.
8. Post the summary **as an issue comment, never as a review**. A submitted `gh pr review` (even `--comment`) creates a review that GitHub's API cannot delete, dismiss, or blank — it is permanent. Post idempotently by marker: search existing PR comments for `<!-- drover-review:summary -->`; if found, update it in place with `gh api -X PATCH repos/{REPO}/issues/comments/{COMMENT_ID} -F body=@<tempfile>`; otherwise create it with `gh pr comment {PR_NUMBER} --body-file <tempfile>`.

**Summary format:**
```markdown
<!-- drover-review:summary -->
## 🤖 Drover AI Review Summary

| | |
|---|---|
| **Subagents invoked** | {N} of 6 (Correctness & Concurrency · Requirements · Test Coverage · Architecture & Idioms · Regression · SQL & Performance) |
| **Docs loaded** | `CLAUDE.md`, `docs/adr/*`, `.specs/features/{slug}/*` |
| **Findings** | {N} across {M} files |

---

### 🔒 Security ({N})
- [`path/file.go:L42`] Finding title

### 🚨 Critical ({N})
### ⚡ Performance ({N})
### ⚠️ Warnings ({N})
### 💡 Suggestions ({N})

---
### 🔍 Files With No Inline Comments
- `path/to/file.go` — no findings from any subagent (verify manually or re-run targeted review)

_(Omit this section if all logic files received at least one comment.)_

---
### ✅ Highlights
- [One positive highlight per agent]

---
> See inline comments for details and recommendations.
```

If no findings across all agents: post `✅ No issues found across all review dimensions.` but still include the metadata table.

---

## Step 4: Teardown / re-run

The review's artifacts must be fully removable so a re-run never duplicates them and the author can clear the PR. All review output is therefore inline comments and issue comments only (Step 3.8) — never a submitted review.

**Resolve a thread after its finding is fixed.** Reply, then resolve, via GraphQL:

```bash
# Reply in the thread
gh api graphql -f query='mutation($t:ID!,$b:String!){addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:$t,body:$b}){comment{id}}}' -f t="$THREAD_ID" -f b="Fixed in <hash> — <one line>."
# Resolve it
gh api graphql -f query='mutation($t:ID!){resolveReviewThread(input:{threadId:$t}){thread{isResolved}}}' -f t="$THREAD_ID"
```

Thread IDs come from `repository.pullRequest.reviewThreads` (each node has `id`, `isResolved`, and its `comments`).

**Remove all bot comments.** Everything the review posts is deletable:

```bash
# Inline review comments (findings + replies)
for id in $(gh api repos/{REPO}/pulls/{PR_NUMBER}/comments --paginate --jq '.[].id'); do
  gh api -X DELETE repos/{REPO}/pulls/comments/$id
done
# PR-level issue comments (requirements + summary) — filter by marker if selective
for id in $(gh api repos/{REPO}/issues/{PR_NUMBER}/comments --paginate --jq '.[].id'); do
  gh api -X DELETE repos/{REPO}/issues/comments/$id
done
```

**Do not create what you cannot remove.** There is no API to delete or dismiss a `COMMENTED` review, and an empty review body is rejected (HTTP 422). That is why summaries are issue comments — keep it that way.
