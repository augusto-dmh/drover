---
name: drover-finalize
description: Finalizes and publishes drover changes with consistent Git branch names, Conventional Commit messages, verification notes, and structured pull requests. Use when the user asks to finalize work, prepare a commit, commit changes, push a branch, open a pull request, write a PR description, or publish completed drover work. Do not use for implementing features, reviewing code, or debugging CI unless the user also asks to publish the resulting changes.
license: CC-BY-4.0
metadata:
  author: Drover contributors
  version: 1.1.0
---

# Drover Finalize

Apply drover's repository conventions when preparing or publishing completed work. Treat a request to finalize completed work as authorization to inspect the diff, choose metadata, stage intended files, commit, push, and create an open ready-for-review PR. Keep narrower requests proportional: generate names and text when that is all the user requests, and stop after committing when the user asks only for a commit.

## Conventions

Use Conventional Commits for commit messages and PR titles. **Scope is required** — match recent `main` history (`feat(driver): …`, `fix(ops): …`), never bare `feat: …` / `fix: …`:

```text
<type>(<scope>)<optional-!>: <imperative summary>
```

Use one of these types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `perf`, `style`, `revert`.

Pick a short lowercase scope for the area touched, for example: `driver`, `memdriver`, `pgdriver`, `inspector`, `cli`, `client`, `loop`, `pool`, `migrate`, `metrics`, `ops`, `stats`, `readme`, `planning`, `release`, `workflow`.

Use branch names in this format:

```text
<type>/<optional-issue-number-><short-kebab-summary>
```

Keep the branch type aligned with the primary change, and derive the summary from what the change does — never from planning labels. Keep summaries concise, specific, and lowercase. Examples:

```text
feat/retry-backoff
fix/lease-expiry-race
docs/storage-decision
```

PR titles follow the same Conventional Commit format as commits (scoped) and summarize the whole PR.

## Commit And PR Hygiene

Never add authorship or tooling attribution to commits or pull requests. Commit messages and PR bodies must not contain `Co-Authored-By` trailers, "Generated with" lines, model names, agent emails (`cursoragent@cursor.com`), or any other identification of an AI assistant or the tool used to produce the change.

**Agent git wrappers often append `Co-authored-by` after the message you pass.** A clean HEREDOC is not enough. After every `git commit`:

1. Run `git log -1 --format=%B` and confirm there is no attribution trailer.
2. If a trailer appeared, strip it before pushing — rewrite with `git filter-branch --msg-filter` / equivalent plumbing, or soft-reset and recommit through a path that does not re-inject trailers. Do not leave attribution on the branch.

`validate_metadata.py` **fails** (hard) on attribution in `--commit`, `--commit-body`, `--pr-title`, and every commit in `--range`.

Write PR-body and issue paragraphs as single unwrapped lines. Never hard-wrap prose at a column width — GitHub renders the wraps literally, squeezing the text into a narrow left column.

## Self-Contained History

Commit messages, PR titles, and PR bodies must be understandable by an outside reader with no access to drover's internal planning. They must NOT contain internal references:

- task or phase IDs from `tlc-spec-driven` (`T1`, `T10`, `Phase 3`);
- decision or requirement IDs (`ADR-0002`, `AD-003`, `RFC-0001`, `CORE-03`, `AC-2`, `G1`);
- roadmap cycle letters (`cycle A`, `Cycle B`), `Gate:` labels, `SPEC_DEVIATION`, `design §N`;
- paths into internal working state (`.specs/…`).

Explain each change in plain terms instead. Internal traceability lives in `.specs/` (STATE.md, tasks.md, validation.md), never in the permanent git history or on the PR. Name a document (for example an ADR) only when the change actually adds or edits that file — not as a cross-reference the reader must look up. The register is: a short Summary, bulleted Changes, bulleted Verification, no lookups.

`validate_metadata.py` fails on these tokens in a commit subject/body or PR title; `render_pr_body.py` warns when the assembled body contains them (and warns on attribution). Clear both before publishing, and do not mirror the internal-reference style of surrounding commits — that style is the problem being corrected.

## Workflow

### Step 1: Inspect The Repository

1. Run `git status --short`, `git branch --show-current`, and `git diff --stat`.
2. Read the relevant diff before proposing metadata.
3. Identify unrelated working-tree changes and leave them unstaged.
4. If the task is only to suggest names or draft a PR description, stop before mutating Git state.

### Step 2: Choose The Metadata

1. Select the primary Conventional Commit type.
2. **Always** choose a scope (required) — the package or area the change primarily touches.
3. Write an imperative summary that describes the outcome, not implementation mechanics.
4. Derive the branch name from the same primary change.
5. Run:

```bash
python3 .claude/skills/drover-finalize/scripts/validate_metadata.py \
  --branch '<branch-name>' \
  --commit '<commit-subject>' \
  --pr-title '<pr-title>'
```

Fix validation errors before continuing.

### Step 3: Verify The Change

1. Run the narrowest relevant checks for the changed files.
2. For Go changes, run `go build ./... && go vet ./...` and `go test -race ./...`; when storage, migration, or end-to-end behavior changed, also run `go test -race -tags=integration ./...` (requires Docker).
3. Run `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run` before publishing — CI enforces it.
4. When `.sql` query files changed, regenerate with `go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate` and commit the generated code — CI fails on drift.
5. For docs-only or skill-only changes, run `git diff --check` and any relevant helper script validation.
6. Report any verification that could not run.
7. Do not publish changes with known failing verification unless the user explicitly accepts that risk.

### Step 4: Commit Intentionally

1. Stage only files that belong to the requested change.
2. Prefer one atomic commit per logical concern.
3. When changes span unrelated concerns, present the proposed commit breakdown and wait for user approval before committing.
4. Review `git diff --cached --stat` and `git diff --cached` before each commit.
5. Validate the commit subject (and body, if any) with the metadata validator **before** committing.
6. Commit via HEREDOC as usual.
7. **Immediately** run `git log -1 --format=%B` and re-validate:

```bash
python3 .claude/skills/drover-finalize/scripts/validate_metadata.py \
  --commit "$(git log -1 --format=%s)" \
  --commit-body "$(git log -1 --format=%b)"
```

If attribution appeared, rewrite before any push. Do not continue with a dirty tip.

8. Run `git status --short` after committing and report remaining unstaged or untracked files.

### Step 5: Push And Open The PR

1. **History gate (mandatory before push):** validate every commit that will be published:

```bash
python3 .claude/skills/drover-finalize/scripts/validate_metadata.py \
  --range 'main..HEAD' \
  --branch "$(git branch --show-current)" \
  --pr-title '<pr-title>'
```

Use the actual base branch if it is not `main`. A single failing commit blocks publish — fix history first (scoped subjects, strip attribution).

2. Run `gh auth status` before publishing. If authentication is unavailable, report the blocker.
3. Push the branch with an upstream automatically when the user asks to finalize or publish completed work.
4. Draft concise Markdown for Summary, Changes, and Verification from the diff and verification output.
5. Run `scripts/render_pr_body.py` to assemble the PR body. Pass screenshots only when the PR contains visible UI changes. Pass related issues only when applicable.

```bash
python3 .claude/skills/drover-finalize/scripts/render_pr_body.py \
  --summary-file /tmp/drover-pr-summary.md \
  --changes-file /tmp/drover-pr-changes.md \
  --verification-file /tmp/drover-pr-verification.md \
  --output /tmp/drover-pr-body.md
```

6. Read [assets/pull_request_template.md](assets/pull_request_template.md) only when the renderer cannot be used or the user explicitly asks to inspect the template.
7. Create an open ready-for-review PR with `gh pr create --base <base-branch> --head <branch-name> --title '<pr-title>' --body-file <pr-body-file>`. Do not create draft PRs unless the user asks for one.
8. When a PR already exists, update its title or body with `gh pr edit`.

## Examples

### Feature Change

User says: "Finalize the retry work."

Use:

```text
Branch: feat/retry-backoff
Commit: feat(loop): retry failed jobs with jittered exponential backoff
PR title: feat(loop): retry failed jobs with jittered exponential backoff
```

### Decision Change

User says: "Commit the storage ADR."

Use:

```text
Branch: docs/storage-decision
Commit: docs(storage): record the queue storage backend decision
PR title: docs(storage): record the queue storage backend decision
```

### Naming Only

User says: "Suggest a branch name and commit message for the metrics work."

Return:

```text
Branch: feat/queue-metrics
Commit: feat(metrics): expose queue depth and latency to prometheus
```

Do not modify Git state.

## Troubleshooting

### Validation Rejects The Metadata

Keep the type lowercase, use kebab-case after the branch slash, and format commits and PR titles as `<type>(<scope>): <summary>` with a **non-empty scope**. Bare `feat: …` fails.

### Attribution Appeared After Commit

Agent tooling injected a trailer. Strip it with a message-only history rewrite before pushing; re-run `--range main..HEAD` until clean. Do not amend if amend rules in the user/session policy forbid it — use filter-branch / recommit on a replacement branch instead.

### The Working Tree Contains Unrelated Changes

Stage only the files that belong to the requested change. Report the remaining files without reverting or including them.

### GitHub PR Creation Is Unavailable

Run `gh auth status` and report the authentication or repository-access blocker. Return the validated PR title and populated Markdown body.

### The PR Body Renderer Cannot Run

Read [assets/pull_request_template.md](assets/pull_request_template.md), populate the applicable sections manually, and remove optional sections that do not apply.
