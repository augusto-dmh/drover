# CLI + Introspection Validation

**Date**: 2026-08-08
**Spec**: `.specs/features/cycle-f-cli-introspection/spec.md`
**Diff range**: `main..HEAD` (`7e34138..e7a3153`)
**Verifier**: independent sub-agent (author ≠ verifier)

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1 | ✅ Done | driver + sqlc |
| T2 | ✅ Done | memdriver + unit tests |
| T3 | ✅ Done | pgdriver + integration |
| T4 | ✅ Done | lint absorbed later (no empty commit) |
| T5 | ✅ Done | Inspector Stats/List/Get/Enqueue |
| T6 | ✅ Done | Cancel/Retry |
| T7 | ✅ Done | lint absorbed later |
| T8 | ✅ Done | CLI skeleton + version |
| T9 | ✅ Done | stats + jobs list |
| T10 | ✅ Done | retry/cancel/enqueue |
| T11 | ✅ Done | full gate + lint fix commit |
| T12 | ✅ Done | `.goreleaser.yaml` |
| T13 | ✅ Done | README + STATE |

---

## Spec-Anchored Acceptance Criteria

### P1: Exported Inspector API

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `NewInspector(pool)` THEN ready without `Start` | usable Inspector; Stats works without Start | `inspector_test.go:14-25` — `newInspector(mem)`; `Stats` err nil, non-nil stats (public `NewInspector` is thin wrap over same path) | ✅ PASS |
| WHEN `Stats` THEN depths + oldest-claimable ages (Cycle E semantics) | published-state depths; oldest ages present | `inspector_test.go:28-84` — depths / published-state set asserted; **no assertion on `Oldest` / ages** | ❌ GAP |
| WHEN `ListJobs` THEN filter + limit + id DESC | matching rows, cap, newest id first | `inspector_test.go:102-172` — descending ids; queue/state filters; limit 1; default 100 for ≤0 | ✅ PASS |
| WHEN `GetJob` existing THEN current row | row fields match inserted | `inspector_test.go:183-192` — `ID`/`Kind`/`State`/`Args` | ✅ PASS |
| WHEN `GetJob` unknown THEN not-found sentinel | `errors.Is(..., ErrNotFound)` | `inspector_test.go:194-197` | ✅ PASS |
| WHEN `CancelJob` cancellable THEN `cancelled` + `finalized_at` | state cancelled, finalized set | `inspector_test.go:374-381` — `StateCancelled` + `FinalizedAt != nil` (available/scheduled/retryable/dead) | ✅ PASS |
| WHEN `CancelJob` non-cancellable THEN unchanged + refusal error | `ErrInvalidTransition` / `ErrNotFound`; state unchanged | `inspector_test.go:356-368` — `errors.Is` + state unchanged | ✅ PASS |
| WHEN `RetryJob` on dead THEN available, attempt 0, lease cleared, errors retained, scheduled ≤ now | ASM-04 | `inspector_test.go:405-417` — state/attempt/lease/finalized/errors/`ScheduledAt` | ✅ PASS |
| WHEN `RetryJob` non-dead THEN unchanged + error | `ErrInvalidTransition`; row unchanged | `inspector_test.go:449-456` | ✅ PASS |
| WHEN `Enqueue` valid THEN insert + return row | kind/queue/state/args stored | `inspector_test.go:209-222` | ✅ PASS |
| WHEN `Enqueue` empty kind / invalid JSON THEN error, no insert | `ErrInvalidKind` / error; empty list | `inspector_test.go:230-247` | ✅ PASS |

### P1: `drover stats`

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| Human stats exit 0 | per-queue depth + oldest age printed | `cmd/drover/cmd_stats_jobs_test.go:15-34` — exit 0; depth + `1.500` age | ✅ PASS |
| `--json` single document | same facts as JSON | `cmd/drover/cmd_stats_jobs_test.go:37-59` — unmarshal `QueueStats` depths + oldest | ✅ PASS |
| DB unreachable → stderr + non-zero | exit ≠ 0, stderr error | `cmd/drover/cmd_stats_jobs_test.go:62-72` — Stats err → exit 1 + stderr (fake); `withInspector` open-fail path exists at `main.go:95-97` without dedicated open-fail test | ✅ PASS (proxy) |

### P1: `drover jobs list`

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| Default list newest first | print jobs, id DESC | `cmd/drover/cmd_stats_jobs_test.go:112-142` — opts + id 2 then 1 | ✅ PASS |
| `--state` / `--queue` filters | only matching | same — `listOpts.Queue`/`State` | ✅ PASS |
| `--limit N` | at most N | `listOpts.Limit == 5` | ✅ PASS |
| `--json` array | JSON array of jobs | `cmd/drover/cmd_stats_jobs_test.go:145-156` — `[]` empty case | ✅ PASS |

### P1: `drover retry` / `cancel`

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| retry dead → redrive + confirmation | exit 0; confirmation / JSON row | `cmd/drover/cmd_mutate_test.go:14-45` — exit 0; human/JSON | ✅ PASS (transition correctness via Inspector) |
| cancel cancellable → cancel + confirmation | exit 0; confirmation | `cmd/drover/cmd_mutate_test.go:80-92` | ✅ PASS |
| missing/ineligible → exit non-zero, DB unchanged (ineligible) | exit 1; Inspector asserts unchanged | `cmd/drover/cmd_mutate_test.go:48-68`, `95-102`; Inspector `inspector_test.go:356-368`, `449-456` | ✅ PASS |

### P1: `drover enqueue`

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| valid → insert + print id | exit 0; id printed; kind/args/queue | `cmd/drover/cmd_mutate_test.go:105-126` | ✅ PASS |
| missing/empty kind → non-zero, no insert | exit 2; `enqueued` false | `cmd/drover/cmd_mutate_test.go:154-164` | ✅ PASS |
| invalid `--args` JSON → non-zero, no insert | exit 2; not enqueued | `cmd/drover/cmd_mutate_test.go:167-180` | ✅ PASS |

### P2: GoReleaser + version

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| `.goreleaser.yaml` builds matrix + CGO=0 + ldflags | linux/darwin/windows × amd64/arm64; `CGO_ENABLED=0`; `-X main.version` | `.goreleaser.yaml:6-22` — present as specified | ✅ PASS |
| `version` / `--version` prints embedded (default `dev`) | prints `version` var | `cmd/drover/main_test.go:9-35`; `main.go:13` default `"dev"` | ✅ PASS |

**Status**: ❌ Gaps present — Inspector `Stats` oldest-claimable ages unasserted (AC CLI-02 / Inspector AC #2)

---

## Discrimination Sensor

Scratch: detached git worktree at `HEAD` (`/tmp/drover-sensor`, `/tmp/drover-sensor2`); mutations discarded after each run. Real working tree untouched.

| Mutation | File:line | Description | Killed? |
| -------- | --------- | ----------- | ------- |
| 1 | `internal/memdriver/memdriver.go` `cancellable` | Include `running` as cancellable | ✅ Killed (`OperatorCancel`/`InspectorCancel` running_refused) |
| 2 | `memdriver.RedriveDead` | Skip `Attempt = 0` | ✅ Killed |
| 3 | `memdriver.RedriveDead` | Remove dead-only guard | ✅ Killed |
| 4 | `memdriver.ListJobs` | Sort id ascending | ✅ Killed |
| 5 | `memdriver.OperatorCancel` | Skip `FinalizedAt` | ✅ Killed |
| 6 | `inspector.go` | `defaultListLimit = 100000` | ✅ Killed |
| 7 | `inspector.go` `Enqueue` | Skip `json.Valid` | ✅ Killed |
| 8 | `inspector.go` `Stats` | Drop `Oldest` age mapping (nil Oldest) | ❌ Survived — `TestInspectorStats*` and CLI fake stats tests still PASS |

**Sensor depth**: expanded (operator state transitions; ≥5 mutations + ages probe)
**Result**: 7/8 killed — FAIL ❌ (surviving mutant on oldest-claimable ages)

---

## Interactive UAT Results

Not performed — operator CLI / library API; automated checks sufficient per validate.md.

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| Minimum code | ✅ |
| Surgical changes | ✅ |
| No scope creep | ✅ (no web/SPA/discard/batch) |
| Matches patterns | ✅ (stdlib flag, memdriver seam, driver interface) |
| Spec-anchored outcome check | ❌ ages portion of Stats AC |
| Per-layer Coverage Expectation | ❌ Inspector Stats ages missing; cancel/redrive/list otherwise strong (memdriver + pgdriver + Inspector) |
| Every test maps to spec AC / edge / Done-when | ✅ |
| Documented guidelines followed | ✅ `CLAUDE.md` (table-driven, `-race`, memdriver unit, testcontainers integration) |

---

## Edge Cases

- [x] Zero-match list → empty table / `[]` exit 0 (`cmd_stats_jobs_test.go:145-170`)
- [x] Cancel/retry vs running claim → refusal; worker claim intact (running refused + state-conditioned SQL; `TestOperatorCancelRaceReportsInvalidTransition` covers post-transition refusal)
- [x] Omit `--args` → `{}` (`cmd_mutate_test.go:129-140`)
- [x] `--limit` ≤0 → default 100 (`inspector_test.go:156-171`; CLI passes 0 through)
- [x] Unknown subcommand → usage + exit 2 (`main_test.go:38-50`)

---

## Gate Check

- **Gate command (Build)**: `go build ./... && go vet ./...` → **PASS**
- **Quick**: `go test -race ./...` → **PASS** (246 verbose PASS lines; packages ok)
- **Full**: `go test -race -tags=integration ./...` → **PASS** (Docker; pgdriver operator tests PASS including List/Get/Cancel/Redrive)
- **Test count before feature**: baseline on `main` had no Cycle F CLI/Inspector tests (0 `inspector_test.go`)
- **Test count after feature**: +45 `func Test` lines across new/changed `*_test.go` in diff; unit suite 246 PASS under `-race -v`
- **Skipped tests**: none observed
- **Failures**: none on gates

---

## Fix Plans

### Fix 1: Assert Inspector Stats oldest-claimable ages

- **Root cause**: `TestInspectorStatsMatchesPublishedStates` only checks published-state depths; `Inspector.Stats` copies `Oldest` with no test. Mutating away the Oldest mapping leaves the suite green.
- **Fix task**: Extend Inspector Stats unit test (seed claimable backlog with known age, or at least assert `len(Oldest) ≥ 1` / matching queue when claimable work exists). Prefer asserting `AgeSeconds` against a seeded past `scheduled_at` via memdriver.
- **Verify**: Re-run discrimination — drop Oldest mapping → test FAIL; `go test -race . -run TestInspectorStats`
- **Done when**: Inspector AC #2 ages outcome has `file:line` evidence; mutant M8 killed
- **Priority**: Major (spec AC gap + surviving mutant on correctness-adjacent observability path)

---

## Requirement Traceability Update

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| CLI-01 | Done | ✅ Verified |
| CLI-02 | Done | ❌ Needs Fix (ages evidence) |
| CLI-03 | Done | ✅ Verified |
| CLI-04 | Done | ✅ Verified |
| CLI-05 | Done | ✅ Verified |
| CLI-06 | Done | ✅ Verified |
| CLI-07 | Done | ✅ Verified |
| CLI-08 | Done | ✅ Verified |
| CLI-09 | Done | ✅ Verified |
| CLI-10 | Done | ✅ Verified |
| CLI-11 | Done | ✅ Verified |

---

## Summary

**Overall**: ❌ Not Ready

**Spec-anchored check**: 27/28 ACs matched; 1 GAP (Inspector Stats ages)
**Sensor**: 7/8 mutations killed (1 survived: drop Oldest mapping)
**Gate**: build + unit `-race` + integration `-race` all green

**What works**: Inspector cancel/redrive/list/get/enqueue transitions; CLI wiring exit codes/JSON; memdriver + pgdriver operator paths; GoReleaser + version; edge cases for empty list, bad JSON, default limit, unknown command.

**Issues found**: Inspector `Stats` oldest-claimable ages untested; ages-mapping mutant survives.

**Next steps**: Apply Fix 1; re-verify (max 3 fix→re-verify iterations).
