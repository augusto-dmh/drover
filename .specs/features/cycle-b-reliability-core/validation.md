# Cycle B — Reliability Core Validation

**Date**: 2026-07-25
**Spec**: `.specs/features/cycle-b-reliability-core/spec.md`
**Diff range**: `7a30297..HEAD` (feature commits `e813748..HEAD`) on `feat/retries-leases-and-rescue`
**Verifier**: independent sub-agent (author ≠ verifier), re-derived from spec + code

**Verdict (iteration 1)**: ❌ **FAIL** — 1 surviving mutant (RESCUE-01). All gates green; all other checks pass.

**Verdict (iteration 2, after Fix 1)**: ✅ **PASS** — 17/17 mutations killed, 38/38 criteria covered.
See "Iteration 2" at the end of this report.

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1–T9 (4 phases) | ✅ Done | 9 feature commits, one per task, Conventional Commits, no internal artifact names in messages |

---

## Spec-Anchored Acceptance Criteria

### P1: Transient failures retry instead of dying

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 error, attempt < max ⇒ `retryable` | state `retryable`, `finalized_at` unset, `scheduled_at` = policy time | `loop_test.go:221` state; `:225` FinalizedAt nil; `:557` `stored.ScheduledAt.Equal(want)` vs `atTimePolicy` | ✅ PASS |
| AC2 exactly one error entry (attempt, time, message) | `len(errors)==1`, `Attempt:1`, `Error:"boom"` | `loop_test.go:242` — `len(recorded) != 1 \|\| recorded[0].Error != "boom" \|\| recorded[0].Attempt != 1` | ✅ PASS |
| AC3 attempt at max ⇒ `dead` finalized + final error | state `dead`, `finalized_at` set, one entry per attempt | `loop_test.go:309,313,316,320` — attempt 3, FinalizedAt non-nil, 3 entries | ✅ PASS |
| AC4 `retryable` due ⇒ claimed | claimed like `available`, attempt+1, future lease | `memdriver_test.go:379,403-415`; `pgdriver_integration_test.go:438,462-474` | ✅ PASS |
| AC5 `retryable` not due ⇒ not claimed | 0 rows claimed | `memdriver_test.go:382,397`; `pgdriver_integration_test.go:441,456` | ✅ PASS |
| AC6 panic ⇒ retryable + stack trace | one entry naming the panic, `Trace` non-empty | `loop_test.go:378,381` | ✅ PASS |
| AC7 unregistered kind ⇒ retryable | state `retryable`, entry names the kind | `loop_test.go:397,401` | ✅ PASS |
| AC8 retry then success ⇒ `completed`, errors preserved | attempt 3, two entries numbered 1,2 | `loop_test.go:279,283,286-289` | ✅ PASS |

### P1: The default backoff is exponential with jitter

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 delay in `[0.9·N⁴, 1.1·N⁴]` s | both bounds, attempts 1/2/3/5/25, 200 samples each | `retry_test.go:68,71` — clock-bracketed `over`/`under` compared to each bound | ✅ PASS |
| AC2 varies between calls | not constant | `retry_test.go:96` — spread ≥ 1s across 100 samples | ✅ PASS |
| AC3 strictly larger for N when M+1<N | max(M) < min(N) | `retry_test.go:122` over pairs {1,3},{2,5},{3,10},{5,25} | ✅ PASS |
| AC4 configured policy replaces default | scheduled at policy's answer, default not consulted | `retry_test.go:151,157` (`policy.calls != 1`); `loop_test.go:557` | ✅ PASS |
| AC5 nil policy ⇒ default | `ExponentialRetryPolicy` | `retry_test.go:191` | ✅ PASS |
| AC6 past answer ⇒ claimable now | clamped to `now` | `retry_test.go:141,151,154` | ✅ PASS |

### P1: A crashed worker's job is rescued

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 expired lease ⇒ `retryable` at policy time | state `retryable`, unfinalized, **`scheduled_at` from the retry policy** | `rescue_test.go:67` state; `:71` FinalizedAt — **no assertion on `ScheduledAt`** | ❌ **GAP** (see Sensor M16) |
| AC2 at ceiling ⇒ `dead` finalized | state `dead`, `finalized_at` set | `rescue_test.go:101,104` | ✅ PASS |
| AC3 exactly one lease-expiry entry | `len(errors)==1`, message names expired lease | `rescue_test.go:80,83` | ✅ PASS |
| AC4 rescue does not change `attempt` | attempt stays 1 | `rescue_test.go:75` | ✅ PASS |
| AC5 live lease ⇒ untouched | 0 rescued, still `running`, no errors | `rescue_test.go:129,134,137` | ✅ PASS |
| AC6 sweep every `RescueInterval`, survives errors | exactly 3 sweeps in 35s at 10s (synctest) | `rescue_test.go:241` — `broken.calls.Load() != 3` | ✅ PASS |
| AC7 concurrent sweeps ⇒ exactly once | each of 25 jobs `retryable`, attempt 1, exactly 1 error | `rescue_test.go:186,189,194` | ✅ PASS |

### P1: A long-running job keeps its lease

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 extend every interval to lease beyond now | exactly 3 beats in 35s at 10s; each `until == beatTime + 30s`; ids `[7]` | `heartbeat_test.go:86,92,96,99` (synctest, exact times) | ✅ PASS |
| AC2 outlives lease ⇒ not rescued | still `running` after 5 lease durations, handler ran once | `rescue_test.go:282,287,294` | ✅ PASS |
| AC3 finish ⇒ stop extending | exactly 2 beats, none after removal | `heartbeat_test.go:126` | ✅ PASS |
| AC4 failed extension ⇒ log + continue | 3 beats despite failures, log record present | `heartbeat_test.go:170,174` | ✅ PASS |
| AC5 cancelled + draining ⇒ keep beating | lease advances after cancel; still `running` and leased 100ms later | `heartbeat_test.go:286,295,297` | ✅ PASS |
| AC6 loop returns ⇒ no goroutine left | `goleak.VerifyNone` on 19 lifecycle tests | `loop_test.go:180`, `heartbeat_test.go:211,257`, `rescue_test.go:252`, … | ✅ PASS |

### P2: Handlers classify their own outcome

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 `Cancel` ⇒ `cancelled` finalized, no retry | state `cancelled`, `finalized_at` set, handler ran once under immediate policy | `loop_test.go:453,457,460` | ✅ PASS |
| AC2 one entry with the reason | `len(errors)==1` carrying "bad input" | `loop_test.go:464` | ✅ PASS |
| AC3 `Snooze(d)` ⇒ `scheduled` at now+d, unfinalized | state `scheduled`, wait ≈ 1h, FinalizedAt nil | `loop_test.go:482,490,494` | ✅ PASS |
| AC4 attempt restored | attempt 0 after claim+snooze | `loop_test.go:486` | ✅ PASS |
| AC5 attempt 0 ⇒ floor at 0 | never negative | `memdriver_test.go:505`; `pgdriver_integration_test.go:561`; `loop_test.go:526` | ✅ PASS |
| AC6 snooze appends no error | `len(errors)==0` | `loop_test.go:497` | ✅ PASS |
| AC7 `scheduled` due ⇒ claimed | claimed on next fetch | `memdriver_test.go:380`; `pgdriver_integration_test.go:439` | ✅ PASS |
| AC8 sentinels through `%w` | both recognized when wrapped | `errors_test.go:36,46` (wrapped cases), `:76` | ✅ PASS |
| AC9 repeated snooze never ⇒ `dead` | 5 snoozes at ceiling 1 then completed, attempt 1 | `loop_test.go:522,529` | ✅ PASS |

**Status**: ⚠️ 37/38 criteria covered with spec-matching assertions; **1 gap** (RESCUE-01 `scheduled_at`).

---

## Discrimination Sensor

**Depth**: P0-full (data-integrity path) — 17 behaviour-level mutations, run against the affected package only, each reverted with `git checkout` before the next.

| # | File | Mutation | Killed? |
| --- | --- | --- | --- |
| **16** | `loop.go:194` | **Rescue path skips the retry policy: `at = time.Now()` when `jobErr == errLeaseExpired`** | ❌ **SURVIVED** — full unit **and** integration suites pass |
| 1 | `loop.go:185` | Ceiling `>=` → `>` (dead unreachable) | ✅ Killed (2 tests) |
| 2 | `loop.go:185` | Ceiling `>= max` → `>= max-1` (dead one attempt early) | ✅ Killed |
| 3 | `loop.go:37` | Heartbeat stops at ctx cancellation instead of after drain (AD-018) | ✅ Killed — `TestHeartbeatOutlivesCancellationUntilTheDrainFinishes` |
| 4 | `heartbeat.go:39` | `extendLeases` never renews (`len(ids) >= 0` early return) | ✅ Killed (6 tests, incl. `TestStartDoesNotRescueAJobThatIsStillRunning`) |
| 5 | `memdriver.go:110` | `FetchExpired` increments `attempt` (rescue consumes an attempt) | ✅ Killed (3 tests) |
| 6 | `memdriver.go:184` | `MarkSnoozed` does not give back the attempt | ✅ Killed (3 tests) |
| 7 | `memdriver.go:184` | `MarkSnoozed` decrements without the zero floor | ✅ Killed — `TestMarkSnoozedFloorsAttemptAtZero` |
| 8 | `memdriver.go:75` | Due-time gate widened by 24h (not-yet-due rows claimable) | ✅ Killed (4 root + 3 subtests) |
| 9 | `heartbeat.go:23` | Heartbeat ticker interval halved | ✅ Killed (3 synctest cadence tests) — **L-001 discharged** |
| 10 | `rescue.go:27` | Rescue ticker interval halved | ✅ Killed — `TestRescueLoopKeepsSweepingAfterAFailedSweep` |
| 11 | `memdriver.go:230` | Two error entries appended per attempt | ✅ Killed (14 tests) |
| 12 | `errors.go:79-83` | `errors.Is`/`errors.As` → `==` / type assertion (no unwrapping) | ✅ Killed (3 tests + 3 subtests) |
| 13 | `memdriver.go:181` | Snooze appends an error entry | ✅ Killed (3 tests) |
| 14 | `loop.go:48-49` | `background.Wait()` removed — goroutines outlive `Start` | ✅ Killed (19 goleak tests) |
| 15 | `retry.go:56-58` | Past-answer clamp removed in `retryAt` | ✅ Killed |
| 17 | `memdriver.go:230` | Zero error entries appended | ✅ Killed (8+ tests) |

**Result**: 16/17 killed, **1 survived** — ❌ FAIL

### Surviving mutant M16 — detail

`dispose` computes the retry time via `retryAt(c.logger, c.retryPolicy, …)` and is shared by the fetch
loop and the rescuer, which is exactly the design's stated guarantee. But **no test distinguishes
"rescue consulted the policy" from "rescue scheduled at `now`"**: every rescue test builds its client
with `immediatePolicy{}` (`rescue_test.go:46`), whose answer *is* `time.Now()`, and the Postgres e2e
rescue test does the same (`e2e_integration_test.go:310`). Replacing the policy call with
`time.Now()` on the lease-expiry path alone leaves both suites fully green.

The shipped code is **correct**; the gap is in the sensor, not the behaviour. The regression it fails
to catch is real, though: a rescued job would become immediately re-claimable with no backoff, so a
persistently failing job whose worker keeps dying would spin at fetch speed instead of backing off —
the same busy-loop failure the design's Risks table guards against on the fetch predicate, unguarded
here.

---

## Code Quality

| Principle | Status |
| --- | --- |
| Minimum code | ✅ |
| Surgical changes | ✅ — extends Cycle A; `runJob`/`runProtected`/`finalizeFailure` reused, not rewritten |
| No scope creep | ✅ — no pools, no `Stop`, no named queues, no metrics, no redrive (all in the Out of Scope table) |
| Matches patterns | ✅ — context-first, `%w` wrapping, package-prefixed sentinels, `*slog.Logger` via config |
| Spec-anchored outcome check | ⚠️ — 37/38; RESCUE-01 asserts state but not the policy-derived `scheduled_at` |
| Per-layer coverage (domain 1:1 ACs; drivers happy+edge+error) | ✅ — every transition covered on both drivers |
| Every test maps to a spec requirement — no unclaimed tests | ✅ |
| Documented guidelines followed | ✅ — `CLAUDE.md`: table-driven, `t.Parallel`, `-race`, testcontainers, `testing/synctest`, goleak |

---

## Edge Cases

- [x] Retry policy panics ⇒ recover, log, fall back to default — `retry_test.go:164-184` (bounds + log assertion)
- [x] `max_attempts` ≤ 0 ⇒ dead on first failure — `loop_test.go:328-351`
- [x] Empty sweep ⇒ no writes — `rescue_test.go:142-158` (`expiryCounter.marks == 0`)
- [x] Driver error finalizing ⇒ log + continue, job left for rescuer — `loop_test.go:562-598`
- [x] Zero/negative `LeaseDuration`/`HeartbeatInterval`/`RescueInterval` ⇒ documented defaults — `client_test.go:145-192`
- [x] `HeartbeatInterval` ≥ `LeaseDuration` ⇒ clamped below the lease — `client_test.go:173,180`
- [x] `Snooze` ≤ 0 ⇒ claimable now — `errors_test.go:87-119`
- [x] Stale heartbeat on a finalized row ⇒ no resurrection — `heartbeat_test.go:179-208`; `pgdriver_integration_test.go:689`

---

## Gate Check

| Gate | Command | Result |
| --- | --- | --- |
| Build + vet | `go build ./... && go vet ./...` | ✅ clean |
| Unit | `go test -race -count=1 ./...` | ✅ `ok drover 1.892s`, `ok internal/memdriver 1.016s` |
| Integration | `go test -race -count=1 -tags=integration ./...` | ✅ all 5 packages ok (`drover` 7.0s, `pgdriver` 6.5s, `migrate` 4.8s) |
| Lint | `golangci-lint v2 run` | ✅ **0 issues** |
| Docker-free unit suite | `DOCKER_HOST=unix:///nonexistent/… go test -race ./...` | ✅ passes — integration tests are behind the `integration` build tag |

- **Test count before feature** (at `7a30297`): **44**
- **Test count after feature**: **105** (delta **+61**) — author's claim independently verified via `git grep` at both revisions
- **Skipped tests**: none
- **Failures**: none
- **Environment**: Go 1.26.2, Docker running

---

## Independent Confirmations

- **No goroutine outlives `Start`** — `goleak.VerifyNone(t, goleak.IgnoreCurrent())` on 19 lifecycle tests; mutation M14 (dropping `background.Wait()`) is killed by all of them.
- **Unit suite runs without Docker** — verified by running with an unreachable `DOCKER_HOST`; `pgdriver`/`migrate`/`testdb` report "no test files" without the `integration` tag.
- **Documentation carries no stale claim** — `doc.go:19-53` documents retry-not-death, the lease/heartbeat/rescue loop, and both sentinels; `README.md:20-28` shows heartbeat and rescuer. Grep for "immediately"/"unenforced"/"first failure"/"placeholder" over `*.go` and non-`.specs` `*.md` found no surviving claim that a failure is immediately fatal or the lease unenforced. The only remaining occurrences are historical rows in `.specs/STATE.md` (AD-003, AD-005), both explicitly marked superseded/discharged.
- **L-001 audit (lower-bound-only timing assertions)** — every cadence assertion added by this feature is an **exact equality** under `testing/synctest` (`len(calls) != 3`, `!= 2`, `!= 0`, `broken.calls != 3`), plus exact per-beat timestamps at `heartbeat_test.go:92`. The one real-clock count assertion (`TestStartKeepsPollingWhileIdle`, `loop_test.go:661-670`) now states **both** bounds (≥2 and ≤20), fixing the Cycle A flaw. Mutations M9 and M10 confirm the interval assertions fail when the interval changes.

---

## Requirement Traceability Update

| Requirement | Previous | New |
| --- | --- | --- |
| RETRY-01 … RETRY-04 | Design | ✅ Verified |
| POLICY-01, POLICY-02 | Design | ✅ Verified |
| **RESCUE-01** | Design | ✅ Verified at iteration 2 (state/attempt at iteration 1; policy-derived `scheduled_at` after Fix 1) |
| RESCUE-02, RESCUE-03 | Design | ✅ Verified |
| LEASE-01, LEASE-02, LEASE-03 | Design | ✅ Verified |
| SENT-01, SENT-02, SENT-03 | Design | ✅ Verified |

---

## Fix Plans

### Fix 1: Rescue path has no evidence it consults the retry policy

- **Root cause**: `rescueClient` (`rescue_test.go:46`) and the Postgres e2e rescue test
  (`e2e_integration_test.go:310`) both configure `immediatePolicy{}`, whose answer equals
  `time.Now()`. Every rescue assertion therefore holds identically whether the policy is consulted or
  ignored. Spec P1-Story3 AC1 / RESCUE-01 requires `scheduled_at` **set by the retry policy**.
- **Fix task**: In `rescue_test.go`, add (or retune) one test that builds its client with
  `atTimePolicy{at: time.Now().Add(72 * time.Hour).Round(0)}` and, after `rescueOnce`, asserts
  `stored.ScheduledAt.Equal(want)` — mirroring `TestStartSchedulesRetryFromConfiguredPolicy`
  (`loop_test.go:537-560`) for the rescue path.
- **Verify**: re-run mutation M16 (`at = time.Now()` when `jobErr == errLeaseExpired` in
  `loop.go:194`) and confirm the new test fails.
- **Done when**: M16 is killed by the unit suite and `go test -race ./...` is green unmutated.
- **Priority**: **Major** — no defect in shipped behaviour; an undetectable-regression gap on a P1
  criterion.

---

## Summary

**Overall**: ⚠️ Issues — one Major coverage gap, everything else verified.

**Spec-anchored check**: 37/38 ACs matched the spec-defined outcome; 1 gap (RESCUE-01).
**Sensor**: 16/17 mutations killed, 1 survived.
**Gate**: build+vet clean, unit green, integration green, lint 0 issues, 105 tests (up from 44).

**What works**: The retry curve and its bounds; the attempt ceiling in both directions; the
rescue/heartbeat interaction including the drain path (AD-018); attempt arithmetic for snooze
(give-back and zero floor) and rescue (unchanged); the due-time claim gate on both drivers; exactly-one
error recording in both directions and none for a snooze; sentinel classification through `%w`;
goroutine lifetime; concurrent rescue exactly-once; every listed edge case; the Docker-free unit suite.

**Issues found**: Fix 1 — the rescue path's `scheduled_at` is not asserted against the retry policy,
so a rescue that dropped the backoff entirely would pass both suites.

**Next steps**: Apply Fix 1, then re-run the sensor's M16 to confirm the mutant is killed.

---

## Iteration 2 — Fix 1 applied

**Fix**: `rescue_test.go` gains `TestRescueSchedulesFromTheConfiguredRetryPolicy`, which builds its
client with `atTimePolicy{at: now + 72h}` and asserts `stored.ScheduledAt.Equal(want)` after
`rescueOnce`. The other rescue tests keep their immediate policy — the point of the new test is that
exactly one rescue assertion uses an answer that *cannot* be produced by a rescue which skipped the
policy.

**Behaviour change**: none. The shipped code already consulted the policy on the rescue path; only
the sensor was blind.

**Re-run of M16** (`at = time.Now()` when `jobErr == errLeaseExpired`, `loop.go:194`):

```
--- FAIL: TestRescueSchedulesFromTheConfiguredRetryPolicy
    rescue_test.go:111: ScheduledAt = …19:51:26 (today), want the retry policy's answer
                        …19:51:26 (+72h) — a rescued job must wait out a backoff,
                        not become claimable immediately
```

✅ **M16 killed.** Mutation reverted; `git diff loop.go` is empty.

**Gates after the fix**: build+vet clean · unit `ok drover 1.900s`, `ok internal/memdriver 1.018s` ·
integration all 4 packages ok · lint **0 issues** · **106 tests** (was 105).

**Updated results**: spec-anchored check **38/38**; sensor **17/17 killed**; RESCUE-01 → ✅ Verified.

**Lesson**: L-002 (recorded at iteration 1) stands — a fixture whose stub returns the same value the
un-implemented path would produce cannot distinguish "consulted" from "ignored". It is what this
iteration fixed, and it generalizes past this feature.
