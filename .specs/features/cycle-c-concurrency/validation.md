# Concurrency: Worker Pool and Graceful Shutdown — Validation

**Date**: 2026-07-26
**Spec**: `.specs/features/cycle-c-concurrency/spec.md`
**Diff range**: `main..HEAD` on `feat/worker-pool-and-graceful-shutdown` (12 commits, `4772040`..`5204dc7`)
**Verifier**: independent sub-agent (author ≠ verifier), read-only over the implementation
**Iteration**: 2 (re-verification after fix commit `5204dc7`)

**Verdict**: ❌ **FAIL** — one surviving mutant on an explicit P1 acceptance criterion.

Three of the first pass's four gaps are genuinely closed with discriminating sensors. The fourth —
P1-Story2 AC6, claimed-but-undispatched jobs going back to the queue — is now **half** closed: the
hand-back routine is proven, but nothing proves the fetch loop ever calls it. Deleting the call
site passes both the unit and the integration suite.

---

## What changed since iteration 1

`5204dc7` added four tests to `pool_test.go` and one to `loop_test.go`, deleted one shallow test,
and amended `spec.md` (P1-Story3 gains AC7 and AC8; SAFE-03 rewritten to cover them).

| First-pass finding | Status now |
| --- | --- |
| Fix 1 — `runner.abandon` 0% covered (M11) | ⚠️ **Partially closed** — `abandon`'s body is proven; its *call site* is not. See Finding 1. |
| Fix 2 — lost-lease takeover branch untested (M13) | ✅ Closed — `loop_test.go:316` |
| Fix 3 — `Stop` during a poll-interval sleep (M9) | ✅ Closed — `pool_test.go:676` |
| Fix 4 — requeue failure at shutdown not sensed (M14) | ✅ Closed — `pool_test.go:628` |
| Fix 5a — `TestExhaustingTheBudgetCancelsTheRunningHandlers` shallow (M6) | ✅ Closed — test deleted; criterion still covered, see below |
| Fix 5b — AC3 "before it begins waiting" not sensed | ✅ Closed — `pool_test.go:702` |
| Fix 6 — `TestALongRunningJobCanStillRecordItsOutcome` unclaimed | ✅ Closed — promoted to spec AC7 |
| Fix 6 — `Concurrency = 1` behavioural sensor; P2 AC2/AC4 by inspection | ⚠️ Still open (cosmetic, within the spec's own stated bar) |

**Deleted-test check**: `TestExhaustingTheBudgetCancelsTheRunningHandlers` was introduced on this
branch and removed on this branch — `git diff main...HEAD -- '*_test.go' | grep '^-func Test'` is
empty, so no test that existed on `main` was deleted. Its criterion (escalation cancels running
handlers) is still covered, and covered *more strongly*, by
`TestEscalationCancelsHandlersBeforeReturningTheirJobs` (`pool_test.go:776`): its handler blocks on
`<-ctx.Done()` and `pool_test.go:812` asserts `ordered.cancelFirst.Load()` — the cancel must land
**before** the requeue write, which the deleted test could not distinguish (it passed under mutant
M6 because `drain`'s `defer r.cancelJobs()` fires before `Stop` returns either way). No test was
skipped and no assertion was loosened.

---

## Judgement on the new sensors

### `TestClaimsNeverHandedToAWorkerGoBackToTheQueue` / `TestOneFailedHandBackDoesNotStopTheRest`

Both call `runner.abandon` directly on a hand-built runner rather than driving it through a running
pool. Judgement:

- `TestOneFailedHandBackDoesNotStopTheRest` — **legitimate**. Its subject is `requeueAll`'s
  continue-on-failure loop (`pool.go:264-271`), which *is* on the live escalation path
  (`escalate` → `requeueAll`). Calling it via `abandon` is a convenience, not a substitute; M14 is
  killed and the behaviour is real.
- `TestClaimsNeverHandedToAWorkerGoBackToTheQueue` — **strictly weaker than AC6**. It proves: given
  a slice of rows, `abandon` requeues them without decrementing `attempt`, drops them from the
  in-flight set, and returns their slots. It does **not** prove the sentence AC6 actually states —
  that *the fetch loop*, on the `case <-r.stopFetch` arm of its hand-off select (`pool.go:370-375`),
  passes its undispatched remainder to `abandon`. The test hand-rolls a model of that caller
  (`<-r.slots` + `c.inflight.add` per row) and then bypasses it, so the wiring, the `rows[i:]`
  slicing, and the caller's bookkeeping are all outside the sensor.

  Confirmed empirically: mutant **M16** below — replace the arm with a bare `return` — passes
  `go test -race ./...` and `go test -race -tags=integration ./...`.

### `TestStopDoesNotWaitOutThePollInterval` / `TestClaimingStopsBeforeTheDrainBeginsWaiting`

Both wall-clock; both judged **reliable and discriminating**, and empirically stable at
`-count=10` under `-race` (20/20 passes, no flakes; observed 0.05 s and 0.60 s per iteration).

- `TestStopDoesNotWaitOutThePollInterval` (`pool_test.go:676`): bound is 1 s against a 3 s poll
  interval, and the observed value is 0.05 s — a 20× margin on the pass side and a 3× margin on the
  discriminating side. Under mutant M9 (`sleep` ignores `stopFetch`) `Stop` must take ~3 s, so the
  1 s bound separates the two cleanly. Not a lower-bound-only assertion.
- `TestClaimingStopsBeforeTheDrainBeginsWaiting` (`pool_test.go:702`): samples `fetches` at ~150 ms
  and ~400 ms into a 600 ms `Stop` budget, both while a blocked handler holds the drain open, and
  requires the two samples to be **equal**. At a 1 ms poll interval a loop still claiming would add
  hundreds of fetches between the samples, so the test discriminates "stopped claiming before it
  began waiting" from the weaker "stopped claiming by the time `Stop` returned" — which is exactly
  the distinction AC3 draws. Its only timing dependency is that the `go func(){ stopped <- c.Stop(budget) }()`
  goroutine gets scheduled within 150 ms, which is generous even under `-race` on a loaded machine.

### `TestALostLeaseIsReportedAsATakeoverNotAFailure`

**Genuine and two-sided.** `loop_test.go:336` asserts the takeover *is* reported
(`!strings.Contains(out, "taken over by another worker")` → error) and `loop_test.go:339` asserts
the generic failure line is *absent* (`strings.Contains(out, `msg="drover: finalize job"`)` →
error). Driven through `leaseLostDriver`, whose `MarkCompleted` returns `driver.ErrLeaseLost`, so
mutant M13 (log the lost lease as an ordinary `ERROR` finalize failure) fails both assertions.

---

## Spec-Anchored Acceptance Criteria

Only rows that changed since iteration 1 carry new evidence; unchanged rows are re-stated with the
same evidence.

### P1-Story1: Jobs execute concurrently

| Criterion | `file:line` + assertion | Result |
| --- | --- | --- |
| AC1 n handlers executing simultaneously | `pool_test.go:91-97` — drains `concurrency` (=4) values from `entered` before `close(release)`; `t.Fatalf("only %d of %d handlers were running at once", …)` | ✅ PASS |
| AC2 `Concurrency ≤ 0` → default 10 | `client_test.go:168` — `if c.concurrency != tt.want` over `{0→10, -4→10, 1→1, 64→64}` | ✅ PASS |
| AC3 One fetch round claims ≤ idle-worker count | `pool_test.go:140` — `if running := countMemRows(t, mem, ids, "running"); running != concurrency` | ✅ PASS |
| AC4 Executed exactly once by exactly one worker | `pool_test.go:185` — `if runs[id] != 1`; `e2e_integration_test.go:506` — same, `Concurrency: 8`, 60 jobs on Postgres | ✅ PASS |
| AC5 Panic isolated; job retryable with stack | `pool_test.go:233-239`; `loop_test.go:497` state `retryable`; `:502` error contains `"kaboom"`; `:505` `Trace != ""` | ✅ PASS |
| AC6 Idle → poll interval and no faster, bounded both sides | `loop_test.go:849` `fetches < 2`; `loop_test.go:854` `fetches > 20` | ✅ PASS |
| AC7 One blocking handler → others stay free | `pool_test.go:233-239` | ✅ PASS |

### P1-Story2: Shutdown is ordered, bounded and honest

| Criterion | `file:line` + assertion | Result |
| --- | --- | --- |
| AC1 `Start` non-blocking, returns nil | `pool_test.go:87` + assertions running while 4 handlers block | ✅ PASS |
| AC2 Second `Start` → error, no second pool | `pool_test.go:257` — `!errors.Is(err, ErrAlreadyStarted)` | ✅ PASS |
| AC3 Cease claiming **before** it begins waiting | **NEW** `pool_test.go:739` — two `counting.fetches.Load()` samples taken 250 ms apart *while the drain is still waiting* must be equal, at a 1 ms poll interval | ✅ PASS (iteration-1 precision gap closed) |
| AC4 Drains inside budget → nil, every job terminal | `pool_test.go:177`; `e2e_integration_test.go:537` — `if running := countInState(t, pool, "running"); running != 0` | ✅ PASS |
| AC5 Budget expires → cancel, requeue, error names the count | `pool_test.go:463` `ErrDrainIncomplete`; `:466` `"2 job(s)"`; `:481` `row.State == "retryable"` | ✅ PASS |
| AC6 Jobs claimed but **not yet handed to a worker** → returned to the queue | ⚠️ **Partial** — `pool_test.go:596` `row.State != "retryable"`, `:599` `row.Attempt != 1`, `:606` `free slots` — but all via a **direct call** to `runner.abandon`. The fetch loop's `case <-r.stopFetch: r.abandon(rows[i:])` (`pool.go:372-373`) has no sensor; mutant **M16** deleting it survives unit **and** integration. | ❌ **GAP** |
| AC7 Requeue claimable now, attempt not decremented, reason recorded | `pool_test.go:483-486`; `:535` `requeued.Attempt != claimed.Attempt`; `:545` error contains `"shut down"`; `e2e_integration_test.go:608` `attempt != 1` | ✅ PASS |
| AC8 `Stop` before `Start` → error immediately | `pool_test.go:270` — `!errors.Is(err, ErrNotStarted)` | ✅ PASS |
| AC9 Second `Stop` → same verdict, no block/panic | `pool_test.go:294` `!errors.Is(second, first)`; `:297` 2 s bound | ✅ PASS |
| AC10 Cancelling `Start`'s ctx → shutdown begins unaided | `pool_test.go:362` `case <-c.runner.done:`; `heartbeat_test.go:287-301` | ✅ PASS |

### P1-Story3: Reliability guarantees survive the pool

| Criterion | `file:line` + assertion | Result |
| --- | --- | --- |
| AC1 Each tick renews all n leases in one call | `pool_test.go:832` — `counting.largestBatch() == concurrency` (4) | ✅ PASS |
| AC2 Heartbeat renews during drain, stops only after | `heartbeat_test.go:289` `current.LeasedUntil.After(leaseAtCancel)`; `:295` state `running`; `:298` lease not lapsed | ✅ PASS |
| AC3 Lost lease → refused, logged as takeover not failure | **NEW** `loop_test.go:336` `!strings.Contains(out, "taken over by another worker")`; `loop_test.go:339` `strings.Contains(out, `msg="drover: finalize job"`)` | ✅ PASS (M13 killed) |
| AC4 Cancelled job ctx → outcome still recorded, not on the cancelled ctx | `loop_test.go:248` `!<-sawCancellation`; `:255` `stored.State != tt.wantState`, driven through the context-honouring `ctxDriver` | ✅ PASS |
| AC5 No goroutine leaks across `Start`/`Stop` | `defer goleak.VerifyNone(t, goleak.IgnoreCurrent())` on 17 lifecycle tests incl. `pool_test.go:677,703` | ✅ PASS |
| AC6 Concurrent claims race → no double execution | `e2e_integration_test.go:506` (Postgres, `SKIP LOCKED`, 8×60); `pool_test.go:185` | ✅ PASS |
| **AC7 (new)** Job outliving its lease can still record its outcome; deadline starts at the write | `loop_test.go:298` — `if stored.State != "completed"` with `LeaseDuration: 10ms` and a 40 ms handler, through `ctxDriver` (which honours context, so it discriminates). Implementation: `loop.go:142-144` `context.WithTimeout(context.WithoutCancel(jobCtx), c.leaseDuration)` | ✅ PASS |
| **AC8 (new)** Lost lease reported as takeover rather than failed write | `loop_test.go:336` and `loop_test.go:339` (both directions asserted — see above) | ✅ PASS |

### P2-Story4: A runnable example program

| Criterion | Evidence | Result |
| --- | --- | --- |
| AC1 Compiles and vets | Gate — exit 0 | ✅ PASS |
| AC2 Migrates, registers, enqueues, pool > 1 | `examples/email/main.go:88,93,103,35` — inspection only | ⚠️ Within the spec's own Independent Test bar |
| AC3 Simulated failure → ordinary retry, visible in output | `examples/email/delivery_test.go:103` — `if (err != nil) != tt.wantErr`; `main.go:57,142` | ✅ PASS |
| AC4 SIGINT → bounded `Stop`, reports what drained | `examples/email/main.go:112-124` — inspection only | ⚠️ Within the spec's stated bar |
| AC5 No new module dependency | `git diff main...HEAD -- go.mod go.sum` empty | ✅ PASS |

**Status**: 33/34 ACs matched their spec-defined outcome. 1 hard gap (P1-Story2 AC6), 2
inspection-only P2 criteria within the spec's own stated bar.

---

## Edge Cases

| Edge case | Evidence | Result |
| --- | --- | --- |
| `Concurrency = 1` → one job in flight | `client_test.go:158` config-level only; no behavioural at-most-one-in-flight assertion | ⚠️ Partial (carried forward, cosmetic) |
| `Stop` while the fetch loop sleeps a poll interval | **NEW** `pool_test.go:695` — `if elapsed := time.Since(start); elapsed > time.Second` against a 3 s `PollInterval` | ✅ PASS (M9 killed) |
| Fetch error → log, back off, continue | `loop_test.go:872,876` | ✅ PASS (M15 killed) |
| Requeue failure at shutdown → logged, shutdown continues | **NEW** `pool_test.go:666` `stranded != 1`; `pool_test.go:669` `returned != 2`, via `failFirstRetryableDriver` | ✅ PASS (M14 killed) |
| Late handler return after re-claim → write refused by the fence | **NEW** `loop_test.go:336,339` | ✅ PASS |
| `Concurrency` exceeds due jobs → no per-worker fetch | `pool_test.go:140` + `loop_test.go:849/854` | ✅ PASS |
| `Stop`'s ctx already done → still stops fetching and requeues | `pool_test.go:764,769` | ✅ PASS |

---

## Discrimination Sensor

Mutations M1–M15 were run in iteration 1 and re-confirmed after `5204dc7` by the interrupted
iteration-2 pass; they are recorded here as established. M16 is new to this pass.

All mutation work was done in a throwaway `git archive` extraction outside the repo, deleted
afterwards. The working tree was never modified: `git diff -- '*.go'` and `git status --short` are
both empty, verified before and after.
Command per mutant: `go test -race -count=1 -timeout 300s .`; survivors re-run with `-tags=integration`.

| # | File:line | Mutation | Killed? | Killed by |
| - | --------- | -------- | ------- | --------- |
| M1 | `pool.go:76` | pool size forced to 1 | ✅ Killed | `TestPoolRunsJobsConcurrently` +4 |
| M2 | `pool.go:383-385` | worker returns its slot before running the job | ✅ Killed | `TestPoolClaimsNoMoreJobsThanItCanRun`, `TestSurplusClaimedJobsAreReturnedImmediately` |
| M3 | `pool.go:175-189` | heartbeat stopped before the drain wait | ✅ Killed | `TestHeartbeatOutlivesCancellationUntilTheDrainFinishes` |
| M4 | `pool.go:312` | `MarkRetryable` → `MarkSnoozed` (gives the attempt back) | ✅ Killed | `TestShutdownRequeueDoesNotGiveBackTheAttempt` +3 |
| M5 | `pool.go:221-222` | `escalate` returns `nil` | ✅ Killed | 6 tests |
| M6 | `pool.go:214` | `escalate` no longer calls `r.cancelJobs()` | ✅ Killed | `TestEscalationCancelsHandlersBeforeReturningTheirJobs` (the sole shallow test that also "covered" it was deleted in `5204dc7`) |
| M7 | `loop.go:143` | drop `context.WithoutCancel` in `finalizeContext` | ✅ Killed | `TestJobOutcomeIsRecordedEvenWhenItsContextWasCancelled` |
| M8 | `pool.go:350` | drop the surplus-row `requeueAll` | ✅ Killed | `TestSurplusClaimedJobsAreReturnedImmediately` |
| M9 | `pool.go:433-438` | `sleep()` ignores `stopFetch` | ✅ **Killed (was survivor)** | `TestStopDoesNotWaitOutThePollInterval` |
| M10 | `heartbeat.go:38` | renew only the first in-flight lease | ✅ Killed | `TestHeartbeatRenewsEveryWorkersLease` |
| M11 | `pool.go:234` | `abandon` no longer requeues | ✅ **Killed (was survivor)** | `TestClaimsNeverHandedToAWorkerGoBackToTheQueue` |
| M12 | `client.go:27` | `defaultConcurrency` 10 → 1 | ✅ Killed | `TestConfigZeroValuesGetDefaults`, `TestConcurrencyConfigFallsBackToTheDefault` |
| M13 | `loop.go:247-250` | lost lease logged as an ordinary `ERROR` finalize failure | ✅ **Killed (was survivor)** | `TestALostLeaseIsReportedAsATakeoverNotAFailure` |
| M14 | `pool.go:266-268` | failed requeue `return`s instead of `continue` | ✅ **Killed (was survivor)** | `TestOneFailedHandBackDoesNotStopTheRest` |
| M15 | `pool.go:334-338` | fetch error stops the pool | ✅ Killed | `TestStartLogsAndRetriesAfterFetchErrors` |
| **M16** | `pool.go:372-373` | fetch loop's `case <-r.stopFetch:` arm returns **without** calling `r.abandon(rows[i:])` | ❌ **Survived** | — (also survived `-tags=integration`, 12.6 s, 92 tests) |

**Sensor depth**: P0-full (16 mutations — data-integrity / at-least-once critical path).
**Result**: 15/16 killed, **1 survived** — ❌ FAIL.

---

## Gate Check

| Gate | Command | Result |
| --- | --- | --- |
| Build + vet | `go build ./... && go vet ./...` | ✅ exit 0 |
| Quick | `go test -race -count=1 ./...` | ✅ exit 0 — root 3.1 s, all packages `ok` |
| Full | `go test -race -count=1 -tags=integration ./...` | ✅ exit 0 — root 13.3 s, `pgdriver` 7.1 s, `migrate` 5.2 s, `memdriver` 1.0 s, `examples/email` 1.0 s |
| Lint | `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run` | ✅ exit 0 — **0 issues** |
| Flake probe | `go test -race -count=10 -run 'TestClaimingStopsBeforeTheDrainBeginsWaiting\|TestStopDoesNotWaitOutThePollInterval' .` | ✅ 20/20 PASS, 7.6 s total |

**Test counts** (top-level `--- PASS`, `go test -v`):

- Root unit: **81** (iteration 1: 77; baseline `main`: 58) — +4 net (+5 added, −1 deleted-on-branch). ✅
- Root with `-tags=integration`: **92**.
- **No test deleted relative to `main`, none skipped, no assertion loosened.** ✅

---

## Code Quality

| Principle | Status |
| --- | --- |
| Minimum code — no features beyond what was asked | ✅ |
| No abstractions for single-use code | ✅ |
| Only touched files required for the task | ✅ (`5204dc7` touches only test files + spec/lessons) |
| Matches existing patterns/style | ✅ table-driven where applicable, `t.Parallel` where safe, `goleak` on lifecycle tests |
| Tests map to ACs and are non-shallow | ⚠️ One structural weakness: `TestClaimsNeverHandedToAWorkerGoBackToTheQueue` unit-tests `abandon` against a hand-written model of its caller instead of the caller itself |
| Would a senior engineer approve? | ✅ The reasoning comments on the new tests are honest — the author explicitly flags that the hand-back is exercised directly because the path is hard to provoke. That candour is right; the conclusion (leave the call site unsensed) is what this report disputes. |
| Every test maps to a spec requirement | ✅ `TestALongRunningJobCanStillRecordItsOutcome` is now claimed by AC7; no unclaimed tests remain |

---

## Requirement Traceability Update

| Requirement | Iteration 1 | Now |
| --- | --- | --- |
| POOL-01 … POOL-05 | ✅ Verified | ✅ Verified |
| SHUT-01 | ✅ Verified | ✅ Verified |
| SHUT-02 | ⚠️ Partial | ✅ Verified — AC3 ordering now sensed; poll-interval edge closed (M9) |
| SHUT-03 | ✅ Verified | ✅ Verified |
| SHUT-04 | ⚠️ Partial | ✅ Verified — requeue-failure edge closed (M14) |
| SHUT-05 | ❌ Needs Fix | ⚠️ **Partial** — `abandon` proven (M11 killed); its invocation from the fetch loop unproven (M16) |
| SAFE-01, SAFE-02, SAFE-04 | ✅ Verified | ✅ Verified |
| SAFE-03 | ❌ Needs Fix | ✅ Verified — AC3/AC4/AC7/AC8 all sensed (M7, M13 killed) |
| EX-01 | ⚠️ Partial | ⚠️ Partial — AC2/AC4 by inspection only, within the spec's stated bar |

---

## Findings

### Finding 1 — the fetch loop's hand-back is unsensed (P1-Story2 AC6 / SHUT-05) — **Major**

- **What is unproven**: that the fetch loop, on shutdown, gives its undispatched rows to `abandon`.
  `pool.go:370-375`'s `case <-r.stopFetch: r.abandon(rows[i:]); return` can be reduced to a bare
  `return` and the whole suite — 81 unit tests and 92 with `-tags=integration`, `-race` — still
  passes (mutant M16). Also unsensed by extension: the `rows[i:]` slice boundary (an off-by-one
  would strand the row currently being handed off, or double-requeue one already taken by a worker)
  and the caller's `inflight.add`-before-hand-off bookkeeping that `abandon` unwinds.
- **What *is* proven**: `abandon`'s own contract — requeue at `now`, `attempt` untouched, in-flight
  entries removed, one slot returned per row (`pool_test.go:596,599,602,606`).
- **Why it matters**: the failure mode is precisely the one the slot accounting exists to prevent —
  a row `running` and leased with nothing executing it, untouchable until the lease lapses. The
  behaviour is correct in the code today; what is missing is the regression guard.
- **Suggested sensor (deterministic, no race needed)**: the runner does not have to be `Start`ed to
  exercise `fetch`. Construct a runner with `newRunner`, do **not** launch workers, run `r.fetch()`
  in a goroutine against a driver that returns ≥ 1 row, wait for the row to be claimed
  (`running` in the driver), then `close(r.stopFetch)` and assert the row went back to `retryable`
  with `attempt` unchanged. With no worker receiving on `r.jobs`, the hand-off select is guaranteed
  to take the `stopFetch` arm — no timing race, and it exercises the real loop, the real slice
  bound, and the real bookkeeping.
- **Verify**: mutant M16 must fail the suite.

### Finding 2 — `Concurrency = 1` has no behavioural sensor — **Cosmetic** (carried forward)

`client_test.go:158` asserts only that `Concurrency: 1` is not mistaken for unset. Several tests run
with `concurrency = 1` but none asserts at-most-one-handler-in-flight. Either add that assertion or
amend the spec to state the config-level check is the intended bar.

### Finding 3 — P2-Story4 AC2/AC4 satisfied by inspection only — **Cosmetic** (carried forward)

`examples/email/main.go:88,93,103,112-124` are read, not asserted. The spec's own Independent Test
asks only for `go vet ./examples/...` plus a stub unit test, so this is within the stated bar —
accept it explicitly in the spec or add a testcontainer-driven example run.

---

## Summary

**Overall**: ❌ FAIL — one Major finding, two cosmetic carry-forwards.

**Spec-anchored check**: 33/34 ACs matched their spec-defined outcome (1 hard gap). Edge cases: 6/7
clean, 1 partial.
**Sensor**: 15/16 mutations killed, 1 survived (M16).
**Gate**: build ✅, vet ✅, `-race` ✅, `-race -tags=integration` ✅, golangci-lint ✅ (0 issues) —
all exit 0, 0 failures, 0 skips.

**The fix commit is substantially good work.** Four survivors became four kills with sensors that
genuinely discriminate: the takeover log is asserted in both directions, the poll-interval bound has
a 20× margin and is stable at `-count=10` under `-race`, the requeue-failure test uses a purpose-built
refusing driver, and the AC3 ordering test distinguishes "stopped before waiting" from "stopped by
the time it returned" — the exact distinction the first pass said was missing. Deleting the shallow
budget-cancellation test was the right call; its criterion survives in a stronger test.

The one thing that did not land is the hardest one, and the author's own test comment says why: the
path takes a race the design makes rare. But it does not require a race to test — driving `r.fetch()`
with no workers attached reaches the branch deterministically. Until that exists, the sentence
P1-Story2 AC6 actually asserts is unproven.

**Next step**: one fix task for Finding 1, then re-verify (iteration 3 of max 3). Findings 2 and 3
are spec amendments, not code.
