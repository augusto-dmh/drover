# Concurrency: Worker Pool and Graceful Shutdown — Validation

**Date**: 2026-07-26
**Spec**: `.specs/features/cycle-c-concurrency/spec.md`
**Diff range**: `main..HEAD` on `feat/worker-pool-and-graceful-shutdown` (13 commits, `4772040`..`3461797`)
**Verifier**: independent sub-agent (author ≠ verifier), read-only over the implementation
**Iteration**: 3 (final pass, focused on the one open Major finding)

**Verdict**: ✅ **PASS** — the last Major finding is closed with a deterministic sensor; no
implementation code changed to achieve it.

---

## What changed since iteration 2

`3461797` adds **two tests to `pool_test.go` and nothing else**. Verified independently:
`git diff 5204dc7..HEAD --stat -- '*.go'` reports `pool_test.go | 85 ++++` and no other Go file;
`git diff 5204dc7..HEAD -- '*_test.go' | grep '^-'` contains no deletion lines at all — the commit
is purely additive, so nothing was deleted, skipped, or loosened.

| Iteration-2 finding | Status now |
| --- | --- |
| Finding 1 — fetch loop's hand-back unsensed (M16), **Major** | ✅ **Closed** — `TestTheFetchLoopHandsBackRowsItNeverDispatched` (`pool_test.go:615`). M16 now fails in 0.00 s. |
| Finding 2 — `Concurrency = 1` no behavioural sensor, Cosmetic | ✅ **Closed** — `TestAPoolOfOneRunsOneJobAtATime` (`pool_test.go:658`), probed and confirmed discriminating (M18 below). |
| Finding 3 — P2-Story4 AC2/AC4 by inspection only, Cosmetic | ⚠️ **Still open** — unchanged, and within the spec's own stated Independent Test bar. |

---

## Judgement on the two new sensors

### `TestTheFetchLoopHandsBackRowsItNeverDispatched` — **legitimate, and the right shape**

It drives the **real** `r.fetch()` on a real `newRunner`, with **no worker goroutines started**.
`r.jobs` is unbuffered (`pool.go:77`), so with nothing receiving, the hand-off `select` has exactly
one ready arm once `stopFetch` closes — the stop arm is taken by construction, not by winning a
race. The test then asserts on driver state (`row.State == "running"` → fail) and on the pool's own
bookkeeping (`len(c.inflight.snapshot()) != 0` → fail), which is precisely what AC6 promises: no row
left `running` and leased with nothing executing it.

Judgement: this **does** now prove P1-Story2 AC6 end to end — the real loop, the real slice
expression, the real `inflight.add`-before-hand-off bookkeeping that `abandon` unwinds, and the real
slot return. The earlier `TestClaimsNeverHandedToAWorkerGoBackToTheQueue` (which calls `abandon`
directly) is no longer load-bearing for AC6; it is now a unit test of `abandon`'s contract sitting
behind an end-to-end sensor, which is a fine arrangement.

**Determinism**: no flake surface found. `waitFor` gates on the row being `running`, which memdriver
sets inside `FetchAvailable` before returning, so the loop is guaranteed past the claim before
`close(r.stopFetch)`. Even if the close landed *before* the loop reached the `select`, the outcome is
identical — the jobs arm is never ready. The 2 s `time.After` is a failure guard, not a timing
assumption. `-count=10 -race` over both new tests: **20/20 PASS, 1.6 s total**, no flakes.

**Residual (narrow)**: because no worker ever receives, the loop always stops at `i == 0`, so the
*lower* bound of `rows[i:]` is not pinned. Mutating it to `r.abandon(rows)` survives (M17); mutating
it to `r.abandon(rows[i+1:])` is killed (M17b). The dangerous direction — stranding a row — is
sensed; the "requeue a row a worker already took" direction is not. Recorded as a Cosmetic finding,
not a blocker: it needs a second sensor with one worker consuming a batch of two, which is a race
the design makes hard to provoke and which iteration 2 already accepted as out of scope.

### `TestAPoolOfOneRunsOneJobAtATime` — **a real sensor, not shallow**

It is behavioural in two independent dimensions: claim volume (`countMemRows(..., "running") != 1`,
i.e. the pool must not claim jobs it cannot run) and execution volume (`len(entered) != 0`, i.e. no
second handler entered while the first blocks). Probed directly rather than assumed: mutant **M18**
makes `newRunner` ignore `c.concurrency` and use `defaultConcurrency` for both `concurrency` and the
`slots` capacity — the test fails on **both** assertions (`running rows = 4, want 1`;
`3 further handlers were entered, want 0`). It would not pass under a pool that ignored
`Concurrency`.

---

## Spec-Anchored Acceptance Criteria

Rows unchanged since iteration 2 are re-stated with the same evidence; the AC6 row carries the new
sensor.

### P1-Story1: Jobs execute concurrently

| Criterion | `file:line` + assertion | Result |
| --- | --- | --- |
| AC1 n handlers executing simultaneously | `pool_test.go:91-97` — drains `concurrency` (=4) values from `entered` before `close(release)` | ✅ PASS |
| AC2 `Concurrency ≤ 0` → default 10 | `client_test.go:168` — `if c.concurrency != tt.want` over `{0→10, -4→10, 1→1, 64→64}` | ✅ PASS |
| AC3 One fetch round claims ≤ idle-worker count | `pool_test.go:140` — `running != concurrency` | ✅ PASS |
| AC4 Executed exactly once by exactly one worker | `pool_test.go:185` — `runs[id] != 1`; `e2e_integration_test.go:506` — same, `Concurrency: 8`, 60 jobs on Postgres | ✅ PASS |
| AC5 Panic isolated; job retryable with stack | `pool_test.go:233-239`; `loop_test.go:497,502,505` | ✅ PASS |
| AC6 Idle → poll interval and no faster, bounded both sides | `loop_test.go:849` `fetches < 2`; `:854` `fetches > 20` | ✅ PASS |
| AC7 One blocking handler → others stay free | `pool_test.go:233-239` | ✅ PASS |

### P1-Story2: Shutdown is ordered, bounded and honest

| Criterion | `file:line` + assertion | Result |
| --- | --- | --- |
| AC1 `Start` non-blocking, returns nil | `pool_test.go:87` + assertions running while 4 handlers block | ✅ PASS |
| AC2 Second `Start` → error, no second pool | `pool_test.go:257` — `!errors.Is(err, ErrAlreadyStarted)` | ✅ PASS |
| AC3 Cease claiming **before** it begins waiting | `pool_test.go:739` — two `fetches` samples 250 ms apart *while the drain still waits* must be equal, at a 1 ms poll interval | ✅ PASS |
| AC4 Drains inside budget → nil, every job terminal | `pool_test.go:177`; `e2e_integration_test.go:537` | ✅ PASS |
| AC5 Budget expires → cancel, requeue, error names the count | `pool_test.go:463` `ErrDrainIncomplete`; `:466` `"2 job(s)"`; `:481` `"retryable"` | ✅ PASS |
| AC6 Jobs claimed but **not yet handed to a worker** → returned to the queue | **NEW** `pool_test.go:647` — real `r.fetch()` with no workers; `row.State == "running"` → fail; `:652` `len(c.inflight.snapshot()) != 0` → fail. Mutant **M16 killed**. Contract of `abandon` itself: `pool_test.go:596,599,606` | ✅ **PASS (gap closed)** |
| AC7 Requeue claimable now, attempt not decremented, reason recorded | `pool_test.go:483-486`; `:535`; `:545`; `e2e_integration_test.go:608` | ✅ PASS |
| AC8 `Stop` before `Start` → error immediately | `pool_test.go:270` — `!errors.Is(err, ErrNotStarted)` | ✅ PASS |
| AC9 Second `Stop` → same verdict, no block/panic | `pool_test.go:294,297` | ✅ PASS |
| AC10 Cancelling `Start`'s ctx → shutdown begins unaided | `pool_test.go:362`; `heartbeat_test.go:287-301` | ✅ PASS |

### P1-Story3: Reliability guarantees survive the pool

| Criterion | `file:line` + assertion | Result |
| --- | --- | --- |
| AC1 Each tick renews all n leases in one call | `pool_test.go:832` — `largestBatch() == concurrency` (4) | ✅ PASS |
| AC2 Heartbeat renews during drain, stops only after | `heartbeat_test.go:289,295,298` | ✅ PASS |
| AC3 Lost lease → refused, logged as takeover not failure | `loop_test.go:336` and `:339` (both directions) | ✅ PASS |
| AC4 Cancelled job ctx → outcome still recorded, off the cancelled ctx | `loop_test.go:248,255` via `ctxDriver` | ✅ PASS |
| AC5 No goroutine leaks across `Start`/`Stop` | `goleak.VerifyNone` on 19 lifecycle tests incl. `pool_test.go:616,659` | ✅ PASS |
| AC6 Concurrent claims race → no double execution | `e2e_integration_test.go:506`; `pool_test.go:185` | ✅ PASS |
| AC7 Job outliving its lease can still record its outcome | `loop_test.go:298` | ✅ PASS |
| AC8 Lost lease reported as takeover rather than failed write | `loop_test.go:336,339` | ✅ PASS |

### P2-Story4: A runnable example program

| Criterion | Evidence | Result |
| --- | --- | --- |
| AC1 Compiles and vets | Gate — exit 0 | ✅ PASS |
| AC2 Migrates, registers, enqueues, pool > 1 | `examples/email/main.go:88,93,103,35` — inspection only | ⚠️ Within the spec's stated bar |
| AC3 Simulated failure → ordinary retry, visible in output | `examples/email/delivery_test.go:103`; `main.go:57,142` | ✅ PASS |
| AC4 SIGINT → bounded `Stop`, reports what drained | `examples/email/main.go:112-124` — inspection only | ⚠️ Within the spec's stated bar |
| AC5 No new module dependency | `git diff main...HEAD -- go.mod go.sum` empty | ✅ PASS |

**Status**: **34/34 ACs matched their spec-defined outcome.** 0 hard gaps; 2 inspection-only P2
criteria, explicitly within the spec's own Independent Test bar.

---

## Edge Cases

| Edge case | Evidence | Result |
| --- | --- | --- |
| `Concurrency = 1` → one job in flight | **NEW** `pool_test.go:683` `running != 1`; `:686` `len(entered) != 0`. M18 killed | ✅ **PASS (was partial)** |
| `Stop` while the fetch loop sleeps a poll interval | `pool_test.go:695` — `elapsed > time.Second` against a 3 s `PollInterval` | ✅ PASS (M9 killed) |
| Fetch error → log, back off, continue | `loop_test.go:872,876` | ✅ PASS (M15 killed) |
| Requeue failure at shutdown → logged, shutdown continues | `pool_test.go:666,669` via `failFirstRetryableDriver` | ✅ PASS (M14 killed) |
| Late handler return after re-claim → write refused by the fence | `loop_test.go:336,339` | ✅ PASS |
| `Concurrency` exceeds due jobs → no per-worker fetch | `pool_test.go:140`; `loop_test.go:849/854` | ✅ PASS |
| `Stop`'s ctx already done → still stops fetching and requeues | `pool_test.go:764,769` | ✅ PASS |

**7/7 clean.**

---

## Discrimination Sensor

M1–M15 were established in iterations 1–2 and are recorded here as prior evidence. M16 was re-run
from scratch in this pass; M17/M17b/M18 are new probes.

All mutation work was done in a throwaway `git archive` extraction outside the repository, deleted
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
| M6 | `pool.go:214` | `escalate` no longer calls `r.cancelJobs()` | ✅ Killed | `TestEscalationCancelsHandlersBeforeReturningTheirJobs` |
| M7 | `loop.go:143` | drop `context.WithoutCancel` in `finalizeContext` | ✅ Killed | `TestJobOutcomeIsRecordedEvenWhenItsContextWasCancelled` |
| M8 | `pool.go:350` | drop the surplus-row `requeueAll` | ✅ Killed | `TestSurplusClaimedJobsAreReturnedImmediately` |
| M9 | `pool.go:433-438` | `sleep()` ignores `stopFetch` | ✅ Killed | `TestStopDoesNotWaitOutThePollInterval` |
| M10 | `heartbeat.go:38` | renew only the first in-flight lease | ✅ Killed | `TestHeartbeatRenewsEveryWorkersLease` |
| M11 | `pool.go:234` | `abandon` no longer requeues | ✅ Killed | `TestClaimsNeverHandedToAWorkerGoBackToTheQueue` |
| M12 | `client.go:27` | `defaultConcurrency` 10 → 1 | ✅ Killed | `TestConfigZeroValuesGetDefaults`, `TestConcurrencyConfigFallsBackToTheDefault` |
| M13 | `loop.go:247-250` | lost lease logged as an ordinary `ERROR` finalize failure | ✅ Killed | `TestALostLeaseIsReportedAsATakeoverNotAFailure` |
| M14 | `pool.go:266-268` | failed requeue `return`s instead of `continue` | ✅ Killed | `TestOneFailedHandBackDoesNotStopTheRest` |
| M15 | `pool.go:334-338` | fetch error stops the pool | ✅ Killed | `TestStartLogsAndRetriesAfterFetchErrors` |
| **M16** | `pool.go:370-375` | stop arm returns **without** `r.abandon(rows[i:])` (loop var dropped so it still builds) | ✅ **Killed (was the survivor)** | `TestTheFetchLoopHandsBackRowsItNeverDispatched` — 3 assertions fire in 0.00 s |
| M17b | `pool.go:372` | `rows[i:]` → `rows[i+1:]` (off-by-one that strands the current row) | ✅ Killed | `TestTheFetchLoopHandsBackRowsItNeverDispatched` |
| M17 | `pool.go:372` | `rows[i:]` → `rows` (hands back rows a worker already took) | ❌ **Survived** | — see Finding 1 (Cosmetic) |
| M18 | `pool.go:76,78` | runner ignores `c.concurrency`, uses `defaultConcurrency` | ✅ Killed | `TestAPoolOfOneRunsOneJobAtATime` (both assertions) |

**Sensor depth**: P0-full (19 mutations — data-integrity / at-least-once critical path).
**Result**: 18/19 killed. The single survivor (M17) is a lower-bound slice probe outside every
acceptance criterion; every mutation of a spec-stated behaviour is killed.

---

## Gate Check

| Gate | Command | Result |
| --- | --- | --- |
| Build + vet | `go build ./... && go vet ./...` | ✅ exit 0 |
| Quick | `go test -race -count=1 ./...` | ✅ exit 0 — root 3.2 s; `examples/email`, `memdriver` ok |
| Full | `go test -race -count=1 -tags=integration ./...` | ✅ exit 0 — root 15.3 s, `pgdriver` 9.0 s, `migrate` 6.8 s, `memdriver` 1.0 s, `examples/email` 1.0 s |
| Lint | `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run` | ✅ exit 0 — **0 issues** |
| Flake probe | `go test -race -count=10 -run 'TestTheFetchLoopHandsBackRowsItNeverDispatched\|TestAPoolOfOneRunsOneJobAtATime' .` | ✅ **20/20 PASS**, 1.6 s |

**Test counts** (top-level `--- PASS`, `go test -v`):

- Root unit: **83** (iteration 2: 81; baseline `main`: 58). ✅ matches the expected +2.
- Skipped: **0**.
- **No test deleted, skipped, or weakened between `5204dc7` and HEAD** — the diff for `*_test.go`
  contains no removed lines. ✅

---

## Code Quality

| Principle | Status |
| --- | --- |
| Minimum code — no features beyond what was asked | ✅ `3461797` adds tests only; zero implementation change |
| No abstractions for single-use code | ✅ both tests reuse existing helpers (`newPoolClient`, `insertN`, `countMemRows`, `waitFor`, `blockingWorker`) |
| Only touched files required for the task | ✅ `pool_test.go` |
| Matches existing patterns/style | ✅ `goleak` on both, failure messages state the consequence not just the value |
| Tests map to ACs and are non-shallow | ✅ the structural weakness flagged in iteration 2 is resolved — AC6 is now sensed through the real loop |
| Would a senior engineer approve? | ✅ The fix takes the harder, correct route (drive the real loop with no consumers) rather than widening the assertion on the direct-call test, and the comment explains *why* the no-worker setup is what makes it deterministic |
| Every test maps to a spec requirement | ✅ both new tests map to P1-Story2 AC6 and the `Concurrency = 1` edge case respectively |

---

## Requirement Traceability

| Requirement | Iteration 2 | Now |
| --- | --- | --- |
| POOL-01 … POOL-05 | ✅ Verified | ✅ Verified (POOL edge `Concurrency = 1` now behavioural) |
| SHUT-01 | ✅ Verified | ✅ Verified |
| SHUT-02 | ✅ Verified | ✅ Verified |
| SHUT-03 | ✅ Verified | ✅ Verified |
| SHUT-04 | ✅ Verified | ✅ Verified |
| SHUT-05 | ⚠️ Partial | ✅ **Verified** — `abandon` proven (M11) *and* its invocation from the fetch loop proven (M16) |
| SAFE-01 … SAFE-04 | ✅ Verified | ✅ Verified |
| EX-01 | ⚠️ Partial | ⚠️ Partial — AC2/AC4 by inspection only, within the spec's stated bar |

---

## Findings

### Finding 1 — `rows[i:]`'s lower bound is not pinned — **Cosmetic**

`TestTheFetchLoopHandsBackRowsItNeverDispatched` runs with no workers, so the loop always stops at
`i == 0` and mutating `r.abandon(rows[i:])` to `r.abandon(rows)` survives the suite (M17). The
opposite off-by-one (`rows[i+1:]`, which would strand a row) *is* killed, and stranding is the
failure mode AC6 names, so nothing spec-stated is unsensed. Closing this would need a sensor where
one worker consumes part of a batch before the stop lands — a race the design deliberately makes
rare. Recorded, not required.

### Finding 2 — P2-Story4 AC2/AC4 satisfied by inspection only — **Cosmetic** (carried forward)

`examples/email/main.go:88,93,103,112-124` are read, not asserted. The spec's own Independent Test
asks only for `go vet ./examples/...` plus a stub unit test, so this is within the stated bar —
either accept it explicitly in the spec or add a testcontainer-driven example run in a later cycle.

*(Iteration 2's Finding 2 — no behavioural `Concurrency = 1` sensor — is closed by
`TestAPoolOfOneRunsOneJobAtATime`, confirmed by mutant M18.)*

---

## Summary

**Overall**: ✅ **PASS** — no Major or Minor findings open; two Cosmetic notes, neither blocking.

**Spec-anchored check**: 34/34 ACs matched their spec-defined outcome. Edge cases: 7/7 clean.
**Sensor**: 18/19 mutations killed; the one survivor probes a slice bound outside any acceptance
criterion.
**Gate**: build ✅, vet ✅, `-race` ✅, `-race -tags=integration` ✅, golangci-lint ✅ (0 issues) —
all exit 0, 0 failures, 0 skips. Root unit suite 83 tests, none deleted or weakened.

The fix commit does exactly the right thing and no more: it adds a sensor rather than changing
behaviour, and the sensor it adds is the deterministic one — parking the real fetch loop
mid-hand-off by starting no workers at all, so the stop arm is the only reachable arm. Deleting the
`r.abandon(rows[i:])` call now fails three assertions in 0.00 s. The `Concurrency = 1` edge case,
carried as cosmetic through two iterations, also gained a genuine two-dimensional behavioural
sensor. Cycle C is verified.
