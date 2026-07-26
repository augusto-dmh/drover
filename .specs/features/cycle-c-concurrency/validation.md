# Concurrency: Worker Pool and Graceful Shutdown — Validation

**Date**: 2026-07-26
**Spec**: `.specs/features/cycle-c-concurrency/spec.md`
**Diff range**: `main..HEAD` on `feat/worker-pool-and-graceful-shutdown` (11 commits, `4772040`..`d4cf8b3`)
**Verifier**: independent sub-agent (author ≠ verifier), read-only over the implementation

**Verdict**: ❌ **FAIL** — 3 surviving mutants and 5 uncovered criteria. Everything the cycle
advertises as its headline behaviour is real and well proven; the gaps are on the second-order
shutdown paths the spec also names as requirements.

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1 `Concurrency` setting | ✅ Done | `client.go:27,178-180`; `client_test.go:148` covers both defaulting branches + explicit value. Count `client_test.go` = 8 (was 7) ✅ |
| T2 Handler/finalization context split | ✅ Done | `loop.go:142-144`; `loop_test.go:210` proves it with a context-honouring driver. Count `loop_test.go` = 19 (was 17) ✅ |
| T3 Worker pool + lifecycle | ✅ Done | `pool.go` (new, 439 lines). Root unit total 77 (was 58), ≥ 70 required ✅ |
| T4 Bound the drain, escalate, requeue | ⚠️ **Partial** | Two of its own "Done when" criteria have no sensor: *"jobs the fetch loop holds claimed but undispatched at shutdown are requeued"* (`runner.abandon` is **0% covered**) and *"a requeue that loses the race … is logged as expected, not as a failure"* (no assertion on that log; M14 survived). Root unit 77 ≥ 76 ✅ |
| T5 Prove against PostgreSQL | ✅ Done | `e2e_integration_test.go` = 10 tests (was 7) ✅ |
| T6 Example program | ✅ Done | `examples/email/`; `go.mod`/`go.sum` unchanged in the diff ✅ |
| T7 Documentation | ✅ Done | `doc.go`, `README.md`, `loop.go:15-33,53-68` |

---

## Spec-Anchored Acceptance Criteria

### P1-Story1: Jobs execute concurrently

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 `Concurrency = n > 0`, ≥ n jobs due → n handlers executing simultaneously | n concurrent handler entries observed before any release | `pool_test.go:91-97` — loop draining `concurrency` (=4) values from `entered` before `close(release)`; `t.Fatalf("only %d of %d handlers were running at once", i, concurrency)` | ✅ PASS |
| AC2 `Concurrency ≤ 0` → default 10, no error | `c.concurrency == 10` | `client_test.go:168` — `if c.concurrency != tt.want` over `{0→10, -4→10, 1→1, 64→64}`; `client_test.go:143` — `if c.concurrency != 10` | ✅ PASS |
| AC3 One fetch round claims ≤ idle-worker count | rows in `running` never exceed pool size | `pool_test.go:140` — `if running := countMemRows(t, mem, ids, "running"); running != concurrency` (3 workers, 8 queued, all blocked) | ✅ PASS |
| AC4 Each claimed job executed exactly once by exactly one worker | `runs[id] == 1` for every id | `pool_test.go:185` — `if runs[id] != 1`; `e2e_integration_test.go:506` — `if worker.runs[id] != 1` (Postgres, `Concurrency: 8`, 60 jobs) | ✅ PASS |
| AC5 Panic on one worker → others keep working; panicking job retryable with stack | other jobs complete; row `retryable`, error names the panic, `Trace` non-empty | `pool_test.go:233-239` — 4 healthy jobs drained alongside a panic + a stuck handler; `loop_test.go:497` — `waitFor(t, h.rowInState(bad.ID, "retryable"), …)`; `loop_test.go:502` — `!strings.Contains(recorded[0].Error, "kaboom")`; `loop_test.go:505` — `if recorded[0].Trace == ""` | ✅ PASS |
| AC6 Idle → fetch at poll interval and no faster, **bounded count over a fixed span** | count in a 150 ms window at 30 ms bounded both sides | `loop_test.go:849` — `if fetches < 2`; `loop_test.go:854` — `if fetches > 20` | ✅ PASS (upper bound present — lesson L-001 honoured) |
| AC7 One handler blocking indefinitely → other workers stay free | healthy jobs still complete | `pool_test.go:233-239` (the `"block"` job never returns until `close(release)` at `:241`) | ✅ PASS |

### P1-Story2: Shutdown is ordered, bounded and honest

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 `Start` on a stopped client → starts pool, returns `nil`, does not block | `err == nil`, control returns while handlers still run | `pool_test.go:87` — `if err := c.Start(ctx); err != nil` followed by assertions that run while 4 handlers are blocked | ✅ PASS (structural: a blocking `Start` would deadlock the test) |
| AC2 Second `Start` → error, no second pool | `errors.Is(err, ErrAlreadyStarted)` | `pool_test.go:257` — `if err := c.Start(ctx); !errors.Is(err, ErrAlreadyStarted)` | ✅ PASS ("no second pool" itself asserted only indirectly, via `goleak.VerifyNone` at `:248`) |
| AC3 `Stop` ceases claiming before waiting; nothing claimed after | fetch count and rescue-sweep count frozen | `pool_test.go:324` — `if after := counting.fetches.Load(); after != settled`; `pool_test.go:405` — same for `sweeping.sweeps` | ⚠️ **Spec-precision gap** — both assert "no claim *after `Stop` returned*"; nothing pins "*before* it begins waiting". The ordering is real in `pool.go:166-176` but not sensed. |
| AC4 Everything drains inside budget → `nil`, every job terminal | `Stop == nil`; zero rows `running` | `pool_test.go:177` — `if err := c.Stop(context.Background()); err != nil` after `completed == 24`; `loop_test.go:819-826`; `e2e_integration_test.go:537` — `if running := countInState(t, pool, "running"); running != 0` | ✅ PASS |
| AC5 Budget expires → cancel job contexts, requeue best-effort, non-`nil` error naming the count | error wraps `ErrDrainIncomplete` and contains the count; handlers cancelled; rows requeued | `pool_test.go:463` — `!errors.Is(err, ErrDrainIncomplete)`; `pool_test.go:466` — `!strings.Contains(err.Error(), "2 job(s)")`; `pool_test.go:481` — `if row.State != "retryable"`; `pool_test.go:591-595` — `select { case <-cancelled: … }` | ✅ PASS |
| AC6 Jobs claimed but **not yet handed to a worker** at shutdown → returned to the queue | those rows become claimable, not left leased | **no evidence** — `runner.abandon` (`pool.go:229`) is 0.0% covered; mutant M11 (delete its `requeueAll`) survived unit **and** integration suites. The covered case at `pool_test.go:721` is the *different* surplus path (`pool.go:347-352`). | ❌ **GAP** |
| AC7 Requeued job claimable immediately, `attempt` not decremented, reason recorded | `ScheduledAt ≤ now`; `attempt` unchanged; error history names the shutdown | `pool_test.go:483-486` — `if !row.ScheduledAt.After(time.Now()) { continue }` else `t.Errorf`; `pool_test.go:535` — `if requeued.Attempt != claimed.Attempt`; `pool_test.go:545` — `!strings.Contains(recorded[0].Error, "shut down")`; `e2e_integration_test.go:608` — `if attempt != 1` | ✅ PASS |
| AC8 `Stop` before `Start` → error immediately, no block | `errors.Is(err, ErrNotStarted)` | `pool_test.go:270` — `if err := c.Stop(context.Background()); !errors.Is(err, ErrNotStarted)` | ✅ PASS |
| AC9 Second `Stop` → returns without blocking, no panic/double-close | first call's verdict, inside 2 s | `pool_test.go:294` — `if !errors.Is(second, first)`; `pool_test.go:297` — `case <-time.After(2*time.Second): t.Fatal("second Stop blocked…")` | ✅ PASS |
| AC10 Cancelling `Start`'s context → shutdown begins on its own, same ordering | shutdown completes unaided; heartbeat still ordered after the fetch loop | `pool_test.go:362` — `case <-c.runner.done:` (else `t.Fatal`); `heartbeat_test.go:287-301` — lease still renewed and un-lapsed while draining post-cancel | ✅ PASS |

### P1-Story3: Reliability guarantees survive the pool

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 n in flight → each tick renews all n leases in **one call** | a single `ExtendLeases` carrying n entries | `pool_test.go:832` — `waitFor(t, func() bool { return counting.largestBatch() == concurrency }, …)` with `concurrency = 4` | ✅ PASS |
| AC2 Heartbeat keeps renewing during drain, stops only after the last worker | lease strictly advances after cancel; row still `running`; lease not lapsed | `heartbeat_test.go:289` — `current.LeasedUntil.After(leaseAtCancel)`; `heartbeat_test.go:295` — `if draining.State != "running"`; `heartbeat_test.go:298` — `draining.LeasedUntil.Before(time.Now())` | ✅ PASS ("stops only after" proven indirectly by `goleak` + mutant M3) |
| AC3 Job re-claimed elsewhere → this worker's write refused with `ErrLeaseLost`, logged as a **takeover, not a failure** | `writeFailed` takes the `ErrLeaseLost` branch and logs at WARN with the takeover message | **no evidence at client level** — `loop.go:247-250` has no test anywhere; mutant M13 (log it as an ordinary `ERROR` finalize failure) survived. The *driver-side* fence is proven at `internal/memdriver/memdriver_test.go:574` and `internal/pgdriver/pgdriver_integration_test.go:182`, but that is the driver refusing, not the client classifying. | ❌ **GAP** (pre-existing code, unchanged by this cycle, but claimed as an AC here) |
| AC4 Job context cancelled by escalation → outcome still recorded; finalization not on the cancelled context | handler observes cancellation; row reaches `completed`/`retryable` anyway | `loop_test.go:248` — `if !<-sawCancellation`; `loop_test.go:255` — `if stored.State != tt.wantState` (`completed` / `retryable`), driven through `ctxDriver` (`loop_test.go:189-203`), which **does** honour context — so the assertion discriminates | ✅ PASS |
| AC5 Full `Start`/`Stop` cycle leaks no goroutine | `goleak.VerifyNone` passes | `pool_test.go:73,110,152,193,248,276,305,335,383,431,498,558,626,684,739,803` — `defer goleak.VerifyNone(t, goleak.IgnoreCurrent())` (16 lifecycle tests) | ✅ PASS |
| AC6 Concurrent claims race the same rows → no row run by two workers of one client | `runs[id] == 1` under real `SKIP LOCKED` | `e2e_integration_test.go:506` — `if worker.runs[id] != 1` (8 workers, 60 rows, Postgres); `pool_test.go:185` in-memory | ✅ PASS |

### P2-Story4: A runnable example program

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --- | --- | --- | --- |
| AC1 Compiles and vets | `go build ./... && go vet ./...` exit 0 | Gate run — exit 0 | ✅ PASS |
| AC2 Against Postgres: migrates, registers a worker, enqueues a batch, pool > 1 | all four present | `examples/email/main.go:88` `drover.Migrate`, `:93` `drover.Register`, `:103` `enqueueBatch`, `:35` `workerConcurrency = 4` | ⚠️ **Inspection only, no assertion** — but the spec's own Independent Test asks only for `go vet ./examples/...` + a stub unit test, so this is within the spec's stated bar |
| AC3 Simulated delivery fails → ordinary retry path, retry visible in output | flaky subset fails attempt 1, succeeds after | `examples/email/delivery_test.go:103` — `if (err != nil) != tt.wantErr` over `{ada@…,1→err}, {ada@…,2→nil}, {grace@…,1→nil}`; `main.go:57` returns the error to drover; `main.go:142` logs the predicted retry count | ✅ PASS (retry mechanism itself is library-tested) |
| AC4 SIGINT → `Stop` with a bounded context, reports what drained | `signal.NotifyContext` + 30 s budget + verdict logged | `examples/email/main.go:112-124` | ⚠️ **Inspection only, no assertion** |
| AC5 No new module dependency | `go.mod`/`go.sum` unchanged | `git diff main...HEAD -- go.mod go.sum` → empty | ✅ PASS |

**Status**: ❌ 2 hard gaps in P1 (Story2 AC6, Story3 AC3) + 1 spec-precision gap (Story2 AC3) + 2 inspection-only P2 criteria.

---

## Edge Cases

| Edge case | Evidence | Result |
| --- | --- | --- |
| `Concurrency = 1` → previous single-worker semantics, one job in flight | `client_test.go:158` — `{name: "one is honoured, not mistaken for unset", cfg: Config{Concurrency: 1}, want: 1}` is **config-level only**. Several tests run with `concurrency = 1` (`pool_test.go:506,571,641,693,747`) but none asserts at-most-one-in-flight. | ⚠️ Partial — no behavioural assertion |
| `Stop` while the fetch loop is sleeping out a poll interval → shutdown does not wait it out | **no evidence** — mutant M9 (make `sleep` ignore `stopFetch`) survived unit + integration. No test bounds `Stop`'s elapsed time for an idle pool. | ❌ **GAP** |
| Fetch error → log, back off one poll interval, continue; pool does not stop | `loop_test.go:872` — `waitFor(t, h.rowInState(row.ID, "completed"), …)` after 2 forced fetch failures; `loop_test.go:876` — `!strings.Contains(logs, 'level=ERROR msg="drover: fetch jobs"')` | ✅ PASS (mutant M15 killed). The *duration* of the back-off is not asserted — minor spec-precision note. |
| Requeue of an unfinished job fails at shutdown → logged, shutdown continues | **no evidence** — mutant M14 (`return` instead of `continue` after a failed requeue) survived; no test injects a requeue failure during escalation. `requeueFailed` is 75% covered — only the lost-lease/expected branch executes, and nothing asserts on either log line. | ❌ **GAP** |
| Handler returns after cancellation, job already requeued and re-claimed → write refused by the fence | **no evidence** — same hole as P1-Story3 AC3 (`loop.go:247-250` untested) | ❌ **GAP** |
| `Concurrency` exceeds due jobs → idle workers consume no DB capacity; one fetch sized to the idle count | `pool_test.go:140` (never claims past idle count) + `loop_test.go:849/854` (one bounded fetch cadence when idle, not one per worker) | ✅ PASS |
| `Stop`'s context already done → still stops fetching and requeues before returning its error | `pool_test.go:764` — `if err := c.Stop(spent); !errors.Is(err, ErrDrainIncomplete)`; `pool_test.go:769` — `if row.State != "retryable"` | ✅ PASS |

---

## Discrimination Sensor

Run in a throwaway `tar`-copy of the repo at
`$SCRATCH/mut` (deleted afterwards). The real working tree was never modified —
`git status --short` and `git diff --stat` are both empty, verified before and after.
Command per mutant: `go test -race -count=1 -timeout 300s .` (survivors re-run with
`-tags=integration`).

| # | File:line | Mutation | Killed? | Killed by |
| - | --------- | -------- | ------- | --------- |
| M1 | `pool.go:76` | `concurrency: c.concurrency` → `1` (pool size always 1) | ✅ Killed | `TestPoolRunsJobsConcurrently`, `TestPoolClaimsNoMoreJobsThanItCanRun`, `TestOneMisbehavingJobDoesNotStallThePool`, `TestStopReportsAndRequeuesTheJobsItCouldNotFinish`, `TestHeartbeatRenewsEveryWorkersLease` |
| M2 | `pool.go:383-385` | Worker returns its slot **before** running the job → fetch loop claims past the idle-worker count | ✅ Killed | `TestPoolClaimsNoMoreJobsThanItCanRun`, `TestSurplusClaimedJobsAreReturnedImmediately` |
| M3 | `pool.go:175-189` | `close(r.stopHeartbeat)` + `background.Wait()` moved **before** the drain wait | ✅ Killed | `TestHeartbeatOutlivesCancellationUntilTheDrainFinishes` |
| M4 | `pool.go:312` | `MarkRetryable(ctx, lease, now, detail)` → `MarkSnoozed(ctx, lease, now)` (gives the attempt back) | ✅ Killed | `TestShutdownRequeueDoesNotGiveBackTheAttempt` + 3 others |
| M5 | `pool.go:221-222` | `escalate` returns `nil` instead of the `ErrDrainIncomplete` error | ✅ Killed | 6 tests incl. `TestStopReportsAndRequeuesTheJobsItCouldNotFinish` |
| M6 | `pool.go:214` | `escalate` no longer calls `r.cancelJobs()` | ✅ Killed | `TestEscalationCancelsHandlersBeforeReturningTheirJobs` **only** — see note below |
| M7 | `loop.go:143` | `finalizeContext`: drop `context.WithoutCancel` (finalize on the cancelled handler context) | ✅ Killed | `TestJobOutcomeIsRecordedEvenWhenItsContextWasCancelled` (both subtests) |
| M8 | `pool.go:350` | Drop the surplus-row `requeueAll` (keep the truncation) | ✅ Killed | `TestSurplusClaimedJobsAreReturnedImmediately` |
| M9 | `pool.go:433-438` | `sleep()` waits the timer unconditionally, ignoring `stopFetch` | ❌ **Survived** | — (also survived `-tags=integration`) |
| M10 | `heartbeat.go:38` | `leases = leases[:1]` — renew only the first in-flight lease | ✅ Killed | `TestHeartbeatRenewsEveryWorkersLease` |
| M11 | `pool.go:234` | `abandon` no longer requeues claimed-but-undispatched rows | ❌ **Survived** | — (also survived `-tags=integration`) |
| M12 | `client.go:27` | `defaultConcurrency = 10` → `1` | ✅ Killed | `TestConfigZeroValuesGetDefaults`, `TestConcurrencyConfigFallsBackToTheDefault` |
| M13 | `loop.go:247-250` | `writeFailed` logs a lost lease as an ordinary `ERROR` finalize failure instead of a WARN takeover | ❌ **Survived** | — |
| M14 | `pool.go:266-268` | A failed requeue `return`s instead of `continue`, abandoning the remaining requeues | ❌ **Survived** | — |
| M15 | `pool.go:334-338` | A fetch error `return`s (stops the pool) instead of backing off and continuing | ✅ Killed | `TestStartLogsAndRetriesAfterFetchErrors` |

**Sensor depth**: P0-full (15 mutations — data-integrity / at-least-once critical path).
**Result**: 11/15 killed, **4 survived** — ❌ FAIL.

**Note on M6 (weak-but-covered):** `TestExhaustingTheBudgetCancelsTheRunningHandlers`
(`pool_test.go:557`) passes under M6 and therefore does **not** prove what its name claims.
`drain`'s `defer r.cancelJobs()` (`pool.go:161`) fires before `Stop` returns, so the handler is
cancelled either way and the test's 2 s wait at `:591` is satisfied. Only
`TestEscalationCancelsHandlersBeforeReturningTheirJobs` (`pool_test.go:625`) actually
discriminates, via the cancel-before-requeue ordering. The AC is covered; the first test is
decorative.

**Coverage cross-check** (`go tool cover -func`, root unit suite):

```
pool.go:229  abandon         0.0%     <- P1-Story2 AC6 has no execution at all
pool.go:282  requeueFailed  75.0%     <- error branch never taken
pool.go:253  requeueAll     88.9%
pool.go:316  fetch          86.7%
```

---

## Code Quality

| Principle | Status |
| --- | --- |
| Minimum code — no features beyond what was asked | ✅ |
| No abstractions for single-use code | ✅ `runner` is one value per run, not a framework |
| No unnecessary "flexibility" added | ✅ `queue` is a field rather than a hard-coded constant, which the design justifies as the Cycle D seam |
| Only touched files required for the task | ✅ 21 files, all in scope |
| Didn't "improve" unrelated code | ✅ |
| Matches existing patterns/style | ✅ context-first, `%w` wrapping, package-prefixed sentinels, `*slog.Logger` via config — per CLAUDE.md |
| Would a senior engineer approve? | ✅ The reasoning comments on `pool.go:154-193`, `loop.go:79-89` and `inflight.go:71-84` are unusually good; the shutdown ordering is correct and explained |
| Tests map to ACs and are non-shallow | ⚠️ 14/15 spot-checked well; `TestExhaustingTheBudgetCancelsTheRunningHandlers` is shallow (see M6 note) |
| Spec-anchored outcome check | ⚠️ 2 hard gaps + 1 precision gap (see tables) |
| Per-layer coverage expectation met | ⚠️ Domain logic 1:1 except `abandon` (0%) and the takeover branch |
| Every test maps to a spec requirement — no unclaimed tests | ⚠️ One near-miss: `TestALongRunningJobCanStillRecordItsOutcome` (`loop_test.go:267`) maps to design decision AD-027 / commit `95ade96`, not to any spec AC. It is a legitimate regression guard adjacent to SAFE-03 AC4 — recommend adding it to the spec rather than removing it. No other unclaimed tests. |
| Documented guidelines followed | ✅ `CLAUDE.md` (table-driven, `t.Parallel` where safe, `-race`, testcontainers, `testing/synctest` in `heartbeat_test.go`, `goleak` on every lifecycle test) |

---

## Gate Check

| Gate | Command | Result |
| --- | --- | --- |
| Build | `go build ./... && go vet ./...` | ✅ exit 0 |
| Quick | `go test -race ./...` | ✅ exit 0 — all packages `ok` |
| Full | `go test -race -tags=integration ./...` | ✅ exit 0 — root 12.9 s, `pgdriver` 7.4 s, `migrate` 4.8 s, all `ok` |

**Test counts** (top-level `--- PASS`, `go test -v`):

- Root unit: **77** (baseline 58) — **+19**. T3 required ≥ 70, T4 required ≥ 76. ✅
- Root with `-tags=integration`: **88** (= 77 unit + 10 e2e + 1 client-integration). `e2e_integration_test.go` = 10 (baseline 7), T5 required ≥ 10. ✅
- `examples/email`: 3 (new)
- `internal/memdriver`: unchanged
- **No test count decreased; no assertion weakened.** ✅

**Skipped tests**: none.
**Failures**: none.

---

## Fix Plans

### Fix 1 — `runner.abandon` has no test at all (P1-Story2 AC6 / SHUT-05, T4 done-when)

- **Root cause**: no test can reach `pool.go:370-375`'s `case <-r.stopFetch:` branch. It needs the
  fetch loop parked on the unbuffered hand-off (`r.jobs <- row`) at the instant `stopFetch` closes —
  i.e. a fetch that returns more rows than there are workers *ready to receive*, with `Stop` timed
  into the hand-off. `TestSurplusClaimedJobsAreReturnedImmediately` looks like it covers this but
  exercises the separate over-claim guard at `pool.go:347-352`.
- **Fix task**: add a test with `Concurrency: 1` and a driver that blocks inside `FetchAvailable`
  until the worker is occupied, returning ≥ 2 rows; call `Stop` while the loop is mid-hand-off;
  assert every undispatched row is `retryable` with `attempt` unchanged, and that the slot
  accounting still lets `Stop` return.
- **Verify**: mutant M11 (delete `r.requeueAll(leases)` at `pool.go:234`) must fail the suite.
- **Priority**: **Blocker** — this is an explicit P1 acceptance criterion with zero execution, and
  the failure mode it guards (a row `running` + leased with nothing executing it) is the exact state
  the whole slot-accounting design exists to prevent.

### Fix 2 — the lost-lease takeover branch is untested (P1-Story3 AC3 + late-handler-return edge case)

- **Root cause**: `loop.go:246-254` (`writeFailed`) predates this cycle and was never covered. The
  spec re-claims it as an AC because the pool is what makes takeovers routine.
- **Fix task**: with a driver that returns `driver.ErrLeaseLost` from `MarkCompleted`, run `runJob`
  and assert the log carries `msg="drover: job taken over by another worker, discarding this
  outcome"` at `level=WARN` and **not** `level=ERROR msg="drover: finalize job"`; assert the row's
  state is untouched (the new holder's outcome is not overwritten).
- **Verify**: mutant M13 must fail the suite.
- **Priority**: **Major**

### Fix 3 — `Stop` during a poll-interval sleep is unbounded in test (edge case)

- **Root cause**: no test measures how long `Stop` takes when the fetch loop is idle-sleeping.
- **Fix task**: start a pool with `PollInterval: 2 * time.Second` and no jobs, wait for the loop to
  enter its sleep, then assert `Stop(context.Background())` returns in well under the interval.
- **Verify**: mutant M9 (drop the `case <-r.stopFetch` arm of `sleep`) must fail the suite.
- **Priority**: **Major** — this is the difference between a 100 ms deploy shutdown and one that
  waits out the poll interval on every restart.

### Fix 4 — a requeue failure at shutdown is not sensed (edge case, T4 done-when)

- **Root cause**: no test injects a `MarkRetryable` failure during escalation, so neither the
  "log and continue" behaviour nor the expected-vs-genuine failure classification is proven.
- **Fix task**: with 2 in-flight jobs and a driver whose `MarkRetryable` fails for the first lease
  only, exhaust `Stop`'s budget; assert the second job still reaches `retryable`, `Stop` still
  returns `ErrDrainIncomplete` naming 2, and the error log names the failed row.
- **Verify**: mutant M14 (`return` instead of `continue` in `requeueAll`) must fail the suite.
- **Priority**: **Major**

### Fix 5 — tighten two soft assertions

- **5a**: `TestExhaustingTheBudgetCancelsTheRunningHandlers` (`pool_test.go:557`) passes under M6.
  Either assert cancellation is observed **before** `Stop` returns, or fold the test into the
  ordering test and delete it. **Priority: Minor**
- **5b**: P1-Story2 AC3 says "*cease claiming **before** it begins waiting*"; the tests only prove
  "no claim after `Stop` returned". Assert the ordering directly — e.g. a driver whose
  `FetchAvailable` records a timestamp, compared against the first drain-wait entry.
  **Priority: Minor**

### Fix 6 — spec-precision items (no code change; amend `spec.md`)

- Edge "`Concurrency = 1` → previous single-worker semantics": add a behavioural sensor (assert at
  most one concurrent handler entry with `Concurrency: 1`), or state that the config assertion is
  the intended bar.
- P2-Story4 AC2/AC4 are satisfied only by inspection. The spec's own Independent Test sets that bar,
  so either accept it explicitly or add an integration test that runs `examples/email` against a
  testcontainer. **Priority: Cosmetic**
- Add `TestALongRunningJobCanStillRecordItsOutcome` to the traceability table (currently an
  unclaimed but valuable test).

---

## Requirement Traceability Update

| Requirement | Previous | New |
| --- | --- | --- |
| POOL-01 | Design | ✅ Verified |
| POOL-02 | Design | ✅ Verified |
| POOL-03 | Design | ✅ Verified |
| POOL-04 | Design | ✅ Verified |
| POOL-05 | Design | ✅ Verified |
| SHUT-01 | Design | ✅ Verified |
| SHUT-02 | Design | ⚠️ Partial — AC3 ordering not sensed; poll-interval edge case uncovered (M9) |
| SHUT-03 | Design | ✅ Verified |
| SHUT-04 | Design | ⚠️ Partial — requeue-failure edge case uncovered (M14) |
| SHUT-05 | Design | ❌ Needs Fix — claimed-but-undispatched requeue has 0% coverage (M11) |
| SAFE-01 | Design | ✅ Verified |
| SAFE-02 | Design | ✅ Verified |
| SAFE-03 | Design | ❌ Needs Fix — AC4 verified, AC3 + late-return edge case uncovered (M13) |
| SAFE-04 | Design | ✅ Verified |
| EX-01 | Design | ⚠️ Partial — AC1/AC3/AC5 verified; AC2/AC4 by inspection only (within the spec's stated bar) |

---

## Summary

**Overall**: ⚠️ Issues — not ready to mark done.

**Spec-anchored check**: 27/32 ACs matched their spec-defined outcome; 2 hard gaps, 1 spec-precision
gap, 2 inspection-only. Edge cases: 3/7 clean, 3 uncovered, 1 partial.
**Sensor**: 11/15 mutations killed, 4 survived.
**Gate**: build ✅, vet ✅, `-race` ✅, `-race -tags=integration` ✅ — all exit 0, 0 failures, 0 skips.

**What works** — and works genuinely, not just nominally:

- Real concurrency: 4 handlers proven inside their bodies simultaneously; a serial executor cannot
  pass `TestPoolRunsJobsConcurrently`.
- Slot accounting: the pool never claims past its idle-worker count, proven directly and by two
  independent mutants (M2, M8).
- Exactly-once at pool scale, proven against real `SELECT … FOR UPDATE SKIP LOCKED` with 8 workers
  over 60 rows.
- The ownership fence on the requeue path: the attempt is **not** given back, asserted in-memory and
  against Postgres, and the mutant that would break it dies loudly (M4).
- Shutdown ordering: cancel-then-requeue is sensed by a purpose-built driver, and the heartbeat
  outliving the drain is sensed by a mutant that reverses it (M3).
- The finalization-context split (`context.WithoutCancel`) is tested through a **context-honouring**
  wrapper (`ctxDriver`), so it discriminates despite `memdriver` ignoring context — the author's own
  flagged risk is correctly handled.
- The idle-poll assertion is bounded above *and* below, honouring the recorded lesson.

**Issues found**: see Fix Plans 1–6. One Blocker (`abandon` unexecuted), three Major, two Minor.

**Next steps**: route Fixes 1–4 to an implementer as fix tasks, re-verify (iteration 1 of max 3).
Fixes 5–6 can ride along or be deferred to a spec amendment.
