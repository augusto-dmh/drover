# Validation — Middleware and Scheduling

Independent verification of `feat/middleware-and-scheduling` against `spec.md`,
performed by a verifier who did not write the code. Coverage was re-derived from
the tests themselves (evidence-or-zero) and every claim of coverage was tested by
injecting a behaviour-level fault and confirming the suite kills it.

## Verdict

**PASS, with four acceptance criteria unverified and four only partially verified.**

No defect was found in the shipped behaviour. Every gate is green, every requirement
is implemented as the decision record describes, and the implementation survived
every reading. What did not hold up is the *discriminating power* of the suite in
four places: the code is right, but nothing would fail if it stopped being right.
Two of those sit on `MW-02` and `MW-04`, whose whole content is the panic and
unregistered-kind containment the design record calls load-bearing — each of those
protections can be deleted outright and the suite stays green.

Criteria tally over the 45 numbered acceptance criteria in the five stories:

| Outcome | Count |
| --- | --- |
| Fully covered — a named test fails if the behaviour is wrong | 37 |
| Partially covered — the assertion misses part of the criterion | 4 |
| Uncovered — no test discriminates | 4 |

## Gates

| Gate | Result |
| --- | --- |
| `go build ./... && go vet ./...` | pass |
| `go test -race ./...` | pass |
| `go test -race -tags=integration ./...` | pass (all packages, Docker-backed) |
| `golangci-lint v2 run` | pass — 0 issues |
| Stability: `go test -race -count=5 .` | pass — no flake, no goroutine leak |

## Per-requirement evidence

| Req | Covering test(s) | Verdict |
| --- | --- | --- |
| MW-01 | `TestWrapRunsFirstMiddlewareOutermost`, `TestConfiguredMiddlewareWrapsTheWorkerOutermostFirst`, `TestWrapWithNoMiddlewareReturnsTheHandler`, `TestMiddlewareSliceIsCopiedAtConstruction` | Covered — but the copy test is inert (see G-7); AC10 holds structurally, not because of the copy |
| MW-02 | `TestMiddlewareCanRefuseAJobWithoutRunningTheWorker` (AC3), whole `loop_test.go` suite (AC6) | **Partly unverified** — AC7 (chain runs for an unregistered kind) has no discriminating test |
| MW-03 | `TestMiddlewareContextReachesTheWorker` (AC4), `TestMiddlewareCancellationDoesNotReachTheFinalizeContext` (AC5) | Covered |
| MW-04 | `TestPanickingMiddlewareDoesNotKillThePool` (AC9) | **Partly unverified** — AC8 (worker panic seen by middleware as an ordinary error) has no discriminating test |
| MW-05 | `TestNilMiddlewarePanicsNamingItsIndex` | Covered |
| TMO-01 | `TestTimeoutGivesTheHandlerADeadline`, `TestTimeoutPassesAFastHandlerThroughUntouched`, `TestTimeoutKeepsTheErrorOfAHandlerThatOverranIt`, `TestTimeoutOfZeroOrLessAppliesNoDeadline` | Covered |
| TMO-02 | `TestTimedOutJobIsFinalizedAndRetried`, `TestShutdownCancellationReachesAHandlerUnderATimeout` | Covered; AC4's "dead if attempts are spent" arm is untested |
| LOG-01 | `TestLoggingReportsOneStartAndOneEndPerSuccessfulJob`, `TestLoggingReportsAFailedExecutionAtWarnNotError` | **Partly unverified** — the `INFO` level of the start and success records is never asserted |
| LOG-02 | `TestLoggingRunsEvenWhenTheCallerConfiguresMiddleware`, `TestLoggingReportsOneStartAndOneEndPerSuccessfulJob`, `TestStartExecutesJobToCompletion` | Covered |
| SCHED-01 | `TestInsertOptsChooseQueueAndSchedule`, `TestInsertTxHonoursInsertOpts` (integration) | Covered |
| SCHED-02 | `TestScheduledJobRunsOnlyOnceItIsDue`, `memdriver.TestScheduledJobBecomesClaimableWhenItsTimeArrives`, `pgdriver.TestScheduledJobBecomesClaimableWhenItsTimeArrives` | Covered |
| SCHED-03 | `memdriver.TestInsertHonoursScheduledAt`, `pgdriver.TestInsertHonoursScheduledAt` | **Partly unverified** — AC7/AC8 covered; AC9 (database clock, not the client's) is unfalsifiable by the suite |
| QUEUE-01 | `TestCheckedQueuesDefaultsToOneQueue`, `TestCheckedQueuesSortsByNameAndCorrectsWeights`, `TestEmptyQueueNamePanics`, `TestPoolWorksEveryConfiguredQueue`, `TestOneQueueIssuesOneFetchPerRound` | Covered |
| QUEUE-02 | `TestWeightedOrderPicksFirstInProportionToWeight`, `TestWeightedOrderIsAlwaysAFullPermutation`, `TestWeightedOrderSurvivesAVeryLargeWeight` | Covered |
| QUEUE-03 | `TestPoolWorksEveryConfiguredQueue`, `TestAnEmptyQueueDoesNotCostAPollInterval`, `TestAClaimRoundNeverExceedsTheCapacityItWasGiven`, `TestARoundNeverClaimsMoreThanTheIdleWorkerCount` | Covered, with one surviving mutant on AC6 (see G-4) |
| QUEUE-04 | `TestNoWeightingStarvesAQueue`, `TestWeightedOrderIsAlwaysAFullPermutation` | AC5 covered; **AC10 (a queue nobody works keeps its jobs) has no test** |

## Discrimination sensor

Each fault was injected into the source, the affected package alone was run, and the
tree was restored afterwards. `git status` is clean and `git diff` empty.

| # | Injected fault | Result | Killed by |
| --- | --- | --- | --- |
| M1 | `wrap` composes forward, so index 0 becomes innermost | killed | `TestWrapRunsFirstMiddlewareOutermost`, `TestConfiguredMiddlewareWrapsTheWorkerOutermostFirst`, `TestLoggingRunsEvenWhenTheCallerConfiguresMiddleware` |
| M2 | `checkedMiddleware` returns the caller's slice instead of a clone | **survived** | — (equivalent mutant; see G-7) |
| M3 | outermost recovery removed from `execute` | killed | `TestPanickingMiddlewareDoesNotKillThePool` (process panics) |
| M4 | innermost recovery removed from `dispatch` | **survived** | — (G-1) |
| M5a | `finalizeContext` drops `context.WithoutCancel` | killed | `TestJobOutcomeIsRecordedEvenWhenItsContextWasCancelled` |
| M5b | finalize context derived from the chain's context *and* cancellable | killed | `TestMiddlewareCancellationDoesNotReachTheFinalizeContext`, `TestTimedOutJobIsFinalizedAndRetried`, `TestJobOutcomeIsRecordedEvenWhenItsContextWasCancelled` |
| M5c | finalize context derived from the chain's context, `WithoutCancel` kept | survived | — equivalent: `WithoutCancel` strips the deadline too, so the parent choice only leaks context values |
| M6 | `Timeout` substitutes `ctx.Err()` for the handler's returned error | killed | `TestTimeoutKeepsTheErrorOfAHandlerThatOverranIt` |
| M7 | `Timeout` applies a deadline even for a non-positive duration | killed | `TestTimeoutOfZeroOrLessAppliesNoDeadline` |
| M8 | `Logging` reports a failed execution at ERROR instead of WARN | killed | `TestLoggingReportsAFailedExecutionAtWarnNotError` |
| M8b | `Logging` reports the start and success records at ERROR instead of INFO | **survived** | — (G-3) |
| M9 | memdriver stores a delayed job as `available` | killed | `memdriver.TestInsertHonoursScheduledAt`, `memdriver.TestScheduledJobBecomesClaimableWhenItsTimeArrives`, `TestScheduledJobRunsOnlyOnceItIsDue`, `TestInsertOptsChooseQueueAndSchedule` |
| M9pg | `InsertJob` SQL always writes `available` | killed | `pgdriver.TestInsertHonoursScheduledAt`, `pgdriver.TestScheduledJobBecomesClaimableWhenItsTimeArrives` |
| M10 | dueness decided on the client clock: state computed in Go and passed as a parameter | **survived** | — (G-5) |
| M11 | `InsertOpts.Queue` ignored | killed | `TestInsertOptsChooseQueueAndSchedule`, `TestPoolWorksEveryConfiguredQueue`, `TestNoWeightingStarvesAQueue`, `TestAFetchErrorDoesNotStrandRowsAlreadyClaimed` |
| M12 | nil `*InsertOpts` dereferenced | killed | `TestStartKeepsLeaseAliveWhileJobRuns` and most of the suite |
| M13 | `weightedOrder` returns only the highest-weighted queue | killed | `TestWeightedOrderIsAlwaysAFullPermutation`, `TestWeightedOrderPicksFirstInProportionToWeight`, `TestWeightedOrderSurvivesAVeryLargeWeight`, `TestNoWeightingStarvesAQueue`, `TestPoolWorksEveryConfiguredQueue`, `TestAnEmptyQueueDoesNotCostAPollInterval` |
| M14 | `weightedOrder` returns a fixed configured ordering | killed | `TestWeightedOrderPicksFirstInProportionToWeight` |
| M15 | the fetch round stops after its first queue | killed | `TestAnEmptyQueueDoesNotCostAPollInterval` |
| M16 | each queue is asked for the full capacity rather than the remaining capacity | **survived** | — (G-4) |
| M17 | the surplus trim is removed from `claimRound` | killed | `TestSurplusClaimedJobsAreReturnedImmediately` |
| M18 | the `remaining -= len(rows)` decrement is removed | killed | `TestAClaimRoundNeverExceedsTheCapacityItWasGiven`, `TestARoundNeverClaimsMoreThanTheIdleWorkerCount`, `TestNoWeightingStarvesAQueue` |
| M19 | the `remaining == 0` early break is removed | survived | — informational: the extra fetches use a limit of zero and claim nothing |
| M20 | the unregistered-kind failure is decided outside the chain, in `runJob` | **survived** | — (G-2) |
| M21 | `Timeout` detaches the handler's context from cancellation | killed | `TestShutdownCancellationReachesAHandlerUnderATimeout` |

### On the author's overlapping-protection claim

The claim was that "pass full capacity per queue" and "disable the surplus guard"
are each masked by the other two of the three protections, while removing the
`remaining` decrement is caught. Independently re-run:

- Removing the `remaining` decrement (M18) — **caught**, as claimed, by three tests.
- Passing full capacity per queue (M16) — **survives**, as claimed. But the
  defence-in-depth reading does not fully hold: see G-4.
- Disabling the surplus guard (M17) — **not** masked. It is killed immediately by
  `TestSurplusClaimedJobsAreReturnedImmediately` in `pool_test.go`. That half of
  the claim is incorrect; the surplus guard is genuinely covered.

## Gaps, ranked

**G-1 (Medium-High) — Story 1 AC8 / MW-04: no test proves a worker panic reaches
middleware as an ordinary error.** The recovery in `dispatch` can be deleted and the
suite stays green, because `execute`'s outer recovery still converts the panic into a
retryable failure. What silently changes is what the middleware sees: the panic
unwinds straight past every wrapper, so `Logging` emits `job started` and then
*nothing* — no failure record, no duration — and Cycle E's metrics middleware would
undercount every panicking job. `TestPanicStackSurvivesErrorWrapping` tests this
property against a hand-rolled recovery closure in the test file, not against
`c.dispatch`, so it does not defend the shipped code. Needed: a job-level test with a
panicking worker and a recording middleware that asserts the middleware observed a
non-nil error, plus a `job execution failed` record.

**G-2 (Medium) — Story 1 AC7 / MW-02: no test proves the chain runs for an
unregistered kind.** Lifting the unregistered-kind check out of `dispatch` and into
`runJob` — so middleware never runs at all for such a job — passes the whole suite.
`TestStartRetriesUnregisteredKind` only asserts the resulting state;
`TestMiddlewareSliceIsCopiedAtConstruction` calls `c.chain` directly and so cannot
see the difference. AD-014's mid-deploy scenario is exactly when an operator most
needs the log line. Needed: a job-level test with a recording middleware, an
unregistered kind, and an assertion that the middleware ran and saw the error.

**G-3 (Medium) — Story 3 AC1 and AC2: the INFO level of the start and success
records is never asserted.** Raising both to ERROR passes the suite. The failure
record's level *is* asserted, so the cycle's stated concern (not training operators
to ignore WARNs) is protected in one direction only — a regression that made every
job start an ERROR would go unnoticed. Needed: assert `level=INFO` on the two
records the way `TestLoggingReportsAFailedExecutionAtWarnNotError` asserts
`level=WARN`.

**G-4 (Medium) — Story 5 AC6 / QUEUE-03: asking each queue for the full capacity
survives, and the masking path lies to the operator.** With `capacity` substituted
for `remaining`, the AD-022 invariant still holds — the surplus trim catches the
overrun — so no assertion fires. But the trim is a fallback for a *misbehaving
driver*, and routing our own bug through it produces three false statements: a WARN
blaming the driver for returning more jobs than requested; an attempt error reading
"worker shut down before this attempt finished" when no shutdown occurred; and a
consumed attempt that `requeue` deliberately does not give back, so ordinary jobs
burn their retry budget. So the defence-in-depth explanation is right about the
invariant and wrong about the consequences. Needed: assert that a multi-queue round
asks each queue for no more than the remaining capacity — the tracking driver in
`queues_test.go` already records the queues, so it only needs to record the limits
too — or assert the surplus warning is absent in normal operation.

**G-5 (Low-Medium) — Story 4 AC9 / SCHED-03: "decided by the database clock" is
unfalsifiable by the suite.** Moving the decision to the client (computing the state
in Go from `time.Now()` and passing it as a parameter) passes both the unit and the
Postgres integration suites, because the container's clock and the test process's
clock agree. This is genuinely hard to test without a skew harness; the property is
currently assured by code review of `queries.sql` alone. Either accept that
explicitly in the requirement record, or add a skewed-clock harness later.

**G-6 (Low) — Story 5 AC10: no test covers a job enqueued into a queue no client
works.** The behaviour follows from the per-queue fetch predicate and is hard to
falsify, but `tasks.md` claims T4.2 covers it and nothing does.

**G-7 (Low) — `TestMiddlewareSliceIsCopiedAtConstruction` does not prove what its
comment claims.** Removing `slices.Clone` from `checkedMiddleware` leaves the test
green, because the chain is composed eagerly at construction: a later append to the
caller's slice cannot reach an already-nested set of closures, and the surrounding
`append([]Middleware{Logging(...)}, ...)` reallocates in any case. AC10 therefore
holds — but structurally, not because of the copy, and the test's stated reasoning
("only a genuine copy survives it") is wrong. Either delete the misleading comment
or keep the copy as documented belt-and-braces and say so.

**Informational — the `remaining == 0` early break (M19).** Removing it costs one
extra fetch per remaining queue at a limit of zero, which claims nothing in either
driver. No criterion forbids it; noted only so a future reader knows it is unguarded.

**Pre-existing, out of scope —** `errShutdownRequeued` ("worker shut down before this
attempt finished") is recorded by `requeueAll` on every hand-back path, including the
surplus trim where no shutdown happened. That text predates this branch, but G-4
would amplify it, and it belongs on the list of things to correct when the requeue
reason gains a second caller.

## What else was checked

- **Goroutine lifecycle.** All eleven new job-level tests carry
  `goleak.VerifyNone(t, goleak.IgnoreCurrent())`. Five consecutive `-race` runs of the
  root package were clean, including `TestShutdownCancellationReachesAHandlerUnderATimeout`,
  which escalates a shutdown on an already-spent budget while a handler blocks under
  an hour-long timeout.
- **Races and deadlocks.** `runner.order` is written only by the fetch goroutine;
  `Client.chain` is composed once at construction and never rewritten, so the pool
  workers share it read-only. No new lock is taken across a channel send. `-race`
  is clean in both the unit and the integration suites.
- **Wrong-verdict / wrong-level hunt.** Deliberately hunted for the previous cycle's
  defect class — correct behaviour reported with the wrong verdict or level. Two
  instances found and reported above: G-3 (levels unasserted, so a regression is
  invisible) and G-4 (a real bug would be reported as a driver fault and written into
  the job's history as a shutdown). `Logging`'s WARN-not-ERROR choice, `writeFailed`'s
  lease-lost demotion, and `escalate`'s empty-snapshot check were all re-read and are
  correct as written.

---

## Disposition of the gaps (author, after verification)

Verification returned PASS with seven gaps. Six were closed by adding sensors; one is
accepted and recorded. No shipped behaviour was found wrong, so every change below is to
the suite or to a comment that misdescribed the code — except the removal of one redundant
line noted under G-7.

| Gap | Disposition |
| --- | --- |
| G-1 — a worker panic reaching middleware as an ordinary error was unsensed | **Fixed.** A test now asserts the chain *observed* the failure as a return value naming the panic, and that the logging middleware still emitted an end record. Deleting the inner recovery now fails it; previously the outer recovery masked the deletion. |
| G-2 — the unregistered-kind failure being raised inside the chain was unsensed | **Fixed.** A test asserts configured middleware runs, and sees the unregistered-kind error, for a job whose kind has no worker. Moving the check out to the caller now fails it. |
| G-3 — only the failure record's level was asserted | **Fixed.** The start and success records are now asserted at `INFO` as well as counted. |
| G-4 — each queue being asked only for the *remaining* capacity was unsensed | **Fixed.** A test drives the claim round over 200 orderings and asserts the limit passed to the queue visited after a partial claim. Asking every queue for the full capacity now fails it. |
| G-5 — dueness being decided by the database clock cannot be falsified while the test container and the client share a host clock | **Accepted, not fixed.** Proving it needs a container with a deliberately skewed clock. This is the same limitation already recorded against lease deadlines in the previous cycle, and it is the same fixture that would close both. Carried forward as known weak coverage rather than papered over with a test that cannot fail. |
| G-6 — a job in an unworked queue had no test | **Fixed.** A test asserts such a job is neither rejected nor claimed, stays `available`, and keeps `attempt` at zero, with a marker job proving the pool made a full pass. |
| G-7 — the copy test was inert, and its comment claimed the copy was load-bearing | **Fixed, and a line removed.** The verifier was right: the chain is composed eagerly, so the caller's slice is never retained and `slices.Clone` was doing nothing. The clone is gone; the function, its comment, the `Config.Middleware` doc, and the test now all say what actually holds. Keeping a defensive copy that looks like the reason would have been worse than removing it. |

### On the overlapping-protection reading

The verifier's correction is accepted. The surplus guard is *not* masked — an existing test
kills that mutation — so only one of the three protections was genuinely unsensed, and G-4
closes it. The wider point stands and is the more useful one: the path that would have
"corrected" a full-capacity claim does so by claiming rows and handing them straight back,
which warns about the driver for the pool's own mistake, records an attempt error naming a
shutdown that did not happen, and spends an attempt the hand-back never returns. A
correction that lies three times is not a protection worth relying on, which is why the
limit is now asserted at the point it is computed.

The pre-existing over-broad use of the shutdown-requeue reason on other hand-back paths is
noted and left alone: it predates this work and belongs to whichever cycle next touches the
hand-back, along with the batching already carried forward for it.
