# Review Triage — PR #6

Independent review by six fresh-context agents produced 18 inline findings and one consolidated summary. Each was checked against the code as it exists, not accepted on the reviewer's authority. The PR comments are deleted after this triage, so this file is the surviving record.

**Outcome: 17 real and accepted, 1 real but deferred, 0 rejected.**

The review was strong. Its central finding — that a bounded shutdown logs an `ERROR` per job for the outcome it deliberately chose — is a genuine defect that the cycle's own verification passes missed, because no test asserted on log level for that path.

| # | Comment | Location | Verdict | Action | Rationale |
|---|---|---|---|---|---|
| 1 | `3653073368` | `loop.go:122` | **Real** | Fix | `writeFailed` demotes only `ErrLeaseLost`. Both drivers check **state before attempt** (`memdriver.go` `transition`, `pgdriver.go` `finalizeFailure`, both commented "State first"), so a row this shutdown moved to `retryable` yields `ErrInvalidTransition`. Verified directly. Every escalated shutdown therefore emits one ERROR per requeued job describing its own designed behaviour. Duplicate of #17. |
| 2 | `3653073914` | `loop.go:38` | **Real** | Fix | `Start`'s doc promises `ErrAlreadyStarted` after a completed `Stop`; only the already-running half is tested. Nilling `c.runner` in `Stop` would silently make the client restartable with the suite green. |
| 3 | `3653073930` | `pool.go:283` | **Real** | Fix | `requeueFailed`'s Debug-vs-Error split has no sensor. `TestOneFailedHandBackDoesNotStopTheRest` drives the Error branch and asserts only row state. Dropping the `errors.Is` check leaves the suite green. |
| 4 | `3653074024` | `pool_test.go:412` | **Real** | Fix | `stubbornWorker` and `blockingWorker` are byte-identical while their doc comments claim different behaviour. The "ignores cancellation" property is incidental, never asserted. |
| 5 | `3653074045` | `go.mod:64` | **Real** | Fix | `go mod tidy` carried `x/sync` v0.20.0 → v0.21.0 alongside the deliberate `x/text` bump. Indirect and harmless, but unaccounted for in the PR body. |
| 6 | `3653074400` | `README.md:48` | **Real** | Fix | The quickstart uses `client.InsertTx` before `client, err := drover.NewClient(...)`. Verified: `InsertTx` at line 45, construction at line 49. Duplicate of #15. |
| 7 | `3653074718` | `loop.go:108` | **Real** | Fix | The unregistered-kind branch's `finalizeContext` is unsensed — that path is covered only against bare `memdriver`, which ignores context. Reverting it to `jobCtx` passes the suite. Mid-deploy is when unregistered kinds *and* shutdown escalation both occur, so the conditions correlate. |
| 8 | `3653074737` | `pool.go:264` | **Real** | **Won't fix here** | `requeueAll` is N serial round trips, doubled by `pgdriver.finalizeFailure`'s `GetJob` probe on the common race-loss. The proposed batched `RequeueMany` needs a new `.sql` query plus `sqlc` regeneration and a driver-interface change — real scope growth in an already-large cycle, and performance hardening is a later cycle's stated subject. The *symptom* the reviewer cares about (shutdown overrunning its budget) is addressed under #18. Recorded as follow-up work. |
| 9 | `3653074743` | `pool_test.go:773` | **Real** | Fix | A 50 ms wall-clock sleep establishes the precondition in the one test where elapsed time *is* the assertion. On a loaded runner the fetch loop may not have reached `sleep()`, so the test passes without exercising anything. `countingDriver` + `waitFor` gates on the observable instead. |
| 10 | `3653074757` | `client.go:70` | **Real** | Fix | "may safely exceed the connection count" overstates it. The running-job claim is correct, but finalizes arrive in bursts and the heartbeat competes for the same connections — a starvation mode `rescue.go` already names. Softened to sizing guidance. |
| 11 | `3653074806` | `pool.go:221` | **Real** | Fix | `drain`'s `select` picks randomly when `finished` and `ctx.Done()` are both ready, so a fully drained pool can return `ErrDrainIncomplete` with `0 job(s)` — contradicting both the log line and `Stop`'s documented contract. A caller exiting non-zero on error would flakily fail a clean shutdown. |
| 12 | `3653074818` | `examples/email/main.go:9` | **Real** | Fix | ADR-0004 is the layout authority and enumerates three locations while the tree now has four. The decision was recorded in this cycle's `context.md` but never promoted to the ADR, which the workflow requires. |
| 13 | `3653074832` | `examples/email/main.go:122` | **Real** | Fix | The example logs an incomplete drain and returns nil, so the process exits 0 on precisely the shutdown it exists to demonstrate. A reader copying the shape gets a container reporting success to its orchestrator. |
| 14 | `3653074857` | `examples/email/README.md:35` | **Real** | Fix | "attempt¹ wait" states the wrong exponent; the policy is `attempt⁴`. Right only because 1⁴ = 1¹; a reader would predict ~2s instead of ~16s for the second retry. |
| 15 | `3653074875` | `README.md:50` | **Real** | Fix | Duplicate of #6; fixed together. |
| 16 | `3653076833` | `loop.go:49` | **Real** | Fix | `c.mu` is released before `r.start`, so a `Stop` landing in that window drains a runner with zero goroutines, returns `nil`, and leaves a client that reports success but processes nothing. `r.start` then calls `Add` on WaitGroups whose `Wait` has returned — or is still inside it, which is the documented `WaitGroup misuse` panic. Verified no goroutine spawned by `start` takes `c.mu`, so holding it across the call cannot deadlock. |
| 17 | `3653076972` | `pool.go:283` | **Real** | Fix | Duplicate of #1, argued from the other side of the same asymmetry. Fixed together. |
| 18 | `3653077121` | `pool.go:261` | **Real** | Fix (partial) | `Stop` documents `ctx` as the bound, but `requeueAll` takes a fresh `leaseDuration` *after* that budget is spent, shared across the whole batch, so one wedged write starves the tail. Fixed by giving each hand-back its own bound and documenting the real guarantee. The batching half is #8's deferred work. |

## Requirements review (PR-level comment)

All fifteen requirements were found implemented and sensed. Three deferrals were flagged for visibility rather than as defects; all three are accepted and actioned, because the objection in each case is that the deferral is invisible where the decision lives.

| # | Finding | Verdict | Action |
|---|---|---|---|
| 19 | The roadmap's cycle row still promises "per-queue worker pools" while one pool ships on the default queue | **Real** | Fix — the scope was deliberate and recorded in this feature's spec, but the public roadmap read as though queues shipped. The row now says what actually lands and where named queues arrive. |
| 20 | ADR-0003 specifies a "per-job child context **with timeout**"; only the cancellable half ships | **Real** | Fix — the timeout belongs to the middleware chain in a later cycle, which the spec records but the ADR did not. Noted in the ADR. |
| 21 | `validation.md` records a commit range predating the docs and dependency commits, so its "`go.mod` diff empty" evidence is stale | **Real** | Fix — the report now states which commits its PASS covers and restates the dependency evidence accurately. A verification report that quietly overstates its own coverage is worse than one that admits its edges. |

## Notes

- Findings #1/#17 and #6/#15 are the same defect reported by different reviewers; each pair is fixed once.
- Nothing was rejected. Two findings were argued from an incorrect premise nowhere in the set, and every technical claim spot-checked (driver state/attempt ordering, README line ordering, the example's exit path, the retry exponent) held up.
- The reviewer flagged that no test in this cycle's lifecycle suite uses `testing/synctest`, which `CLAUDE.md` asks for on time-dependent logic. That is a fair observation and is recorded as follow-up rather than actioned here: the tests that most want a bubble are the ones deliberately leaving handlers running past `Stop`, which a bubble will not tolerate.

## Follow-up work recorded (not in this PR)

1. Batch the shutdown hand-back into a single `unnest`-based statement, removing N serial round trips and the per-lease `GetJob` probe (#8).
2. Revisit the lifecycle tests for `testing/synctest` where the handler lifetime allows it.
