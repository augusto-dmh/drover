# Review triage — middleware and scheduling (PR #8)

Six independent reviewers, working from the diff with no access to this cycle's reasoning,
posted 20 inline findings plus a requirements summary. Four pairs/triples are the same
finding reached independently, leaving **15 distinct findings**.

Every finding was checked against the code as it exists. Verdicts are judged on the code,
not on the reviewer's authority — but the reverse discipline matters more here, because
three of these contradict something this cycle asserted confidently, and being contradicted
by evidence is not a reason to look for an excuse.

**Outcome: 14 fixed, 1 rejected with reason.**

## Duplicates collapsed

| Kept | Duplicates | Same finding |
| --- | --- | --- |
| R-01 | 3699644648, 3699642569 | README quickstart does not compile |
| R-02 | 3699642158, 3699644697, 3699644962 | vacuous `time.Since(due)` assertion |
| R-04 | 3699644323, 3699644751 | int64 sum does not prevent the overflow it claims |
| R-07 | 3699641588, 3699648087 | `dst` reuse defeated by an unconditional allocation |

## Findings

| ID | Source | Location | Verdict | Action | Rationale |
| --- | --- | --- | --- | --- | --- |
| R-01 | 3699644648 🚨 | `README.md:59` | **Real** | Fix | `InsertTx` returns `(*JobRow, error)`; the line assigns to one variable, so the quickstart does not compile. Worse, the PR body claims this was corrected — the documentation pass fixed the argument count and left the arity, and the claim was taken on trust. The PR body is corrected too. |
| R-02 | 3699644697 ⚠️ | `client_test.go:323` | **Real** | Fix | `time.Since(due)` is evaluated after the job completed and after the pool stopped, so it reports how long ago `due` passed, never when the job ran. It cannot fail under any implementation. Replaced with the worker recording its own run time. The neighbouring `mem.Row` discarded its `ok`, which would nil-panic instead of failing readably; also fixed. |
| R-03 | 3699643354 ⚠️ | `loop.go:141` | **Real** | Fix | The stated failure mode is factually wrong about Go. An unrecovered panic in any goroutine terminates the **process**; it does not retire one worker and leave the pool running short. There is no interleaving producing the described degradation. The recover is still right — the true justification is stronger than the invented one — but this cycle repeated the wrong version in a code comment, a commit message, and the PR body. Corrected in the comment and the PR body; the commit message stands as history with the correction recorded here. |
| R-04 | 3699644323 💡 | `queues.go:85` | **Real** | Fix | `int` is 64 bits on every 64-bit build, so widening to `int64` is a no-op and the total overflows on exactly the input the comment claims it defends against. The consequence is not a skewed ordering: `rand.Int64N` panics on a non-positive bound, on the fetch goroutine, which has nothing above it to recover — so an absurd-but-legal config terminates the process on the first round. Fixed by clamping the weight in `checkedQueues`, which makes the overflow unreachable by construction rather than merely commented about. |
| R-05 | 3699642575 ⚠️ | `pool.go:438` | **Real** | Fix | The strongest finding. `inflight` documents that a lease is tracked "from the moment the job is claimed", and the fetch loop restates it — but `inflight.add` happens in the dispatch loop after the whole round returns. Rows claimed from the first queue now hold a live, unheartbeated lease across up to N−1 further claim round-trips, and `escalate`'s snapshot cannot see them. Single-queue clients are unaffected, so this appears only in the configuration this cycle exists to enable. Tracking moved into the claim itself. |
| R-06 | 3699640960 ⚠️ | `client.go:256` | **Real, but rejected** | Won't fix | See below. |
| R-07 | 3699648087 💡 | `queues.go:92` | **Real** | Fix | The `dst` parameter exists to avoid a per-round allocation, and the next line allocates a slice of the same length regardless, so the reuse removed one of two allocations while the doc claimed it removed the cost. Fixed by swap-removing over a runner-owned scratch buffer, which also drops `slices.Delete`'s O(n²) shifting. The distribution is unchanged: the accumulator walks whatever order the remaining set is in. |
| R-08 | 3699642086 ⚠️ | `middleware_test.go` | **Real** | Fix | Three chain-semantics edge cases the spec states have no sensor: a middleware swallowing the worker's error must complete the job (the chain's return value is the verdict — the property `Middleware`'s doc promises), calling `next` more than once, and the same middleware value appearing twice. |
| R-09 | 3699642125 💡 | `middleware.go:126` | **Real** | Fix | `Logging(nil)`'s documented fallback to `slog.Default()` is never exercised; deleting it would turn the first job into a nil dereference with the suite green. `Logging` is exported precisely so callers install it themselves, which makes this the path an outside user hits first. |
| R-10 | 3699642183 ⚠️ | `queues_test.go:329` | **Real** | Fix | The test asserts only that *some* query happened and that no other queue was asked. A loop issuing ten queries per round, or one that dropped `sleep` and spun, passes unchanged — yet both are stated properties. Added an anti-spin upper bound. |
| R-11 | 3699642214 ⚠️ | `pool.go:430` | **Real** | Fix | The surplus clamp is exercised only single-queue, where `remaining` always equals the full capacity, so reverting it to the round's original capacity keeps every test green while breaking the capacity invariant across queues. The two new round-level tests do not reach it either, because memdriver honours its limit. Covered with the existing overshoot driver at round level. |
| R-12 | 3699642526 💡 | `examples/email/main.go:96` | **Real** | Fix | The example is a teaching surface. `sync.Map` fits neither of its documented cases here and costs three unchecked `any` assertions over a key set known at construction. A mutex-guarded map keeps the example's actual subject — the middleware shape — in front. |
| R-13 | 3699643372 💡 | `internal/memdriver/memdriver.go:52` | **Real** | Fix | The doc comment still says "stores a new available job" in the one change that makes it sometimes scheduled. |
| R-14 | 3699648065 ⚡ | `pool.go:416` | **Real** | Fix (doc) | An idle client now issues up to one statement per configured queue per poll, where it issued one. The queries are individually cheap and an empty UPDATE assigns no xid, so this is round trips rather than database work — an acceptable price for starvation-freedom, but it scales with a map the config invites callers to grow, and it was documented nowhere. Noted on `Config.Queues`. |
| R-15 | 3699648117 ⚠️ | `internal/pgdriver/queries.sql:12` | **Real** | Fix (doc) | Subtle and correct: `now()` is `transaction_timestamp()`, so on the `InsertTx` path it is the caller's transaction start, not the insert. A job enqueued late in a long transaction can be stored `scheduled` while already due. Delivery is unaffected — the fetch predicate claims it on the next poll — but the comment claims "the same clock" as the reason for deciding state in the database, and that claim is what needs narrowing. The reviewer is also right that `clock_timestamp()` would be worse: being volatile, its two occurrences would evaluate at different instants and reintroduce a genuine disagreement. Code unchanged; claim narrowed. |

## R-06 — rejected, with the reasoning

**The finding:** `newClient` panics for a nil `Middleware` element and an empty `Queues` key,
while `NewClient` returns an error for a nil pool. One constructor therefore reports one
class of caller mistake as a value and another by killing the process, and a caller wiring
`Config` from YAML or a plugin registry cannot recover from the second.

**Why it is not being changed here.** The observation is accurate and the inconsistency is
real. It is declined on two grounds, neither of which is that the fix is inconvenient.

First, it is the recorded decision for this cycle, taken with this option explicitly on the
table and rejected for a stated reason. Re-deciding it inside a review of an unrelated
change is how a recorded decision becomes an unrecorded one.

Second — and this is the substantive half — the codebase already panics for exactly this
class of mistake. `Register` panics on a duplicate kind, and the argument that it "returns
nothing so it has no alternative" cuts the other way: it establishes that a boot-time
programmer error in drover is a panic, and a caller who has internalised that from
`Register` would be surprised to find `Config` validation reporting the same kind of
mistake differently. Consistency within the library is worth more than consistency with a
general Go convention that the library has already, deliberately, departed from.

What the decision record got wrong is its *rationale*: it justified the choice partly by
noting that returning errors would change `newClient`'s signature and churn the unit suite.
That is a cost of the alternative, not a reason the choice is right, and it should not have
been load-bearing. The decision stands on the consistency argument alone.

**Carried forward, not dropped.** If the constructor contract is revisited before 1.0 — the
natural moment being whenever `Config` validation grows past two cases — this should be
reconsidered as a whole rather than per-field, and `Register` reconsidered with it. Recorded
here rather than left to be rediscovered.

## Note on the review itself

Three findings (R-01, R-03, R-15) each contradict something this cycle stated as fact: that
the README was fixed, that an unrecovered panic shrinks the pool, and that the insert
decides dueness on "the same clock" the fetch predicate uses. None were caught by
verification, because the code passes its tests in all three cases — what was wrong was the
explanation attached to it. That is the same shape the previous cycle's review found, and it
is the argument for keeping review independent of the author's reasoning: a reviewer given
the rationale checks the code against it, while a reviewer given only the code checks the
rationale against reality.
