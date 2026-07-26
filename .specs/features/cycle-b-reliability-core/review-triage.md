# Cycle B — Review Triage

**PR**: #3 · **Head**: `49b7734` · **Reviewers**: 6 independent agents (tests, requirements, concurrency, sql-performance, architecture, regression)
**Findings**: 20 inline + 1 requirements summary
**Triaged by**: orchestrator, judging each finding against the code as it is — not against the reviewer's authority.

Two findings were verified empirically before triage rather than accepted on assertion:
`time.Duration(1)/3 == 0` and `time.NewTicker(0)` panics (confirmed by running it); and all five
`Mark*` transitions in **both** drivers guard on `state = 'running'` alone, with no ownership fence
(confirmed by reading the SQL and the memdriver guards).

## Verdicts

| # | Comment | File:line | Verdict | Action | Rationale |
|---|---|---|---|---|---|
| 1 | 3651642873 | `loop.go:43` | real | fix | Deleting `go c.rescueLoop(ctx)` passes the whole Docker-free suite. Same class as the surviving mutant validation already caught: the one unit test starting a real loop asserts a job is *not* rescued, so it passes trivially when the rescuer never runs. The heartbeat's equivalent wiring *is* pinned — this is an asymmetry, not a policy. |
| 2 | 3651642904 | `loop.go:194` | real | fix | `NextRetry`'s doc promises kind/queue/attempt/errors are in scope; every test double discards the argument, so passing a wrong-shaped row is invisible. |
| 3 | 3651642941 | `errors.go:24` | real | fix | `Cancel(nil)` is a plausible call on new exported API and its branch is unexercised. Cheap. |
| 4 | 3651644107 | `002_*.sql:8` | real | fix | Index is `(queue, scheduled_at, id)`; with `queue` equality and `scheduled_at` a range it yields `(scheduled_at, id)` order, so `ORDER BY id` cannot terminate the scan at `LIMIT`. This PR enlarges the sorted set by admitting two more states. Fix in **both** drivers — memdriver orders by id too, and divergence would break driver substitutability. |
| 5 | 3651644117 + 3651645012 | `002_*.sql:5` | real | fix | Duplicate finding from two reviewers. `DROP INDEX` takes `ACCESS EXCLUSIVE` and the transactional runner holds it through both builds, stalling every enqueue and claim; clients call `Migrate` at startup. Create-then-drop, plus `IF EXISTS` so the migration survives an operator having already dropped the index. |
| 6 | 3651644135 | `queries.sql:33` | real | fix | Same class as #4 for the sweep: `ORDER BY id` cannot use `(leased_until)`. Oldest-lease-first is also the better recovery order. |
| 7 | 3651644438 | `rescue.go:68` | real | fix | `FetchExpired` has already committed fresh leases when cancellation lands, so every remaining `dispose` fails and those rows end up *less* recoverable than before the sweep — leased for a full duration with nobody running them. The fetch loop already solves the identical problem with `context.WithoutCancel`; the sweep should be symmetric. |
| 8 | 3651644454 | `e2e_*.go:372` | real | fix | 150ms lease / 40ms heartbeat over a real container under `-race` asserts no round trip is ever delayed >110ms across four windows. Its failure mode reports a production bug that did not happen, which is how tests get muted. |
| 9 | 3651644463 | `retry.go:16` | real | fix | `NextRetry` is called from the fetch loop and the sweep concurrently. Go convention puts the "safe for concurrent use" statement on the library for an extension point it calls on its own schedule. Doc-only. |
| 10 | 3651644907 | `rescue.go:19` | real | fix | `dispose` persists `jobErr.Error()` verbatim into the job's `errors` array, so an unprefixed string sits next to `"drover: ..."` entries and reads as if it came from the handler. |
| 11 | 3651644925 | `errors.go:36` | real | fix | `Cancel` ships an `errors.Is`-testable sentinel; `Snooze` ships nothing exported, so no user middleware or test can classify a deferral. `classifyOutcome` is unexported, so there is no alternative. |
| 12 | 3651644961 | `retry.go:16` | real | fix | Context-first is a locked toolchain idiom and this is the one public departure. It is a user-implemented interface, so adding the parameter after release is breaking; pre-release it is free. |
| 13 | 3651644969 | `client.go:127` | real | fix | The documented contract is "zero values get defaults" — silently discarding a value the caller *set* is different, and its production symptom is duplicate execution. The logger is already in hand. Keep the clamp, log the rejection. |
| 14 | 3651644989 | `loop.go:141` | real | fix | Two layers of `...any` on the single state-machine decision point, for one call shape. A typed parameter says what it is. |
| 15 | 3651646494 | `client.go:128` | real | fix | Verified: a `LeaseDuration` under 3ns passes the `<= 0` check, integer-divides to a zero heartbeat, and panics an unrecovered goroutine. Absurd input, but the config test explicitly promises no setting produces an unusable value. |
| 16 | 5081788005 (a) | `.specs/*`, `STATE.md` | real | fix | Task rows unchecked, traceability still `Pending`, and `STATE.md` says both "in progress" and "COMPLETE" in one file. Stale internal records mislead the next session. |
| 17 | 5081788005 (b) | `client.go` | real | fix | `Config`'s doc comment lists the defaults and omits `RescueInterval`, added last. |
| 18 | 3651644145 | `rescue.go:68` | real | **partial** | The N+1 is genuine, but a batched `unnest` update is fetch-path tuning, which the roadmap's benchmarking-and-hardening cycle owns. Take the cheap half now — cap the rounds one tick may run, so a large backlog cannot monopolise a pool connection the heartbeat needs. Record the batch update as follow-up. |
| 19 | 3651646712 | `002_*.sql:9` | real | **defer** | The waiting-state set is genuinely restated in three places and the new index assertion checks a hardcoded list rather than the query. But no defect exists today, and the proposed `EXPLAIN`-based assertion is its own piece of test machinery. Recorded as follow-up rather than grown into this PR. |
| 20 | 3651644368 | `driver.go:63` | **real — critical** | see scope note | Guards prove liveness, not ownership. Confirmed by inspection. This PR creates the reachability, because before it nothing ever un-claimed a running job. |
| 21 | 3651644396 | `queries.sql:32` | **real** | see scope note | Leases are written from the app clock and enforced against Postgres `now()`. Cycle A wrote the lease without enforcing it, so the clocks never had to agree; this PR makes the skew load-bearing. |

**Rejected: none.** Every finding held up against the code. Two reviewers independently reported the
migration lock (#5), which is corroboration rather than duplication.

## Scope note — findings 20 and 21

Both are real correctness defects in this cycle's own subject matter, and both are reachable only
because this cycle shipped the rescuer. They are also larger than the rest.

**20 — ownership fence.** `attempt` already works as a fence token: `FetchAvailable` increments it and
`FetchExpired` deliberately does not. Adding `AND attempt = @attempt` to the five transitions and to
`ExtendLeases` closes the harmful window (a stale worker finalizing a *later* attempt's outcome). The
narrower window where the sweep and the original worker share an attempt number stays open, but is
benign — the original worker genuinely did the work. Cost: six method signatures, both drivers, sqlc
regeneration, `inflightSet` carrying `(id, attempt)` instead of ids, and tests. No migration.

**21 — clock authority.** Pass the lease *duration* and compute `now() + interval` server-side. The
caller-supplies-the-lease decision this cycle made is still right; only the absolute instant needs to
come from the database. Cost: three query signatures and their drivers.

Shipping without them leaves the delivery contract claiming every duplicate source is "named and
bounded" while an unnamed one exists. Fixing them expands a PR the project constraints say should
stay PR-sized.

**Maintainer decision**: ship this cycle with the 19 fixes, and name the ownership window in the
package documentation as a current limitation so nothing on `main` claims more than it delivers.
Findings 20 and 21 become a dedicated hardening PR opened **immediately after this one merges, in the
same working session**, and before the concurrency cycle — that cycle's worker pool multiplies the
number of workers that can reach the ownership window, so it must not land first.
