# Middleware and Scheduling Cycle — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's
auto-decision rule. Each records the options considered with reasons for and against, the
choice, and why. They are mirrored into `.specs/STATE.md` as `AD-029`–`AD-037`.

Decisions already binding on this cycle and **not** relitigated here: ADR-0003 (the
per-job child context with timeout; named queues with weighted fetch), ADR-0004 (layout,
stdlib-first, `func(Handler) Handler` as the chain shape), AD-007 (boot-time programmer
errors panic), AD-017 (jitter and other randomness use `math/rand/v2` directly and are
asserted distributionally), AD-019 (the `attempt` fence), AD-020 (the database clock owns
time-based decisions), AD-021/AD-022 (one fetch loop feeding a fixed pool, claiming only
as many rows as there are idle workers), AD-027 (the handler's context may be cancelled;
the context recording an outcome never is).

---

## D-1 — What a middleware sees

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `Handler func(ctx context.Context, job *JobRow) error` over the existing public `JobRow` | `JobRow` is already exported, already returned by `Insert`, and already carries every field a middleware could want — id, kind, queue, attempt, max attempts, args, scheduled time. No new public type. One conversion from the driver row per execution. | Exposes fields a middleware has no business writing (it gets a pointer). Carries the full args payload even for middleware that only needs the id. |
| (b) A new minimal `JobInfo` type with only the metadata | Smallest possible read-only surface; no args payload copied. | A second public type describing the same row, differing from `JobRow` by omission. Users would have to learn which of two job types a given API hands them, and any field added later must be added twice. |
| (c) Keep the internal `driver.JobRow` and expose it | Zero conversion cost. | `internal/driver` cannot appear in an exported signature — the compiler forbids it for importers. Not an option in practice. |
| (d) Generic `Handler[T]` per job kind | Middleware could see decoded args. | Middleware is kind-agnostic by definition; a chain configured on the client cannot be generic over each registered kind. Would force a chain per registration and make cross-cutting concerns impossible to express once. |

**Chosen: (a).** The deciding argument is (b)'s cost over time: two public job types that
differ only by omission is a permanent tax on every future field, paid to avoid copying a
byte slice that is already in memory. The mutability objection is real but is answered by
documentation, the same way `net/http` documents that a handler must not retain its
request.

---

## D-2 — Where the chain is applied

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Around dispatch inside `runJob` | Middleware wraps the whole execution, so it also observes the failures that are not "a worker returned an error": an unregistered kind, a panic. One chain, composed once, shared by every kind. | `runJob` gains a step; the registered-worker lookup moves inside the innermost handler. |
| (b) At `Register` time, wrapping each registered worker | The wrapping is visible where the worker is declared. | `Register` is a package-level function over `*Workers`; the registry is built before and independently of the `Config` that carries the middleware, so it has no access to the chain. Would require either a client-aware registry or a second registration pass. Worse, an unregistered kind never reaches a registered handler, so no middleware could observe it — and the unregistered-kind path is exactly the one an operator needs logged during a rolling deploy (AD-014). |
| (c) Both — a client chain and a per-worker chain | Maximum flexibility. | Two ordering rules to document and reason about, for a capability nobody asked for. The roadmap row says "a chain", singular. |

**Chosen: (a).** Option (b) is disqualified on capability, not taste: the failure modes
most worth wrapping are the ones that never reach a registered worker.

---

## D-3 — Chain order

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Index 0 is outermost | Matches `net/http`, `chi`, `grpc-go`, and every Go middleware convention a user has already internalised. Reads top-to-bottom in the order requests flow. | None of consequence. |
| (b) Index 0 is innermost | Composition by successive wrapping falls out of a left fold with no reversal. | Reads backwards against every other Go library. Saves one `slices.Backward` at construction and costs every reader a moment of doubt forever. |

**Chosen: (a).** This is a convention question and the convention is unambiguous.

---

## D-4 — Panic containment

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Recover innermost (around the registered worker) *and* outermost (around the whole chain) | Middleware always observes a worker panic as an ordinary non-nil error, so a logging or metrics middleware reports it like any other failure (AD-013 already makes a recovered panic a retryable failure). The outer recovery means a bug in a user's middleware cannot kill a pool worker goroutine and silently shrink the pool. | Two deferred recovers per job instead of one. |
| (b) Recover only innermost (today's placement) | One recover; smallest change. | A panicking middleware unwinds past `runJob` and kills its pool goroutine. The pool has no supervisor that notices, so concurrency silently drops by one per occurrence until the pool is empty and the client stops working jobs while still reporting itself running. |
| (c) Recover only outermost | One recover, and it covers everything. | Middleware then observes a panic as an unwinding stack rather than a return value, so no middleware can log, time, or count a panicking job — the failures most worth observing become the ones nothing observes. |

**Chosen: (a).** The cost is one extra `defer` on a path that already does a database write
per job; the benefit is that the pool cannot be silently eroded by a third-party
middleware, and that panics stay visible to the observability the chain exists to carry.

---

## D-5 — How the built-in logging relates to the chain

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) The client always installs `Logging(c.logger)` as the outermost middleware, with `Config.Middleware` nested inside it | Job logging never disappears because a user configured a middleware. `Logging` is still exported, so it is both the implementation and the reference example. Being outermost, its duration covers the configured middleware, which is what an operator wants to know. | The user cannot remove or reposition the built-in logging without a config field this cycle does not add. |
| (b) `Logging` ships exported and the client installs nothing; the default chain is empty | Purest composition; the user gets exactly the chain they wrote. | Silently changes the default experience: a client that logs each job today would stop the moment a user adds an unrelated middleware — or, if the default chain is `[Logging]` only when `Config.Middleware` is nil, stops the moment they add one. Both are a trapdoor. |
| (c) Leave the existing log lines where they are in `runJob` and ship `Logging` as an additional opt-in | No behaviour change at all. | A user who adds `Logging` gets two "job started" lines. Ships a middleware whose only correct use is by someone who has read the source to learn it duplicates a built-in. |

**Chosen: (a).** Rejecting (b) is the substantive call: a configuration change that silently
removes logging is exactly the kind of quiet degradation that gets noticed during an
incident.

**Consequence carried into the design.** The two kinds of record are separated by meaning
rather than merged. The middleware reports the *execution* — it started, it took this
long, it returned this error. `dispose` and `runJob` keep reporting the *outcome* — the
job was completed, a retry was scheduled, it is dead, cancelled, snoozed. Only the
existing "job started" line moves into the middleware, because it is the one line that
describes an execution rather than an outcome. Every outcome line stays exactly where the
decision is made, so no line is duplicated and each means one thing.

---

## D-6 — The shape of the insert options

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `Insert(ctx, args, opts *InsertOpts)`, nil meaning defaults | One exported type for every present and future insert-time option (Cycle G's batch hints, Cycle H's uniqueness). Matches River, so the shape is familiar. Explicit at the call site: `nil` says "no options" out loud. | Breaks every existing call site — they must pass `nil`. Every call site not using options is noisier by four characters. |
| (b) `Insert(ctx, args, opts ...InsertOpts)` | Existing call sites keep compiling untouched; the common case stays `Insert(ctx, args)`. | The signature permits an arity the semantics do not: what two option structs mean has to be answered by documentation, a panic, or a silent last-wins rule. All three are worse than a type system that cannot express the question. |
| (c) Functional options: `Insert(ctx, args, drover.Queue("high"), drover.ScheduledAt(t))` | Type-safe, extensible, no arity ambiguity, idiomatic. | Adds one exported constructor per option, growing the package's exported surface for each future option — against ADR-0001's stated value of a surface small enough to audit in one sitting. Also harder for a caller to build options dynamically from configuration. |

**Chosen: (a).** Pre-1.0 with no released version, so breaking existing call sites costs
nothing outside this repository — the same reasoning that let AD-023 change the default
concurrency. Given that the cost is zero, the tie-breaker is which signature will still be
right at Cycle H, and that is the one where a new option is a struct field rather than a
new exported identifier.

---

## D-7 — Which clock decides that a delayed job is due

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) The database: the insert statement compares the requested time to `now()` and sets the state accordingly | AD-020 already settled that the database clock owns lease expiry, for the reason that a fleet's clocks do not agree. Dueness is the same kind of decision compared against the same kind of clock — the fetch predicate is `scheduled_at <= now()`, evaluated in the database. Keeping the state consistent with the predicate means one clock decides both. | The insert statement carries a `CASE`, so the state is not visible as a literal in the query. |
| (b) The client: compare `ScheduledAt` to `time.Now()` in Go and send the resulting state | The state is decided in readable Go with a plain test. | A client running ahead of the database would insert a job as `available` that the fetch predicate still holds back, and one running behind would insert as `scheduled` a job the predicate would happily claim. The state and the predicate would disagree about the same row, which is precisely the class of bug AD-020 exists to prevent. |
| (c) Do not set the state at all — leave every job `available` and let `scheduled_at` alone hold it back | Simplest; the fetch predicate already covers it, so behaviour would be correct today. | The row then lies about itself: a job that will not run for a day reports `available`. Cycle F's `jobs list` and Cycle I's status page both read that column, so the lie is one an operator will be shown. |

**Chosen: (a).** (c) is the tempting one because it is genuinely correct for fetching, and
it is rejected on honesty rather than correctness: the state column is an operator-facing
fact, and the cycles that display it are already on the roadmap.

---

## D-8 — How the fetch loop chooses among queues

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Weighted sampling without replacement produces a full ordering of the queues each round; the loop tries them in that order until the round's capacity is filled or the list is exhausted | Starvation-free by construction: every queue has a non-zero chance of any position, and every queue is tried within every round regardless. An empty high-weight queue costs one extra query rather than a whole poll interval of latency for the queues behind it. Needs no driver change — `FetchAvailable` already takes one queue name, and the existing partial index serves that equality. | More than one query per round when the earlier queues are empty. Selection is probabilistic, so its correctness must be asserted distributionally. |
| (b) Sample one queue per round by weight and fetch only that one | One query per round, always. Simplest possible implementation. | A round that samples an empty queue does nothing and sleeps a full poll interval while another queue has work. With a strongly skewed weighting the low-weight queues are reached rarely, so their latency becomes a function of the *other* queues' emptiness. |
| (c) One `IN (...)` fetch across all queues, ordered by a weight expression in SQL | One query per round regardless of queue count. | Turns the index's queue equality into a range scan; the existing `(queue, scheduled_at, id)` partial index no longer serves the ordering, so a new index and a migration are needed and the plan must be re-validated. Priority becomes a SQL expression that is hard to make genuinely probabilistic, and weighted-random-in-SQL is the kind of cleverness that is unexplainable in a review — against the project's stated reason to exist. |
| (d) One runner per queue, each with its own pool sized by weight | Reuses the existing runner wholesale; the Cycle C source comment anticipates it. | Weights become a static allocation of goroutines rather than fetch preference, so workers assigned to an idle queue sit unused while another queue backs up. Multiplies polling, heartbeat, and rescuer machinery by the queue count. Contradicts ADR-0003's "weighted random fetch" and the handoff note that named queues extend the single pool. |

**Chosen: (a).** Option (d) deserves explicit burial because a comment in `pool.go` from
the previous cycle guesses at it ("a second queue is a second runner rather than a
rewrite"). That was a forward guess made before this cycle's constraints were written
down, not a recorded decision; ADR-0003 says weighted *fetch*, and the Cycle C handoff
says named queues extend the single pool. A shared pool is also the better behaviour: it
lets one busy queue use the whole pool when the others are idle, which a static per-queue
split cannot. The comment is corrected as part of this cycle.

---

## D-9 — How invalid configuration is reported

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Structural errors panic at construction (nil middleware entry, empty queue name); tuning values warn and are corrected (non-positive weight) | AD-007 already made a boot-time programmer error panic, so this is the established convention rather than a new one. Panics happen at process start, in the same place and at the same time as `Register`'s. Leaves `newClient`, the driver-injection seam the entire unit suite is built on, with its current signature. | A library that panics is contentious, and the caller cannot recover to fall back to a default configuration. |
| (b) Return every validation failure from `NewClient` as an error | Conventional; the caller decides what to do. | `NewClient` already returns an error, but `newClient` — the seam used by essentially every unit test — does not, so this either changes that signature and churns the suite, or splits validation across two constructors and lets the tested path skip it. |
| (c) Correct everything silently, warning where possible | Nothing can ever fail to start. | A nil middleware entry silently skipped is a middleware the user believes is running. That is the failure mode a queue can least afford: the timeout that is not applied looks exactly like a timeout that never fires. |

**Chosen: (a).** The split is deliberate and follows the existing code: a *structural*
mistake makes the configuration unrunnable as written and is caught loudly at boot, while
a *tuning* mistake has an obvious safe reading and follows the `HeartbeatInterval`
precedent of warning and substituting a sane value. Rejecting (c) is the substantive call —
silently dropping a nil middleware would make a missing timeout indistinguishable from a
timeout that has not fired yet.
