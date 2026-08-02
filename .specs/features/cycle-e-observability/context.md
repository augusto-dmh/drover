# Observability Cycle — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's
auto-decision rule. Each records the options considered with reasons for and against, the
choice, and why. They are mirrored into `.specs/STATE.md` as `AD-038`–`AD-048`.

Decisions already binding on this cycle and **not** relitigated here: ADR-0002 (a narrow
storage interface with an in-memory adapter for tests — relevant to D-4), ADR-0003
(Prometheus via `promhttp` on an ops port, oldest-job-age as the primary alerting metric),
ADR-0004 (stdlib-first, single module, `*slog.Logger` injected via config), AD-007 and
AD-037 (structural configuration errors panic; tuning values warn and are corrected),
AD-020 and AD-035 (the database clock owns every time comparison), AD-024 (`Start` returns
once running, `Stop` blocks until drained, single-use lifecycle), AD-030 and AD-033 (the
chain is applied around dispatch; the client installs `Logging` ahead of user middleware).

---

## D-1 — Where the per-execution metrics are recorded

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) A `Metrics` middleware the client installs, immediately inside `Logging` and ahead of `Config.Middleware` | The Cycle D handoff names the chain as where per-job metrics belong, and AD-030 already put the chain where it can observe the failures that never reach a registered worker — an unregistered kind, a panic. Being inside `Logging` conforms to AD-033 without amending it. Being ahead of user middleware means configuring middleware cannot silently disable metrics, the same trapdoor AD-033 closed for logging. | The middleware observes an *execution*, not a job's final disposition — it cannot distinguish a retry from a death. |
| (b) Instrument `dispose` (`loop.go:207`), the single outcome-decision point | `dispose` sees the true terminal disposition — snoozed, cancelled, dead, retryable — and already receives `ran time.Duration`. Both the worker loop and the rescuer pass through it, so nothing is missed. | Contradicts the recorded handoff constraint. More importantly it re-instruments the execution path the chain exists to keep clean, and it cannot see a panicking *middleware*, which the outer recover at `loop.go:145` converts before `dispose` is reached. |
| (c) Both — middleware for duration, `dispose` for outcome counters | Each fact recorded where it is most accurate. | Two instrumentation sites for one cycle's metrics, with two different notions of "a job finished" that an operator would have to reconcile when the numbers disagree. |
| (d) A middleware the *user* installs from an exported constructor, with nothing installed by default | Purest composition; zero cost for a user who does not want metrics. | Metrics that are off by default are metrics nobody has during the first incident. Same argument AD-033 already settled for logging. |

**Chosen: (a).** The deciding argument against (b) is not the handoff note but coverage: the
outer panic recover means a middleware that panics is converted to an error *above*
`dispose`'s caller, so instrumenting `dispose` would miss precisely the failure a
third-party middleware introduces. The cost of (a) — that the counters describe executions
rather than dispositions — is accepted explicitly and recorded as ASM-06 in the spec. It is
also the honest split: a job that fails four times and succeeds on the fifth genuinely had
four failed executions, and the depth gauge already reports how many jobs ended `dead`.

**Consequence.** `drover_jobs_failed_total` counts attempts, not funerals. The README must
say so in the same breath as the metric name, because an operator who reads it as "jobs
that died" will alarm on every ordinary retry.

---

## D-2 — How the database-derived gauges are collected

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) A background refresher goroutine polls on a configured interval and writes into gauges; a scrape reads the last written values | Database load becomes a function of elapsed time only, provably independent of how many Prometheus servers scrape and how often. This is the RFC row's stated concept — "metrics that don't hammer the DB" — implemented literally. Also gives readiness a free, honest signal (see D-7). | Values are up to one interval stale. One more goroutine to join on shutdown. |
| (b) A `prometheus.Collector` that queries on every scrape | Always fresh; no extra goroutine; no interval to configure. | Database load is set by the scrape configuration, which the library does not control and cannot see. Two Prometheus servers, a federation scrape, and a human curling the endpoint during an incident all multiply it — and the incident is exactly when the database is least able to absorb it. A slow query also stalls the scrape rather than returning stale values. |
| (c) Collect on scrape but cache the result for a TTL | Fresh when scraped rarely, bounded when scraped often. Hybrid of the two. | The first scrape after each TTL pays the query latency inline, so scrape duration is bimodal and the tail is the database's tail. Adds cache-invalidation reasoning to a component whose whole selling point is that it is explainable. |

**Chosen: (a).** (b) is rejected on the specific ground the RFC named, and (c) is rejected
because it keeps (b)'s worst property — a scrape that can block on the database — while
adding state. The staleness cost is real but bounded and configurable, and for a metric
whose alerting threshold is measured in minutes it is not a meaningful loss of resolution.

---

## D-3 — The shape of the gauge queries

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Two SQL statements per refresh behind one driver method: one grouped count by `(queue, state)`, one oldest-claimable age per queue | Each statement is trivially reviewable and does one thing. Two round trips per refresh interval is still O(1) in scrapes, which is the property that mattered. sqlc generates one row type per query, which is what each statement naturally returns. | The two statements observe marginally different instants. Two round trips rather than one. |
| (b) One statement returning `(queue, state, count, oldest_age)` with a `FILTER`ed aggregate | One round trip; both numbers describe the same instant. | The oldest-age column is a per-*queue* fact carried on per-*(queue, state)* rows, so it is either repeated identically across a queue's rows or null on all but one — a row shape that has to be explained every time someone reads it. Exactly the SQL cleverness AD-036 rejected for weighted ordering, for the same "unexplainable in review" reason. |
| (c) One statement per configured queue | Reuses the per-queue equality the existing partial index serves. | Turns one refresh into N round trips and cannot see queues outside the configured set, which EDGE-03 requires reporting. |

**Chosen: (a).** The instant-skew objection against (a) is the only real one and it does not
survive contact with the use: both values are gauges refreshed every few seconds and
alerted on over minutes, so a few milliseconds of skew between them is far below the
resolution anyone reads them at. Given that, clarity of each statement wins.

**Consequence.** The oldest-age statement's predicate must be the *fetch* predicate — the
same `state IN (available, retryable, scheduled) AND scheduled_at <= now()` the claim
round uses — so that "oldest job" means "oldest job that should already have run" and a
job deliberately scheduled for next week never inflates it (spec ASM-08).

---

## D-4 — Whether the gauges go through the driver interface

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Add one `Stats(ctx)` method to the unexported `driver.Driver` interface, implemented by both `pgdriver` and `memdriver` | Keeps the single storage seam that ADR-0002 established, so the unit suite exercises the gauge logic through `memdriver` with no Docker — a stated project constraint, not a preference. The refresher then has no knowledge of Postgres at all. | Widens a deliberately narrow interface by one method, and every future adapter must implement it. Costs a `memdriver` implementation whose aggregation logic is written twice, once in SQL and once in Go. |
| (b) Query the `*pgxpool.Pool` directly from the metrics component, bypassing the driver | No interface growth; the SQL lives next to the thing that needs it. | The unit suite could not test any of it without Docker, so every gauge behaviour — empty queue reports zero, refresh failure preserves values, unconfigured queues still reported — becomes integration-only. That is the exact regression the in-memory adapter exists to prevent. Also leaks Postgres into a component that has no other reason to know about it. |
| (c) A second, separate `Inspector`-style interface for read-only introspection | Keeps `Driver` narrow and anticipates Cycle F, which ships an exported `Inspector`. | Cycle F has not been designed; inventing its interface now from one caller's needs is speculative, and a second interface implemented by the same two types is two seams where there was one. If Cycle F wants a different shape, it can have one — this method is unexported and costs nothing to move. |

**Chosen: (a).** ADR-0002's narrowness is a means to an end — one seam, two adapters, tests
without Docker — and (b) sacrifices the end to preserve the means. The duplicated
aggregation in `memdriver` is a genuine cost and is accepted for the same reason the other
eleven methods duplicate theirs: the in-memory implementation *is* the executable
specification the Postgres one is checked against.

---

## D-5 — Who owns the Prometheus registry

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `Config.MetricsRegistry *prometheus.Registry`; nil means the client creates its own | Two clients in one process cannot collide (EDGE-07) because each defaults to a private registry. A user who wants Drover's metrics on their own `/metrics` endpoint passes their registry and leaves the ops address unset. `*prometheus.Registry` is concrete and satisfies both `Registerer` and `Gatherer`, so one field serves registration and serving. | Puts `prometheus` in Drover's public API permanently — a user who imports Drover imports Prometheus's types whether or not they scrape. |
| (b) Register on `prometheus.DefaultRegisterer` | Zero configuration; matches what most Go libraries do. | A second client in the same process panics on duplicate registration at construction — a library that cannot be instantiated twice is broken, not opinionated. Also makes a metric's provenance invisible and takes a global decision out of the embedder's hands. |
| (c) A Drover-local metrics interface with a Prometheus implementation behind it | Keeps `prometheus` out of the public surface; leaves the backend swappable. | An abstraction with exactly one implementation, which the spec already ruled out of scope and ADR-0004's small-surface rule argues against. It would also have to reinvent label and histogram semantics badly enough that the Prometheus implementation becomes lossy. |
| (d) Accept `prometheus.Registerer` (the interface) rather than the concrete registry | Narrower contract; accepts custom registerers and wrappers. | `Registerer` cannot be gathered from, so the ops server would need a second field for the `Gatherer`, and the two could be made to disagree. Two fields to describe one object. |

**Chosen: (a).** The public-surface objection is real but was already paid: ADR-0003 chose
Prometheus by name, so the dependency is a recorded decision and hiding its types behind
(c) would buy nothing but indirection. Between (a) and (d), one field that cannot be made
self-inconsistent beats two that can.

---

## D-6 — How the ops server is started, and what a bind failure does

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `net.Listen` eagerly in `Start`, before any goroutine is launched; on failure return the error and start nothing | Satisfies the spec's requirement that a bad ops address fails `Start` and leaves no partially-started client. A port conflict is a deployment mistake that should stop the process at boot, not surface as a log line nobody reads. Matches AD-024's contract that `Start` returning nil means running. | `newRunner` currently returns no error, so it must grow one — a small signature change on a constructor several tests call. |
| (b) `ListenAndServe` inside the goroutine; a bind failure logs and the client runs on without an ops port | No signature change. The queue keeps working, which is arguably what matters. | A worker that is running but unobservable is the state this entire cycle exists to eliminate, and it would report itself started. During a rolling deploy where the old process still holds the port, every new worker would come up blind and say nothing louder than a warning. |
| (c) Bind lazily on the first scrape | No startup cost when nobody scrapes. | The failure surfaces at the scraper, as a connection refused indistinguishable from the process being down. Worst possible place to learn about a misconfiguration. |

**Chosen: (a).** (b) is the tempting one because it looks resilient, and it is rejected on
the same reasoning AD-037 used against silently dropping a nil middleware: an observability
component that fails open is indistinguishable from one that is working, and the whole
point of a health surface is to be trustworthy when everything else is not.

---

## D-7 — What makes `/readyz` report not-ready

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Ready when the client is started **and** the last gauge refresh succeeded within a documented staleness bound (twice the refresh interval) | Reuses a query the process already runs, so probe rate cannot add database load — the same property D-2 was chosen for, applied to the endpoint a Kubernetes probe hits every couple of seconds. It also tests the real query path Drover depends on rather than a bare connection check, so a database that accepts connections but cannot serve the jobs table is correctly reported unready. The check itself is a memory read, so it cannot hang. | Detection is delayed by up to the staleness bound. Readiness is coupled to a component the operator might reasonably think is only about metrics. |
| (b) Ping the database on every `/readyz` request, bounded by a timeout | Immediate and obvious. Reports the current instant, not a recent one. | Ties database load to probe frequency, which is precisely what this cycle promised not to do — a 2-second liveness probe across a fleet is a meaningful query rate aimed at the database during exactly the outage that made everything probe harder. Also needs a new `Ping` on the driver interface, widening it a second time. |
| (c) Ready whenever the client is started, with no database signal at all | Trivial; never wrong about its own process. | Then `/readyz` and `/healthz` mean the same thing, and the reason for having two endpoints — taking a worker out of rotation on a database outage without restarting it — evaporates. |

**Chosen: (a).** (b) is the conventional answer and it is rejected on this cycle's own
stated constraint. The staleness cost is bounded and configurable, and readiness is not a
signal that needs sub-second resolution: an orchestrator acting on it has its own
`failureThreshold` measured in multiples of the probe period anyway.

**Consequence.** The refresher must run once immediately at `Start` rather than waiting a
full interval, or a freshly started client would report unready for its first interval. It
must also record refresh *failures* distinctly from never-having-run, so the log line an
operator sees names which one it is.

---

## D-8 — Where the saturation gauge gets its number

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) The metrics middleware increments on entry and decrements on exit | Measures exactly what the name claims — executions in flight — and cannot drift from the counters beside it, since one middleware owns all three. Costs two atomic operations per job. | Counts executions, not claimed rows, so a job that is claimed and leased but not yet handed to a worker is not included. |
| (b) Read `inflightSet`'s size at collection time | Already tracked; no new bookkeeping at all. | Since the Cycle D review fix, jobs are tracked at the *claim*, not at the start of execution, so the set includes rows that are leased but not yet executing. Publishing that under a name like "executing" would restate the bug that fix corrected. |
| (c) Both, as two gauges | Distinguishes claimed-but-waiting from actually-running, which is a genuinely different diagnosis. | Two saturation gauges for a P2 story, and the gap between them is bounded by a claim round rather than by anything an operator can act on. |

**Chosen: (a).** (b) looks free and is wrong for a specific, recently-learned reason: the
inflight set deliberately means "leased and this worker's responsibility", which is a
larger set than "running". Naming it `executing` would be the second time this codebase
confused the two.

---

## D-9 — Metric names, labels, and histogram buckets

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `drover_` namespace; counters `drover_jobs_completed_total` / `drover_jobs_failed_total`, histogram `drover_job_duration_seconds`, gauges `drover_queue_depth`, `drover_oldest_job_age_seconds`, `drover_jobs_executing`, `drover_pool_concurrency`. Job metrics labelled `queue`; depth additionally `state`; pool gauges unlabelled. Explicit buckets from 5ms to 10 minutes. | Follows Prometheus naming conventions exactly — unit suffix, `_total` on counters, base units. Label cardinality is bounded by configuration, per spec ASM-05. Pool gauges are genuinely pool-wide facts, so labelling them by queue would invent a per-queue number that does not exist. Default buckets end at 10s, which is short for a job queue and would put every slow job in `+Inf` — the bucket that tells you nothing. | Explicit buckets are a guess about workload shape that will be wrong for someone, and buckets cannot be changed without breaking existing histograms. |
| (b) Default `prometheus.DefBuckets` | No guess; whatever Prometheus considers reasonable. | Tuned for HTTP handlers, topping out at 10 seconds. A queue whose jobs take a minute would report every single one in the overflow bucket, making the histogram unable to answer the one question it exists for. |
| (c) A `Config` field for buckets | The user resolves the guess for their own workload. | One more configuration knob on a first release, for a decision most users will not have an opinion about until they have run it for a month. Deferred, not rejected — noted below. |

**Chosen: (a).** The bucket span is the only genuine judgement call here, and it is
resolved by asking what the histogram is for: a job queue's interesting range is
milliseconds to minutes, so the buckets must cover it even at the cost of more series per
histogram than an HTTP handler would use. Configurable buckets are recorded as a deferred
idea rather than shipped, on the roadmap's own "resist scope growth" instruction.

---

## D-10 — Where the ops server stops in the shutdown sequence

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) The ops server is shut down **last**, after workers have drained and after the heartbeat and refresher have stopped | Keeps `/readyz` answerable — and answering `503` — throughout the drain, which is exactly when an orchestrator needs to be told to stop sending traffic and a human needs the metrics. Metrics stay scrapeable while the interesting part of shutdown happens. | The ops listener outlives the queue machinery by the length of the drain, so a scrape during that window sees gauges that are no longer being refreshed. |
| (b) Shut it down first, with the rest of the stop sequence | Symmetric with startup; the port is released promptly. | Shutdown is the least observable and most failure-prone moment in the lifecycle, and this makes it the one moment with no observability at all. A drain that hangs would go dark precisely as it hung. |
| (c) Tie it to `r.cancelBackground()` alongside the rescuer | One cancellation signal for everything in the background group. | `cancelBackground` is the "stop claiming" signal (`pool.go:178`), which fires at the very start of the drain — so this is option (b) with less intent behind it. |

**Chosen: (a).** The stale-gauge objection is answered by the readiness endpoint itself:
during a drain the refresher has stopped, so the staleness bound from D-7 lapses and
`/readyz` reports `503`, which is the correct answer for a draining worker regardless of
why. The endpoints stay honest without any shutdown-specific special case.

---

## D-11 — Which states the depth gauge counts, and the index that makes it affordable

Discovered while designing D-3: a naive `GROUP BY queue, state` over the whole table is a
sequential scan whose cost grows with the *completed-job history*, which is unbounded. That
would make the refresher itself the thing that hammers the database — the failure this
cycle is named after.

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Count only `available`, `scheduled`, `retryable`, `running`, `dead`, and add a partial index on `dead` | Cost becomes proportional to backlog plus retained dead rows — the operationally interesting set — rather than to total history. The first three states are already served by `drover_jobs_fetch_v2_idx` and `running` by `drover_jobs_lease_idx`; only `dead` has no index today, and a partial index on a state rows rarely enter is close to free to maintain. Keeps the two metrics an operator alerts on — backlog and dead — both cheap. | Needs migration 003. The gauge cannot answer "how many jobs has this queue ever completed", which some will expect from a metric named depth. |
| (b) Count every state, no migration | Complete picture; no schema change. | A sequential scan every refresh interval, growing forever, against the same database the queue is trying to claim from. Directly contradicts the RFC row. |
| (c) Count only the three waiting states — no `running`, no `dead`, no migration | Smallest possible query, entirely served by the existing fetch index. Strictly "depth" in the queueing sense. | Dead jobs then have no metric at all, so the second question every operator asks — is anything failing permanently — has no alertable answer until the Cycle F CLI ships. A CLI you must remember to run is not an alert source. |

**Chosen: (a).** (c) is the tempting minimal option and is rejected because a dead-job count
is an *alerting* signal, not an introspection one, and this is the cycle that ships alerting
signals. The migration is three lines and its index is maintained only when a job dies,
which is by construction the rare path.

**Consequence.** `completed` and `cancelled` are deliberately absent from the depth gauge.
The README must say so, because a missing series otherwise reads as a bug.

---

## Deferred Ideas

Captured so they are not lost; explicitly **not** built this cycle.

- **Configurable histogram buckets** (`Config.DurationBuckets`) — see D-9 option (c). Worth
  revisiting once there is real workload data rather than a guess about it.
- **A rescued-jobs counter** and a **requeued-on-shutdown counter**. Both are cheap and
  genuinely diagnostic, and neither is in the RFC row for this cycle. They belong with
  Cycle F's introspection work, where the operator-facing question they answer is already
  being asked.
- **Exposing the refresher's last-run time as a metric** (`drover_stats_refreshed_at`), so
  a stale exporter is detectable from the scrape rather than only from `/readyz`.
- **A queue-label allowlist** to bound cardinality if EDGE-03's foreign queues turn out to
  be unbounded in practice. Not built, because a queue name is written by an enqueuer in
  the same deployment, not by an untrusted caller.

---

## Specific References

The Cycle D handoff in `.specs/STATE.md` fixes two constraints this cycle honours rather
than decides: per-job metrics hang off the middleware chain (D-1), and every job metric
needs a queue label now that `Config.Queues` exists (D-9). The RFC row's third column —
"metrics that don't hammer the DB" — is treated as a requirement with teeth, and is the
deciding argument in D-2 and again in D-7.
