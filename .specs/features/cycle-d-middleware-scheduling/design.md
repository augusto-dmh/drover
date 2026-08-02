# Middleware and Scheduling — Design

Implements `spec.md`. Decisions it rests on are in `context.md` (`D-1`..`D-9`), promoted to
`.specs/STATE.md` as `AD-029`..`AD-037`.

## Shape of the change

Three independent seams, none of which needs a schema migration:

1. **Execution path** — a `Handler`/`Middleware` pair, composed once at construction and
   applied around dispatch in `runJob`. The existing unexported `workFunc` *becomes* the
   exported `Handler`, so there is one function type rather than two.
2. **Insert path** — `driver.InsertParams` gains a scheduled time; the insert statement sets
   `scheduled_at` and derives `state` from the database clock; `Client.Insert`/`InsertTx`
   gain an `*InsertOpts` parameter.
3. **Fetch path** — the runner holds a weighted queue set instead of one queue name, and a
   fetch round walks a freshly sampled ordering of it. `driver.FetchAvailable` is unchanged.

The pool topology (AD-021, AD-022), the lease fence (AD-019), the shutdown sequence, the
heartbeat, and the rescuer are all untouched.

---

## 1. Execution path

### Types (new file `middleware.go`)

```go
// Handler executes one job.
type Handler func(ctx context.Context, job *JobRow) error

// Middleware wraps a Handler in behaviour that applies to every job.
type Middleware func(Handler) Handler
```

`Handler` replaces `worker.go`'s `type workFunc func(ctx, *driver.JobRow) error`. The
registry becomes `map[string]Handler` and `Register`'s closure takes the public `*JobRow` —
every field it reads (`ID`, `Kind`, `Attempt`, `CreatedAt`, `Args`) exists on both types
(D-1).

### Composition

`newClient` builds the chain once:

```go
chain := append([]Middleware{Logging(c.logger)}, cfg.Middleware...)   // Logging outermost (D-5)
c.execute = wrap(c.dispatch, chain)
```

`wrap` applies the slice back-to-front so index 0 ends up outermost (D-3):

```go
func wrap(h Handler, mws []Middleware) Handler {
    for _, mw := range slices.Backward(mws) { h = mw(h) }
    return h
}
```

`cfg.Middleware` is copied into the new slice by `append`, so a later mutation of the
caller's slice cannot reach the chain (MW-01 / Story 1 AC10). A nil entry panics naming its
index, before composition (MW-05, D-9).

### Base handler and panic containment (D-4)

```go
// dispatch is the innermost handler: it finds the registered worker and runs it.
func (c *Client) dispatch(ctx context.Context, job *JobRow) (err error) {
    defer func() { if r := recover(); r != nil { err = newPanicError(r) } }()   // innermost
    fn, ok := c.workers.handler(job.Kind)
    if !ok { return fmt.Errorf("no worker registered for kind %q", job.Kind) }
    return fn(ctx, job)
}
```

The stack cannot travel as a second return value through a `Handler`, so it travels **in
the error**:

```go
type panicError struct { value any; stack []byte }
func (e *panicError) Error() string { return fmt.Sprintf("job panicked: %v", e.value) }
```

`dispose` recovers it with `errors.As`, which is strictly better than today's out-of-band
`stack []byte` parameter: the trace now survives a middleware that wraps the error with
`%w`. `dispose`'s signature loses its `stack []byte` parameter; `rescue.go`, which passes
`nil` today, simply drops the argument.

The outermost recovery lives in the client, around the composed chain, so a panicking
*middleware* cannot kill a pool worker goroutine (Story 1 AC9):

```go
func (c *Client) execute(ctx context.Context, job *JobRow) (err error) {
    defer func() { if r := recover(); r != nil { err = newPanicError(r) } }()   // outermost
    return c.chain(ctx, job)
}
```

Two recovers, both cheap, and only the inner one ever fires for a worker panic.

### `runJob` after the change

```go
func (c *Client) runJob(jobCtx context.Context, row *driver.JobRow) {
    lease := driver.Lease{ID: row.ID, Attempt: row.Attempt}
    c.inflight.add(lease); defer c.inflight.remove(lease)

    start := time.Now()
    err := c.execute(jobCtx, rowFromDriver(row))

    ctx, cancel := c.finalizeContext(jobCtx)   // from jobCtx, NOT from the chain's context
    defer cancel()
    if err != nil { c.dispose(ctx, row, err, time.Since(start)); return }
    if err := c.drv.MarkCompleted(ctx, lease); err != nil { c.writeFailed(row, err) }
}
```

Two invariants are load-bearing and easy to break:

- **`finalizeContext` derives from `jobCtx`, never from whatever context the chain passed
  down.** If a timeout middleware's narrowed context reached `finalizeContext`, every
  timed-out job would fail to record its outcome and sit `running` until its lease lapsed —
  turning the clean path into the crash path (AD-027, TMO-02 / Story 2 AC5). Because the
  chain's context is a local inside `execute`, this is structurally safe; it is written down
  because it is exactly the edit a future refactor would make "for tidiness".
- **The unregistered-kind branch moves inside `dispatch`.** It must stay inside the chain,
  or middleware stops observing it (MW-02 / Story 1 AC7).

`runJob`'s own `"drover: job started"` and `"drover: job completed"` records move to the
logging middleware (D-5); `dispose`'s disposition records stay exactly where they are.

### The two shipped middleware (`middleware.go`)

```go
func Timeout(d time.Duration) Middleware      // d <= 0 → returns next unchanged (TMO-01)
func Logging(logger *slog.Logger) Middleware  // nil logger → slog.Default()
```

`Timeout` derives `context.WithTimeout(ctx, d)` for the next handler and returns that
handler's error verbatim — it never substitutes an error of its own (Story 2 AC2, AC3).
`Logging` emits start at `INFO`, success at `INFO` with duration, failure at `WARN` with
duration and error (LOG-01; `WARN` because a failed attempt is designed behaviour that the
retry machinery expects — the pattern the previous cycle's review flagged).

---

## 2. Insert path

### Public API

```go
type InsertOpts struct {
    Queue       string     // "" → "default"
    ScheduledAt time.Time  // zero → now
}

func (c *Client) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*JobRow, error)
func (c *Client) InsertTx(ctx context.Context, tx pgx.Tx, args JobArgs, opts *InsertOpts) (*JobRow, error)
```

`nil` opts means defaults (D-6). Every existing call site gains a `nil` argument.

### Driver and SQL

`driver.InsertParams` gains `ScheduledAt time.Time` (zero means "now"). The statement sets
both columns, and lets the **database** decide the state (D-7):

```sql
-- name: InsertJob :one
INSERT INTO drover_jobs (kind, queue, args, scheduled_at, state)
VALUES (
    $1, $2, $3,
    coalesce(sqlc.narg(scheduled_at)::timestamptz, now()),
    CASE WHEN coalesce(sqlc.narg(scheduled_at)::timestamptz, now()) > now()
         THEN 'scheduled'::drover_job_state
         ELSE 'available'::drover_job_state END
)
RETURNING *;
```

`pgdriver` passes a `pgtype.Timestamptz` with `Valid: !params.ScheduledAt.IsZero()`.
`memdriver` mirrors the same rule against its own clock. Requires `sqlc generate` and a
committed `internal/dbsqlc` diff.

**No migration.** `queue` and `scheduled_at` have existed since `001` and the partial index
`drover_jobs_fetch_v2_idx (queue, scheduled_at, id)` already covers the `scheduled` state
and serves the single-queue equality this cycle keeps using.

---

## 3. Fetch path

### Configuration

```go
// Config
Queues map[string]int   // queue name → weight; nil → {"default": 1}
```

Validation at construction (D-9): an empty queue name panics; a weight below one warns
naming the queue and is corrected to one. The map is flattened into a slice **sorted by
queue name**, so the structure is deterministic and only the sampling is random.

`runner.queue string` becomes `runner.queues []weightedQueue`, set from the client.
`Start`'s log record reports the queues and their weights instead of one name.

### Selection (`queues.go`)

```go
// weightedOrder returns a fresh ordering of qs in which the probability that a
// given queue is first is its weight over the total, the second position is
// drawn the same way from what remains, and so on.
func weightedOrder(qs []weightedQueue) []string
```

Weighted sampling without replacement (D-8): pick a value in `[0, total)`, walk the
remaining queues accumulating weights until it is passed, emit that queue, subtract its
weight from the total, repeat. `math/rand/v2` is used directly and is not injectable, per
the AD-017 precedent; correctness is asserted distributionally over many samples.

Weights are summed as `int64` to keep a large configured weight from overflowing the
accumulator (Edge Cases).

### One fetch round

`fetch()` keeps its structure — `acquireSlots`, the surplus guard, `releaseSlots`, the
hand-off loop, `sleep` — and gains an inner walk over the sampled ordering:

```
n := acquireSlots()                       // idle workers; 0 → shutting down
remaining := n
for _, q := range weightedOrder(r.queues) {
    got, err := drv.FetchAvailable(fetchCtx, q, leaseDuration, remaining)
    if err != nil { log; break }          // keep what earlier queues gave us
    if len(got) > remaining { requeue surplus; got = got[:remaining] }
    rows = append(rows, got...)
    remaining -= len(got)
    if remaining == 0 || r.stopping() { break }
}
releaseSlots(n - len(rows))
if len(rows) == 0 { if !sleep() { return }; continue }
hand off rows one at a time, abandoning the tail if stopFetch closes
```

Consequences this shape is chosen for:

- **Capacity is never exceeded**, because `remaining` is decremented across queues and is
  the limit passed to each call — AD-022's invariant holds across a multi-queue round
  (QUEUE-03 / Story 5 AC6).
- **An empty high-weight queue costs one query, not a poll interval** (Story 5 AC4).
- **A fetch error does not discard rows already claimed this round.** Breaking out and
  dispatching what we hold is required: those rows are already `running` and leased, so
  dropping them would strand them until their leases lapsed. If the round ends with zero
  rows the existing sleep-and-retry applies unchanged.
- **One configured queue means exactly one query per round** — the loop runs once, so the
  default configuration issues no more queries than it does today (Edge Cases).

The stale `pool.go` comment claiming "a second queue is a second runner rather than a
rewrite of this one" is corrected: a second queue is a second entry in the weighted set,
served by the same pool (D-8).

---

## Files touched

| File | Change |
| --- | --- |
| `middleware.go` *(new)* | `Handler`, `Middleware`, `wrap`, `Timeout`, `Logging`, `panicError` |
| `queues.go` *(new)* | `weightedQueue`, `weightedOrder`, config flattening and validation |
| `worker.go` | `workFunc` → `Handler`; registry and `Register` closure take `*JobRow` |
| `client.go` | `Config.Middleware`, `Config.Queues`; validation; chain composition; `InsertOpts`; `Insert`/`InsertTx` signatures; `insertParamsFor` |
| `loop.go` | `execute`/`dispatch`; `runJob` restructure; `dispose` loses `stack`; start/completed records move out; `Start` log reports queues |
| `pool.go` | `runner.queues`; multi-queue fetch round; stale comment corrected |
| `rescue.go` | `dispose` call drops its `stack` argument |
| `internal/driver/driver.go` | `InsertParams.ScheduledAt`; doc for the insert-state rule |
| `internal/pgdriver/{queries.sql,pgdriver.go}` | insert statement and params |
| `internal/dbsqlc/*` | regenerated, committed |
| `internal/memdriver/memdriver.go` | insert honours `ScheduledAt` and sets state |
| `doc.go`, `example_test.go`, `examples/email/*` | new `Insert` signature; demonstrate a queue, a delay, and a middleware |
| `README.md` | document middleware, scheduling, queues |

---

## Phases

Each phase is one worker and ends with a green gate and atomic commits.

| Phase | Scope | Why it is one unit |
| --- | --- | --- |
| **1 — Handler and chain** | `Handler`/`Middleware`, `wrap`, panic containment, `dispatch`/`execute`, `runJob` restructure, registry retype, config validation | Every part changes the execution path together; splitting leaves the tree uncompilable. Carries the finalize-context and panic invariants. |
| **2 — Timeout and Logging** | The two shipped middleware; moving the start/success records out of `runJob` | Depends only on phase 1's types. Carries the log-level and no-duplicate-record criteria. |
| **3 — Scheduling** | `InsertOpts`, driver params, SQL + `sqlc generate`, both drivers, call-site updates | One vertical slice from public API to storage; the state rule must land in both drivers at once or the suites disagree. |
| **4 — Named queues** | `Config.Queues`, `weightedOrder`, runner queue set, multi-queue fetch round, start log | Touches claiming — the correctness kernel of the cycle. |
| **5 — Docs and example** | `README.md`, `doc.go`, `example_test.go`, `examples/email` | Pure documentation and demonstration; no invariant. |

Phases 1–4 each carry a correctness invariant or a design decision, so they run on the
strong model; phase 5 is the only downshift-safe unit.
