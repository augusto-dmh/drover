// Package drover is a PostgreSQL-backed task queue: typed jobs are
// enqueued transactionally and executed by registered workers.
//
// # Delivery contract
//
// Drover delivers each job at least once. A job whose worker crashes
// mid-execution is returned to the queue once its lease expires, so that
// job may run twice: duplicates are possible by design and Worker
// implementations must be idempotent. Exactly-once execution is not
// promised by drover or by any queue — see docs/adr/0003 in the
// repository for the full reasoning.
//
// # Enqueueing
//
// Client.Insert persists a job in its own transaction. Client.InsertTx
// persists it inside a caller-owned pgx.Tx, so a domain write and its
// job commit or roll back atomically — no outbox required.
//
// # Failure and retries
//
// A worker that returns an error, panics, or has no registered handler
// leaves its job queued for another attempt rather than killing it. Each
// attempt appends an entry to the job's errors array recording the
// attempt number, the time, the message, and a stack trace for panics.
// The job becomes dead only once it has used its max_attempts, and dead
// rows are retained for inspection rather than deleted.
//
// Waits between attempts come from Config.RetryPolicy, which defaults to
// ExponentialRetryPolicy: attempt⁴ seconds, jittered by ±10% so that
// jobs failing against the same broken dependency do not retry in
// lockstep. Supply any RetryPolicy to replace that schedule.
//
// A Worker can also decide its own outcome. Returning an error that
// wraps Cancel ends the job in the cancelled state without spending its
// remaining attempts, for work that can never succeed. Returning Snooze
// defers the job without consuming an attempt, for work that is not
// ready yet. Both are recognized through %w wrapping, so a worker may
// add its own context, and both are matchable with errors.Is against
// ErrCancelled and ErrSnoozed.
//
// # Crash recovery
//
// Claiming a job takes a lease on it, and a running job's lease is
// renewed every Config.HeartbeatInterval for another
// Config.LeaseDuration. A worker that dies stops renewing, and a rescue
// sweep running every Config.RescueInterval returns any job whose lease
// has lapsed to the queue — or to the dead state if it has no attempts
// left. A rescue does not spend an attempt: the attempt that stranded
// the job was already spent.
//
// The lease therefore bounds how long abandoned work sits idle after a
// SIGKILL or a lost node. Clean shutdown does not rely on it: Stop stops
// fetching and drains in-flight jobs first, keeping their leases alive
// until they finish.
//
// A claim is held under a lease naming both the job and the attempt, and
// every state change is checked against it. A worker starved of
// heartbeats for longer than its lease — a long stop-the-world pause, a
// database outage — may find its job rescued and re-claimed elsewhere
// while it is still running; when it finishes, its write is refused
// rather than allowed to overwrite the attempt now in flight. The work
// may genuinely have run twice, which at-least-once delivery permits,
// but only the current holder records what happened.
//
// Lease deadlines are computed by the database, not the client, so the
// lease means the same thing to every worker regardless of what their
// own clocks say.
//
// # Middleware
//
// Config.Middleware wraps every job a Client runs, whatever its kind,
// as a chain of func(Handler) Handler — the same shape as an HTTP
// middleware chain, and composed the same way: index 0 is outermost,
// seeing a job first and its result last. The client always installs
// Logging outermost of everything, including a caller's own
// middleware, so per-job logging cannot be silently dropped by
// configuration; Timeout bounds how long a handler may run before its
// context is cancelled. Both are exported, ordinary middleware, so
// they double as the reference examples for writing another.
//
// # Queues and scheduling
//
// InsertOpts, passed to Insert or InsertTx, chooses which named queue
// a job waits in and the earliest time it may run; nil, or a zero
// value, means the "default" queue, runnable as soon as a worker is
// free. Config.Queues maps every queue a Client works to a weight:
// the queues share one pool of Config.Concurrency workers rather than
// each getting a slice of it, and weight decides how often a queue is
// tried first in a fetch round, never whether it is tried at all, so
// no configured queue is starved.
//
// # Concurrency and shutdown
//
// Jobs run on a fixed pool of Config.Concurrency goroutines, fed by a
// single fetch loop that claims no more rows than there are idle
// workers. The pool may safely be sized well above the database
// connection count: a job holds a connection only while it is claimed
// and while its outcome is recorded, not for the work in between.
//
// Start(ctx) returns as soon as the pool is running rather than
// blocking for the client's lifetime. Calling it twice, or calling it
// again after Stop, returns ErrAlreadyStarted — a client's lifecycle
// runs once. Cancelling ctx begins the same shutdown Stop performs, with
// no deadline, so a client wired to a cancellable context still drains
// cleanly with nobody calling Stop.
//
// Stop(ctx) stops claiming new work, then waits for everything already
// claimed to finish and record its outcome, returning nil once all of
// it has. ctx bounds that wait. When the budget runs out, Stop cancels
// the contexts running handlers were given, returns the jobs it could
// not finish to the queue so another worker can pick them up, and
// returns an error wrapping ErrDrainIncomplete naming how many there
// were. A non-nil Stop error is at-least-once delivery showing through
// at its most visible: those jobs are claimable again immediately — the
// requeue does not spend their attempt — and because Go offers no way to
// force a goroutine to stop, the handler this process abandoned may
// still be running, so the job can genuinely execute twice. Stop before
// Start returns ErrNotStarted; calling it again returns the first call's
// verdict without blocking.
//
// # Observability
//
// Every client records Prometheus metrics on Config.MetricsRegistry,
// defaulting to a private registry per client. Per-execution counters
// and a duration histogram are recorded by a middleware installed
// inside Logging; queue depth and oldest-job age gauges are refreshed
// from the store on Config.StatsInterval (default fifteen seconds).
//
// Config.OpsAddr binds a dedicated listener for GET /metrics, /healthz,
// and /readyz. An empty address records metrics without serving them.
// /healthz always returns 200; /readyz returns 503 when the client is
// not started or the last gauge refresh is older than twice
// StatsInterval — typically because the database is unreachable.
//
// drover_jobs_failed_total counts failed executions (attempts), not
// jobs that reached dead; use drover_queue_depth{state="dead"} for
// permanent failures. drover_queue_depth deliberately excludes
// completed and cancelled states. See the README observability section
// for every metric family, a compiling configuration snippet, and the
// recommended alerting expression over drover_oldest_job_age_seconds.
package drover
