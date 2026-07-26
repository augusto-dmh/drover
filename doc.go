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
// SIGKILL or a lost node. Clean shutdown does not rely on it: cancelling
// the context passed to Start stops fetching and drains the in-flight
// job first, keeping its lease alive until it finishes.
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
// # Current limitations
//
// This version runs a single worker goroutine per Start call; pooled
// concurrency, a Stop method with a drain deadline, named queues and
// caller-supplied schedules arrive in the next cycles of docs/rfc/0001
// in the repository.
package drover
