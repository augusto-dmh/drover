package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// Start begins working the queue and returns as soon as the pool is
// running — it does not block for the lifetime of the client. Jobs are
// executed by a fixed pool of Config.Concurrency goroutines fed by a
// single fetch loop, which claims only as many jobs as there are idle
// workers.
//
// Shutdown is Stop's job. Cancelling ctx performs the same shutdown on
// its own, with no deadline, so a client wired to a cancellable context
// still drains cleanly without anyone calling Stop; what Stop adds is
// the ability to bound the wait and to learn what did not finish.
//
// A client's lifecycle runs once. Calling Start on a client that is
// already running, or that has already been stopped, returns
// ErrAlreadyStarted.
//
// A worker killed mid-job (crash, SIGKILL) leaves its job running until
// its lease expires and the rescuer returns it to the queue. That is the
// backstop for the paths no shutdown code survives; a clean shutdown
// loses nothing.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.runner != nil {
		c.mu.Unlock()
		return ErrAlreadyStarted
	}
	r := newRunner(ctx, c)
	c.runner = r
	// Launched under the lock, not after it. Publishing the runner and
	// then starting it in two steps leaves a window in which Stop sees a
	// runnable client and drains a pool with no goroutines in it: every
	// wait returns at once, Stop reports a clean shutdown, and the
	// goroutines this call is about to spawn then find the stop signal
	// already closed and exit immediately. Both calls would report
	// success and no job would ever run. Nothing start spawns takes this
	// lock, so holding it across the call cannot deadlock.
	r.start(ctx)
	c.mu.Unlock()

	c.logger.Info("drover: worker pool started",
		"queues", queueNames(r.queues), "concurrency", r.concurrency,
		"poll_interval", c.pollInterval, "lease_duration", c.leaseDuration,
		"heartbeat_interval", c.heartbeatInterval, "rescue_interval", c.rescueInterval)

	return nil
}

// Stop shuts the pool down and waits for it, in that order: it stops
// claiming new work, then waits for everything already claimed to finish
// and record its outcome. It returns nil when all of it did.
//
// ctx bounds the wait. When that budget runs out, Stop cancels the
// contexts the running handlers were given, returns the jobs it could
// not finish to the queue so another worker can take them, and reports
// how many there were. Handlers that ignore cancellation keep running
// regardless — Go offers no way to stop a goroutine — which is why the
// answer is a count rather than a guarantee. Those jobs are at-least-once
// delivery working as documented: they were returned to the queue and may
// well run twice.
//
// ctx bounds the waiting, not quite the whole call: handing the
// unfinished jobs back happens after that budget is spent, and each
// hand-back is allowed one HeartbeatInterval of its own. Budget the
// caller's side of a shutdown accordingly.
//
// A ctx with no deadline waits as long as it takes. Stop before Start
// returns ErrNotStarted; calling it again returns the first call's
// verdict.
func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	r := c.runner
	c.mu.Unlock()
	if r == nil {
		return ErrNotStarted
	}
	return r.stop(ctx)
}

// runJob executes one claimed job and records what became of it.
//
// jobCtx is the handler's context and may be cancelled: that is how a
// shutdown that has run out of patience reaches a job already running.
// The context every state change is written under is derived from it
// with cancellation stripped, and the distinction is load-bearing.
// Finalizing on a cancelled context would fail every write the moment
// shutdown escalated, leaving each of those rows running until its lease
// lapsed — turning the one path that is supposed to end cleanly into the
// crash path. The handler's context may be cancelled; the context that
// records an outcome never is (AD-027).
func (c *Client) runJob(jobCtx context.Context, row *driver.JobRow) {
	start := time.Now()

	// Tracked from claim to finalize, which is exactly the window in
	// which this job's lease must not lapse.
	lease := driver.Lease{ID: row.ID, Attempt: row.Attempt}
	c.inflight.add(lease)
	defer c.inflight.remove(lease)

	err := c.execute(jobCtx, rowFromDriver(row))

	// Derived from jobCtx, deliberately not from whatever context the
	// chain passed down. A timeout middleware narrows the context it
	// hands to the next handler, and finalizing on that one would make
	// every job that ran out of time fail to record its outcome and sit
	// running until its lease lapsed — the clean path turned into the
	// crash path (AD-027).
	ctx, cancel := c.finalizeContext(jobCtx)
	defer cancel()

	if err != nil {
		c.dispose(ctx, row, err, time.Since(start))
		return
	}
	// No record on the way out: the logging middleware already reported
	// the execution and its duration, and a second success line here
	// would be the same event told twice. A write that does not land is
	// still reported, by writeFailed.
	if err := c.drv.MarkCompleted(ctx, lease); err != nil {
		c.writeFailed(row, err)
	}
}

// execute runs the middleware chain for one job.
//
// The recover here is the outer of two: it catches a panic thrown by a
// middleware, which dispatch's own recover cannot see because it is
// deeper in the chain. Without it a middleware panic would unwind past
// this frame with nothing above it to catch, and an unrecovered panic in
// any goroutine terminates the whole process — taking every other
// in-flight job with it, each left running until its lease lapses and a
// rescuer collects it. One misbehaving middleware would be a crash, not
// a degradation.
func (c *Client) execute(ctx context.Context, job *JobRow) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
	}()
	return c.chain(ctx, job)
}

// dispatch is the innermost handler: it finds the worker registered for
// the job's kind and runs it.
//
// Its recover is what makes a panicking worker an ordinary error to
// every middleware wrapped around it, so a chain can log, time, and
// count a panic like any other failure rather than watching a stack
// unwind past it (AD-013).
//
// The unregistered-kind branch lives here, inside the chain, so that
// middleware observes it too. A kind this binary does not know is an
// ordinary failure, not a death sentence: mid-deploy, workers on the old
// build legitimately claim kinds only the new build registers, and the
// job has to survive until one of those runs it (AD-014).
func (c *Client) dispatch(ctx context.Context, job *JobRow) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
	}()

	fn, registered := c.workers.handler(job.Kind)
	if !registered {
		return fmt.Errorf("no worker registered for kind %q", job.Kind)
	}
	return fn(ctx, job)
}

// finalizeContext bounds one attempt at recording a job's outcome.
//
// Cancellation is stripped for the reason on runJob: the write that ends
// an attempt must survive the shutdown that interrupted it. The deadline
// is a bound on the write alone, which is why it is taken here rather
// than when the job started — a handler may legitimately run for far
// longer than a lease, and a deadline started at claim time would have
// expired long before such a job ever got to report its result, leaving
// the row running for the rescuer to collect.
func (c *Client) finalizeContext(jobCtx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(jobCtx), c.leaseDuration)
}

// dispose ends an attempt that did not succeed, deciding what becomes of
// the job: cancelled if the handler declared it hopeless, deferred if it
// asked to be called back, dead once its attempts are spent, and
// otherwise queued for another try after the retry policy's wait.
//
// It is the only place that decision is made. The worker loop and the
// rescuer both come through here, which is what makes a job abandoned by
// a dead worker and a job whose handler returned an error reach exactly
// the same fate rather than merely similar ones.
//
// ran is how long the attempt executed for; the rescuer passes zero,
// because a job abandoned by a dead worker has no duration this process
// can honestly report.
func (c *Client) dispose(ctx context.Context, row *driver.JobRow, jobErr error, ran time.Duration) {
	attrs := func(rest ...any) []any {
		base := []any{"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt}
		if ran > 0 {
			base = append(base, "duration", ran)
		}
		return slices.Concat(base, rest)
	}

	lease := driver.Lease{ID: row.ID, Attempt: row.Attempt}
	outcome, snooze := classifyOutcome(jobErr)

	// A snooze is not a failure: nothing is recorded against the attempt,
	// and the driver gives back the attempt the claim consumed (AD-011),
	// so a handler waiting on a precondition can ask again indefinitely.
	if outcome == outcomeSnoozed {
		runAt := time.Now().Add(snooze)
		if err := c.drv.MarkSnoozed(ctx, lease, runAt); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Info("drover: job snoozed", attrs("run_at", runAt)...)
		return
	}

	detail, err := json.Marshal(driver.AttemptError{
		Attempt: row.Attempt,
		At:      time.Now().UTC(),
		Error:   jobErr.Error(),
		// Carried by the error itself, so it survives a middleware that
		// wraps a panicking worker's failure with %w.
		Trace: string(stackOf(jobErr)),
	})
	if err != nil {
		// Nothing is written, so the job stays running until its lease
		// lapses and the rescuer collects it — the same backstop that
		// covers a worker dying outright.
		c.logger.Error("drover: encode job error", attrs("error", err)...)
		return
	}

	switch {
	case outcome == outcomeCancelled:
		if err := c.drv.MarkCancelled(ctx, lease, detail); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Warn("drover: job cancelled", attrs("error", jobErr)...)

	case row.Attempt >= row.MaxAttempts:
		if err := c.drv.MarkDead(ctx, lease, detail); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Error("drover: job dead",
			attrs("max_attempts", row.MaxAttempts, "error", jobErr)...)

	default:
		at := retryAt(ctx, c.logger, c.retryPolicy, rowFromDriver(row), time.Now())
		if err := c.drv.MarkRetryable(ctx, lease, at, detail); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Warn("drover: job failed, retry scheduled",
			attrs("max_attempts", row.MaxAttempts, "retry_at", at, "error", jobErr)...)
	}
}

// writeFailed reports a state change that did not land. The loop keeps
// going either way.
//
// Losing the lease is reported at a lower level than a genuine write
// failure, because it is not one: the job was rescued and re-claimed
// while this attempt was still running, another worker now owns it, and
// refusing the write is the fence doing its job. A real failure leaves
// the row running with a lease that will lapse, so the rescuer collects
// it later.
func (c *Client) writeFailed(row *driver.JobRow, err error) {
	// Two ways this attempt can no longer be this worker's to record, and
	// neither is a fault.
	//
	// ErrLeaseLost is the rescuer's doing: the job was re-claimed while
	// this attempt ran, and a later attempt now owns the outcome.
	//
	// ErrInvalidTransition is usually this process's own doing. A
	// shutdown that ran out of budget hands its unfinished jobs back to
	// the queue, which leaves the row waiting rather than running; the
	// handler that ignored its cancelled context then finishes and
	// arrives here. Both drivers check state before attempt, so that
	// lands as an invalid transition rather than a lost lease. Reporting
	// the expected consequence of a deliberate shutdown as a write
	// failure would put an error in the log for every job a bounded
	// shutdown returned — teaching operators to ignore the line that
	// matters when a write genuinely does fail.
	if errors.Is(err, driver.ErrLeaseLost) || errors.Is(err, driver.ErrInvalidTransition) {
		c.logger.Warn("drover: job is no longer this worker's to finish, discarding this outcome",
			"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "error", err)
		return
	}
	c.logger.Error("drover: finalize job",
		"job_id", row.ID, "kind", row.Kind, "error", err)
}
