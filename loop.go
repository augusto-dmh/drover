package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
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
	c.mu.Unlock()

	c.logger.Info("drover: worker pool started",
		"queue", r.queue, "concurrency", r.concurrency,
		"poll_interval", c.pollInterval, "lease_duration", c.leaseDuration,
		"heartbeat_interval", c.heartbeatInterval, "rescue_interval", c.rescueInterval)

	r.start(ctx)
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
	c.logger.Info("drover: job started",
		"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt)

	// Tracked from claim to finalize, which is exactly the window in
	// which this job's lease must not lapse.
	lease := driver.Lease{ID: row.ID, Attempt: row.Attempt}
	c.inflight.add(lease)
	defer c.inflight.remove(lease)

	fn, registered := c.workers.handler(row.Kind)
	if !registered {
		// A kind this binary does not know is an ordinary failure, not a
		// death sentence: mid-deploy, workers on the old build
		// legitimately claim kinds only the new build registers, and the
		// job has to survive until one of those runs it (AD-014).
		err := fmt.Errorf("no worker registered for kind %q", row.Kind)
		ctx, cancel := c.finalizeContext(jobCtx)
		defer cancel()
		c.dispose(ctx, row, err, nil, time.Since(start))
		return
	}

	stack, err := runProtected(jobCtx, fn, row)
	if err != nil {
		ctx, cancel := c.finalizeContext(jobCtx)
		defer cancel()
		c.dispose(ctx, row, err, stack, time.Since(start))
		return
	}

	ctx, cancel := c.finalizeContext(jobCtx)
	defer cancel()
	if err := c.drv.MarkCompleted(ctx, lease); err != nil {
		c.writeFailed(row, err)
		return
	}
	c.logger.Info("drover: job completed",
		"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt,
		"duration", time.Since(start))
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

// runProtected calls the worker and converts a panic into an error
// carrying the stack, so one misbehaving job never kills the loop.
func runProtected(ctx context.Context, fn workFunc, row *driver.JobRow) (stack []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job panicked: %v", r)
			stack = debug.Stack()
		}
	}()
	return nil, fn(ctx, row)
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
func (c *Client) dispose(ctx context.Context, row *driver.JobRow, jobErr error, stack []byte, ran time.Duration) {
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
		Trace:   string(stack),
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
	if errors.Is(err, driver.ErrLeaseLost) {
		c.logger.Warn("drover: job taken over by another worker, discarding this outcome",
			"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "error", err)
		return
	}
	c.logger.Error("drover: finalize job",
		"job_id", row.ID, "kind", row.Kind, "error", err)
}
