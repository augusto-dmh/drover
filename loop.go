package drover

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// Start runs the worker loop until ctx is cancelled: claim one due job,
// execute its registered worker, finalize the row, repeat. On
// cancellation it stops fetching, lets the in-flight job finish, and
// returns nil. A worker killed mid-job (crash, SIGKILL) leaves its job
// running until its lease expires and the rescuer returns it to the
// queue; clean shutdown loses nothing.
func (c *Client) Start(ctx context.Context) error {
	c.logger.Info("drover: worker loop started",
		"queue", defaultQueue, "poll_interval", c.pollInterval,
		"lease_duration", c.leaseDuration, "heartbeat_interval", c.heartbeatInterval,
		"rescue_interval", c.rescueInterval)

	// The heartbeat is stopped by closing a channel after the fetch loop
	// returns, rather than by cancelling ctx: a cancelled loop is still
	// draining its last job, and dropping that job's lease mid-drain
	// would invite the rescuer to hand out a duplicate on every clean
	// shutdown (AD-018).
	stopHeartbeat := make(chan struct{})
	var background sync.WaitGroup
	background.Add(2)
	go func() {
		defer background.Done()
		c.heartbeat(stopHeartbeat)
	}()
	// The rescuer holds no work of its own, so cancellation stops it
	// outright while the fetch loop is still draining.
	go func() {
		defer background.Done()
		c.rescueLoop(ctx)
	}()

	c.fetchLoop(ctx)

	close(stopHeartbeat)
	background.Wait()

	c.logger.Info("drover: worker loop stopped")
	return nil
}

// fetchLoop claims and runs one job at a time until ctx is cancelled.
func (c *Client) fetchLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		rows, err := c.drv.FetchAvailable(ctx, defaultQueue, time.Now().Add(c.leaseDuration), 1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("drover: fetch jobs", "error", err)
			if !c.sleep(ctx) {
				return
			}
			continue
		}
		if len(rows) == 0 {
			if !c.sleep(ctx) {
				return
			}
			continue
		}
		// The in-flight job is never interrupted by loop shutdown; the
		// loop exits only after finalize.
		c.runJob(context.WithoutCancel(ctx), rows[0])
	}
}

func (c *Client) runJob(ctx context.Context, row *driver.JobRow) {
	start := time.Now()
	c.logger.Info("drover: job started",
		"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt)

	// Tracked from claim to finalize, which is exactly the window in
	// which this job's lease must not lapse.
	c.inflight.add(row.ID)
	defer c.inflight.remove(row.ID)

	fn, registered := c.workers.handler(row.Kind)
	if !registered {
		// A kind this binary does not know is an ordinary failure, not a
		// death sentence: mid-deploy, workers on the old build
		// legitimately claim kinds only the new build registers, and the
		// job has to survive until one of those runs it (AD-014).
		err := fmt.Errorf("no worker registered for kind %q", row.Kind)
		c.dispose(ctx, row, err, nil, time.Since(start))
		return
	}

	stack, err := runProtected(ctx, fn, row)
	if err != nil {
		c.dispose(ctx, row, err, stack, time.Since(start))
		return
	}

	if err := c.drv.MarkCompleted(ctx, row.ID); err != nil {
		c.writeFailed(row, err)
		return
	}
	c.logger.Info("drover: job completed",
		"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt,
		"duration", time.Since(start))
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

	outcome, snooze := classifyOutcome(jobErr)

	// A snooze is not a failure: nothing is recorded against the attempt,
	// and the driver gives back the attempt the claim consumed (AD-011),
	// so a handler waiting on a precondition can ask again indefinitely.
	if outcome == outcomeSnoozed {
		runAt := time.Now().Add(snooze)
		if err := c.drv.MarkSnoozed(ctx, row.ID, runAt); err != nil {
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
		if err := c.drv.MarkCancelled(ctx, row.ID, detail); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Warn("drover: job cancelled", attrs("error", jobErr)...)

	case row.Attempt >= row.MaxAttempts:
		if err := c.drv.MarkDead(ctx, row.ID, detail); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Error("drover: job dead",
			attrs("max_attempts", row.MaxAttempts, "error", jobErr)...)

	default:
		at := retryAt(ctx, c.logger, c.retryPolicy, rowFromDriver(row), time.Now())
		if err := c.drv.MarkRetryable(ctx, row.ID, at, detail); err != nil {
			c.writeFailed(row, err)
			return
		}
		c.logger.Warn("drover: job failed, retry scheduled",
			attrs("max_attempts", row.MaxAttempts, "retry_at", at, "error", jobErr)...)
	}
}

// writeFailed reports a state change that did not land. The job is still
// running with a lease that will lapse, so the rescuer picks it up
// later; the loop keeps going either way.
func (c *Client) writeFailed(row *driver.JobRow, err error) {
	c.logger.Error("drover: finalize job",
		"job_id", row.ID, "kind", row.Kind, "error", err)
}

// sleep waits one poll interval; it returns false when ctx was
// cancelled instead.
func (c *Client) sleep(ctx context.Context) bool {
	timer := time.NewTimer(c.pollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
