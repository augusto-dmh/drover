package drover

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// Start runs the worker loop until ctx is cancelled: claim one due job,
// execute its registered worker, finalize the row, repeat. On
// cancellation it stops fetching, lets the in-flight job finish, and
// returns nil. A worker killed mid-job (crash, SIGKILL) leaves its job
// running until lease-based rescue lands in a later version; clean
// shutdown loses nothing.
func (c *Client) Start(ctx context.Context) error {
	c.logger.Info("drover: worker loop started",
		"queue", defaultQueue, "poll_interval", c.pollInterval)
	for {
		if ctx.Err() != nil {
			c.logger.Info("drover: worker loop stopped")
			return nil
		}
		rows, err := c.drv.FetchAvailable(ctx, defaultQueue, 1)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("drover: worker loop stopped")
				return nil
			}
			c.logger.Error("drover: fetch jobs", "error", err)
			if !c.sleep(ctx) {
				c.logger.Info("drover: worker loop stopped")
				return nil
			}
			continue
		}
		if len(rows) == 0 {
			if !c.sleep(ctx) {
				c.logger.Info("drover: worker loop stopped")
				return nil
			}
			continue
		}
		// The in-flight job is never interrupted by loop shutdown; the
		// loop exits only after finalize (drain, CORE-03 AC6).
		c.runJob(context.WithoutCancel(ctx), rows[0])
	}
}

func (c *Client) runJob(ctx context.Context, row *driver.JobRow) {
	start := time.Now()
	c.logger.Info("drover: job started",
		"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt)

	fn, registered := c.workers.handler(row.Kind)
	if !registered {
		err := fmt.Errorf("no worker registered for kind %q", row.Kind)
		c.markDead(ctx, row, err, nil)
		c.logger.Warn("drover: unregistered job kind",
			"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt,
			"duration", time.Since(start), "error", err)
		return
	}

	err, stack := runProtected(ctx, fn, row)
	if err != nil {
		c.markDead(ctx, row, err, stack)
		c.logger.Error("drover: job failed",
			"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt,
			"duration", time.Since(start), "error", err)
		return
	}

	if err := c.drv.MarkCompleted(ctx, row.ID); err != nil {
		c.logger.Error("drover: finalize job",
			"job_id", row.ID, "kind", row.Kind, "error", err)
		return
	}
	c.logger.Info("drover: job completed",
		"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt,
		"duration", time.Since(start))
}

// runProtected calls the worker and converts a panic into an error
// carrying the stack, so one misbehaving job never kills the loop.
func runProtected(ctx context.Context, fn workFunc, row *driver.JobRow) (err error, stack []byte) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job panicked: %v", r)
			stack = debug.Stack()
		}
	}()
	return fn(ctx, row), nil
}

func (c *Client) markDead(ctx context.Context, row *driver.JobRow, jobErr error, stack []byte) {
	detail, err := json.Marshal(driver.AttemptError{
		Attempt: row.Attempt,
		At:      time.Now().UTC(),
		Error:   jobErr.Error(),
		Trace:   string(stack),
	})
	if err != nil {
		c.logger.Error("drover: encode job error",
			"job_id", row.ID, "error", err)
		return
	}
	if err := c.drv.MarkDead(ctx, row.ID, detail); err != nil {
		c.logger.Error("drover: finalize job",
			"job_id", row.ID, "kind", row.Kind, "error", err)
	}
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
