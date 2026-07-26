package drover

import (
	"context"
	"errors"
	"time"
)

const (
	// rescueBatch bounds how many expired jobs one round of a sweep
	// re-claims, so no single round holds an unbounded result set in
	// memory.
	rescueBatch = 100

	// rescueRounds bounds how many rounds one tick may run. A backlog of
	// thousands still drains far faster than one batch per tick, but a
	// single sweep can no longer occupy a pool connection indefinitely —
	// and the connection it would be holding is one the heartbeat needs,
	// which is exactly how a stalled renewal turns into a duplicate.
	rescueRounds = 10
)

// errLeaseExpired is recorded against the attempt of a job whose worker
// stopped renewing its lease. There is nothing more specific to say: a
// process that died mid-job left no error behind, and the expired lease
// is the only evidence the queue has.
var errLeaseExpired = errors.New("drover: lease expired, worker presumed dead")

// rescueLoop sweeps for jobs abandoned by dead workers, once per rescue
// interval, until ctx is cancelled.
//
// Unlike the heartbeat, the rescuer holds no in-flight work of its own,
// so cancelling the loop is reason enough to stop it (AD-018).
func (c *Client) rescueLoop(ctx context.Context) {
	ticker := time.NewTicker(c.rescueInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := c.rescueOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				// A sweep that failed changes nothing: the jobs it did
				// not reach still have expired leases and the next tick
				// finds them again.
				c.logger.Error("drover: rescue expired jobs", "error", err)
			}
		}
	}
}

// rescueOnce returns every job whose lease has lapsed to the queue,
// reporting how many it dispositioned.
//
// Re-claiming first is what makes this safe to run from several
// processes at once: the claim is the same skip-locked update the fetch
// loop uses, so a job belongs to exactly one sweep before anyone decides
// its fate (AD-016). Disposition then runs through the ordinary failure
// path, which is why a job abandoned by a dead worker retries on the
// same schedule, and dies at the same ceiling, as one that failed out
// loud.
func (c *Client) rescueOnce(ctx context.Context) (int, error) {
	rescued := 0
	for round := 0; round < rescueRounds; round++ {
		rows, err := c.drv.FetchExpired(ctx, time.Now().Add(c.leaseDuration), rescueBatch)
		if err != nil {
			return rescued, err
		}

		// Re-claiming committed a fresh lease on every one of these rows,
		// so this sweep now owns them, and owning them means having to
		// disposition them. Cancellation must not abandon that obligation:
		// a row dropped here is left leased for a full duration with
		// nobody running it — strictly less recoverable than the expired
		// row the sweep started from. The fetch loop resolves the
		// identical problem the same way for the job it has claimed.
		disposeCtx := context.WithoutCancel(ctx)
		for _, row := range rows {
			c.logger.Warn("drover: job lease expired, worker presumed dead",
				"job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt,
				"max_attempts", row.MaxAttempts)
			c.dispose(disposeCtx, row, errLeaseExpired, nil, 0)
			rescued++
		}

		if len(rows) < rescueBatch || ctx.Err() != nil {
			return rescued, nil
		}
	}
	// Still more to collect; the next tick carries on.
	return rescued, nil
}
