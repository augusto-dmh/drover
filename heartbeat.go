package drover

import (
	"context"
	"time"
)

// heartbeat extends the lease of every job this client is executing,
// once per heartbeat interval, until stop is closed.
//
// It is what keeps the rescuer honest. The rescuer cannot tell a slow
// worker from a dead one except by the lease, so a job that outlives its
// lease without heartbeats is handed to a second worker while the first
// is still running it — turning the safety net into a duplicator.
// Beating on a fraction of the lease leaves room to miss one and still
// renew in time.
//
// stop is closed only after the fetch loop has returned, not when the
// loop's context is cancelled: a cancelled loop is still draining its
// last job, and that job needs its lease for as long as it runs
// (AD-018).
func (c *Client) heartbeat(stop <-chan struct{}) {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			c.extendLeases()
		}
	}
}

// extendLeases pushes every held lease a full lease duration into the
// future.
func (c *Client) extendLeases() {
	leases := c.inflight.snapshot()
	if len(leases) == 0 {
		return
	}

	// Deliberately not the loop's context: the loop may already be
	// cancelled and draining, which is precisely when these leases must
	// not lapse. The timeout keeps a wedged database from stalling the
	// heartbeat past the lease it is renewing.
	ctx, cancel := context.WithTimeout(context.Background(), c.heartbeatInterval)
	defer cancel()

	if err := c.drv.ExtendLeases(ctx, leases, c.leaseDuration); err != nil {
		// The jobs keep running. If a lease does lapse before the next
		// beat lands, the rescuer may hand out a duplicate — the
		// documented price of at-least-once delivery, not a reason to
		// abandon work that is still progressing.
		c.logger.Error("drover: extend job leases",
			"leases", len(leases), "lease_duration", c.leaseDuration, "error", err)
		return
	}
	c.logger.Debug("drover: extended job leases",
		"leases", len(leases), "lease_duration", c.leaseDuration)
}
