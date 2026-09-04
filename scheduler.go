package drover

import (
	"errors"
	"time"
)

func periodicUniqueKey(id string, fire time.Time) string {
	return id + "/" + fire.UTC().Format(time.RFC3339Nano)
}

// schedulePeriodic enqueues due periodic ticks while this client is
// leader. It shares fetchCtx with the rescuer: when claiming stops, so
// does enqueue. memdriver has no locker and is always leader.
func (r *runner) schedulePeriodic() {
	locker, hasLocker := r.client.drv.(leaderLocker)
	if hasLocker {
		defer locker.ReleaseLeader()
	}

	lastFire := make([]time.Time, len(r.client.periodic))
	wasLeader := false

	for {
		if r.fetchCtx.Err() != nil {
			return
		}

		leader := true
		if hasLocker {
			ok, err := locker.TryBecomeLeader(r.fetchCtx)
			if r.fetchCtx.Err() != nil {
				return
			}
			if err != nil {
				r.client.logger.Error("drover: acquire periodic scheduler lock", "error", err)
				if wasLeader {
					r.client.logger.Info("drover: lost periodic scheduler leadership")
					wasLeader = false
				}
				if !r.waitFetch(r.client.pollInterval) {
					return
				}
				continue
			}
			leader = ok
		}

		if !leader {
			if wasLeader {
				r.client.logger.Info("drover: lost periodic scheduler leadership")
				wasLeader = false
			}
			if !r.waitFetch(r.client.pollInterval) {
				return
			}
			continue
		}

		if !wasLeader {
			r.client.logger.Info("drover: became periodic scheduler leader")
			wasLeader = true
			// First fire is strictly after this process gained the lock,
			// not after it started. Replaying from Start would re-insert
			// ticks a previous leader already ran to completion — unique
			// keys only occupy non-terminal rows.
			now := time.Now()
			for i := range lastFire {
				lastFire[i] = now
			}
		}

		now := time.Now()
		wait := r.enqueueDuePeriodic(now, lastFire)
		if !r.waitFetch(wait) {
			return
		}
	}
}

func (r *runner) enqueueDuePeriodic(now time.Time, lastFire []time.Time) time.Duration {
	var soonest time.Time
	for i, entry := range r.client.periodic {
		fire := entry.schedule.Next(lastFire[i])
		for !fire.IsZero() && !fire.After(now) {
			if r.fetchCtx.Err() != nil {
				return 0
			}
			if !r.insertPeriodicTick(entry, fire) {
				break
			}
			lastFire[i] = fire
			fire = entry.schedule.Next(fire)
		}
		if fire.IsZero() {
			continue
		}
		if soonest.IsZero() || fire.Before(soonest) {
			soonest = fire
		}
	}
	if soonest.IsZero() {
		return r.client.pollInterval
	}
	return soonest.Sub(now)
}

// insertPeriodicTick reports whether the watermark should advance:
// a successful insert or a duplicate (already enqueued). A store error
// leaves the fire for the next turn.
func (r *runner) insertPeriodicTick(entry periodicEntry, fire time.Time) bool {
	opts := InsertOpts{}
	if entry.job.Opts != nil {
		opts = *entry.job.Opts
	}
	opts.ScheduledAt = fire
	opts.UniqueKey = periodicUniqueKey(entry.job.ID, fire)

	_, err := r.client.Insert(r.fetchCtx, entry.job.Args, &opts)
	if err == nil {
		return true
	}
	if r.fetchCtx.Err() != nil {
		return false
	}
	if errors.Is(err, ErrDuplicateJob) {
		r.client.logger.Debug("drover: skipped duplicate periodic tick",
			"periodic_id", entry.job.ID, "fire_time", fire)
		return true
	}
	r.client.logger.Error("drover: enqueue periodic job",
		"periodic_id", entry.job.ID, "error", err)
	return false
}

func (r *runner) waitFetch(d time.Duration) bool {
	if d < 0 {
		d = 0
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-r.fetchCtx.Done():
		return false
	case <-timer.C:
		return r.fetchCtx.Err() == nil
	}
}
