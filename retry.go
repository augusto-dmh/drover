package drover

import (
	"log/slog"
	"math/rand/v2"
	"time"
)

// RetryPolicy decides when a job that failed an attempt runs again.
// The whole row is in scope, so a policy may branch on kind, queue,
// attempt or the errors accumulated so far, and it answers with an
// absolute time — which expresses schedules ("retry at 03:00") that a
// plain duration cannot. An answer at or before the present means the
// job is claimable immediately.
type RetryPolicy interface {
	NextRetry(job *JobRow) time.Time
}

// ExponentialRetryPolicy is the default schedule: the wait after
// attempt N is N⁴ seconds, jittered by ±10% so that jobs failing
// against the same broken dependency do not retry in lockstep. Attempt
// 1 waits about a second, attempt 10 about three hours, and attempt 25
// about four and a half days.
type ExponentialRetryPolicy struct{}

// NextRetry implements RetryPolicy.
func (ExponentialRetryPolicy) NextRetry(job *JobRow) time.Time {
	return time.Now().Add(exponentialDelay(job.Attempt))
}

// exponentialDelay returns attempt⁴ seconds scaled by a uniform factor
// in [0.9, 1.1). The randomness is drawn from math/rand/v2 directly:
// the policy promises a bound and non-determinism, both of which are
// established by sampling, so an injectable source would be a seam
// with no caller outside a test.
func exponentialDelay(attempt int) time.Duration {
	n := float64(attempt)
	seconds := n * n * n * n * (0.9 + 0.2*rand.Float64())
	return time.Duration(seconds * float64(time.Second))
}

// retryAt applies p to job and returns a time the caller can schedule
// against: never before now, and never a panic escaping into the
// worker loop. A policy that panics falls back to the default
// schedule, because the failed job has to leave running either way —
// abandoning it there would strand it until the lease expires.
func retryAt(logger *slog.Logger, p RetryPolicy, job *JobRow, now time.Time) time.Time {
	at, recovered := safeNextRetry(p, job)
	if recovered != nil {
		logger.Error("drover: retry policy panicked, falling back to the default schedule",
			"job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt, "panic", recovered)
		at = ExponentialRetryPolicy{}.NextRetry(job)
	}
	if at.Before(now) {
		return now
	}
	return at
}

// safeNextRetry calls p, converting a panic into a non-nil recovered
// value rather than unwinding into the loop.
func safeNextRetry(p RetryPolicy, job *JobRow) (at time.Time, recovered any) {
	defer func() {
		recovered = recover()
	}()
	return p.NextRetry(job), nil
}
