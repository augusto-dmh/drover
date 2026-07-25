package drover

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover/internal/memdriver"
)

// fixedPolicy answers with the same time for every job and records how
// often it was consulted.
type fixedPolicy struct {
	at    time.Time
	calls int
}

func (p *fixedPolicy) NextRetry(*JobRow) time.Time {
	p.calls++
	return p.at
}

// panicPolicy stands in for an adopter's policy with a bug in it.
type panicPolicy struct{}

func (panicPolicy) NextRetry(*JobRow) time.Time { panic("policy exploded") }

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// jitterBounds is the documented envelope for the wait after attempt
// N: N⁴ seconds ±10%.
func jitterBounds(attempt int) (lower, upper time.Duration) {
	n := float64(attempt)
	base := n * n * n * n * float64(time.Second)
	return time.Duration(0.9 * base), time.Duration(1.1 * base)
}

// sampleDelays draws n waits for attempt from the default policy,
// bracketing each call between two clock reads. over[i] can only
// exceed the wait the policy chose and under[i] can only fall short of
// it, so comparing each bound against the estimate that cannot fail it
// absorbs the microseconds a call takes instead of mistaking them for
// jitter, which is measured in seconds.
func sampleDelays(attempt, n int) (over, under []time.Duration) {
	job := &JobRow{Attempt: attempt}
	for range n {
		before := time.Now()
		at := ExponentialRetryPolicy{}.NextRetry(job)
		after := time.Now()
		over = append(over, at.Sub(before))
		under = append(under, at.Sub(after))
	}
	return over, under
}

func TestExponentialRetryPolicyStaysWithinJitterBounds(t *testing.T) {
	t.Parallel()

	for _, attempt := range []int{1, 2, 3, 5, 25} {
		t.Run(fmt.Sprintf("attempt=%d", attempt), func(t *testing.T) {
			t.Parallel()
			lower, upper := jitterBounds(attempt)
			over, under := sampleDelays(attempt, 200)

			for i := range over {
				if over[i] < lower {
					t.Fatalf("attempt %d sample %d: wait %v is below %v", attempt, i, over[i], lower)
				}
				if under[i] > upper {
					t.Fatalf("attempt %d sample %d: wait %v is above %v", attempt, i, under[i], upper)
				}
			}
		})
	}
}

func TestExponentialRetryPolicyVariesBetweenCalls(t *testing.T) {
	t.Parallel()

	delays, _ := sampleDelays(3, 100)
	low, high := delays[0], delays[0]
	for _, d := range delays {
		if d < low {
			low = d
		}
		if d > high {
			high = d
		}
	}

	// Attempt 3 spans 72.9s–89.1s, so genuine jitter spreads samples
	// across seconds; a constant schedule would spread them only by
	// the microseconds the clock advanced between calls.
	if spread := high - low; spread < time.Second {
		t.Fatalf("100 waits for attempt 3 spread only %v, want a jittered schedule", spread)
	}
}

func TestExponentialRetryPolicyGrowsWithAttempt(t *testing.T) {
	t.Parallel()

	for _, pair := range []struct{ lower, higher int }{{1, 3}, {2, 5}, {3, 10}, {5, 25}} {
		t.Run(fmt.Sprintf("attempt=%d_vs_%d", pair.lower, pair.higher), func(t *testing.T) {
			t.Parallel()
			slowest, _ := sampleDelays(pair.lower, 50)
			_, fastest := sampleDelays(pair.higher, 50)

			longestLower, shortestHigher := slowest[0], fastest[0]
			for _, d := range slowest {
				if d > longestLower {
					longestLower = d
				}
			}
			for _, d := range fastest {
				if d < shortestHigher {
					shortestHigher = d
				}
			}

			if shortestHigher <= longestLower {
				t.Fatalf("attempt %d waited at most %v but attempt %d waited as little as %v, want strictly longer",
					pair.lower, longestLower, pair.higher, shortestHigher)
			}
		})
	}
}

func TestRetryAtSchedulesAtThePolicysAnswer(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 25, 20, 14, 2, 0, time.UTC)
	tests := []struct {
		name   string
		answer time.Time
		want   time.Time
	}{
		{name: "future answer is kept", answer: now.Add(30 * time.Second), want: now.Add(30 * time.Second)},
		{name: "answer of now is claimable now", answer: now, want: now},
		{name: "past answer is claimable now", answer: now.Add(-time.Hour), want: now},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			policy := &fixedPolicy{at: tc.answer}

			got := retryAt(discardLogger(), policy, &JobRow{ID: 7, Attempt: 3}, now)

			if !got.Equal(tc.want) {
				t.Errorf("retryAt = %v, want %v", got, tc.want)
			}
			if got.Before(now) {
				t.Errorf("retryAt = %v, which is before now (%v): the job would never be claimed", got, now)
			}
			if policy.calls != 1 {
				t.Errorf("configured policy consulted %d times, want 1", policy.calls)
			}
		})
	}
}

func TestRetryAtFallsBackToDefaultWhenPolicyPanics(t *testing.T) {
	t.Parallel()

	logs := &syncWriter{}
	job := &JobRow{ID: 7, Kind: "greet", Attempt: 3}

	now := time.Now()
	got := retryAt(slog.New(slog.NewTextHandler(logs, nil)), panicPolicy{}, job, now)
	after := time.Now()

	lower, upper := jitterBounds(job.Attempt)
	if d := got.Sub(now); d < lower {
		t.Errorf("wait after a panicking policy = %v, below the default schedule's %v", d, lower)
	}
	if d := got.Sub(after); d > upper {
		t.Errorf("wait after a panicking policy = %v, above the default schedule's %v", d, upper)
	}
	if !strings.Contains(logs.String(), "policy exploded") {
		t.Errorf("logs do not report the recovered panic: %s", logs.String())
	}
}

func TestNilRetryPolicySelectsTheDefault(t *testing.T) {
	t.Parallel()

	c := newClient(memdriver.New(), Config{})

	if _, ok := c.retryPolicy.(ExponentialRetryPolicy); !ok {
		t.Fatalf("retry policy = %T, want ExponentialRetryPolicy", c.retryPolicy)
	}
}

func TestSuppliedRetryPolicyReplacesTheDefault(t *testing.T) {
	t.Parallel()

	policy := &fixedPolicy{at: time.Now().Add(time.Hour)}

	c := newClient(memdriver.New(), Config{RetryPolicy: policy})

	if c.retryPolicy != policy {
		t.Fatalf("retry policy = %v, want the supplied policy", c.retryPolicy)
	}
}
