package drover

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestClassifyOutcomeReadsSentinelsThroughWrapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		wantOutcome  jobOutcome
		wantDuration time.Duration
	}{
		{
			name:        "ordinary error is an ordinary failure",
			err:         errors.New("smtp timeout"),
			wantOutcome: outcomeFailed,
		},
		{
			name:        "wrapped ordinary error is an ordinary failure",
			err:         fmt.Errorf("sending welcome mail: %w", errors.New("smtp timeout")),
			wantOutcome: outcomeFailed,
		},
		{
			name:        "cancel is a cancellation",
			err:         Cancel(errors.New("bad input")),
			wantOutcome: outcomeCancelled,
		},
		{
			name:        "wrapped cancel is a cancellation",
			err:         fmt.Errorf("nope: %w", Cancel(errors.New("bad input"))),
			wantOutcome: outcomeCancelled,
		},
		{
			name:         "snooze is a snooze carrying its duration",
			err:          Snooze(5 * time.Minute),
			wantOutcome:  outcomeSnoozed,
			wantDuration: 5 * time.Minute,
		},
		{
			name:         "wrapped snooze is a snooze carrying its duration",
			err:          fmt.Errorf("account not provisioned yet: %w", Snooze(5*time.Minute)),
			wantOutcome:  outcomeSnoozed,
			wantDuration: 5 * time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			outcome, d := classifyOutcome(tc.err)

			if outcome != tc.wantOutcome {
				t.Errorf("outcome = %d, want %d", outcome, tc.wantOutcome)
			}
			if d != tc.wantDuration {
				t.Errorf("duration = %v, want %v", d, tc.wantDuration)
			}
		})
	}
}

func TestCancelRecordsTheReason(t *testing.T) {
	t.Parallel()

	reason := errors.New("bad input")
	err := fmt.Errorf("nope: %w", Cancel(reason))

	if !errors.Is(err, ErrCancelled) {
		t.Errorf("error %v is not recognized as a cancellation", err)
	}
	if !errors.Is(err, reason) {
		t.Errorf("error %v lost the reason it was cancelled for", err)
	}
	if !strings.Contains(err.Error(), "bad input") {
		t.Errorf("message = %q, want it to state the reason", err)
	}
}

func TestSnoozeNeverWakesInThePast(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 25, 20, 14, 2, 0, time.UTC)
	tests := []struct {
		name     string
		snooze   time.Duration
		wantWake time.Time
	}{
		{name: "negative snooze wakes now", snooze: -time.Hour, wantWake: now},
		{name: "zero snooze wakes now", snooze: 0, wantWake: now},
		{name: "positive snooze wakes later", snooze: time.Minute, wantWake: now.Add(time.Minute)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			outcome, d := classifyOutcome(Snooze(tc.snooze))
			wake := now.Add(d)

			if outcome != outcomeSnoozed {
				t.Fatalf("outcome = %d, want %d", outcome, outcomeSnoozed)
			}
			if !wake.Equal(tc.wantWake) {
				t.Errorf("wake time = %v, want %v", wake, tc.wantWake)
			}
			if wake.Before(now) {
				t.Errorf("wake time = %v is before now (%v): the job would wake in the past", wake, now)
			}
		})
	}
}
