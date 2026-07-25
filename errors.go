package drover

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidKind is returned by Insert and InsertTx when args.Kind()
// returns an empty string.
var ErrInvalidKind = errors.New("drover: job kind must be non-empty")

// ErrCancelled is wrapped by every error Cancel returns. Workers do
// not return it directly; drover recognizes a cancellation with
// errors.Is, so a worker may add its own context with %w and still be
// understood.
var ErrCancelled = errors.New("drover: job cancelled by handler")

// Cancel reports that a job can never succeed. A worker returning it —
// bare, or wrapped in more context — ends the job in the cancelled
// state with reason recorded against the attempt, instead of spending
// the remaining attempts on work that is not going to start working.
func Cancel(reason error) error {
	if reason == nil {
		return ErrCancelled
	}
	return fmt.Errorf("%w: %w", ErrCancelled, reason)
}

// Snooze defers a job by d without consuming an attempt. A worker
// returning it — bare, or wrapped in more context — leaves the job
// scheduled d from now, so a handler waiting on a precondition can
// keep asking without walking itself into the dead state. A zero or
// negative d makes the job claimable immediately rather than
// scheduling it in the past.
func Snooze(d time.Duration) error {
	if d < 0 {
		d = 0
	}
	return snoozeError{d: d}
}

// snoozeError carries the deferral a worker asked for. Callers build
// it with Snooze and drover reads it back with errors.As, so wrapping
// keeps the duration reachable.
type snoozeError struct {
	d time.Duration
}

func (e snoozeError) Error() string {
	return fmt.Sprintf("drover: job snoozed for %s", e.d)
}

// Duration returns the deferral, which is never negative.
func (e snoozeError) Duration() time.Duration { return e.d }

// jobOutcome is what a worker's returned error asks drover to do with
// the job.
type jobOutcome int

const (
	// outcomeFailed is an ordinary failure: retry the job, or let it
	// die once its attempts are exhausted.
	outcomeFailed jobOutcome = iota
	// outcomeCancelled means the job can never succeed and must not be
	// retried.
	outcomeCancelled
	// outcomeSnoozed means the worker asked to be called again later.
	outcomeSnoozed
)

// classifyOutcome maps a worker's non-nil error to the disposition it
// asks for, along with the deferral when that disposition is a snooze;
// the duration is zero for every other outcome. Classification looks
// through %w wrapping, so context a worker adds is preserved without
// hiding the sentinel underneath it. Success is decided by the caller
// before an outcome is classified: a nil error is not a disposition.
func classifyOutcome(err error) (jobOutcome, time.Duration) {
	if errors.Is(err, ErrCancelled) {
		return outcomeCancelled, 0
	}
	var snooze snoozeError
	if errors.As(err, &snooze) {
		return outcomeSnoozed, snooze.Duration()
	}
	return outcomeFailed, 0
}
