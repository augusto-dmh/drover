package drover

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"time"
)

// Handler executes one job and reports whether the attempt succeeded.
//
// It is the unit a Middleware wraps, and it is also what Register builds
// from a typed Worker: by the time a Handler runs, the job's args are
// still raw JSON, because middleware is shared by every job kind and so
// cannot be generic over any one of them.
//
// A Handler must not retain job beyond the call.
type Handler func(ctx context.Context, job *JobRow) error

// Middleware wraps a Handler in behaviour that applies to every job,
// whatever its kind — a timeout, a log record, a metric.
//
// A Middleware is called once per job execution, with the next handler
// in the chain. It may inspect the job, derive a new context, run the
// next handler zero or more times, and return whatever it likes: the
// error the chain finally returns is the verdict on the attempt, so a
// middleware that swallows an error marks the job completed and one that
// returns an error without calling next fails the job without ever
// running its worker.
type Middleware func(Handler) Handler

// wrap composes mws around h so that the first element is outermost —
// the order every Go middleware chain uses, and the order jobs flow
// through: mws[0] sees the job first and its result last.
func wrap(h Handler, mws []Middleware) Handler {
	for _, mw := range slices.Backward(mws) {
		h = mw(h)
	}
	return h
}

// panicError is a recovered panic in the shape of an error.
//
// The stack travels inside the error rather than beside it because a
// Handler returns only an error, and the trace has to survive the whole
// chain — including a middleware that wraps the error with %w — to reach
// the attempt record. AD-013 makes a recovered panic a retryable failure
// with its trace retained; this is what retains it.
type panicError struct {
	value any
	stack []byte
}

func newPanicError(value any) *panicError {
	return &panicError{value: value, stack: debug.Stack()}
}

func (e *panicError) Error() string {
	return fmt.Sprintf("job panicked: %v", e.value)
}

// stackOf returns the stack trace carried by err, or nil if err is not a
// recovered panic.
func stackOf(err error) []byte {
	var pe *panicError
	if errors.As(err, &pe) {
		return pe.stack
	}
	return nil
}

// Timeout bounds how long a job's handler may run. When d elapses the
// context passed on to the rest of the chain is cancelled with
// context.DeadlineExceeded, which is the only way drover can ask a
// handler to stop: Go offers no way to halt a goroutine, so a handler
// that ignores its context runs to completion regardless.
//
// The handler's own outcome is passed through untouched, including when
// it returns after its deadline expired. Substituting an error of the
// middleware's own would misreport a handler that ignored cancellation
// and genuinely succeeded — work that was really done, recorded as
// failed and then done again on the retry.
//
// A d of zero or less applies no deadline, consistent with how every
// other duration drover takes treats a non-positive value.
//
// The deadline covers the handler alone. The context under which the
// attempt's outcome is recorded is derived separately and is never
// cancelled, so a job cut off by its timeout still finalizes rather than
// sitting running until its lease lapses (AD-027).
func Timeout(d time.Duration) Middleware {
	return func(next Handler) Handler {
		if d <= 0 {
			return next
		}
		return func(ctx context.Context, job *JobRow) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, job)
		}
	}
}

// Logging reports each job execution: one record when it starts and one
// when it ends, the latter carrying how long it took.
//
// A failed execution is reported at WARN rather than ERROR. A handler
// returning an error is designed behaviour that the retry machinery
// expects and handles, so logging it at ERROR would fill an operator's
// error budget with jobs that are working exactly as intended. Whether
// anything is actually wrong is decided when the attempt is disposed of,
// and that is where the ERROR lives — on a job that has exhausted its
// attempts.
//
// A nil logger falls back to slog.Default().
//
// The client installs this itself, outermost, so job logging does not
// disappear the moment a caller configures a middleware of their own. It
// is exported because it is also the smallest complete example of
// writing one.
func Logging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, job *JobRow) error {
			attrs := []any{
				"job_id", job.ID, "kind", job.Kind,
				"queue", job.Queue, "attempt", job.Attempt,
			}
			logger.Info("drover: job started", attrs...)

			start := time.Now()
			err := next(ctx, job)
			attrs = append(attrs, "duration", time.Since(start))

			if err != nil {
				logger.Warn("drover: job execution failed", append(attrs, "error", err)...)
				return err
			}
			logger.Info("drover: job execution finished", attrs...)
			return nil
		}
	}
}
