package drover

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
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
