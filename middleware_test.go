package drover

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// record appends name to trace on the way in and name+":out" on the way
// out, so one slice shows both the order middleware were entered and the
// order they returned in.
func record(trace *[]string, name string) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *JobRow) error {
			*trace = append(*trace, name)
			err := next(ctx, job)
			*trace = append(*trace, name+":out")
			return err
		}
	}
}

func TestWrapRunsFirstMiddlewareOutermost(t *testing.T) {
	t.Parallel()

	var trace []string
	base := func(ctx context.Context, job *JobRow) error {
		trace = append(trace, "handler")
		return nil
	}

	h := wrap(base, []Middleware{record(&trace, "a"), record(&trace, "b")})
	if err := h(context.Background(), &JobRow{ID: 1}); err != nil {
		t.Fatalf("chain returned %v, want nil", err)
	}

	want := []string{"a", "b", "handler", "b:out", "a:out"}
	if strings.Join(trace, ",") != strings.Join(want, ",") {
		t.Errorf("trace = %v, want %v", trace, want)
	}
}

func TestWrapWithNoMiddlewareReturnsTheHandler(t *testing.T) {
	t.Parallel()

	called := false
	base := func(ctx context.Context, job *JobRow) error {
		called = true
		return nil
	}

	if err := wrap(base, nil)(context.Background(), &JobRow{ID: 1}); err != nil {
		t.Fatalf("chain returned %v, want nil", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

// A middleware that reports a failure with %w must not cost the attempt
// its stack trace: the trace is the only record of where a panic came
// from, and AD-013 requires it to reach the stored attempt error.
func TestPanicStackSurvivesErrorWrapping(t *testing.T) {
	t.Parallel()

	base := func(ctx context.Context, job *JobRow) error {
		panic("kaboom")
	}
	protected := func(ctx context.Context, job *JobRow) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = newPanicError(r)
			}
		}()
		return base(ctx, job)
	}
	wrapping := func(next Handler) Handler {
		return func(ctx context.Context, job *JobRow) error {
			if err := next(ctx, job); err != nil {
				return fmt.Errorf("middleware saw: %w", err)
			}
			return nil
		}
	}

	err := wrap(protected, []Middleware{wrapping})(context.Background(), &JobRow{ID: 1})
	if err == nil {
		t.Fatal("chain returned nil for a panicking handler")
	}
	if !strings.Contains(err.Error(), "middleware saw:") {
		t.Errorf("error = %q, want it to carry the middleware's wrapping", err)
	}

	stack := stackOf(err)
	if len(stack) == 0 {
		t.Fatal("stack did not survive the middleware's %w wrapping")
	}
	if !strings.Contains(string(stack), "TestPanicStackSurvivesErrorWrapping") {
		t.Errorf("stack does not name the panicking call site:\n%s", stack)
	}
}

func TestStackOfReturnsNothingForAnOrdinaryError(t *testing.T) {
	t.Parallel()

	if stack := stackOf(errors.New("plain")); stack != nil {
		t.Errorf("stackOf(plain error) = %q, want nil", stack)
	}
	if stack := stackOf(nil); stack != nil {
		t.Errorf("stackOf(nil) = %q, want nil", stack)
	}
}
