package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/memdriver"
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

func TestConfiguredMiddlewareWrapsTheWorkerOutermostFirst(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	var mu sync.Mutex
	var trace []string
	note := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				mu.Lock()
				trace = append(trace, name)
				mu.Unlock()
				err := next(ctx, job)
				mu.Lock()
				trace = append(trace, name+":out")
				mu.Unlock()
				return err
			}
		}
	}

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		mu.Lock()
		trace = append(trace, "worker")
		mu.Unlock()
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{note("outer"), note("inner")}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete")
	h.stop(t)

	mu.Lock()
	got := strings.Join(trace, ",")
	mu.Unlock()
	want := "outer,inner,worker,inner:out,outer:out"
	if got != want {
		t.Errorf("trace = %q, want %q", got, want)
	}
}

// A middleware that refuses the job must keep the worker from running at
// all, and its error must still decide the attempt's fate.
func TestMiddlewareCanRefuseAJobWithoutRunningTheWorker(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	var ran atomic.Bool
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		ran.Store(true)
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				return errors.New("refused by policy")
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "refused job to be queued for retry")
	h.stop(t)

	if ran.Load() {
		t.Error("worker ran even though the middleware never called it")
	}
	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "refused by policy") {
		t.Errorf("errors = %+v, want one entry naming the middleware's refusal", recorded)
	}
}

type ctxKey string

func TestMiddlewareContextReachesTheWorker(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	const key = ctxKey("tenant")
	seen := make(chan any, 1)
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(ctx context.Context, _ *Job[greetArgs]) error {
		seen <- ctx.Value(key)
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				return next(context.WithValue(ctx, key, "acme"), job)
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete")
	h.stop(t)

	select {
	case got := <-seen:
		if got != "acme" {
			t.Errorf("worker saw context value %v, want %q", got, "acme")
		}
	default:
		t.Fatal("worker never ran")
	}
}

// The context a middleware derives is the handler's alone. If it reached
// the write that records the outcome, a middleware that cancels — which
// is exactly what a timeout does — would leave every job it interrupted
// stuck running until its lease lapsed.
func TestMiddlewareCancellationDoesNotReachTheFinalizeContext(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	drv := &ctxDriver{Driver: mem}

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(ctx context.Context, _ *Job[greetArgs]) error {
		<-ctx.Done()
		return nil
	}})
	h := startLoopWith(t, drv, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				return next(ctx, job)
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"),
		"job to be finalized despite the middleware cancelling the handler's context")
	h.stop(t)
}

// A panicking middleware must not unwind its pool worker's goroutine:
// nothing supervises those, so the pool would silently lose a worker.
func TestPanickingMiddlewareDoesNotKillThePool(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				var args greetArgs
				if err := json.Unmarshal(job.Args, &args); err != nil {
					return err
				}
				if args.Name == "explode" {
					panic("middleware exploded")
				}
				return next(ctx, job)
			}
		}}
	})

	bad, err := h.client.Insert(context.Background(), greetArgs{Name: "explode"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	good, err := h.client.Insert(context.Background(), greetArgs{Name: "fine"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	waitFor(t, h.rowInState(bad.ID, "retryable"), "job failed by a panicking middleware to be queued for retry")
	waitFor(t, h.rowInState(good.ID, "completed"), "pool to keep working after a middleware panicked")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, bad.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "middleware exploded") {
		t.Fatalf("errors = %+v, want one entry naming the middleware panic", recorded)
	}
	if recorded[0].Trace == "" {
		t.Error("no stack recorded for a middleware panic")
	}
}

func TestMiddlewareSliceIsCopiedAtConstruction(t *testing.T) {
	t.Parallel()

	var ran atomic.Int32
	counting := func(next Handler) Handler {
		return func(ctx context.Context, job *JobRow) error {
			ran.Add(1)
			return next(ctx, job)
		}
	}

	// Spare capacity is the point: appending to a full slice reallocates
	// and would leave the client's copy untouched no matter how it was
	// stored, so the test would pass without proving anything. With room
	// to grow, the append writes into the very array the client was
	// handed — only a genuine copy survives it.
	mws := make([]Middleware, 1, 2)
	mws[0] = counting
	c := newClient(memdriver.New(), Config{Middleware: mws})

	mws = append(mws, counting)
	if len(mws) != 2 {
		t.Fatalf("append produced %d middleware, want 2", len(mws))
	}

	if err := c.chain(context.Background(), &JobRow{ID: 1, Kind: "ghost"}); err == nil {
		t.Fatal("chain returned nil for an unregistered kind")
	}
	if got := ran.Load(); got != 1 {
		t.Errorf("middleware ran %d times, want 1 — the chain grew after construction", got)
	}
}

func TestNilMiddlewarePanicsNamingItsIndex(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a nil middleware did not panic")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, "Middleware[1]") {
			t.Errorf("panic = %q, want it to name index 1", msg)
		}
	}()

	passthrough := func(next Handler) Handler { return next }
	newClient(memdriver.New(), Config{Middleware: []Middleware{passthrough, nil}})
}

// countRecords returns how many logged lines contain each of the given
// substrings, so a test can assert a record appeared exactly once rather
// than merely appeared.
func countRecords(logs, substr string) int {
	n := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func TestLoggingReportsOneStartAndOneEndPerSuccessfulJob(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoop(t, mem, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete")
	h.stop(t)

	logs := h.logs.String()
	if got := countRecords(logs, `msg="drover: job started"`); got != 1 {
		t.Errorf("start records = %d, want 1\nlogs:\n%s", got, logs)
	}
	if got := countRecords(logs, `msg="drover: job execution finished"`); got != 1 {
		t.Errorf("success records = %d, want 1\nlogs:\n%s", got, logs)
	}
	// The line the middleware replaced must not also be emitted, or every
	// successful job would report its success twice.
	if got := countRecords(logs, `msg="drover: job completed"`); got != 0 {
		t.Errorf("the replaced success record still appears %d time(s)\nlogs:\n%s", got, logs)
	}
	for _, want := range []string{"queue=default", "duration="} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %q\nlogs:\n%s", want, logs)
		}
	}
}

// A failed attempt is designed behaviour the retry machinery expects.
// Reporting it at ERROR would put an entry in an operator's error budget
// for a queue that is working exactly as intended.
func TestLoggingReportsAFailedExecutionAtWarnNotError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("boom")
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = atTimePolicy{at: time.Now().Add(time.Hour)}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "failed job to be queued for retry")
	h.stop(t)

	logs := h.logs.String()
	failure := ""
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `msg="drover: job execution failed"`) {
			failure = line
			break
		}
	}
	if failure == "" {
		t.Fatalf("no execution-failure record\nlogs:\n%s", logs)
	}
	if !strings.Contains(failure, "level=WARN") {
		t.Errorf("execution failure logged as %q, want level=WARN", failure)
	}
	for _, want := range []string{"boom", "duration="} {
		if !strings.Contains(failure, want) {
			t.Errorf("execution-failure record missing %q: %s", want, failure)
		}
	}
	// The disposition is still reported separately and exactly once: it
	// carries what the execution record cannot, namely what became of the
	// job afterwards.
	if got := countRecords(logs, `msg="drover: job failed, retry scheduled"`); got != 1 {
		t.Errorf("retry-scheduled records = %d, want 1\nlogs:\n%s", got, logs)
	}
}

func TestLoggingRunsEvenWhenTheCallerConfiguresMiddleware(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})

	// The harness's own writer cannot be read from inside the middleware —
	// the harness does not exist yet when the closure is built — so the
	// test supplies the logger it will assert on.
	logs := &syncWriter{}
	var sawStart atomic.Bool
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Logger = newTestLogger(logs)
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				// The built-in logging is outermost, so by the time a
				// configured middleware runs the start record already exists.
				sawStart.Store(strings.Contains(logs.String(), `msg="drover: job started"`))
				return next(ctx, job)
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete")
	h.stop(t)

	out := logs.String()
	if got := countRecords(out, `msg="drover: job started"`); got != 1 {
		t.Errorf("start records = %d, want 1 — configuring middleware lost the built-in logging\nlogs:\n%s", got, out)
	}
	if !sawStart.Load() {
		t.Error("the built-in logging did not run outside the configured middleware")
	}
}
