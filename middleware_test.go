package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
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

	bad, err := h.client.Insert(context.Background(), greetArgs{Name: "explode"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	good, err := h.client.Insert(context.Background(), greetArgs{Name: "fine"}, nil)
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

func TestChainIsFixedAtConstruction(t *testing.T) {
	t.Parallel()

	var ran atomic.Int32
	counting := func(next Handler) Handler {
		return func(ctx context.Context, job *JobRow) error {
			ran.Add(1)
			return next(ctx, job)
		}
	}

	// Spare capacity so the append writes into the very array the client
	// was handed, rather than reallocating and trivially leaving it alone.
	// What holds is that the chain was already composed at construction:
	// wrap closed over this one middleware, so there is nothing left for
	// the append to reach.
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete")
	h.stop(t)

	logs := h.logs.String()
	// Level as well as count: a start or success record emitted at ERROR
	// would train an operator to ignore the level that means something.
	for _, want := range []struct{ msg, level string }{
		{`msg="drover: job started"`, "level=INFO"},
		{`msg="drover: job execution finished"`, "level=INFO"},
	} {
		if got := countRecords(logs, want.msg); got != 1 {
			t.Errorf("records matching %s = %d, want 1\nlogs:\n%s", want.msg, got, logs)
			continue
		}
		found := false
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, want.msg) {
				found = strings.Contains(line, want.level)
				break
			}
		}
		if !found {
			t.Errorf("record %s was not logged at %s\nlogs:\n%s", want.msg, want.level, logs)
		}
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
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

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
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

// probeWriter runs probe at the instant a log record containing on is
// written, so a test can observe what had happened by then rather than
// only what had happened by the end.
type probeWriter struct {
	on    string
	probe func()
}

func (w *probeWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.on) {
		w.probe()
	}
	return len(p), nil
}

// Where the metrics middleware sits is asserted from what the chain
// does, not from how it is composed: the start record is written before
// the metrics middleware is entered, and by the time the job's own
// handler runs it has been.
//
// The in-flight gauge is the probe because it is the one metric that
// changes on the way *in*. A composition that put metrics outermost
// would leave logging reporting a job that had already been counted as
// executing.
func TestJobMetricsAreRecordedInsideTheLoggingMiddleware(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	executing := func() float64 {
		s, _ := seriesFor(t, reg, "drover_jobs_executing", map[string]string{})
		return s.value
	}

	var atStartRecord atomic.Value
	logs := &probeWriter{on: `msg="drover: job started"`, probe: func() {
		atStartRecord.Store(executing())
	}}

	ws := NewWorkers()
	var duringHandler atomic.Value
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		duringHandler.Store(executing())
		return nil
	}})
	c := newClient(memdriver.New(), Config{
		Workers:         ws,
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
		MetricsRegistry: reg,
	})

	job := &JobRow{ID: 1, Kind: "greet", Queue: "default", Args: []byte(`{"name":"ada"}`)}
	if err := c.chain(context.Background(), job); err != nil {
		t.Fatalf("chain returned %v, want nil", err)
	}

	before, ok := atStartRecord.Load().(float64)
	if !ok {
		t.Fatal("no start record was written — the logging middleware did not run")
	}
	if before != 0 {
		t.Errorf("drover_jobs_executing = %v when the start record was written, want 0 — "+
			"the metrics middleware ran outside the logging one", before)
	}
	if during, _ := duringHandler.Load().(float64); during != 1 {
		t.Errorf("drover_jobs_executing = %v while the handler ran, want 1", during)
	}
}

// Metrics are installed ahead of whatever the caller configured, so a
// middleware of theirs cannot decide what is counted. A middleware that
// swallows the worker's error is the sensor: the chain's verdict is
// success, so the counters must say success too — and would say failure
// if the metrics middleware had been composed inside the caller's.
func TestJobMetricsAreRecordedAheadOfConfiguredMiddleware(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("worker failed")
	}})
	c := newClient(memdriver.New(), Config{
		Workers:         ws,
		MetricsRegistry: reg,
		Middleware: []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				_ = next(ctx, job)
				return nil
			}
		}},
	})

	job := &JobRow{ID: 1, Kind: "greet", Queue: "default", Args: []byte(`{"name":"ada"}`)}
	if err := c.chain(context.Background(), job); err != nil {
		t.Fatalf("chain returned %v, want nil", err)
	}

	completed, ok := seriesFor(t, reg, "drover_jobs_completed_total", map[string]string{"queue": "default"})
	if !ok || completed.value != 1 {
		t.Errorf("drover_jobs_completed_total = %v (published: %t), want 1 — the metrics middleware "+
			"was composed inside the configured one, so it saw the error the caller swallowed",
			completed.value, ok)
	}
	if failed, ok := seriesFor(t, reg, "drover_jobs_failed_total", map[string]string{"queue": "default"}); ok && failed.value != 0 {
		t.Errorf("drover_jobs_failed_total = %v, want 0", failed.value)
	}
}

// The same trapdoor the built-in logging closed: configuring middleware
// must not be a way to end up with no metrics during an incident.
func TestConfiguringMiddlewareCannotDisableJobMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	c := newClient(memdriver.New(), Config{
		Workers:         ws,
		MetricsRegistry: reg,
		Middleware: []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error { return next(ctx, job) }
		}},
	})

	job := &JobRow{ID: 1, Kind: "greet", Queue: "critical", Args: []byte(`{"name":"ada"}`)}
	if err := c.chain(context.Background(), job); err != nil {
		t.Fatalf("chain returned %v, want nil", err)
	}

	labels := map[string]string{"queue": "critical"}
	completed, ok := seriesFor(t, reg, "drover_jobs_completed_total", labels)
	if !ok || completed.value != 1 {
		t.Errorf("drover_jobs_completed_total{queue=critical} = %v (published: %t), want 1", completed.value, ok)
	}
	duration, ok := seriesFor(t, reg, "drover_job_duration_seconds", labels)
	if !ok || duration.value != 1 {
		t.Errorf("drover_job_duration_seconds{queue=critical} holds %v observations (published: %t), want 1",
			duration.value, ok)
	}
}

func TestTimeoutGivesTheHandlerADeadline(t *testing.T) {
	t.Parallel()

	var got error
	h := wrap(func(ctx context.Context, job *JobRow) error {
		<-ctx.Done()
		got = ctx.Err()
		return got
	}, []Middleware{Timeout(20 * time.Millisecond)})

	start := time.Now()
	if err := h(context.Background(), &JobRow{ID: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("chain returned %v, want context.DeadlineExceeded", err)
	}
	if !errors.Is(got, context.DeadlineExceeded) {
		t.Errorf("handler's context reported %v, want context.DeadlineExceeded", got)
	}
	// Upper bound, not merely "it finished": without one, a middleware
	// that never applied a deadline and simply waited for the handler
	// would pass just as happily.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("handler ran for %v, want it cut off near 20ms", elapsed)
	}
}

func TestTimeoutPassesAFastHandlerThroughUntouched(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("the handler's own error")
	tests := []struct {
		name string
		ret  error
	}{
		{"success", nil},
		{"failure", sentinel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var deadline bool
			h := wrap(func(ctx context.Context, job *JobRow) error {
				_, deadline = ctx.Deadline()
				return tt.ret
			}, []Middleware{Timeout(time.Hour)})

			if err := h(context.Background(), &JobRow{ID: 1}); !errors.Is(err, tt.ret) {
				t.Errorf("chain returned %v, want %v", err, tt.ret)
			}
			if !deadline {
				t.Error("handler's context carried no deadline")
			}
		})
	}
}

// A handler that ignores cancellation and reports its own failure after
// the deadline must have *that* error recorded. Substituting the
// middleware's would misreport what actually happened.
func TestTimeoutKeepsTheErrorOfAHandlerThatOverranIt(t *testing.T) {
	t.Parallel()

	own := errors.New("what the handler decided")
	h := wrap(func(ctx context.Context, job *JobRow) error {
		<-ctx.Done()
		return own
	}, []Middleware{Timeout(10 * time.Millisecond)})

	err := h(context.Background(), &JobRow{ID: 1})
	if !errors.Is(err, own) {
		t.Errorf("chain returned %v, want the handler's own error %v", err, own)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Error("the middleware substituted its own error for the handler's")
	}
}

func TestTimeoutOfZeroOrLessAppliesNoDeadline(t *testing.T) {
	t.Parallel()

	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			t.Parallel()
			var deadline bool
			h := wrap(func(ctx context.Context, job *JobRow) error {
				_, deadline = ctx.Deadline()
				return nil
			}, []Middleware{Timeout(d)})

			if err := h(context.Background(), &JobRow{ID: 1}); err != nil {
				t.Fatalf("chain returned %v, want nil", err)
			}
			if deadline {
				t.Errorf("Timeout(%v) applied a deadline, want none", d)
			}
		})
	}
}

// The whole point of a timeout in a queue: the job has to end up
// finalized, not left running for the rescuer to collect a lease later.
func TestTimedOutJobIsFinalizedAndRetried(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	drv := &ctxDriver{Driver: mem}

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(ctx context.Context, _ *Job[greetArgs]) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	h := startLoopWith(t, drv, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{Timeout(20 * time.Millisecond)}
		cfg.RetryPolicy = atTimePolicy{at: time.Now().Add(time.Hour)}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "timed-out job to be queued for retry")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, context.DeadlineExceeded.Error()) {
		t.Fatalf("errors = %+v, want one entry naming the deadline", recorded)
	}
}

// Shutdown must still reach a handler that a timeout has already wrapped:
// the deadline narrows the context, it does not detach it.
func TestShutdownCancellationReachesAHandlerUnderATimeout(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	running := make(chan struct{})
	var once sync.Once
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(ctx context.Context, _ *Job[greetArgs]) error {
		once.Do(func() { close(running) })
		<-ctx.Done()
		return ctx.Err()
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		// Far longer than the test will wait, so only the shutdown can
		// end this handler.
		cfg.Middleware = []Middleware{Timeout(time.Hour)}
	})

	if _, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	// A budget that is already spent, so the shutdown escalates straight
	// to cancelling the job context.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = h.client.Stop(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("shutdown took %v — cancellation did not reach the handler under the timeout", elapsed)
	}
	h.cancel()
}

// The inner recovery, around the registered worker, is what turns a
// panicking job into an ordinary error *for the middleware wrapped
// around it*. Deleting it leaves the outer recovery to catch the panic,
// so the job still retries and a test watching only the job's fate stays
// green — while every middleware is skipped on the way out. This asserts
// what the outer recovery cannot provide: that the chain observed the
// failure as a return value.
func TestMiddlewareObservesAWorkerPanicAsAnOrdinaryError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	var mu sync.Mutex
	var seen []string
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		panic("kaboom")
	}})

	logs := &syncWriter{}
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Logger = newTestLogger(logs)
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				err := next(ctx, job)
				mu.Lock()
				if err == nil {
					seen = append(seen, "nil")
				} else {
					seen = append(seen, err.Error())
				}
				mu.Unlock()
				return err
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "panicked job to be queued for retry")
	h.stop(t)

	mu.Lock()
	got := slices.Clone(seen)
	mu.Unlock()

	if len(got) == 0 {
		t.Fatal("the middleware never returned — the panic unwound past it instead of becoming an error")
	}
	if !strings.Contains(got[0], "kaboom") {
		t.Errorf("middleware saw %q, want an error naming the panic", got[0])
	}

	// The logging middleware is wrapped around the same handler, so it
	// too must have reported an end record rather than being skipped.
	if n := countRecords(logs.String(), `msg="drover: job execution failed"`); n == 0 {
		t.Errorf("no execution-failure record — the panic bypassed the logging middleware\nlogs:\n%s", logs.String())
	}
}

// The unregistered-kind failure is raised inside the chain so middleware
// observes it. Moving it out to the caller leaves the job retrying
// exactly as before, which is why the job's fate alone cannot detect the
// move.
func TestMiddlewareRunsForAnUnregisteredKind(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	var ran atomic.Int32
	var saw atomic.Value
	logs := &syncWriter{}
	h := startLoopWith(t, mem, mem, NewWorkers(), func(cfg *Config) {
		cfg.Logger = newTestLogger(logs)
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				ran.Add(1)
				err := next(ctx, job)
				if err != nil {
					saw.Store(err.Error())
				}
				return err
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), ghostArgs{}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "job of an unregistered kind to be queued for retry")
	h.stop(t)

	if ran.Load() == 0 {
		t.Fatal("middleware never ran for a job whose kind has no worker")
	}
	msg, _ := saw.Load().(string)
	if !strings.Contains(msg, "no worker registered") {
		t.Errorf("middleware saw %q, want the unregistered-kind error", msg)
	}
	if n := countRecords(logs.String(), `msg="drover: job started"`); n == 0 {
		t.Errorf("the logging middleware did not run for an unregistered kind\nlogs:\n%s", logs.String())
	}
}

// The chain's return value is the attempt's verdict — that is what
// Middleware's doc promises, and it is the property an implementation
// that finalized on the *worker's* error instead would break while the
// mirror case (a middleware refusing before calling next) stayed green.
func TestChainVerdictDecidesTheAttempt(t *testing.T) {
	tests := []struct {
		name      string
		worker    error
		mw        Middleware
		wantState string
	}{
		{
			name:   "a middleware that swallows the worker's error completes the job",
			worker: errors.New("worker failed"),
			mw: func(next Handler) Handler {
				return func(ctx context.Context, job *JobRow) error {
					_ = next(ctx, job)
					return nil
				}
			},
			wantState: "completed",
		},
		{
			name:   "a middleware that replaces success with an error fails the job",
			worker: nil,
			mw: func(next Handler) Handler {
				return func(ctx context.Context, job *JobRow) error {
					if err := next(ctx, job); err != nil {
						return err
					}
					return errors.New("rejected after the fact")
				}
			},
			wantState: "retryable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			mem := memdriver.New()
			ws := NewWorkers()
			Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
				return tt.worker
			}})
			h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
				cfg.Middleware = []Middleware{tt.mw}
				cfg.RetryPolicy = atTimePolicy{at: time.Now().Add(time.Hour)}
			})

			row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			waitFor(t, h.rowInState(row.ID, tt.wantState), "the chain's verdict to decide the attempt")
			h.stop(t)
		})
	}
}

// Calling next more than once is permitted: the chain decides, and the
// error it finally returns is what the job is disposed on.
func TestMiddlewareMayCallTheHandlerMoreThanOnce(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	var calls atomic.Int32
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		// Fails once, then succeeds — so the job completes only if the
		// middleware's second call actually reached the worker.
		if calls.Add(1) == 1 {
			return errors.New("first attempt failed")
		}
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Middleware = []Middleware{func(next Handler) Handler {
			return func(ctx context.Context, job *JobRow) error {
				if err := next(ctx, job); err != nil {
					return next(ctx, job)
				}
				return nil
			}
		}}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "the retried call inside the middleware to complete the job")
	h.stop(t)

	if got := calls.Load(); got != 2 {
		t.Errorf("worker ran %d times, want 2 — the middleware's second call did not reach it", got)
	}
}

// The same value appearing twice must run at both positions, which also
// pins that wrap composes per element rather than deduplicating.
func TestTheSameMiddlewareValueRunsAtEveryPositionItOccupies(t *testing.T) {
	t.Parallel()

	var trace []string
	base := func(ctx context.Context, job *JobRow) error {
		trace = append(trace, "handler")
		return nil
	}
	repeated := record(&trace, "a")

	h := wrap(base, []Middleware{repeated, record(&trace, "b"), repeated})
	if err := h(context.Background(), &JobRow{ID: 1}); err != nil {
		t.Fatalf("chain returned %v, want nil", err)
	}

	want := "a,b,a,handler,a:out,b:out,a:out"
	if got := strings.Join(trace, ","); got != want {
		t.Errorf("trace = %q, want %q", got, want)
	}
}

// Logging is exported so callers can install it themselves, which makes
// the nil-logger fallback the path an outside user hits first. Nothing
// else exercises it: the client always passes its own logger.
func TestLoggingWithANilLoggerFallsBackToTheDefault(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("handler failed")
	for _, tt := range []struct {
		name string
		ret  error
	}{{"success", nil}, {"failure", sentinel}} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := wrap(func(ctx context.Context, job *JobRow) error {
				return tt.ret
			}, []Middleware{Logging(nil)})

			if err := h(context.Background(), &JobRow{ID: 1, Kind: "greet"}); !errors.Is(err, tt.ret) {
				t.Errorf("chain returned %v, want %v", err, tt.ret)
			}
		})
	}
}
