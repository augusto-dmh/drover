package drover

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/memdriver"
)

type badArgs struct {
	Ch chan int
}

func (badArgs) Kind() string { return "bad" }

type emptyKindArgs struct{}

func (emptyKindArgs) Kind() string { return "" }

func assertNothingPersisted(t *testing.T, mem *memdriver.Driver) {
	t.Helper()
	rows, err := mem.FetchAvailable(context.Background(), "default", time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("found %d persisted jobs, want 0", len(rows))
	}
}

func TestInsertPersistsTypedJob(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	row, err := c.Insert(context.Background(), greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if row.Kind != "greet" {
		t.Errorf("Kind = %q, want greet", row.Kind)
	}
	if row.Queue != "default" {
		t.Errorf("Queue = %q, want default", row.Queue)
	}
	if row.State != StateAvailable {
		t.Errorf("State = %q, want %q", row.State, StateAvailable)
	}
	if string(row.Args) != `{"name":"ada"}` {
		t.Errorf("Args = %s, want {\"name\":\"ada\"}", row.Args)
	}
	if stored, ok := mem.Row(row.ID); !ok || stored.State != "available" {
		t.Errorf("stored row missing or not available: %+v", stored)
	}
}

func TestInsertRejectsEmptyKind(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	_, err := c.Insert(context.Background(), emptyKindArgs{}, nil)

	if !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("error = %v, want ErrInvalidKind", err)
	}
	assertNothingPersisted(t, mem)
}

func TestInsertWrapsMarshalFailure(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	_, err := c.Insert(context.Background(), badArgs{}, nil)

	if err == nil {
		t.Fatal("Insert succeeded with unmarshalable args")
	}
	if !strings.Contains(err.Error(), `kind "bad"`) {
		t.Errorf("error = %q, want it to name the kind", err)
	}
	assertNothingPersisted(t, mem)
}

func TestInsertTxValidatesBeforeTouchingTransaction(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	if _, err := c.InsertTx(context.Background(), nil, emptyKindArgs{}, nil); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("InsertTx empty kind: %v, want ErrInvalidKind", err)
	}
	if _, err := c.InsertTx(context.Background(), nil, badArgs{}, nil); err == nil || !strings.Contains(err.Error(), `kind "bad"`) {
		t.Fatalf("InsertTx marshal failure: %v, want wrapped error naming the kind", err)
	}
	assertNothingPersisted(t, mem)
}

func TestNewClientRejectsNilPool(t *testing.T) {
	t.Parallel()

	_, err := NewClient(nil, Config{})
	if err == nil {
		t.Fatal("NewClient(nil) returned no error")
	}
}

func TestConfigZeroValuesGetDefaults(t *testing.T) {
	t.Parallel()

	c := newClient(memdriver.New(), Config{})

	if c.logger != slog.Default() {
		t.Error("Logger default is not slog.Default()")
	}
	if c.pollInterval != time.Second {
		t.Errorf("PollInterval default = %v, want 1s", c.pollInterval)
	}
	if c.workers == nil {
		t.Error("Workers default is nil, want empty registry")
	}
	if _, ok := c.retryPolicy.(ExponentialRetryPolicy); !ok {
		t.Errorf("RetryPolicy default = %T, want ExponentialRetryPolicy", c.retryPolicy)
	}
	if c.leaseDuration != time.Minute {
		t.Errorf("LeaseDuration default = %v, want 1m", c.leaseDuration)
	}
	if c.heartbeatInterval != 20*time.Second {
		t.Errorf("HeartbeatInterval default = %v, want a third of the lease", c.heartbeatInterval)
	}
	if c.rescueInterval != time.Minute {
		t.Errorf("RescueInterval default = %v, want the lease duration", c.rescueInterval)
	}
	if c.inflight == nil {
		t.Error("in-flight set is nil, want an empty set")
	}
	if c.concurrency != 10 {
		t.Errorf("Concurrency default = %d, want 10", c.concurrency)
	}
	if c.statsInterval != 15*time.Second {
		t.Errorf("StatsInterval default = %v, want 15s", c.statsInterval)
	}
}

// Prometheus refuses the same collector twice on one registry, so a
// client that registered its metrics on a shared or global one could not
// be constructed twice in a process: the second would panic at
// construction. Every client gets a registry of its own instead.
func TestTwoClientsInOneProcessBothConstruct(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("constructing a second client panicked: %v", r)
		}
	}()

	first := newClient(memdriver.New(), Config{})
	second := newClient(memdriver.New(), Config{})

	if first.metrics == nil || second.metrics == nil {
		t.Fatal("a client was built without a metric set")
	}
	if first.metrics == second.metrics {
		t.Error("both clients were given the same metric set")
	}
}

func TestConcurrencyConfigFallsBackToTheDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want int
	}{
		{name: "zero is treated as unset", cfg: Config{Concurrency: 0}, want: 10},
		{name: "negative is treated as unset", cfg: Config{Concurrency: -4}, want: 10},
		{name: "one is honoured, not mistaken for unset", cfg: Config{Concurrency: 1}, want: 1},
		{name: "an explicit value is kept", cfg: Config{Concurrency: 64}, want: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newClient(memdriver.New(), tt.cfg)

			if c.concurrency != tt.want {
				t.Errorf("concurrency = %d, want %d", c.concurrency, tt.want)
			}
		})
	}
}

func TestTimingConfigFallsBackToUsableValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfg           Config
		wantLease     time.Duration
		wantHeartbeat time.Duration
		wantRescue    time.Duration
	}{
		{
			name:          "negative values are treated as unset",
			cfg:           Config{LeaseDuration: -time.Second, HeartbeatInterval: -time.Second, RescueInterval: -time.Second},
			wantLease:     time.Minute,
			wantHeartbeat: 20 * time.Second,
			wantRescue:    time.Minute,
		},
		{
			name:          "a shortened lease pulls the derived intervals with it",
			cfg:           Config{LeaseDuration: 30 * time.Second},
			wantLease:     30 * time.Second,
			wantHeartbeat: 10 * time.Second,
			wantRescue:    30 * time.Second,
		},
		{
			// A heartbeat this slow could only ever renew after the lease
			// it renews has already lapsed, so every job outliving one
			// lease would be rescued while still running.
			name:          "a heartbeat as slow as the lease is replaced",
			cfg:           Config{LeaseDuration: time.Minute, HeartbeatInterval: time.Minute},
			wantLease:     time.Minute,
			wantHeartbeat: 20 * time.Second,
			wantRescue:    time.Minute,
		},
		{
			name:          "a heartbeat slower than the lease is replaced",
			cfg:           Config{LeaseDuration: time.Minute, HeartbeatInterval: 5 * time.Minute},
			wantLease:     time.Minute,
			wantHeartbeat: 20 * time.Second,
			wantRescue:    time.Minute,
		},
		{
			name:          "explicit values are kept",
			cfg:           Config{LeaseDuration: time.Minute, HeartbeatInterval: 5 * time.Second, RescueInterval: 2 * time.Minute},
			wantLease:     time.Minute,
			wantHeartbeat: 5 * time.Second,
			wantRescue:    2 * time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(memdriver.New(), tc.cfg)

			if c.leaseDuration != tc.wantLease {
				t.Errorf("leaseDuration = %v, want %v", c.leaseDuration, tc.wantLease)
			}
			if c.heartbeatInterval != tc.wantHeartbeat {
				t.Errorf("heartbeatInterval = %v, want %v", c.heartbeatInterval, tc.wantHeartbeat)
			}
			if c.rescueInterval != tc.wantRescue {
				t.Errorf("rescueInterval = %v, want %v", c.rescueInterval, tc.wantRescue)
			}
			if c.heartbeatInterval >= c.leaseDuration {
				t.Errorf("heartbeatInterval %v is not shorter than the lease %v — a lease "+
					"renewed this slowly always lapses first",
					c.heartbeatInterval, c.leaseDuration)
			}
		})
	}
}

func TestInsertOptsChooseQueueAndSchedule(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour)
	tests := []struct {
		name      string
		opts      *InsertOpts
		wantQueue string
		wantState JobState
		scheduled bool
	}{
		{"nil opts keep today's defaults", nil, "default", StateAvailable, false},
		{"a zero value is the same as nil", &InsertOpts{}, "default", StateAvailable, false},
		{"an empty queue name means default", &InsertOpts{Queue: ""}, "default", StateAvailable, false},
		{"a named queue is stored", &InsertOpts{Queue: "critical"}, "critical", StateAvailable, false},
		{"a future time waits", &InsertOpts{ScheduledAt: future}, "default", StateScheduled, true},
		{
			"a queue and a delay together",
			&InsertOpts{Queue: "digest", ScheduledAt: future},
			"digest", StateScheduled, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(memdriver.New(), Config{})

			row, err := c.Insert(context.Background(), greetArgs{Name: "ada"}, tt.opts)
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if row.Queue != tt.wantQueue {
				t.Errorf("Queue = %q, want %q", row.Queue, tt.wantQueue)
			}
			if row.State != tt.wantState {
				t.Errorf("State = %q, want %q", row.State, tt.wantState)
			}
			if tt.scheduled && !row.ScheduledAt.Equal(future) {
				t.Errorf("ScheduledAt = %v, want %v", row.ScheduledAt, future)
			}
			if !tt.scheduled && row.ScheduledAt.After(time.Now().Add(time.Minute)) {
				t.Errorf("ScheduledAt = %v, want a job due now", row.ScheduledAt)
			}
		})
	}
}

// A delayed job must not be handed to a worker before its time, and must
// be once it passes — with nothing having to promote it in between.
func TestScheduledJobRunsOnlyOnceItIsDue(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()

	// Recorded by the worker itself, because when the job *ran* is the
	// only thing that can show the schedule was honoured. Reading the
	// clock after the test has already waited for completion measures
	// when the assertion executed, which is unconditionally later than
	// the due time and so can never fail.
	var ranAt atomic.Pointer[time.Time]
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		now := time.Now()
		ranAt.CompareAndSwap(nil, &now)
		return nil
	}})
	h := startLoop(t, mem, mem, ws)

	due := time.Now().Add(150 * time.Millisecond)
	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, &InsertOpts{ScheduledAt: due})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Long enough for several poll intervals to pass, so an unhonoured
	// schedule would have been claimed by now.
	time.Sleep(60 * time.Millisecond)
	stored, ok := mem.Row(row.ID)
	if !ok {
		t.Fatal("job disappeared before its scheduled time")
	}
	if stored.State != "scheduled" {
		t.Fatalf("state = %q before the scheduled time, want scheduled", stored.State)
	}

	waitFor(t, h.rowInState(row.ID, "completed"), "scheduled job to run once it came due")
	h.stop(t)

	at := ranAt.Load()
	if at == nil {
		t.Fatal("worker never recorded a run time")
	}
	if at.Before(due) {
		t.Errorf("job ran at %v, before its scheduled time %v", at, due)
	}
}

func TestStatsIntervalZeroTakesTheDefaultSilently(t *testing.T) {
	t.Parallel()

	logs := &syncWriter{}
	c := newClient(memdriver.New(), Config{
		Logger:        newTestLogger(logs),
		StatsInterval: 0,
	})
	if c.statsInterval != defaultStatsInterval {
		t.Errorf("statsInterval = %v, want %v", c.statsInterval, defaultStatsInterval)
	}
	if strings.Contains(logs.String(), "stats interval") {
		t.Errorf("zero StatsInterval logged a warning; unset must default silently\nlogs:\n%s", logs)
	}
}

func TestStatsIntervalNonPositiveWarnsAndDefaults(t *testing.T) {
	t.Parallel()

	logs := &syncWriter{}
	c := newClient(memdriver.New(), Config{
		Logger:        newTestLogger(logs),
		StatsInterval: -time.Second,
	})
	if c.statsInterval != defaultStatsInterval {
		t.Errorf("statsInterval = %v, want %v", c.statsInterval, defaultStatsInterval)
	}
	if !strings.Contains(logs.String(), `level=WARN msg="drover: stats interval must be positive; using the default instead"`) {
		t.Errorf("logs missing the stats-interval warning\nlogs:\n%s", logs)
	}
}

func TestStatsIntervalExplicitPositiveIsKept(t *testing.T) {
	t.Parallel()

	c := newClient(memdriver.New(), Config{StatsInterval: 3 * time.Second})
	if c.statsInterval != 3*time.Second {
		t.Errorf("statsInterval = %v, want 3s", c.statsInterval)
	}
}

// A client that is never started must not touch the store for gauges:
// constructing one with the ops surface configured is not a signal to
// begin polling.
func TestNeverStartedClientIssuesNoStatsCall(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	_ = newClient(drv, Config{
		MetricsRegistry: prometheus.NewRegistry(),
		StatsInterval:   time.Millisecond,
	})
	if n := drv.calls.Load(); n != 0 {
		t.Errorf("Stats calls = %d on a never-started client, want 0", n)
	}
}

// Scrapes read gauges; they must not pull a fresh Stats reading. The
// call count across many gathers inside one interval is the sensor —
// not an inspection of whether Gather calls the driver.
func TestGatherWithinOneIntervalIssuesNoStatsCall(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	reg := prometheus.NewRegistry()
	c := newClient(drv, Config{
		Workers:         NewWorkers(),
		Logger:          newTestLogger(&syncWriter{}),
		PollInterval:    time.Hour,
		MetricsRegistry: reg,
		StatsInterval:   time.Hour,
		Concurrency:     1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, func() bool { return drv.calls.Load() >= 1 }, "the immediate first refresh")
	settled := drv.calls.Load()

	for i := 0; i < 50; i++ {
		if _, err := reg.Gather(); err != nil {
			t.Fatalf("Gather #%d: %v", i, err)
		}
	}

	if after := drv.calls.Load(); after != settled {
		t.Errorf("Stats calls rose from %d to %d across gathers inside one interval — scrape must not query the store",
			settled, after)
	}

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStopJoinsTheStatsRefresher(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	c := newClient(drv, Config{
		Workers:       NewWorkers(),
		Logger:        newTestLogger(&syncWriter{}),
		PollInterval:  time.Hour,
		StatsInterval: 5 * time.Millisecond,
		Concurrency:   1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, func() bool { return drv.calls.Load() >= 1 }, "the refresher to start")

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
