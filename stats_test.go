package drover

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

// scriptedStatsDriver returns a fixed Stats result, or an error once the
// script says so. It is the seam that lets refresh tests control what
// the gauges see without going through Insert.
type scriptedStatsDriver struct {
	driver.Driver
	mu    sync.Mutex
	stats *driver.Stats
	err   error
	calls atomic.Int64
}

func (d *scriptedStatsDriver) Stats(context.Context) (*driver.Stats, error) {
	d.calls.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	if d.stats == nil {
		return &driver.Stats{}, nil
	}
	out := &driver.Stats{
		Depths: append([]driver.QueueDepth(nil), d.stats.Depths...),
		Oldest: append([]driver.QueueAge(nil), d.stats.Oldest...),
	}
	return out, nil
}

func (d *scriptedStatsDriver) set(stats *driver.Stats, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats = stats
	d.err = err
}

func newTestRefresher(
	t *testing.T,
	queues []weightedQueue,
	drv *scriptedStatsDriver,
) (*statsRefresher, *prometheus.Registry, *syncWriter) {
	t.Helper()
	m, reg, logs := newTestMetricSet(t, 1)
	r := newStatsRefresher(drv, m, queues, time.Hour, newTestLogger(logs))
	return r, reg, logs
}

func TestRefreshSeedsConfiguredQueuesToZero(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	r, reg, _ := newTestRefresher(t, []weightedQueue{
		{name: "critical", weight: 1},
		{name: "bulk", weight: 1},
	}, drv)

	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	for _, queue := range []string{"critical", "bulk"} {
		for _, state := range publishedDepthStates {
			s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
				"queue": queue, "state": state,
			})
			if !ok {
				t.Errorf("depth{%s,%s} missing — configured idle queue must read zero, not absent", queue, state)
				continue
			}
			if s.value != 0 {
				t.Errorf("depth{%s,%s} = %v, want 0", queue, state, s.value)
			}
		}
		s, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": queue})
		if !ok {
			t.Errorf("oldest_job_age{%s} missing — configured idle queue must read zero, not absent", queue)
			continue
		}
		if s.value != 0 {
			t.Errorf("oldest_job_age{%s} = %v, want 0", queue, s.value)
		}
	}
}

// blockingStatsDriver holds the first Stats call until release is closed,
// so a Gather can run while the first refresh is still in flight.
type blockingStatsDriver struct {
	driver.Driver
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *blockingStatsDriver) Stats(ctx context.Context) (*driver.Stats, error) {
	d.once.Do(func() { close(d.entered) })
	select {
	case <-d.release:
		return &driver.Stats{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A scrape that arrives while the first Stats call is still blocked must
// already see configured-queue depth and age series at zero. Seeding
// those children only after Stats returns would omit them; waiting on
// Stats would couple scrape latency to the database.
func TestGatherBeforeFirstRefreshServesConfiguredQueueZeros(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	drv := &blockingStatsDriver{
		Driver:  memdriver.New(),
		entered: entered,
		release: release,
	}
	m, reg, _ := newTestMetricSet(t, 1)
	r := newStatsRefresher(drv, m, []weightedQueue{
		{name: "critical", weight: 1},
		{name: "bulk", weight: 1},
	}, time.Hour, newTestLogger(&syncWriter{}))

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- r.refresh(context.Background())
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Stats was never called")
	}

	gatherDone := make(chan error, 1)
	go func() {
		_, err := reg.Gather()
		gatherDone <- err
	}()

	var gatherErr error
	select {
	case gatherErr = <-gatherDone:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("Gather blocked while the first refresh was in flight")
	}
	if gatherErr != nil {
		close(release)
		t.Fatalf("Gather: %v", gatherErr)
	}

	for _, queue := range []string{"critical", "bulk"} {
		for _, state := range publishedDepthStates {
			s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
				"queue": queue, "state": state,
			})
			if !ok {
				t.Errorf("depth{%s,%s} missing before first refresh completed", queue, state)
				continue
			}
			if s.value != 0 {
				t.Errorf("depth{%s,%s} = %v, want 0", queue, state, s.value)
			}
		}
		s, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": queue})
		if !ok {
			t.Errorf("oldest_job_age{%s} missing before first refresh completed", queue)
			continue
		}
		if s.value != 0 {
			t.Errorf("oldest_job_age{%s} = %v, want 0", queue, s.value)
		}
	}

	close(release)
	select {
	case err := <-refreshDone:
		if err != nil {
			t.Fatalf("refresh after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not return after Stats was released")
	}
}

func TestRefreshPublishesDatabaseOnlyQueues(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	drv.set(&driver.Stats{
		Depths: []driver.QueueDepth{
			{Queue: "foreign", State: "available", Count: 3},
			{Queue: "foreign", State: "dead", Count: 1},
		},
		Oldest: []driver.QueueAge{{Queue: "foreign", AgeSeconds: 12}},
	}, nil)
	r, reg, _ := newTestRefresher(t, []weightedQueue{{name: "default", weight: 1}}, drv)

	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
		"queue": "foreign", "state": "available",
	})
	if !ok {
		t.Fatal("depth{foreign,available} missing — a DB-only queue must still be published")
	}
	if s.value != 3 {
		t.Errorf("depth{foreign,available} = %v, want 3", s.value)
	}
	s, ok = seriesFor(t, reg, "drover_queue_depth", map[string]string{
		"queue": "foreign", "state": "dead",
	})
	if !ok || s.value != 1 {
		t.Errorf("depth{foreign,dead} = %v present=%v, want 1", s.value, ok)
	}
	s, ok = seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "foreign"})
	if !ok || s.value != 12 {
		t.Errorf("oldest_job_age{foreign} = %v present=%v, want 12", s.value, ok)
	}
}

func TestRefreshAppliesDepthAndAgeFromStats(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	drv.set(&driver.Stats{
		Depths: []driver.QueueDepth{
			{Queue: "default", State: "available", Count: 4},
			{Queue: "default", State: "running", Count: 2},
			{Queue: "default", State: "dead", Count: 1},
		},
		Oldest: []driver.QueueAge{{Queue: "default", AgeSeconds: 42.5}},
	}, nil)
	r, reg, _ := newTestRefresher(t, []weightedQueue{{name: "default", weight: 1}}, drv)

	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	cases := []struct {
		state string
		want  float64
	}{
		{"available", 4},
		{"running", 2},
		{"dead", 1},
		{"scheduled", 0},
		{"retryable", 0},
	}
	for _, tc := range cases {
		s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
			"queue": "default", "state": tc.state,
		})
		if !ok {
			t.Errorf("depth{default,%s} missing", tc.state)
			continue
		}
		if s.value != tc.want {
			t.Errorf("depth{default,%s} = %v, want %v", tc.state, s.value, tc.want)
		}
	}
	s, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "default"})
	if !ok {
		t.Fatal("oldest_job_age{default} missing")
	}
	if s.value != 42.5 {
		t.Errorf("oldest_job_age{default} = %v, want 42.5", s.value)
	}
}

func TestRefreshFailureLeavesGaugesAndFreshnessUntouched(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	drv.set(&driver.Stats{
		Depths: []driver.QueueDepth{{Queue: "default", State: "available", Count: 7}},
		Oldest: []driver.QueueAge{{Queue: "default", AgeSeconds: 9}},
	}, nil)
	r, reg, logs := newTestRefresher(t, []weightedQueue{{name: "default", weight: 1}}, drv)

	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	successAt := time.Now()
	if !r.fresh(successAt, time.Minute) {
		t.Fatal("fresh after success = false, want true")
	}

	drv.set(nil, errors.New("database unreachable"))
	if err := r.refresh(context.Background()); err == nil {
		t.Fatal("refresh error = nil, want database error")
	}

	s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
		"queue": "default", "state": "available",
	})
	if !ok || s.value != 7 {
		t.Errorf("depth after failed refresh = %v present=%v, want 7 unchanged", s.value, ok)
	}
	s, ok = seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "default"})
	if !ok || s.value != 9 {
		t.Errorf("age after failed refresh = %v present=%v, want 9 unchanged", s.value, ok)
	}
	// Freshness must not advance on failure: a clock past the prior
	// success by more than the bound must read stale.
	if r.fresh(successAt.Add(2*time.Minute), time.Minute) {
		t.Error("fresh advanced after a failed refresh — lastSuccess must stay put")
	}
	if strings.Contains(logs.String(), "refresh queue stats") {
		t.Error("refresh itself logged; warnings belong to run, which owns the loop")
	}
}

func TestRunLogsWarningOnRefreshFailure(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	drv.set(nil, errors.New("connection refused"))
	m, _, logs := newTestMetricSet(t, 1)
	r := newStatsRefresher(drv, m, []weightedQueue{{name: "default", weight: 1}}, time.Hour, newTestLogger(logs))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.run(ctx)
	}()

	waitFor(t, func() bool {
		return strings.Contains(logs.String(), `level=WARN msg="drover: refresh queue stats"`)
	}, "the refresher to warn on a failed refresh")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func TestFreshIsPureFunctionOfRecordedState(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	r, _, _ := newTestRefresher(t, []weightedQueue{{name: "default", weight: 1}}, drv)

	now := time.Now()
	bound := 30 * time.Second

	if r.fresh(now, bound) {
		t.Error("fresh before any success = true, want false")
	}

	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	r.mu.Lock()
	r.lastSuccess = now
	r.mu.Unlock()

	tests := []struct {
		name string
		at   time.Time
		want bool
	}{
		{name: "at the success instant", at: now, want: true},
		{name: "inside the bound", at: now.Add(bound / 2), want: true},
		{name: "exactly at the bound", at: now.Add(bound), want: true},
		{name: "beyond the bound", at: now.Add(bound + time.Nanosecond), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := r.fresh(tt.at, bound); got != tt.want {
				t.Errorf("fresh(%v) = %v, want %v", tt.at.Sub(now), got, tt.want)
			}
		})
	}
}

func TestRunRefreshesImmediatelyAndOnEachTick(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	synctest.Test(t, func(t *testing.T) {
		drv := &scriptedStatsDriver{Driver: memdriver.New()}
		m, _, _ := newTestMetricSet(t, 1)
		r := newStatsRefresher(
			drv, m, []weightedQueue{{name: "default", weight: 1}},
			time.Second, newTestLogger(&syncWriter{}),
		)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.run(ctx)
		}()

		synctest.Wait()
		if n := drv.calls.Load(); n != 1 {
			t.Fatalf("Stats calls after immediate refresh = %d, want 1", n)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if n := drv.calls.Load(); n < 2 {
			t.Fatalf("Stats calls after first tick = %d, want at least 2", n)
		}

		cancel()
		<-done
	})
}

func TestRefreshDeletesDrainedSeriesWithoutBlankingSurvivors(t *testing.T) {
	t.Parallel()

	drv := &scriptedStatsDriver{Driver: memdriver.New()}
	drv.set(&driver.Stats{
		Depths: []driver.QueueDepth{
			{Queue: "keep", State: "available", Count: 5},
			{Queue: "gone", State: "available", Count: 3},
		},
		Oldest: []driver.QueueAge{
			{Queue: "keep", AgeSeconds: 10},
			{Queue: "gone", AgeSeconds: 8},
		},
	}, nil)
	r, reg, _ := newTestRefresher(t, nil, drv)

	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	survivor := map[string]string{"queue": "keep", "state": "available"}
	vanished := map[string]string{"queue": "gone", "state": "available"}

	var absent atomic.Bool
	r.observe = func() {
		// Widen each mutation so a Reset-first implementation leaves a
		// window a concurrent gatherer (and this check) can catch.
		time.Sleep(time.Millisecond)
		if _, ok := seriesFor(t, reg, "drover_queue_depth", survivor); !ok {
			absent.Store(true)
		}
	}

	stop := make(chan struct{})
	var gatherer sync.WaitGroup
	gatherer.Add(1)
	go func() {
		defer gatherer.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, ok := seriesFor(t, reg, "drover_queue_depth", survivor); !ok {
					absent.Store(true)
				}
			}
		}
	}()

	drv.set(&driver.Stats{
		Depths: []driver.QueueDepth{{Queue: "keep", State: "available", Count: 9}},
		Oldest: []driver.QueueAge{{Queue: "keep", AgeSeconds: 11}},
	}, nil)
	if err := r.refresh(context.Background()); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	close(stop)
	gatherer.Wait()

	if absent.Load() {
		t.Fatal("surviving series was momentarily absent — refresh must update in place, not Reset")
	}
	if _, ok := seriesFor(t, reg, "drover_queue_depth", vanished); ok {
		t.Error("depth{gone,available} still published after the queue drained")
	}
	if _, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "gone"}); ok {
		t.Error("oldest_job_age{gone} still published after the queue drained")
	}
	s, ok := seriesFor(t, reg, "drover_queue_depth", survivor)
	if !ok || s.value != 9 {
		t.Errorf("depth{keep,available} = %v present=%v, want 9", s.value, ok)
	}
}
