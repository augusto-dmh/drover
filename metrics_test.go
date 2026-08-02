package drover

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/augusto-dmh/drover/internal/memdriver"
)

// sample is one published series, flattened out of what Gather returns so
// the assertions below can be written in ordinary values.
type sample struct {
	labels map[string]string
	// value is a counter's or gauge's value, or how many observations a
	// histogram's series holds.
	value float64
	// bounds are a histogram series' bucket upper bounds, nil otherwise.
	bounds []float64
}

// published gathers the registry and returns the type and every series of
// the family named name, exactly as a scrape would see them. ok reports
// whether the family was published at all.
func published(t *testing.T, reg *prometheus.Registry, name string) (kind string, samples []sample, ok bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			s := sample{labels: map[string]string{}}
			for _, pair := range metric.GetLabel() {
				s.labels[pair.GetName()] = pair.GetValue()
			}
			switch {
			case metric.GetCounter() != nil:
				s.value = metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				s.value = metric.GetGauge().GetValue()
			case metric.GetHistogram() != nil:
				s.value = float64(metric.GetHistogram().GetSampleCount())
				for _, bucket := range metric.GetHistogram().GetBucket() {
					s.bounds = append(s.bounds, bucket.GetUpperBound())
				}
			}
			samples = append(samples, s)
		}
		return family.GetType().String(), samples, true
	}
	return "", nil, false
}

// seriesFor returns the series of family name carrying exactly the given
// labels.
func seriesFor(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (sample, bool) {
	t.Helper()
	_, samples, ok := published(t, reg, name)
	if !ok {
		return sample{}, false
	}
	for _, s := range samples {
		if maps.Equal(s.labels, labels) {
			return s, true
		}
	}
	return sample{}, false
}

// newTestMetricSet builds a metric set on a private registry — never the
// default one, which would make the suite order-dependent — and returns
// the registry plus whatever the set logged.
func newTestMetricSet(t *testing.T, concurrency int) (*metricSet, *prometheus.Registry, *syncWriter) {
	t.Helper()
	reg := prometheus.NewRegistry()
	logs := &syncWriter{}
	return newMetricSet(reg, concurrency, newTestLogger(logs)), reg, logs
}

func TestMetricSetPublishesTheDocumentedFamilies(t *testing.T) {
	t.Parallel()

	m, reg, _ := newTestMetricSet(t, 4)
	m.observeExecution("default", 250*time.Millisecond, nil)
	m.observeExecution("default", 10*time.Millisecond, errors.New("boom"))
	m.setDepth("default", "available", 3)
	m.setOldestAge("default", 42)

	// The names, types, and labels are the contract an operator's alerts
	// and dashboards are written against, so they are spelled out here
	// rather than read back from the collectors that define them.
	tests := []struct {
		name   string
		kind   string
		labels []string
	}{
		{"drover_jobs_completed_total", "COUNTER", []string{"queue"}},
		{"drover_jobs_failed_total", "COUNTER", []string{"queue"}},
		{"drover_job_duration_seconds", "HISTOGRAM", []string{"queue"}},
		{"drover_jobs_executing", "GAUGE", nil},
		{"drover_pool_concurrency", "GAUGE", nil},
		{"drover_queue_depth", "GAUGE", []string{"queue", "state"}},
		{"drover_oldest_job_age_seconds", "GAUGE", []string{"queue"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kind, samples, ok := published(t, reg, tt.name)
			if !ok {
				t.Fatalf("%s was not published", tt.name)
			}
			if kind != tt.kind {
				t.Errorf("%s is a %s, want %s", tt.name, kind, tt.kind)
			}
			if len(samples) == 0 {
				t.Fatalf("%s published no series", tt.name)
			}
			got := slices.Sorted(maps.Keys(samples[0].labels))
			want := slices.Clone(tt.labels)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("%s is labelled %v, want %v", tt.name, got, want)
			}
		})
	}
}

func TestGaugeAccessorsPublishTheValueTheyAreGiven(t *testing.T) {
	t.Parallel()

	m, reg, _ := newTestMetricSet(t, 4)
	m.setDepth("critical", "retryable", 7)
	m.setOldestAge("critical", 12.5)

	depth, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{"queue": "critical", "state": "retryable"})
	if !ok {
		t.Fatal("no drover_queue_depth series for queue critical in state retryable")
	}
	if depth.value != 7 {
		t.Errorf("drover_queue_depth = %v, want 7", depth.value)
	}

	age, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "critical"})
	if !ok {
		t.Fatal("no drover_oldest_job_age_seconds series for queue critical")
	}
	if age.value != 12.5 {
		t.Errorf("drover_oldest_job_age_seconds = %v, want 12.5", age.value)
	}
}

// Saturation has to be computable from one scrape: an operator reading
// drover_jobs_executing needs the denominator beside it, not out of band.
func TestPoolConcurrencyGaugeReportsTheConfiguredWorkerCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		concurrency int
		want        float64
	}{
		{"a single worker", 1, 1},
		{"the default pool", 10, 10},
		{"a large pool", 64, 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, reg, _ := newTestMetricSet(t, tt.concurrency)

			got, ok := seriesFor(t, reg, "drover_pool_concurrency", map[string]string{})
			if !ok {
				t.Fatal("drover_pool_concurrency was not published")
			}
			if got.value != tt.want {
				t.Errorf("drover_pool_concurrency = %v, want %v", got.value, tt.want)
			}
		})
	}
}

// The default buckets stop at ten seconds, so a queue whose jobs take a
// minute would report every one of them in +Inf — the bucket that says
// nothing. This is the sensor for inheriting them by accident.
func TestJobDurationHistogramUsesTheDocumentedBuckets(t *testing.T) {
	t.Parallel()

	m, reg, _ := newTestMetricSet(t, 4)
	m.observeExecution("default", time.Second, nil)

	got, ok := seriesFor(t, reg, "drover_job_duration_seconds", map[string]string{"queue": "default"})
	if !ok {
		t.Fatal("no drover_job_duration_seconds series for queue default")
	}
	want := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
	if !slices.Equal(got.bounds, want) {
		t.Errorf("buckets = %v, want %v", got.bounds, want)
	}
}

// A queue name prometheus refuses as a label value must cost that one
// series and nothing else. The convenient WithLabelValues panics on it,
// and a panic raised on the execution path unwinds a pool worker's
// goroutine — losing a worker permanently over a name.
func TestAnUnusableQueueNameSkipsOneSeriesWithoutPanicking(t *testing.T) {
	t.Parallel()

	// Invalid UTF-8, which is what prometheus rejects. The readable
	// prefix is there so the warning can be checked for naming the queue.
	const unusable = "payments-\xff"

	tests := []struct {
		name   string
		family string
		labels map[string]string
		record func(m *metricSet, queue string)
	}{
		{
			name:   "a completed execution",
			family: "drover_jobs_completed_total",
			labels: map[string]string{"queue": "default"},
			record: func(m *metricSet, queue string) { m.observeExecution(queue, time.Millisecond, nil) },
		},
		{
			name:   "a failed execution",
			family: "drover_jobs_failed_total",
			labels: map[string]string{"queue": "default"},
			record: func(m *metricSet, queue string) {
				m.observeExecution(queue, time.Millisecond, errors.New("boom"))
			},
		},
		{
			name:   "a depth gauge",
			family: "drover_queue_depth",
			labels: map[string]string{"queue": "default", "state": "available"},
			record: func(m *metricSet, queue string) { m.setDepth(queue, "available", 3) },
		},
		{
			name:   "an oldest-age gauge",
			family: "drover_oldest_job_age_seconds",
			labels: map[string]string{"queue": "default"},
			record: func(m *metricSet, queue string) { m.setOldestAge(queue, 12) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, reg, logs := newTestMetricSet(t, 4)
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("recording %s under an unusable queue name panicked: %v", tt.name, r)
					}
				}()
				tt.record(m, unusable)
			}()

			// The queue that can be labelled is recorded afterwards, so the
			// assertion below distinguishes "skipped one series" from
			// "gave up on the family".
			tt.record(m, "default")

			_, samples, ok := published(t, reg, tt.family)
			if !ok {
				t.Fatalf("%s was not published at all — the unusable name cost the whole family", tt.family)
			}
			if len(samples) != 1 {
				t.Fatalf("%s published %d series, want 1: %v", tt.family, len(samples), samples)
			}
			if !maps.Equal(samples[0].labels, tt.labels) {
				t.Errorf("%s published %v, want the usable queue's series %v", tt.family, samples[0].labels, tt.labels)
			}

			out := logs.String()
			if got := countRecords(out, "cannot be used as a metric label"); got != 1 {
				t.Fatalf("warnings about the label = %d, want 1\nlogs:\n%s", got, out)
			}
			for _, want := range []string{"level=WARN", "payments-"} {
				if !strings.Contains(out, want) {
					t.Errorf("the warning is missing %q\nlogs:\n%s", want, out)
				}
			}
		})
	}
}

// An implementation that incremented both counters, or neither, would
// still satisfy a test that only watched the one it expected to move —
// so both are asserted on both paths, and the duration alongside them.
func TestMetricsMiddlewareMovesExactlyOneCounterPerExecution(t *testing.T) {
	t.Parallel()

	failure := errors.New("the handler's own error")
	tests := []struct {
		name          string
		ret           error
		wantCompleted float64
		wantFailed    float64
	}{
		{"a handler that returns nil", nil, 1, 0},
		{"a handler that returns an error", failure, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, reg, _ := newTestMetricSet(t, 4)
			h := metricsMiddleware(m)(func(ctx context.Context, job *JobRow) error {
				return tt.ret
			})

			// A queue of its own, so a label taken from anywhere but the job
			// fails the assertion rather than matching by coincidence.
			err := h(context.Background(), &JobRow{ID: 1, Kind: "greet", Queue: "critical"})

			// The chain's error is the attempt's verdict: a middleware that
			// recorded the failure and then swallowed it would leave every
			// counter right and every job wrongly completed.
			if !errors.Is(err, tt.ret) {
				t.Errorf("middleware returned %v, want the handler's own %v", err, tt.ret)
			}

			labels := map[string]string{"queue": "critical"}
			for _, want := range []struct {
				family string
				value  float64
			}{
				{"drover_jobs_completed_total", tt.wantCompleted},
				{"drover_jobs_failed_total", tt.wantFailed},
			} {
				got, ok := seriesFor(t, reg, want.family, labels)
				if !ok && want.value != 0 {
					t.Errorf("%s published no series for queue critical, want %v", want.family, want.value)
					continue
				}
				if got.value != want.value {
					t.Errorf("%s{queue=critical} = %v, want %v", want.family, got.value, want.value)
				}
			}

			duration, ok := seriesFor(t, reg, "drover_job_duration_seconds", labels)
			if !ok {
				t.Fatal("no drover_job_duration_seconds series for queue critical")
			}
			if duration.value != 1 {
				t.Errorf("drover_job_duration_seconds{queue=critical} holds %v observations, want 1", duration.value)
			}
		})
	}
}

// A panicking worker must be counted, and counted as a failure. The
// recovery around the registered worker is what turns its panic into an
// ordinary error for everything wrapped around it; without that, the
// panic would unwind past this middleware and the execution would be
// missing from the metrics entirely rather than merely miscounted. The
// real dispatch is used here because that recovery is the thing under
// test.
func TestMetricsMiddlewareCountsAPanickingWorkerAsFailed(t *testing.T) {
	t.Parallel()

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		panic("kaboom")
	}})
	c := newClient(memdriver.New(), Config{Workers: ws})
	m, reg, _ := newTestMetricSet(t, 4)

	// Args the registered kind can decode: without them the typed worker
	// is never reached, and the execution would be counted for a decoding
	// failure while claiming to be about a panic.
	job := &JobRow{ID: 1, Kind: "greet", Queue: "default", Args: []byte(`{"name":"ada"}`)}
	h := wrap(c.dispatch, []Middleware{metricsMiddleware(m)})
	err := h(context.Background(), job)
	if err == nil {
		t.Fatal("the chain returned nil for a panicking worker")
	}
	if !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("the chain returned %q, want the worker's panic", err)
	}

	labels := map[string]string{"queue": "default"}
	failed, ok := seriesFor(t, reg, "drover_jobs_failed_total", labels)
	if !ok {
		t.Fatal("a panicking worker was not counted at all")
	}
	if failed.value != 1 {
		t.Errorf("drover_jobs_failed_total{queue=default} = %v, want 1", failed.value)
	}
	if completed, ok := seriesFor(t, reg, "drover_jobs_completed_total", labels); ok && completed.value != 0 {
		t.Errorf("drover_jobs_completed_total{queue=default} = %v, want 0 — a panic was counted as a success", completed.value)
	}
	if duration, ok := seriesFor(t, reg, "drover_job_duration_seconds", labels); !ok || duration.value != 1 {
		t.Errorf("drover_job_duration_seconds{queue=default} holds %v observations (published: %t), want 1", duration.value, ok)
	}
}

// The gauge answers "how much of the pool is busy", so it has to reach
// the number of jobs actually inside the chain and come back down. A
// decrement that ran only on the success path would drift upwards until
// the gauge read saturated forever.
func TestExecutingGaugeRisesWithJobsInTheChainAndReturnsToZero(t *testing.T) {
	t.Parallel()

	const concurrency = 3
	m, reg, _ := newTestMetricSet(t, concurrency)

	entered := make(chan struct{}, concurrency)
	release := make(chan struct{})
	failure := errors.New("boom")
	h := metricsMiddleware(m)(func(ctx context.Context, job *JobRow) error {
		entered <- struct{}{}
		<-release
		// One of the three fails, so the decrement is exercised on both
		// paths within the same run.
		if job.ID == 1 {
			return failure
		}
		return nil
	})

	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h(context.Background(), &JobRow{ID: int64(i) + 1, Kind: "greet", Queue: "default"})
		}()
	}
	for range concurrency {
		<-entered
	}

	peak, ok := seriesFor(t, reg, "drover_jobs_executing", map[string]string{})
	if !ok {
		t.Fatal("drover_jobs_executing was not published")
	}
	// Equality, not "at least": the gauge counts executions inside the
	// chain, and a value above the pool's concurrency would be reporting
	// jobs that cannot exist.
	if peak.value != concurrency {
		t.Errorf("drover_jobs_executing = %v with %d jobs in the chain, want %d", peak.value, concurrency, concurrency)
	}

	close(release)
	wg.Wait()

	settled, ok := seriesFor(t, reg, "drover_jobs_executing", map[string]string{})
	if !ok {
		t.Fatal("drover_jobs_executing was not published after the jobs finished")
	}
	if settled.value != 0 {
		t.Errorf("drover_jobs_executing = %v once nothing is running, want 0", settled.value)
	}
}

// Prometheus panics when one collector is registered twice on a registry,
// so a library that reached for a shared one could not be instantiated
// twice in a process.
func TestTwoMetricSetsOnDistinctRegistriesDoNotCollide(t *testing.T) {
	t.Parallel()

	first, firstReg, _ := newTestMetricSet(t, 4)
	_, secondReg, _ := newTestMetricSet(t, 4)

	first.observeExecution("default", time.Millisecond, nil)

	got, ok := seriesFor(t, firstReg, "drover_jobs_completed_total", map[string]string{"queue": "default"})
	if !ok || got.value != 1 {
		t.Errorf("the first set's completed counter = %v (published: %t), want 1", got.value, ok)
	}
	if _, _, ok := published(t, secondReg, "drover_jobs_completed_total"); ok {
		t.Error("the second registry published the first set's counter — the two share state")
	}
}
