package drover

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// jobDurationBuckets are the upper bounds, in seconds, of the job
// duration histogram: five milliseconds to ten minutes.
//
// Prometheus's default buckets stop at ten seconds. That is the right
// range for an HTTP handler and the wrong one for a job queue, where
// every job slower than the last boundary lands in +Inf — the one bucket
// that reports nothing beyond "slower than ten seconds". Buckets cannot
// be widened later without invalidating the histograms already recorded
// against them, so the range is chosen for the work drover runs rather
// than inherited.
var jobDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
}

// metricSet owns every collector drover publishes and is the single
// place a metric name, label set, or bucket boundary is written down.
//
// The collectors are registered on the registry handed to newMetricSet,
// which the client owns: two clients in one process default to registries
// of their own, so neither can collide with the other's registration and
// a metric's provenance stays visible.
//
// Job metrics carry a queue label and nothing else. Queue is bounded by
// configuration; job kind is a user-supplied string with no bound, and
// labelling by it would turn one histogram into one per kind.
type metricSet struct {
	// SPEC_DEVIATION: the design gives newMetricSet the signature
	// (registry, concurrency); this one also takes a logger.
	// Reason: an unusable queue name must be reported by name and its
	// series skipped, and the metric set is the only place that failure
	// is observable — every accessor would otherwise have to return an
	// error for both of its callers to log identically.
	logger *slog.Logger

	completed   *prometheus.CounterVec
	failed      *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	executing   prometheus.Gauge
	concurrency prometheus.Gauge
	depth       *prometheus.GaugeVec
	oldestAge   *prometheus.GaugeVec
}

// newMetricSet creates every collector, registers it on reg, and
// publishes the configured worker count.
//
// Registration is fatal on conflict, as a structural mistake should be:
// the only way to reach it is to hand two clients the same registry,
// which is a decision made in code and never at runtime.
func newMetricSet(reg *prometheus.Registry, concurrency int, logger *slog.Logger) *metricSet {
	if logger == nil {
		logger = slog.Default()
	}
	m := &metricSet{
		logger: logger,
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "drover_jobs_completed_total",
			Help: "Job executions that returned no error, by queue.",
		}, []string{"queue"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "drover_jobs_failed_total",
			Help: "Job executions that returned an error, including recovered panics, by queue. Counts attempts, not jobs that died.",
		}, []string{"queue"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "drover_job_duration_seconds",
			Help:    "Wall-clock time one job execution took, successful or not, by queue.",
			Buckets: jobDurationBuckets,
		}, []string{"queue"}),
		executing: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "drover_jobs_executing",
			Help: "Job executions currently inside the middleware chain.",
		}),
		concurrency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "drover_pool_concurrency",
			Help: "Worker count this client is configured to run, so saturation is computable from one scrape.",
		}),
		depth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drover_queue_depth",
			Help: "Jobs held in one state on one queue.",
		}, []string{"queue", "state"}),
		oldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drover_oldest_job_age_seconds",
			Help: "How long the oldest job claimable now has been waiting, by queue; zero when none is.",
		}, []string{"queue"}),
	}
	reg.MustRegister(
		m.completed, m.failed, m.duration,
		m.executing, m.concurrency, m.depth, m.oldestAge,
	)
	m.concurrency.Set(float64(concurrency))
	return m
}

// observeExecution records one finished execution: its duration, and
// exactly one of the two outcome counters, chosen by whether the chain
// returned an error.
//
// Both series are resolved before either is written, so a queue label
// prometheus refuses costs the whole observation rather than leaving a
// counter incremented with no duration beside it.
func (m *metricSet) observeExecution(queue string, d time.Duration, err error) {
	outcome := m.completed
	if err != nil {
		outcome = m.failed
	}
	counter, counterErr := outcome.GetMetricWithLabelValues(queue)
	histogram, histogramErr := m.duration.GetMetricWithLabelValues(queue)
	if labelErr := errors.Join(counterErr, histogramErr); labelErr != nil {
		m.skipSeries(queue, labelErr)
		return
	}
	counter.Inc()
	histogram.Observe(d.Seconds())
}

// setDepth publishes how many jobs are held in one state on one queue.
func (m *metricSet) setDepth(queue, state string, n float64) {
	gauge, err := m.depth.GetMetricWithLabelValues(queue, state)
	if err != nil {
		m.skipSeries(queue, err)
		return
	}
	gauge.Set(n)
}

// setOldestAge publishes how long a queue's oldest claimable job has
// been waiting.
func (m *metricSet) setOldestAge(queue string, seconds float64) {
	gauge, err := m.oldestAge.GetMetricWithLabelValues(queue)
	if err != nil {
		m.skipSeries(queue, err)
		return
	}
	gauge.Set(seconds)
}

// skipSeries reports a queue name prometheus will not accept as a label
// value, having dropped that one series.
//
// Every accessor above resolves its series through the error-returning
// GetMetricWith family for the sake of this path. The convenient
// WithLabelValues panics instead, and a panic raised here would unwind
// the pool worker that called it — silently shrinking the pool over an
// unusable name. One queue's metrics are worth losing; a worker is not.
func (m *metricSet) skipSeries(queue string, err error) {
	m.logger.Warn("drover: queue name cannot be used as a metric label; skipping its series",
		"queue", queue, "error", err)
}

// metricsMiddleware records each execution as it happens: that it is
// running while it runs, how long it took, and which way it went.
//
// It reads as a sibling of Logging and sits immediately inside it, which
// is what lets it observe the failures that never reach a registered
// worker — a panicking handler, a kind this binary does not know —
// because both arrive as ordinary errors from further down the chain.
//
// The in-flight gauge is decremented from a defer rather than after the
// call, so an execution that unwinds past this frame still leaves the
// gauge where it found it. Its counters, though, describe executions
// rather than jobs: a job that fails four times and then succeeds is
// four failures and one completion, not one of either.
//
// Unexported: the client installs it on every client it builds, so there
// is nothing for a caller to do with a constructor except install it
// twice.
func metricsMiddleware(m *metricSet) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *JobRow) error {
			m.executing.Inc()
			defer m.executing.Dec()

			start := time.Now()
			err := next(ctx, job)
			m.observeExecution(job.Queue, time.Since(start), err)
			return err
		}
	}
}
