package drover

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// defaultStatsInterval is how often a client refreshes its queue gauges
// when the caller does not say. Fifteen seconds keeps the primary alert
// metric fresh enough for pages without turning every scrape interval
// into a database round trip.
const defaultStatsInterval = 15 * time.Second

// publishedDepthStates are the job states the depth gauge exposes.
// completed and cancelled are deliberately absent: they accumulate
// forever, and counting them would make the reading cost grow with
// history instead of with backlog.
var publishedDepthStates = []string{
	"available", "scheduled", "retryable", "running", "dead",
}

type depthKey struct {
	queue string
	state string
}

// statsRefresher turns one periodic Driver.Stats call into gauge values
// and records how fresh those values are.
//
// Scrapes read the gauges; they never call Stats. That is what keeps
// database load a function of elapsed time rather than of scrape rate.
type statsRefresher struct {
	drv      driver.Driver
	metrics  *metricSet
	queues   []weightedQueue
	interval time.Duration
	logger   *slog.Logger

	mu          sync.Mutex
	lastSuccess time.Time

	// depthKeys and ageKeys are the label sets published on the last
	// successful refresh. The next refresh writes every new value first,
	// then deletes only the keys that vanished — never Reset — so a
	// scrape that lands mid-refresh cannot see a surviving series briefly
	// absent.
	depthKeys map[depthKey]struct{}
	ageKeys   map[string]struct{}

	// observe is invoked after Stats succeeds and after every gauge
	// mutation. Tests use it to prove a surviving series is never
	// momentarily absent during a refresh.
	observe func()
}

func newStatsRefresher(
	drv driver.Driver,
	m *metricSet,
	queues []weightedQueue,
	interval time.Duration,
	logger *slog.Logger,
) *statsRefresher {
	if logger == nil {
		logger = slog.Default()
	}
	r := &statsRefresher{
		drv:       drv,
		metrics:   m,
		queues:    queues,
		interval:  interval,
		logger:    logger,
		depthKeys: make(map[depthKey]struct{}),
		ageKeys:   make(map[string]struct{}),
	}
	// Seed configured-queue series before the first Stats call returns so
	// a scrape that arrives while that call is still in flight reads zero
	// rather than omitting the series or waiting on the database.
	r.seedConfiguredZeros()
	return r
}

// seedConfiguredZeros publishes depth and age series at zero for every
// configured queue. It does not advance lastSuccess: readiness still
// waits on a real Stats reading.
func (r *statsRefresher) seedConfiguredZeros() {
	for _, q := range r.queues {
		for _, state := range publishedDepthStates {
			r.metrics.setDepth(q.name, state, 0)
			r.depthKeys[depthKey{queue: q.name, state: state}] = struct{}{}
		}
		r.metrics.setOldestAge(q.name, 0)
		r.ageKeys[q.name] = struct{}{}
	}
}

// run refreshes once immediately, then on each tick, until ctx is
// cancelled. The immediate first run is what lets readiness see a
// success before the first interval elapses.
func (r *statsRefresher) run(ctx context.Context) {
	if err := r.refresh(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		r.logger.Warn("drover: refresh queue stats", "error", err)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.refresh(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				r.logger.Warn("drover: refresh queue stats", "error", err)
			}
		}
	}
}

// refresh pulls one Stats reading and publishes it. On error it returns
// without touching any gauge or the freshness timestamp, so the previous
// values stand.
func (r *statsRefresher) refresh(ctx context.Context) error {
	stats, err := r.drv.Stats(ctx)
	if err != nil {
		return err
	}

	depths := make(map[depthKey]float64)
	ages := make(map[string]float64)

	// Configured queues are seeded to zero for every published state and
	// for age so an idle configured queue reads zero rather than missing.
	for _, q := range r.queues {
		for _, state := range publishedDepthStates {
			depths[depthKey{queue: q.name, state: state}] = 0
		}
		ages[q.name] = 0
	}

	for _, d := range stats.Depths {
		depths[depthKey{queue: d.Queue, state: d.State}] = float64(d.Count)
		// A queue seen only in the database still needs an age series;
		// without a claimable job the honest age is zero.
		if _, ok := ages[d.Queue]; !ok {
			ages[d.Queue] = 0
		}
	}
	for _, a := range stats.Oldest {
		ages[a.Queue] = a.AgeSeconds
	}

	// Observe before any mutation so a Reset-first implementation is
	// caught: the series published last time must still be present.
	if r.observe != nil {
		r.observe()
	}

	for key, n := range depths {
		r.metrics.setDepth(key.queue, key.state, n)
		if r.observe != nil {
			r.observe()
		}
	}
	for queue, seconds := range ages {
		r.metrics.setOldestAge(queue, seconds)
		if r.observe != nil {
			r.observe()
		}
	}

	for key := range r.depthKeys {
		if _, ok := depths[key]; !ok {
			r.metrics.deleteDepth(key.queue, key.state)
			if r.observe != nil {
				r.observe()
			}
		}
	}
	for queue := range r.ageKeys {
		if _, ok := ages[queue]; !ok {
			r.metrics.deleteOldestAge(queue)
			if r.observe != nil {
				r.observe()
			}
		}
	}

	nextDepths := make(map[depthKey]struct{}, len(depths))
	for key := range depths {
		nextDepths[key] = struct{}{}
	}
	nextAges := make(map[string]struct{}, len(ages))
	for queue := range ages {
		nextAges[queue] = struct{}{}
	}
	r.depthKeys = nextDepths
	r.ageKeys = nextAges

	r.mu.Lock()
	r.lastSuccess = time.Now()
	r.mu.Unlock()
	return nil
}

// fresh reports whether the last successful refresh is still within
// bound of now. It is a pure function of recorded state so readiness can
// be tested without running the loop.
func (r *statsRefresher) fresh(now time.Time, bound time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSuccess.IsZero() {
		return false
	}
	return !now.After(r.lastSuccess.Add(bound))
}
