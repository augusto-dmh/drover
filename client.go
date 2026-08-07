package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/pgdriver"
)

const (
	defaultQueue        = "default"
	defaultPollInterval = time.Second

	// defaultConcurrency is how many jobs one client runs at once when
	// the caller does not say. It is deliberately not one: a queue that
	// executes serially by default makes throughput a property of the
	// slowest handler rather than a choice.
	defaultConcurrency = 10

	// heartbeatsPerLease is how many renewals fit in one lease by
	// default. Three leaves two beats of slack: a job survives a missed
	// renewal without the rescuer taking it away.
	heartbeatsPerLease = 3

	// minLeaseDuration is the shortest lease that still divides into a
	// usable heartbeat interval. Anything shorter is treated as unset.
	minLeaseDuration = time.Millisecond
)

// JobRow is the persisted representation of a job.
type JobRow struct {
	ID          int64
	Kind        string
	Queue       string
	Args        json.RawMessage
	State       JobState
	Attempt     int
	MaxAttempts int
	Errors      json.RawMessage
	ScheduledAt time.Time
	LeasedUntil *time.Time
	CreatedAt   time.Time
	FinalizedAt *time.Time
}

// Config configures a Client. Zero values get defaults: slog.Default()
// for Logger, one second for PollInterval, an empty registry for
// Workers, ExponentialRetryPolicy for RetryPolicy, one minute for
// LeaseDuration, a third of the lease for HeartbeatInterval, the lease
// duration itself for RescueInterval, fifteen seconds for StatsInterval,
// and a registry of the client's own for MetricsRegistry.
type Config struct {
	Workers      *Workers
	Logger       *slog.Logger
	PollInterval time.Duration
	RetryPolicy  RetryPolicy

	// Middleware wraps every job this client executes, whatever its
	// kind. The first element is outermost: it sees a job before the
	// others and its result after them.
	//
	// The chain is composed once, when the client is built, so appending
	// to the slice afterwards changes nothing about what the client runs.
	// A nil element is a programmer error and panics,
	// because the alternative is a middleware the caller believes is
	// running: a timeout that was silently dropped looks exactly like a
	// timeout that has not fired yet.
	Middleware []Middleware

	// Queues maps each queue this client works to its weight. An unset
	// or empty map means the single queue "default".
	//
	// Weight decides how often a queue is tried *first* in a fetch round,
	// not whether it is tried at all: every configured queue is visited
	// in every round, so no weighting can starve one. Giving "critical"
	// nine times the weight of "bulk" means critical jobs are usually
	// claimed before bulk ones — it does not mean bulk waits for critical
	// to empty.
	//
	// The queues share one pool of Concurrency workers rather than each
	// getting a slice of it, so a busy queue can use the whole pool while
	// the others are idle.
	//
	// A weight below one is corrected to one and reported, as is one
	// above the internal maximum — priority is a ratio, and an enormous
	// weight buys nothing a large one does not.
	//
	// An empty queue name panics: an empty Queue at enqueue time means
	// "default", so no caller could ever address it.
	//
	// Cost scales with the map. A round asks each queue in turn until the
	// idle workers are spoken for, so an idle client polls once per
	// configured queue per interval where a single-queue client polls
	// once. The queries are cheap — each is served by the fetch index and
	// one matching no rows does no write — but they are round trips, so
	// widen PollInterval as the map grows.
	Queues map[string]int

	// Concurrency is how many jobs this client executes at once. It
	// defaults to ten; a value of zero or less is treated as unset.
	//
	// It need not match the connection count of the pool the client was
	// built with. A running job holds no connection: drover takes one to
	// claim the job and one to record its outcome, and the handler's own
	// work happens in between, holding nothing.
	//
	// That is not the same as the pool size being irrelevant. Claims and
	// finalizations arrive in bursts — up to Concurrency workers can want
	// a connection at the same instant — and the heartbeat competes for
	// the same connections on a deadline of HeartbeatInterval. A renewal
	// that cannot get a connection in time lets a lease lapse, which is
	// how a slow database turns into a duplicated job. Leave the
	// connection pool some headroom above the fetch loop, the heartbeat
	// and the rescuer rather than sizing it to Concurrency exactly.
	//
	// The other cost of a high concurrency is handler-side: sockets,
	// memory, and load on whatever the handler talks to.
	Concurrency int

	// LeaseDuration is how long a claimed job may run before the rescuer
	// treats its worker as dead. It bounds how long work sits idle after
	// a crash, so a shorter lease recovers faster and a longer one
	// tolerates more heartbeat trouble.
	LeaseDuration time.Duration

	// HeartbeatInterval is how often a running job's lease is renewed.
	// It must be shorter than LeaseDuration or every job outliving one
	// lease would be rescued while still running; a value that is not is
	// replaced with a third of the lease.
	HeartbeatInterval time.Duration

	// RescueInterval is how often the client sweeps for jobs whose
	// workers died. Together with LeaseDuration it bounds how long
	// abandoned work sits idle. It defaults to LeaseDuration, so
	// shortening the lease to recover faster does not leave the sweep
	// running on the old, slower cadence.
	RescueInterval time.Duration

	// MetricsRegistry is where this client registers its metrics. Unset,
	// it gets a registry of its own.
	//
	// A private registry by default is what lets two clients exist in one
	// process: prometheus rejects the same collector twice, so a shared
	// registry would panic the second construction. For the same reason,
	// handing the *same* registry to two clients is a programmer error
	// and panics — give each its own, or give the second one nothing.
	//
	// Pass a registry to have drover's metrics gathered by an endpoint
	// you already serve; the client's own ops server is the alternative,
	// not a requirement.
	MetricsRegistry *prometheus.Registry

	// StatsInterval is how often this client refreshes its queue depth
	// and oldest-job-age gauges from the store. Unset, it defaults to
	// fifteen seconds. A non-positive value is corrected to the default
	// and reported: the gauges would otherwise never move, and an
	// operator's alerts would go blind without a log line to explain why.
	StatsInterval time.Duration

	// OpsAddr is the address the client binds for /metrics, /healthz, and
	// /readyz. Empty means no listener and no ops goroutine — the client
	// still records metrics on its registry; it just does not serve them.
	//
	// Bind failure fails Start and starts nothing: a worker that is
	// running but unreachable is the state this surface exists to remove.
	OpsAddr string
}

// Client enqueues jobs and runs the worker loop.
type Client struct {
	drv               driver.Driver
	workers           *Workers
	logger            *slog.Logger
	pollInterval      time.Duration
	retryPolicy       RetryPolicy
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	rescueInterval    time.Duration
	concurrency       int
	statsInterval     time.Duration
	opsAddr           string
	queues            []weightedQueue
	inflight          *inflightSet
	metrics           *metricSet
	registry          *prometheus.Registry

	// chain is the composed middleware stack with dispatch at its
	// centre. It is built once at construction and never rebuilt: every
	// pool worker calls it concurrently, so anything mutable in here
	// would be a data race on every job.
	chain Handler

	// mu guards runner, which is nil until Start and holds the running
	// pool thereafter. It is the whole of the client's lifecycle state:
	// everything else above is configuration and is never written after
	// construction.
	mu     sync.Mutex
	runner *runner
}

// NewClient returns a Client backed by the given PostgreSQL pool.
func NewClient(pool *pgxpool.Pool, cfg Config) (*Client, error) {
	if pool == nil {
		return nil, errors.New("drover: pool must not be nil")
	}
	return newClient(pgdriver.New(pool), cfg), nil
}

// newClient is the driver-injection seam the unit suite uses with
// memdriver.
func newClient(drv driver.Driver, cfg Config) *Client {
	c := &Client{
		drv:               drv,
		workers:           cfg.Workers,
		logger:            cfg.Logger,
		pollInterval:      cfg.PollInterval,
		retryPolicy:       cfg.RetryPolicy,
		leaseDuration:     cfg.LeaseDuration,
		heartbeatInterval: cfg.HeartbeatInterval,
		rescueInterval:    cfg.RescueInterval,
		concurrency:       cfg.Concurrency,
		inflight:          newInflightSet(),
	}
	if c.workers == nil {
		c.workers = NewWorkers()
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if c.pollInterval <= 0 {
		c.pollInterval = defaultPollInterval
	}
	if c.retryPolicy == nil {
		c.retryPolicy = ExponentialRetryPolicy{}
	}
	// Floored, not merely checked for positivity: the heartbeat interval
	// is derived by integer division, so a lease of a nanosecond or two
	// would yield an interval of zero and panic the ticker.
	if c.leaseDuration < minLeaseDuration {
		c.leaseDuration = driver.DefaultLeaseDuration
	}
	// A heartbeat at or beyond the lease it renews can only ever renew
	// too late, so every job outliving one lease would be rescued while
	// still running.
	if c.heartbeatInterval >= c.leaseDuration {
		// Distinct from an unset value: this one the caller chose, and
		// its production symptom — jobs duplicated while still running —
		// is far easier to diagnose from one line at startup than from
		// the behaviour.
		c.logger.Warn("drover: heartbeat interval must be shorter than the lease; using the default instead",
			"heartbeat_interval", c.heartbeatInterval, "lease_duration", c.leaseDuration)
		c.heartbeatInterval = 0
	}
	if c.heartbeatInterval <= 0 {
		c.heartbeatInterval = c.leaseDuration / heartbeatsPerLease
	}
	if c.rescueInterval <= 0 {
		c.rescueInterval = c.leaseDuration
	}
	if c.concurrency <= 0 {
		c.concurrency = defaultConcurrency
	}
	// Zero means unset and takes the default silently. A negative value
	// is an explicit choice the caller got wrong, so it is corrected and
	// reported the same way a heartbeat at or beyond the lease is.
	switch {
	case cfg.StatsInterval < 0:
		c.logger.Warn("drover: stats interval must be positive; using the default instead",
			"stats_interval", cfg.StatsInterval)
		c.statsInterval = defaultStatsInterval
	case cfg.StatsInterval == 0:
		c.statsInterval = defaultStatsInterval
	default:
		c.statsInterval = cfg.StatsInterval
	}
	// After the logger is settled, so a corrected weight is reported
	// through the logger the caller configured.
	c.queues = checkedQueues(cfg.Queues, c.logger)
	// After the concurrency is settled too: the pool gauge publishes the
	// number this client will actually run, not the one it was handed.
	registry := cfg.MetricsRegistry
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	c.registry = registry
	c.opsAddr = cfg.OpsAddr
	c.metrics = newMetricSet(registry, c.concurrency, c.logger)
	// Logging goes outermost, ahead of whatever the caller configured, so
	// that per-job logging survives a caller adding middleware of their
	// own — and so its duration covers their middleware too. Metrics go
	// immediately inside it and ahead of the caller's middleware for the
	// same reason: an operator's dashboards should not go blank because
	// someone configured a middleware. Recording them there also means
	// the counters agree with the chain's verdict, which is what actually
	// decides the attempt.
	c.chain = wrap(c.dispatch, append(
		[]Middleware{Logging(c.logger), metricsMiddleware(c.metrics)},
		checkedMiddleware(cfg.Middleware)...,
	))
	return c
}

// checkedMiddleware rejects a chain that cannot be composed, and returns
// it unchanged.
//
// It deliberately does not copy. What keeps the client independent of a
// caller who goes on appending to their slice is that the chain is
// composed here and now: wrap closes over each middleware, not over the
// slice, so once construction returns there is nothing left for a later
// append to reach. A defensive copy would only look like the reason.
//
// The returned slice is therefore the caller's, and must stay
// composed-from and never stored. Anything that retained it would
// reintroduce the aliasing this note explains away.
func checkedMiddleware(mws []Middleware) []Middleware {
	for i, mw := range mws {
		if mw == nil {
			panic(fmt.Sprintf("drover: Config.Middleware[%d] is nil", i))
		}
	}
	return mws
}

// InsertOpts are the per-job choices made at enqueue time. A nil
// *InsertOpts, or a zero value, means the defaults: the "default" queue,
// runnable immediately.
type InsertOpts struct {
	// Queue is which named queue the job waits in. Empty means
	// "default".
	//
	// A queue no running client is configured to work is not an error:
	// the process that enqueues a job is very often not the one that
	// runs it, so the job simply waits until something works that queue.
	Queue string

	// ScheduledAt is the earliest time the job may run. The zero value
	// means as soon as possible.
	//
	// It is a floor, not a promise: the job becomes claimable then, and
	// runs once a worker is free to take it. A time in the past is
	// treated as now rather than rejected, so a caller computing a delay
	// from a stale clock still enqueues a runnable job.
	ScheduledAt time.Time
}

// Insert enqueues a job in its own transaction. The job is available to
// workers as soon as Insert returns, unless opts delays it.
func (c *Client) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*JobRow, error) {
	params, err := insertParamsFor(args, opts)
	if err != nil {
		return nil, err
	}
	row, err := c.drv.Insert(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("drover: insert job kind %q: %w", params.Kind, err)
	}
	return rowFromDriver(row), nil
}

// InsertTx enqueues a job inside the caller's transaction: the job
// exists if and only if tx commits, so a domain write and its job are
// atomic.
func (c *Client) InsertTx(ctx context.Context, tx pgx.Tx, args JobArgs, opts *InsertOpts) (*JobRow, error) {
	params, err := insertParamsFor(args, opts)
	if err != nil {
		return nil, err
	}
	row, err := c.drv.InsertTx(ctx, tx, params)
	if err != nil {
		return nil, fmt.Errorf("drover: insert job kind %q in tx: %w", params.Kind, err)
	}
	return rowFromDriver(row), nil
}

func insertParamsFor(args JobArgs, opts *InsertOpts) (driver.InsertParams, error) {
	kind := args.Kind()
	if kind == "" {
		return driver.InsertParams{}, ErrInvalidKind
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return driver.InsertParams{}, fmt.Errorf("drover: marshal args for kind %q: %w", kind, err)
	}

	params := driver.InsertParams{Kind: kind, Queue: defaultQueue, Args: encoded}
	if opts != nil {
		if opts.Queue != "" {
			params.Queue = opts.Queue
		}
		// Passed through as the zero time when unset, which the driver
		// reads as "now" and resolves against the store's own clock.
		params.ScheduledAt = opts.ScheduledAt
	}
	return params, nil
}

func rowFromDriver(row *driver.JobRow) *JobRow {
	return &JobRow{
		ID:          row.ID,
		Kind:        row.Kind,
		Queue:       row.Queue,
		Args:        row.Args,
		State:       JobState(row.State),
		Attempt:     row.Attempt,
		MaxAttempts: row.MaxAttempts,
		Errors:      row.Errors,
		ScheduledAt: row.ScheduledAt,
		LeasedUntil: row.LeasedUntil,
		CreatedAt:   row.CreatedAt,
		FinalizedAt: row.FinalizedAt,
	}
}
