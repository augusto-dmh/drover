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
// LeaseDuration, a third of the lease for HeartbeatInterval, and the
// lease duration itself for RescueInterval.
type Config struct {
	Workers      *Workers
	Logger       *slog.Logger
	PollInterval time.Duration
	RetryPolicy  RetryPolicy

	// Concurrency is how many jobs this client executes at once. It
	// defaults to ten; a value of zero or less is treated as unset.
	//
	// It may safely exceed the connection count of the pool the client
	// was built with. A running job holds no connection: drover takes one
	// to claim the job and one to record its outcome, and the handler's
	// own work happens in between, holding nothing. What a high
	// concurrency does cost is handler-side resources — sockets, memory,
	// load on whatever the handler talks to — so size it against those
	// rather than against the database.
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
	inflight          *inflightSet

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
	return c
}

// Insert enqueues a job in its own transaction. The job is available
// to workers as soon as Insert returns.
func (c *Client) Insert(ctx context.Context, args JobArgs) (*JobRow, error) {
	params, err := insertParamsFor(args)
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
func (c *Client) InsertTx(ctx context.Context, tx pgx.Tx, args JobArgs) (*JobRow, error) {
	params, err := insertParamsFor(args)
	if err != nil {
		return nil, err
	}
	row, err := c.drv.InsertTx(ctx, tx, params)
	if err != nil {
		return nil, fmt.Errorf("drover: insert job kind %q in tx: %w", params.Kind, err)
	}
	return rowFromDriver(row), nil
}

func insertParamsFor(args JobArgs) (driver.InsertParams, error) {
	kind := args.Kind()
	if kind == "" {
		return driver.InsertParams{}, ErrInvalidKind
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return driver.InsertParams{}, fmt.Errorf("drover: marshal args for kind %q: %w", kind, err)
	}
	return driver.InsertParams{Kind: kind, Queue: defaultQueue, Args: encoded}, nil
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
