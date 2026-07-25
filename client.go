package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/pgdriver"
)

const (
	defaultQueue        = "default"
	defaultPollInterval = time.Second
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
// Workers.
type Config struct {
	Workers      *Workers
	Logger       *slog.Logger
	PollInterval time.Duration
}

// Client enqueues jobs and runs the worker loop.
type Client struct {
	drv          driver.Driver
	workers      *Workers
	logger       *slog.Logger
	pollInterval time.Duration
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
		drv:          drv,
		workers:      cfg.Workers,
		logger:       cfg.Logger,
		pollInterval: cfg.PollInterval,
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
