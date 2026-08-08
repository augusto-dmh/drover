package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/pgdriver"
)

const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

// Inspector is the operator API over a drover queue store. It reads and
// mutates jobs without running workers: construct one with NewInspector
// and call methods directly — there is no Start/Stop lifecycle.
type Inspector struct {
	drv driver.Driver
}

// NewInspector returns an Inspector backed by pool. It is ready for use
// without calling Start.
func NewInspector(pool *pgxpool.Pool) *Inspector {
	return newInspector(pgdriver.New(pool))
}

// newInspector is the driver-injection seam the unit suite uses with
// memdriver.
func newInspector(drv driver.Driver) *Inspector {
	return &Inspector{drv: drv}
}

// QueueDepth is the number of jobs sitting in one state on one queue.
type QueueDepth struct {
	Queue string
	State string
	Count int64
}

// QueueAge is how long the oldest claimable job on one queue has been
// waiting, measured by the store's clock.
type QueueAge struct {
	Queue      string
	AgeSeconds float64
}

// QueueStats is one reading of what the queues hold: depths for the
// published states and oldest-claimable ages per queue.
type QueueStats struct {
	Depths []QueueDepth
	Oldest []QueueAge
}

// ListJobsOpts filters and bounds a ListJobs read. Empty Queue or State
// means no filter on that dimension. A Limit of zero or less defaults
// to 100. Limits above 1000 are refused.
type ListJobsOpts struct {
	Queue string
	State JobState
	Limit int
}

// Stats returns per-queue depth counts for the published states and
// oldest-claimable ages, matching Driver.Stats semantics.
func (in *Inspector) Stats(ctx context.Context) (*QueueStats, error) {
	stats, err := in.drv.Stats(ctx)
	if err != nil {
		return nil, fmt.Errorf("drover: stats: %w", err)
	}
	out := &QueueStats{
		Depths: make([]QueueDepth, len(stats.Depths)),
		Oldest: make([]QueueAge, len(stats.Oldest)),
	}
	for i, d := range stats.Depths {
		out.Depths[i] = QueueDepth{Queue: d.Queue, State: d.State, Count: d.Count}
	}
	for i, a := range stats.Oldest {
		out.Oldest[i] = QueueAge{Queue: a.Queue, AgeSeconds: a.AgeSeconds}
	}
	return out, nil
}

// ListJobs returns jobs matching the optional filters, newest id first,
// capped by Limit (default 100, maximum 1000).
func (in *Inspector) ListJobs(ctx context.Context, opts *ListJobsOpts) ([]*JobRow, error) {
	limit := defaultListLimit
	var queue string
	var state string
	if opts != nil {
		queue = opts.Queue
		state = string(opts.State)
		if opts.Limit > 0 {
			limit = opts.Limit
		}
	}
	if limit > maxListLimit {
		return nil, fmt.Errorf("drover: list limit %d exceeds maximum %d", limit, maxListLimit)
	}
	rows, err := in.drv.ListJobs(ctx, driver.ListJobsParams{
		Queue: queue,
		State: state,
		Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("drover: list jobs: %w", err)
	}
	out := make([]*JobRow, len(rows))
	for i, row := range rows {
		out[i] = rowFromDriver(row)
	}
	return out, nil
}

// GetJob returns the current row for id, or an error wrapping
// ErrNotFound when the id is unknown.
func (in *Inspector) GetJob(ctx context.Context, id int64) (*JobRow, error) {
	row, err := in.drv.GetJob(ctx, id)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return rowFromDriver(row), nil
}

// Enqueue inserts a job with the given kind and raw JSON args. An empty
// kind or invalid JSON is refused without inserting. A nil opts uses
// the default queue and schedules the job immediately.
func (in *Inspector) Enqueue(ctx context.Context, kind string, args json.RawMessage, opts *InsertOpts) (*JobRow, error) {
	if kind == "" {
		return nil, ErrInvalidKind
	}
	if !json.Valid(args) {
		return nil, fmt.Errorf("drover: args must be valid JSON")
	}
	params := driver.InsertParams{Kind: kind, Queue: defaultQueue, Args: []byte(args)}
	if opts != nil {
		if opts.Queue != "" {
			params.Queue = opts.Queue
		}
		params.ScheduledAt = opts.ScheduledAt
	}
	row, err := in.drv.Insert(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("drover: enqueue job kind %q: %w", kind, err)
	}
	return rowFromDriver(row), nil
}

// CancelJob moves a waiting or dead job to cancelled. Running,
// completed, and already-cancelled jobs are refused with
// ErrInvalidTransition; a missing id is ErrNotFound.
func (in *Inspector) CancelJob(ctx context.Context, id int64) (*JobRow, error) {
	row, err := in.drv.OperatorCancel(ctx, id)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return rowFromDriver(row), nil
}

// RetryJob redrives a dead job to available with attempt reset to 0,
// lease cleared, and prior errors retained. Any other state is
// ErrInvalidTransition; a missing id is ErrNotFound.
func (in *Inspector) RetryJob(ctx context.Context, id int64) (*JobRow, error) {
	row, err := in.drv.RedriveDead(ctx, id)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return rowFromDriver(row), nil
}

// mapDriverErr translates driver sentinels to root-package ones so
// callers can errors.Is without importing internal/driver, while
// keeping the driver's detail message.
func mapDriverErr(err error) error {
	switch {
	case errors.Is(err, driver.ErrNotFound):
		return fmt.Errorf("%w: %w", err, ErrNotFound)
	case errors.Is(err, driver.ErrInvalidTransition):
		return fmt.Errorf("%w: %w", err, ErrInvalidTransition)
	default:
		return err
	}
}
