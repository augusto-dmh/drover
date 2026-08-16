// Package memdriver is an in-memory driver.Driver used by the unit
// suite so it runs without Docker. It enforces the same state
// transitions as the Postgres driver but supports no caller-owned
// transactions (ADR-0002: transactional enqueue is a Postgres
// capability).
package memdriver

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// Driver implements driver.Driver over a mutex-guarded map.
type Driver struct {
	mu     sync.Mutex
	nextID int64
	jobs   map[int64]*driver.JobRow
}

// New returns an empty in-memory driver.
func New() *Driver {
	return &Driver{jobs: make(map[int64]*driver.JobRow)}
}

// Migrate is a no-op: the in-memory store has no schema.
func (d *Driver) Migrate(context.Context) error { return nil }

// Insert stores a new job — available, or scheduled when it is due
// later — and returns a copy of its row.
func (d *Driver) Insert(_ context.Context, params driver.InsertParams) (*driver.JobRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextID++
	row := jobFromParams(d.nextID, params, time.Now())
	d.jobs[row.ID] = row
	return copyRow(row), nil
}

// InsertTx always fails: there is no transaction to join in memory.
func (d *Driver) InsertTx(context.Context, any, driver.InsertParams) (*driver.JobRow, error) {
	return nil, driver.ErrTxUnsupported
}

// InsertMany persists every job in batch in one atomic write and
// returns a row per item, in input order. An empty or nil batch is
// success with no write.
func (d *Driver) InsertMany(_ context.Context, batch []driver.InsertParams) ([]*driver.JobRow, error) {
	if len(batch) == 0 {
		return []*driver.JobRow{}, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	pending := make([]*driver.JobRow, len(batch))
	for i, params := range batch {
		pending[i] = jobFromParams(d.nextID+int64(i)+1, params, now)
	}
	for _, row := range pending {
		d.jobs[row.ID] = row
	}
	d.nextID += int64(len(batch))

	out := make([]*driver.JobRow, len(pending))
	for i, row := range pending {
		out[i] = copyRow(row)
	}
	return out, nil
}

// InsertManyTx always fails: there is no transaction to join in memory.
func (d *Driver) InsertManyTx(context.Context, any, []driver.InsertParams) ([]*driver.JobRow, error) {
	return nil, driver.ErrTxUnsupported
}

// jobFromParams builds a stored row the way Insert does, against this
// store's clock: the zero time means now, and a job due later waits in
// scheduled rather than claiming to be available for a run it cannot
// have yet.
func jobFromParams(id int64, params driver.InsertParams, now time.Time) *driver.JobRow {
	scheduledAt := params.ScheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = now
	}
	state := "available"
	if scheduledAt.After(now) {
		state = "scheduled"
	}
	return &driver.JobRow{
		ID:          id,
		Kind:        params.Kind,
		Queue:       params.Queue,
		Args:        params.Args,
		State:       state,
		MaxAttempts: 25,
		Errors:      json.RawMessage("[]"),
		ScheduledAt: scheduledAt,
		CreatedAt:   now,
	}
}

// waiting reports whether a job in this state is queued for execution:
// never run, awaiting a retry backoff, or snoozed.
func waiting(state string) bool {
	return state == "available" || state == "retryable" || state == "scheduled"
}

// FetchAvailable claims up to limit due jobs, earliest due time first:
// each becomes running with an incremented attempt and a lease running
// to leaseUntil.
func (d *Driver) FetchAvailable(_ context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	due := d.selectRows(byScheduledAt, func(row *driver.JobRow) bool {
		return waiting(row.State) && row.Queue == queue && !row.ScheduledAt.After(now)
	})

	var claimed []*driver.JobRow
	for _, row := range due {
		if len(claimed) == limit {
			break
		}
		row.State = "running"
		row.Attempt++
		lease := now.Add(leaseFor)
		row.LeasedUntil = &lease
		claimed = append(claimed, copyRow(row))
	}
	return claimed, nil
}

// FetchExpired re-claims up to limit running jobs whose lease has
// passed, oldest lease first, giving each a fresh lease that runs to
// leaseUntil. attempt is deliberately left alone: the attempt that
// stranded the row was spent.
func (d *Driver) FetchExpired(_ context.Context, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	expired := d.selectRows(byLeasedUntil, func(row *driver.JobRow) bool {
		return row.State == "running" && row.LeasedUntil != nil && !row.LeasedUntil.After(now)
	})

	var reclaimed []*driver.JobRow
	for _, row := range expired {
		if len(reclaimed) == limit {
			break
		}
		lease := now.Add(leaseFor)
		row.LeasedUntil = &lease
		reclaimed = append(reclaimed, copyRow(row))
	}
	return reclaimed, nil
}

// ExtendLeases pushes each held lease out by another leaseFor. A lease
// whose job is gone, no longer running, or already on a later attempt is
// skipped rather than reported: the job finished or was taken over, and
// a heartbeat must not resurrect it or steal it back.
func (d *Driver) ExtendLeases(_ context.Context, leases []driver.Lease, leaseFor time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	until := time.Now().Add(leaseFor)
	for _, held := range leases {
		row, ok := d.jobs[held.ID]
		if !ok || row.State != "running" || row.Attempt != held.Attempt {
			continue
		}
		lease := until
		row.LeasedUntil = &lease
	}
	return nil
}

// counted reports whether jobs in this state are worth reporting a depth
// for. completed and cancelled are absent deliberately: nothing removes
// those rows, so counting them would tie the cost of a reading to the
// whole history of the queue rather than to its backlog — in the SQL
// driver, where that cost is a scan rather than a map walk, that is the
// difference between a cheap gauge and a source of load.
func counted(state string) bool {
	switch state {
	case "available", "scheduled", "retryable", "running", "dead":
		return true
	default:
		return false
	}
}

// Stats reports the depth of every queue by state, and how long the
// oldest job each queue could hand a worker right now has been waiting,
// against this store's clock.
func (d *Driver) Stats(context.Context) (*driver.Stats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	type depth struct{ queue, state string }
	counts := make(map[depth]int64)
	oldest := make(map[string]time.Time)

	for _, row := range d.jobs {
		if counted(row.State) {
			counts[depth{row.Queue, row.State}]++
		}
		// The claim's own predicate, not a restatement of it: a job the
		// fetch would pass over is not waiting on a worker, it is early,
		// and ageing it would report a deliberate delay as lateness.
		if !waiting(row.State) || row.ScheduledAt.After(now) {
			continue
		}
		if since, seen := oldest[row.Queue]; !seen || row.ScheduledAt.Before(since) {
			oldest[row.Queue] = row.ScheduledAt
		}
	}

	stats := &driver.Stats{}
	for key, count := range counts {
		stats.Depths = append(stats.Depths, driver.QueueDepth{
			Queue: key.queue, State: key.state, Count: count,
		})
	}
	for queue, since := range oldest {
		stats.Oldest = append(stats.Oldest, driver.QueueAge{
			Queue: queue, AgeSeconds: now.Sub(since).Seconds(),
		})
	}
	// Ordered because the contract is, and for the reason it is: a map
	// walk yields queues in a different order every call, and the two
	// drivers can only be compared reading against reading if neither
	// leaves the order to chance.
	slices.SortFunc(stats.Depths, func(a, b driver.QueueDepth) int {
		if c := cmp.Compare(a.Queue, b.Queue); c != 0 {
			return c
		}
		return cmp.Compare(a.State, b.State)
	})
	slices.SortFunc(stats.Oldest, func(a, b driver.QueueAge) int {
		return cmp.Compare(a.Queue, b.Queue)
	})
	return stats, nil
}

// ListJobs returns jobs matching optional queue and state filters,
// newest id first, capped at p.Limit. A non-positive Limit is refused.
func (d *Driver) ListJobs(_ context.Context, p driver.ListJobsParams) ([]*driver.JobRow, error) {
	if p.Limit <= 0 {
		return nil, fmt.Errorf("list limit must be positive, got %d", p.Limit)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	var matched []*driver.JobRow
	for _, row := range d.jobs {
		if p.Queue != "" && row.Queue != p.Queue {
			continue
		}
		if p.State != "" && row.State != p.State {
			continue
		}
		matched = append(matched, row)
	}
	slices.SortFunc(matched, func(a, b *driver.JobRow) int {
		return cmp.Compare(b.ID, a.ID)
	})
	if p.Limit < len(matched) {
		matched = matched[:p.Limit]
	}
	out := make([]*driver.JobRow, len(matched))
	for i, row := range matched {
		out[i] = copyRow(row)
	}
	return out, nil
}

// GetJob returns a copy of the stored row for id, or ErrNotFound.
func (d *Driver) GetJob(_ context.Context, id int64) (*driver.JobRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	row, ok := d.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %d: %w", id, driver.ErrNotFound)
	}
	return copyRow(row), nil
}

// cancellable reports whether an operator may cancel a job in this state
// without fighting a live lease holder.
func cancellable(state string) bool {
	switch state {
	case "available", "scheduled", "retryable", "dead":
		return true
	default:
		return false
	}
}

// OperatorCancel moves a waiting or dead job to cancelled. Running and
// terminal completed/cancelled rows are refused so the write never races
// a worker's attempt fence. When a refusal re-read shows an allowed
// state, the cancel is retried once (same TOCTOU hardening as pgdriver).
func (d *Driver) OperatorCancel(_ context.Context, id int64) (*driver.JobRow, error) {
	return d.operatorCancel(id, true)
}

func (d *Driver) operatorCancel(id int64, allowRetry bool) (*driver.JobRow, error) {
	d.mu.Lock()
	row, ok := d.jobs[id]
	if !ok {
		d.mu.Unlock()
		return nil, fmt.Errorf("job %d: %w", id, driver.ErrNotFound)
	}
	if !cancellable(row.State) {
		state := row.State
		d.mu.Unlock()
		return d.afterOperatorMiss(id, state, "cancellable", allowRetry, cancellable, func() (*driver.JobRow, error) {
			return d.operatorCancel(id, false)
		})
	}
	now := time.Now()
	row.State = "cancelled"
	row.FinalizedAt = &now
	row.LeasedUntil = nil
	out := copyRow(row)
	d.mu.Unlock()
	return out, nil
}

// RedriveDead returns a dead job to available with attempt reset and
// lease cleared, keeping the errors array for triage provenance. When a
// refusal re-read shows dead, the redrive is retried once.
func (d *Driver) RedriveDead(_ context.Context, id int64) (*driver.JobRow, error) {
	return d.redriveDead(id, true)
}

func (d *Driver) redriveDead(id int64, allowRetry bool) (*driver.JobRow, error) {
	d.mu.Lock()
	row, ok := d.jobs[id]
	if !ok {
		d.mu.Unlock()
		return nil, fmt.Errorf("job %d: %w", id, driver.ErrNotFound)
	}
	if row.State != "dead" {
		state := row.State
		d.mu.Unlock()
		return d.afterOperatorMiss(id, state, "dead", allowRetry, func(s string) bool {
			return s == "dead"
		}, func() (*driver.JobRow, error) {
			return d.redriveDead(id, false)
		})
	}
	now := time.Now()
	row.State = "available"
	row.Attempt = 0
	row.LeasedUntil = nil
	row.FinalizedAt = nil
	row.ScheduledAt = now
	out := copyRow(row)
	d.mu.Unlock()
	return out, nil
}

// afterOperatorMiss re-reads after a refused operator mutate. When the
// row is now eligible and allowRetry is set, retry runs once.
func (d *Driver) afterOperatorMiss(
	id int64,
	observed string,
	want string,
	allowRetry bool,
	eligible func(string) bool,
	retry func() (*driver.JobRow, error),
) (*driver.JobRow, error) {
	if !allowRetry {
		return nil, fmt.Errorf("job %d is %s, want %s: %w", id, observed, want, driver.ErrInvalidTransition)
	}
	d.mu.Lock()
	row, ok := d.jobs[id]
	if !ok {
		d.mu.Unlock()
		return nil, fmt.Errorf("job %d: %w", id, driver.ErrNotFound)
	}
	state := row.State
	can := eligible(state)
	d.mu.Unlock()
	if can {
		return retry()
	}
	return nil, fmt.Errorf("job %d is %s, want %s: %w", id, state, want, driver.ErrInvalidTransition)
}

// selectRows returns the stored rows matching keep, ordered by order and
// then by id. The caller must hold d.mu; the rows are live pointers, not
// copies.
//
// The order matters for parity, not for tidiness: it decides which rows
// a limited fetch picks, so it has to be the order the Postgres driver's
// query asks for, or the two drivers claim different jobs from the same
// queue and the in-memory suite stops proving anything about production.
func (d *Driver) selectRows(order func(*driver.JobRow) time.Time, keep func(*driver.JobRow) bool) []*driver.JobRow {
	var rows []*driver.JobRow
	for _, row := range d.jobs {
		if keep(row) {
			rows = append(rows, row)
		}
	}
	slices.SortFunc(rows, func(a, b *driver.JobRow) int {
		if c := order(a).Compare(order(b)); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return rows
}

// byScheduledAt orders waiting jobs the way the claim does: due time
// first, ties broken by id.
func byScheduledAt(row *driver.JobRow) time.Time { return row.ScheduledAt }

// byLeasedUntil orders running jobs oldest-lease-first, the way the
// rescue sweep does. A row reaches the sweep only with a lease set.
func byLeasedUntil(row *driver.JobRow) time.Time {
	if row.LeasedUntil == nil {
		return time.Time{}
	}
	return *row.LeasedUntil
}

// MarkCompleted finalizes a running job as completed.
func (d *Driver) MarkCompleted(_ context.Context, lease driver.Lease) error {
	return d.finalize(lease, "completed", nil)
}

// MarkDead finalizes a running job as dead, appending errDetail to its
// errors array.
func (d *Driver) MarkDead(_ context.Context, lease driver.Lease, errDetail []byte) error {
	return d.finalize(lease, "dead", errDetail)
}

// MarkCancelled finalizes a running job as cancelled, appending
// errDetail as the reason it will never be retried.
func (d *Driver) MarkCancelled(_ context.Context, lease driver.Lease, errDetail []byte) error {
	return d.finalize(lease, "cancelled", errDetail)
}

// MarkRetryable returns a running job to the queue at retryAt, appending
// errDetail. The job is not finalized: it is waiting, not finished.
func (d *Driver) MarkRetryable(_ context.Context, lease driver.Lease, retryAt time.Time, errDetail []byte) error {
	return d.transition(lease, func(row *driver.JobRow) error {
		if err := appendError(row, errDetail); err != nil {
			return err
		}
		row.State = "retryable"
		row.ScheduledAt = retryAt
		return nil
	})
}

// MarkSnoozed defers a running job to runAt, giving back the attempt the
// claim consumed and recording no error.
func (d *Driver) MarkSnoozed(_ context.Context, lease driver.Lease, runAt time.Time) error {
	return d.transition(lease, func(row *driver.JobRow) error {
		row.State = "scheduled"
		row.ScheduledAt = runAt
		row.Attempt = max(row.Attempt-1, 0)
		return nil
	})
}

func (d *Driver) finalize(lease driver.Lease, state string, errDetail []byte) error {
	return d.transition(lease, func(row *driver.JobRow) error {
		if err := appendError(row, errDetail); err != nil {
			return err
		}
		row.State = state
		now := time.Now()
		row.FinalizedAt = &now
		return nil
	})
}

// transition applies apply to the job the lease holds, clearing that
// lease. It is the single guard behind every Mark method: an unknown id
// is ErrNotFound, an attempt that has moved on is ErrLeaseLost, and any
// state but running is ErrInvalidTransition.
//
// Checking the attempt is what makes holding the lease mean something.
// Without it the guard would only prove that some worker owns the row,
// letting a worker whose lease lapsed record the outcome of its own
// stale attempt over the one now running.
func (d *Driver) transition(lease driver.Lease, apply func(*driver.JobRow) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	row, ok := d.jobs[lease.ID]
	if !ok {
		return fmt.Errorf("job %d: %w", lease.ID, driver.ErrNotFound)
	}
	// State first: a job that is not running was never this caller's to
	// finish, whoever holds it. Only once it *is* running does the
	// attempt distinguish "mine" from "someone else's".
	if row.State != "running" {
		return fmt.Errorf("job %d is %s, want running: %w", lease.ID, row.State, driver.ErrInvalidTransition)
	}
	if row.Attempt != lease.Attempt {
		return fmt.Errorf("job %d is on attempt %d, held attempt %d: %w",
			lease.ID, row.Attempt, lease.Attempt, driver.ErrLeaseLost)
	}
	if err := apply(row); err != nil {
		return err
	}
	row.LeasedUntil = nil
	return nil
}

func appendError(row *driver.JobRow, errDetail []byte) error {
	if errDetail == nil {
		return nil
	}
	var list []json.RawMessage
	if err := json.Unmarshal(row.Errors, &list); err != nil {
		return fmt.Errorf("decode errors array for job %d: %w", row.ID, err)
	}
	list = append(list, errDetail)
	merged, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("encode errors array for job %d: %w", row.ID, err)
	}
	row.Errors = merged
	return nil
}

// Row returns a copy of a stored job row, for tests that assert on
// persisted state.
func (d *Driver) Row(id int64) (*driver.JobRow, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	row, ok := d.jobs[id]
	if !ok {
		return nil, false
	}
	return copyRow(row), true
}

func copyRow(row *driver.JobRow) *driver.JobRow {
	out := *row
	out.Args = slices.Clone(row.Args)
	out.Errors = slices.Clone(row.Errors)
	if row.LeasedUntil != nil {
		t := *row.LeasedUntil
		out.LeasedUntil = &t
	}
	if row.FinalizedAt != nil {
		t := *row.FinalizedAt
		out.FinalizedAt = &t
	}
	return &out
}
