// Package memdriver is an in-memory driver.Driver used by the unit
// suite so it runs without Docker. It enforces the same state
// transitions as the Postgres driver but supports no caller-owned
// transactions (ADR-0002: transactional enqueue is a Postgres
// capability).
package memdriver

import (
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

// Insert stores a new available job and returns a copy of its row.
func (d *Driver) Insert(_ context.Context, params driver.InsertParams) (*driver.JobRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextID++
	now := time.Now()
	row := &driver.JobRow{
		ID:          d.nextID,
		Kind:        params.Kind,
		Queue:       params.Queue,
		Args:        params.Args,
		State:       "available",
		MaxAttempts: 25,
		Errors:      json.RawMessage("[]"),
		ScheduledAt: now,
		CreatedAt:   now,
	}
	d.jobs[row.ID] = row
	return copyRow(row), nil
}

// InsertTx always fails: there is no transaction to join in memory.
func (d *Driver) InsertTx(context.Context, any, driver.InsertParams) (*driver.JobRow, error) {
	return nil, driver.ErrTxUnsupported
}

// FetchAvailable claims up to limit due jobs in id order: each becomes
// running with an incremented attempt and a fresh lease.
func (d *Driver) FetchAvailable(_ context.Context, queue string, limit int) ([]*driver.JobRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	var due []*driver.JobRow
	for _, row := range d.jobs {
		if row.State == "available" && row.Queue == queue && !row.ScheduledAt.After(now) {
			due = append(due, row)
		}
	}
	slices.SortFunc(due, func(a, b *driver.JobRow) int {
		return int(a.ID - b.ID)
	})

	var claimed []*driver.JobRow
	for _, row := range due {
		if len(claimed) == limit {
			break
		}
		row.State = "running"
		row.Attempt++
		lease := now.Add(driver.DefaultLeaseDuration)
		row.LeasedUntil = &lease
		claimed = append(claimed, copyRow(row))
	}
	return claimed, nil
}

// MarkCompleted finalizes a running job as completed.
func (d *Driver) MarkCompleted(_ context.Context, id int64) error {
	return d.finalize(id, "completed", nil)
}

// MarkDead finalizes a running job as dead, appending errDetail to its
// errors array.
func (d *Driver) MarkDead(_ context.Context, id int64, errDetail []byte) error {
	return d.finalize(id, "dead", errDetail)
}

func (d *Driver) finalize(id int64, state string, errDetail []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	row, ok := d.jobs[id]
	if !ok {
		return fmt.Errorf("job %d: %w", id, driver.ErrNotFound)
	}
	if row.State != "running" {
		return fmt.Errorf("job %d is %s, want running: %w", id, row.State, driver.ErrInvalidTransition)
	}
	if errDetail != nil {
		var list []json.RawMessage
		if err := json.Unmarshal(row.Errors, &list); err != nil {
			return fmt.Errorf("decode errors array for job %d: %w", id, err)
		}
		list = append(list, errDetail)
		merged, err := json.Marshal(list)
		if err != nil {
			return fmt.Errorf("encode errors array for job %d: %w", id, err)
		}
		row.Errors = merged
	}
	row.State = state
	now := time.Now()
	row.FinalizedAt = &now
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
