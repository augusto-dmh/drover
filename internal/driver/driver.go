// Package driver defines the narrow storage contract drover runs on.
// It exists for testability: the production implementation is
// internal/pgdriver; internal/memdriver backs the Docker-free unit
// suite. See AD-008.
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// DefaultLeaseDuration bounds how long a claimed job may run before a
// future rescuer (Cycle B) may consider its worker dead. Cycle A writes
// the lease on claim but does not yet enforce it (AD-005).
const DefaultLeaseDuration = time.Minute

var (
	// ErrInvalidTransition reports a state change the lifecycle does
	// not allow (e.g. completing a job that is not running).
	ErrInvalidTransition = errors.New("driver: invalid job state transition")

	// ErrNotFound reports an operation on a job id that does not exist.
	ErrNotFound = errors.New("driver: job not found")

	// ErrTxUnsupported is returned by drivers that cannot participate
	// in a caller-owned transaction.
	ErrTxUnsupported = errors.New("driver: transactional insert not supported")
)

// InsertParams carries everything needed to persist a new job.
type InsertParams struct {
	Kind  string
	Queue string
	Args  []byte
}

// JobRow is the stored representation of a job.
type JobRow struct {
	ID          int64
	Kind        string
	Queue       string
	Args        []byte
	State       string
	Attempt     int
	MaxAttempts int
	Errors      json.RawMessage
	ScheduledAt time.Time
	LeasedUntil *time.Time
	CreatedAt   time.Time
	FinalizedAt *time.Time
}

// AttemptError is one recorded execution failure, appended to a job's
// errors array when the job is marked dead.
type AttemptError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

// Driver is the storage contract. FetchAvailable claims jobs: it moves
// them to the running state, increments attempt, and writes a lease of
// DefaultLeaseDuration, returning only jobs this caller now owns.
type Driver interface {
	Migrate(ctx context.Context) error
	Insert(ctx context.Context, params InsertParams) (*JobRow, error)
	InsertTx(ctx context.Context, tx any, params InsertParams) (*JobRow, error)
	FetchAvailable(ctx context.Context, queue string, limit int) ([]*JobRow, error)
	MarkCompleted(ctx context.Context, id int64) error
	MarkDead(ctx context.Context, id int64, errDetail []byte) error
}
