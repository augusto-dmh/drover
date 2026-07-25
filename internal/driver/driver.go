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

// DefaultLeaseDuration bounds how long a claimed job may run before the
// rescuer considers its worker dead and returns the job to the queue.
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
// errors array whenever an attempt ends without success.
type AttemptError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

// Driver is the storage contract. Every Mark method is guarded on the
// job being running: from any other state it reports
// ErrInvalidTransition, and for an unknown id ErrNotFound. All of them
// clear the lease, so no finalized or waiting row carries a stale one.
type Driver interface {
	Migrate(ctx context.Context) error
	Insert(ctx context.Context, params InsertParams) (*JobRow, error)
	InsertTx(ctx context.Context, tx any, params InsertParams) (*JobRow, error)

	// FetchAvailable claims up to limit due jobs from queue: each waiting
	// job whose scheduled time has passed becomes running with an
	// incremented attempt and a lease running to leaseUntil. A job waits
	// in one of three states — available (never run), retryable (failed,
	// waiting out its backoff) or scheduled (snoozed) — and all three are
	// claimed alike.
	//
	// The lease comes from the caller rather than the driver because the
	// caller is what renews it: a claim lease the caller did not choose
	// could not be kept in step with its heartbeat.
	FetchAvailable(ctx context.Context, queue string, leaseUntil time.Time, limit int) ([]*JobRow, error)

	// FetchExpired re-claims up to limit running jobs whose lease has
	// passed, writing leaseUntil as the new lease so the caller owns
	// them. It does not touch attempt: the attempt that stranded the row
	// was really spent, and counting it twice would halve the effective
	// ceiling of every job whose worker ever crashed.
	FetchExpired(ctx context.Context, leaseUntil time.Time, limit int) ([]*JobRow, error)

	// ExtendLeases pushes the lease of every running job in ids out to
	// until. An id that is missing or no longer running is skipped rather
	// than reported: it means the job finalized first, which races a
	// heartbeat routinely and must never resurrect the row.
	ExtendLeases(ctx context.Context, ids []int64, until time.Time) error

	MarkCompleted(ctx context.Context, id int64) error

	// MarkRetryable returns a failed job to the queue at retryAt without
	// finalizing it, appending errDetail to its errors array.
	MarkRetryable(ctx context.Context, id int64, retryAt time.Time, errDetail []byte) error

	MarkDead(ctx context.Context, id int64, errDetail []byte) error

	// MarkCancelled finalizes a job whose handler declared it
	// unretryable, appending errDetail as the reason.
	MarkCancelled(ctx context.Context, id int64, errDetail []byte) error

	// MarkSnoozed defers a job to runAt without finalizing it. It records
	// no error and gives back the attempt the claim consumed, floored at
	// zero, so snoozing can never exhaust a job's attempts.
	MarkSnoozed(ctx context.Context, id int64, runAt time.Time) error
}
