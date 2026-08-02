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

	// ErrLeaseLost reports that the job has moved on to an attempt this
	// caller does not hold — its lease lapsed, the rescuer returned the
	// job to the queue, and someone else claimed it. It is the expected
	// outcome of a starved heartbeat, not a fault: the work may well have
	// been done twice, which at-least-once delivery permits, but only the
	// current holder may record what happened.
	ErrLeaseLost = errors.New("driver: job reclaimed by another worker")
)

// Lease identifies one claim on a job: the row, and the attempt the
// holder is executing. Every state change carries it, so a worker can
// only finalize the attempt it actually owns.
//
// The attempt number is the fence. FetchAvailable increments it on every
// claim and FetchExpired deliberately does not, so a rescued job reaches
// its next worker with a number no earlier holder can present.
type Lease struct {
	ID      int64
	Attempt int
}

// InsertParams carries everything needed to persist a new job.
type InsertParams struct {
	Kind  string
	Queue string
	Args  []byte

	// ScheduledAt is when the job first becomes claimable. The zero
	// value means now.
	//
	// Whether that instant has already passed is decided by the store's
	// own clock, not the caller's, and the stored state follows from it:
	// a job due later waits in scheduled, one due now is available. The
	// fetch predicate compares scheduled_at against the same clock, so
	// letting the caller decide would let a client running fast or slow
	// record a state its own store disagrees with.
	ScheduledAt time.Time
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
// caller's lease: the job must still be running on the exact attempt the
// lease names. From any other state it reports ErrInvalidTransition, for
// an unknown id ErrNotFound, and for an attempt that has moved on
// ErrLeaseLost. All of them clear the lease, so no finalized or waiting
// row carries a stale one.
//
// Lease durations are passed rather than deadlines because the database
// clock is the one that decides expiry. Computing the instant on the
// caller would make every claim depend on the two clocks agreeing, and
// across a fleet they do not: a client running behind would write leases
// the database already considers expired, and one running ahead would
// stretch recovery past what the configuration advertises.
type Driver interface {
	Migrate(ctx context.Context) error
	Insert(ctx context.Context, params InsertParams) (*JobRow, error)
	InsertTx(ctx context.Context, tx any, params InsertParams) (*JobRow, error)

	// FetchAvailable claims up to limit due jobs from queue: each waiting
	// job whose scheduled time has passed becomes running with an
	// incremented attempt and a lease lasting leaseFor. A job waits in one
	// of three states — available (never run), retryable (failed, waiting
	// out its backoff) or scheduled (snoozed) — and all three are claimed
	// alike.
	FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*JobRow, error)

	// FetchExpired re-claims up to limit running jobs whose lease has
	// passed, giving each a fresh lease lasting leaseFor so the caller
	// owns them. It does not touch attempt: the attempt that stranded the
	// row was really spent, and counting it twice would halve the
	// effective ceiling of every job whose worker ever crashed.
	FetchExpired(ctx context.Context, leaseFor time.Duration, limit int) ([]*JobRow, error)

	// ExtendLeases pushes each held lease out by another leaseFor. A lease
	// whose job is missing, no longer running, or already on a later
	// attempt is skipped rather than reported: it means the job finished
	// or moved on, which races a heartbeat routinely and must never
	// resurrect the row or steal it back.
	ExtendLeases(ctx context.Context, leases []Lease, leaseFor time.Duration) error

	MarkCompleted(ctx context.Context, lease Lease) error

	// MarkRetryable returns a failed job to the queue at retryAt without
	// finalizing it, appending errDetail to its errors array.
	MarkRetryable(ctx context.Context, lease Lease, retryAt time.Time, errDetail []byte) error

	MarkDead(ctx context.Context, lease Lease, errDetail []byte) error

	// MarkCancelled finalizes a job whose handler declared it
	// unretryable, appending errDetail as the reason.
	MarkCancelled(ctx context.Context, lease Lease, errDetail []byte) error

	// MarkSnoozed defers a job to runAt without finalizing it. It records
	// no error and gives back the attempt the claim consumed, floored at
	// zero, so snoozing can never exhaust a job's attempts.
	MarkSnoozed(ctx context.Context, lease Lease, runAt time.Time) error
}
