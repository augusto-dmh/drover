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

	// ErrDuplicateJob reports that a non-terminal job with the same
	// queue, kind, and unique key already exists. No row is inserted.
	ErrDuplicateJob = errors.New("driver: job with this unique key already exists")
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

	// UniqueKey, when non-empty, occupies a unique slot among
	// non-terminal jobs of this queue and kind. Empty means the job
	// does not participate in uniqueness and is stored as NULL.
	UniqueKey string
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
	UniqueKey   string
}

// AttemptError is one recorded execution failure, appended to a job's
// errors array whenever an attempt ends without success.
type AttemptError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

// QueueDepth is the number of jobs sitting in one state on one queue.
type QueueDepth struct {
	Queue string
	State string
	Count int64
}

// QueueAge is how long the oldest job that is claimable right now has
// been waiting on one queue.
//
// It measures lateness, not delay: a job deliberately scheduled for later
// is not waiting for anything, so only the jobs a claim would take this
// instant count. Counting the rest would report every deferred job as an
// outage. The elapsed time is measured by the store's own clock — the one
// that decides whether a job is due — because a caller subtracting
// against its own would publish an age no other caller agrees with.
type QueueAge struct {
	Queue      string
	AgeSeconds float64
}

// Stats is one reading of what the queues hold: how many jobs sit in each
// state worth acting on, and how far behind each queue is.
//
// Both slices are ordered, Depths by queue then state and Oldest by
// queue, so two drivers over the same jobs return the same reading rather
// than the same rows in some order.
//
// A queue with nothing claimable has no Oldest entry at all: the store
// reports what it holds, and deciding that "nothing waiting" publishes as
// an age of zero belongs to the caller that has to name the queues.
// States nothing ever cleans up — completed, cancelled — are deliberately
// absent from Depths, so the cost of a reading tracks the backlog rather
// than the entire history of the queue.
type Stats struct {
	Depths []QueueDepth
	Oldest []QueueAge
}

// ListJobsParams filters and bounds a ListJobs read. Empty Queue or State
// means no filter on that dimension. Limit must be greater than zero;
// adapters refuse non-positive values.
type ListJobsParams struct {
	Queue string
	State string
	Limit int
}

// Driver is the storage contract. Every Mark method is guarded on the
// caller's lease: the job must still be running on the exact attempt the
// lease names. From any other state it reports ErrInvalidTransition, for
// an unknown id ErrNotFound, and for an attempt that has moved on
// ErrLeaseLost. All of them clear the lease, so no finalized or waiting
// row carries a stale one.
//
// OperatorCancel and RedriveDead are not lease-fenced: they are
// state-conditioned updates for waiting or dead rows, and refuse running
// jobs so they never fight a live worker's attempt.
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

	// InsertMany persists every job in batch in one atomic write and
	// returns a row per item, in input order. An empty or nil batch is
	// success with no write.
	InsertMany(ctx context.Context, batch []InsertParams) ([]*JobRow, error)

	// InsertManyTx is InsertMany inside the caller's transaction. Drivers
	// that cannot join a caller-owned transaction return ErrTxUnsupported.
	InsertManyTx(ctx context.Context, tx any, batch []InsertParams) ([]*JobRow, error)

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

	// Stats reports what the queues hold at this instant: a depth per
	// queue and state, and how long the oldest job each queue could hand a
	// worker has been waiting.
	//
	// Both readings are taken by the store, and the age in particular is
	// subtracted there, for the same reason lease durations are passed
	// rather than deadlines: the clock that decides a job is due is the
	// only one that can say how late it is.
	Stats(ctx context.Context) (*Stats, error)

	// ListJobs returns jobs matching optional queue and state filters,
	// newest id first, capped at p.Limit.
	ListJobs(ctx context.Context, p ListJobsParams) ([]*JobRow, error)

	// GetJob returns the current row for id, or ErrNotFound.
	GetJob(ctx context.Context, id int64) (*JobRow, error)

	// OperatorCancel moves available, scheduled, retryable, or dead jobs
	// to cancelled with finalized_at set. Running, completed, and already
	// cancelled jobs are refused with ErrInvalidTransition; a missing id
	// is ErrNotFound.
	OperatorCancel(ctx context.Context, id int64) (*JobRow, error)

	// RedriveDead moves a dead job back to available with attempt reset
	// to 0, lease cleared, finalized_at cleared, and scheduled_at set to
	// the store clock's now, keeping the errors array. Any other state is
	// ErrInvalidTransition; a missing id is ErrNotFound.
	RedriveDead(ctx context.Context, id int64) (*JobRow, error)
}
