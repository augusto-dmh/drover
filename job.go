package drover

import "time"

// JobArgs is implemented by any type that can be enqueued as a job.
// Kind uniquely identifies the job type and links enqueued rows to the
// Worker registered for that kind. Args are serialized to JSON at
// enqueue time and decoded back before the Worker runs.
type JobArgs interface {
	Kind() string
}

// JobState is the lifecycle state of a job row.
type JobState string

// All states exist in the schema from the start; Cycle A transitions
// only between Available, Running, Completed, and Dead. See AD-002.
const (
	StateAvailable JobState = "available"
	StateScheduled JobState = "scheduled"
	StateRunning   JobState = "running"
	StateRetryable JobState = "retryable"
	StateCompleted JobState = "completed"
	StateCancelled JobState = "cancelled"
	StateDead      JobState = "dead"
)

// Job carries the decoded args payload plus row metadata to a Worker.
type Job[T JobArgs] struct {
	ID        int64
	Attempt   int
	CreatedAt time.Time
	Args      T
}
