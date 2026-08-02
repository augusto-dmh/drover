package drover

import (
	"context"
	"encoding/json"
	"fmt"
)

// Worker executes jobs whose args decode to T. Implementations must be
// idempotent: drover delivers at least once, so the same job may run
// more than once after a crash or lease expiry.
type Worker[T JobArgs] interface {
	Work(ctx context.Context, job *Job[T]) error
}

// WorkerDefaults is embedded by Worker implementations to keep them
// forward-compatible with optional Worker methods added in later
// versions.
type WorkerDefaults[T JobArgs] struct{}

// Workers maps job kinds to registered workers.
type Workers struct {
	handlers map[string]Handler
}

// NewWorkers returns an empty registry.
func NewWorkers() *Workers {
	return &Workers{handlers: make(map[string]Handler)}
}

func (w *Workers) handler(kind string) (Handler, bool) {
	fn, ok := w.handlers[kind]
	return fn, ok
}

// Register adds worker as the handler for T's kind, taken from T's
// zero value. Registration happens at boot: registering the same kind
// twice is a programmer error and panics.
func Register[T JobArgs](ws *Workers, worker Worker[T]) {
	var zero T
	kind := zero.Kind()
	if _, dup := ws.handlers[kind]; dup {
		panic(fmt.Sprintf("drover: worker already registered for kind %q", kind))
	}
	ws.handlers[kind] = func(ctx context.Context, job *JobRow) error {
		var args T
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return fmt.Errorf("decode args for job %d (kind %q): %w", job.ID, job.Kind, err)
		}
		return worker.Work(ctx, &Job[T]{
			ID:        job.ID,
			Attempt:   job.Attempt,
			CreatedAt: job.CreatedAt,
			Args:      args,
		})
	}
}
