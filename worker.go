package drover

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/augusto-dmh/drover/internal/driver"
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

// workFunc adapts a typed Worker to the untyped row the loop fetches.
type workFunc func(ctx context.Context, row *driver.JobRow) error

// Workers maps job kinds to registered workers.
type Workers struct {
	handlers map[string]workFunc
}

// NewWorkers returns an empty registry.
func NewWorkers() *Workers {
	return &Workers{handlers: make(map[string]workFunc)}
}

func (w *Workers) handler(kind string) (workFunc, bool) {
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
	ws.handlers[kind] = func(ctx context.Context, row *driver.JobRow) error {
		var args T
		if err := json.Unmarshal(row.Args, &args); err != nil {
			return fmt.Errorf("decode args for job %d (kind %q): %w", row.ID, row.Kind, err)
		}
		return worker.Work(ctx, &Job[T]{
			ID:        row.ID,
			Attempt:   row.Attempt,
			CreatedAt: row.CreatedAt,
			Args:      args,
		})
	}
}
