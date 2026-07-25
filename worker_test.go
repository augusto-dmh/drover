package drover

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

type greetArgs struct {
	Name string `json:"name"`
}

func (greetArgs) Kind() string { return "greet" }

type greetWorker struct {
	WorkerDefaults[greetArgs]
	got *Job[greetArgs]
}

func (w *greetWorker) Work(_ context.Context, job *Job[greetArgs]) error {
	w.got = job
	return nil
}

func TestRegisterDispatchesDecodedJobToWorker(t *testing.T) {
	t.Parallel()
	ws := NewWorkers()
	worker := &greetWorker{}
	Register(ws, worker)

	created := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	fn, ok := ws.handler("greet")
	if !ok {
		t.Fatal("no handler registered for kind greet")
	}
	err := fn(context.Background(), &driver.JobRow{
		ID:        42,
		Kind:      "greet",
		Args:      []byte(`{"name":"ada"}`),
		Attempt:   1,
		CreatedAt: created,
	})
	if err != nil {
		t.Fatalf("workFunc: %v", err)
	}

	if worker.got == nil {
		t.Fatal("Work was not called")
	}
	if worker.got.ID != 42 || worker.got.Attempt != 1 || !worker.got.CreatedAt.Equal(created) {
		t.Errorf("job metadata = {ID:%d Attempt:%d CreatedAt:%v}, want {ID:42 Attempt:1 CreatedAt:%v}",
			worker.got.ID, worker.got.Attempt, worker.got.CreatedAt, created)
	}
	if worker.got.Args.Name != "ada" {
		t.Errorf("decoded Args.Name = %q, want %q", worker.got.Args.Name, "ada")
	}
}

func TestRegisterPanicsOnDuplicateKindNamingIt(t *testing.T) {
	t.Parallel()
	ws := NewWorkers()
	Register(ws, &greetWorker{})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("second Register did not panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, `"greet"`) {
			t.Fatalf("panic = %v, want message naming kind %q", r, "greet")
		}
	}()
	Register(ws, &greetWorker{})
}

func TestWorkFuncReturnsDecodeErrorForMalformedArgs(t *testing.T) {
	t.Parallel()
	ws := NewWorkers()
	worker := &greetWorker{}
	Register(ws, worker)

	fn, _ := ws.handler("greet")
	err := fn(context.Background(), &driver.JobRow{ID: 7, Kind: "greet", Args: []byte(`{not-json`)})

	if err == nil {
		t.Fatal("workFunc returned nil for malformed args")
	}
	if !strings.Contains(err.Error(), "job 7") {
		t.Errorf("error = %q, want it to reference job 7", err)
	}
	if worker.got != nil {
		t.Error("Work must not run when args fail to decode")
	}
}

func TestHandlerLookupUnknownKind(t *testing.T) {
	t.Parallel()
	ws := NewWorkers()

	if _, ok := ws.handler("nope"); ok {
		t.Fatal("handler() = ok for unregistered kind, want false")
	}
}
