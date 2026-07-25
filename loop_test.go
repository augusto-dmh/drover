package drover

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

type ghostArgs struct{}

func (ghostArgs) Kind() string { return "ghost" }

type funcWorker struct {
	WorkerDefaults[greetArgs]
	fn func(ctx context.Context, job *Job[greetArgs]) error
}

func (w *funcWorker) Work(ctx context.Context, job *Job[greetArgs]) error {
	return w.fn(ctx, job)
}

type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// loopHarness wires a client to a fast-polling loop over drv and
// returns hooks to observe rows, logs, and shutdown.
type loopHarness struct {
	mem    *memdriver.Driver
	client *Client
	logs   *syncWriter
	cancel context.CancelFunc
	done   chan error
}

func startLoop(t *testing.T, drv driver.Driver, mem *memdriver.Driver, workers *Workers) *loopHarness {
	t.Helper()
	logs := &syncWriter{}
	c := newClient(drv, Config{
		Workers:      workers,
		Logger:       slog.New(slog.NewTextHandler(logs, nil)),
		PollInterval: 3 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h := &loopHarness{mem: mem, client: c, logs: logs, cancel: cancel, done: make(chan error, 1)}
	go func() { h.done <- c.Start(ctx) }()
	return h
}

// stop cancels the loop and asserts it returns nil promptly.
func (h *loopHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for " + msg)
}

func (h *loopHarness) rowInState(id int64, state string) func() bool {
	return func() bool {
		row, ok := h.mem.Row(id)
		return ok && row.State == state
	}
}

func decodeAttemptErrors(t *testing.T, mem *memdriver.Driver, id int64) []driver.AttemptError {
	t.Helper()
	row, ok := mem.Row(id)
	if !ok {
		t.Fatalf("job %d not found", id)
	}
	var recorded []driver.AttemptError
	if err := json.Unmarshal(row.Errors, &recorded); err != nil {
		t.Fatalf("decode errors: %v", err)
	}
	return recorded
}

func TestStartExecutesJobToCompletion(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoop(t, mem, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if stored.FinalizedAt == nil {
		t.Error("FinalizedAt not set on completed job")
	}
	logs := h.logs.String()
	for _, want := range []string{
		`msg="drover: job started"`, `msg="drover: job completed"`,
		"job_id=" + fmt.Sprint(row.ID), "kind=greet", "attempt=1", "duration=",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %q\nlogs:\n%s", want, logs)
		}
	}
}

func TestStartMarksFailingJobDeadWithRecordedError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("boom")
	}})
	h := startLoop(t, mem, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "dead"), "job to be marked dead")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want one entry {Attempt:1 Error:boom}", recorded)
	}
	if !strings.Contains(h.logs.String(), `msg="drover: job failed"`) {
		t.Error("logs missing job failed record")
	}
}

func TestStartRecoversPanicAndKeepsRunning(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		if job.Args.Name == "explode" {
			panic("kaboom")
		}
		return nil
	}})
	h := startLoop(t, mem, mem, ws)

	bad, err := h.client.Insert(context.Background(), greetArgs{Name: "explode"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	good, err := h.client.Insert(context.Background(), greetArgs{Name: "fine"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(bad.ID, "dead"), "panicked job to be marked dead")
	waitFor(t, h.rowInState(good.ID, "completed"), "loop to keep running after a panic")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, bad.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "kaboom") {
		t.Errorf("errors = %+v, want one entry mentioning kaboom", recorded)
	}
	if recorded[0].Trace == "" {
		t.Error("panic Trace not recorded")
	}
}

func TestStartMarksUnregisteredKindDead(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	h := startLoop(t, mem, mem, NewWorkers())

	row, err := h.client.Insert(context.Background(), ghostArgs{})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "dead"), "unregistered-kind job to be marked dead")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, `no worker registered for kind "ghost"`) {
		t.Errorf("errors = %+v, want unregistered-kind entry", recorded)
	}
}

func TestStartMarksUndecodableArgsDead(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoop(t, mem, mem, ws)

	row, err := mem.Insert(context.Background(), driver.InsertParams{
		Kind:  "greet",
		Queue: "default",
		Args:  []byte(`{not-json`),
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "dead"), "undecodable job to be marked dead")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "decode args") {
		t.Errorf("errors = %+v, want decode-failure entry", recorded)
	}
}

func TestStartDrainsInFlightJobBeforeReturning(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	started := make(chan struct{})
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		close(started)
		<-release
		return nil
	}})
	h := startLoop(t, mem, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "slow"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	<-started
	h.cancel()

	select {
	case <-h.done:
		t.Fatal("Start returned while a job was in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after in-flight job finished")
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "drained job to be finalized")
}

func TestStartKeepsPollingWhileIdle(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	counting := &countingDriver{Driver: mem}
	h := startLoop(t, counting, mem, NewWorkers())

	waitFor(t, func() bool { return counting.fetches.Load() >= 3 },
		"loop to poll repeatedly while idle")
	h.stop(t)
}

func TestStartLogsAndRetriesAfterFetchErrors(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	flaky := &flakyDriver{Driver: mem, failures: 2}
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoop(t, flaky, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "loop to recover from fetch errors")
	h.stop(t)

	if !strings.Contains(h.logs.String(), `msg="drover: fetch jobs"`) {
		t.Error("logs missing fetch-error record")
	}
}

type countingDriver struct {
	*memdriver.Driver
	fetches atomic.Int64
}

func (d *countingDriver) FetchAvailable(ctx context.Context, queue string, limit int) ([]*driver.JobRow, error) {
	d.fetches.Add(1)
	return d.Driver.FetchAvailable(ctx, queue, limit)
}

type flakyDriver struct {
	*memdriver.Driver
	failures int32
	calls    atomic.Int32
}

func (d *flakyDriver) FetchAvailable(ctx context.Context, queue string, limit int) ([]*driver.JobRow, error) {
	if d.calls.Add(1) <= d.failures {
		return nil, errors.New("connection refused")
	}
	return d.Driver.FetchAvailable(ctx, queue, limit)
}
