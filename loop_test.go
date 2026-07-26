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

// newTestLogger writes structured records into w so a test can assert on
// what the client reported.
func newTestLogger(w *syncWriter) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, nil))
}

// immediatePolicy retries with no wait, so a test can watch a job walk
// its whole attempt ladder without waiting out the real backoff.
type immediatePolicy struct{}

func (immediatePolicy) NextRetry(context.Context, *JobRow) time.Time { return time.Now() }

// atTimePolicy always answers with the same instant, so a test can prove
// the loop scheduled a retry from the configured policy and not from the
// built-in one. It keeps no call counter, so the loop goroutine can
// consult it without a data race.
type atTimePolicy struct{ at time.Time }

func (p atTimePolicy) NextRetry(context.Context, *JobRow) time.Time { return p.at }

// cappedDriver hands out claimed rows with a lowered attempt ceiling, so
// exhaustion is reachable in a test without burning 25 attempts.
type cappedDriver struct {
	*memdriver.Driver
	max int
}

func (d *cappedDriver) FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	rows, err := d.Driver.FetchAvailable(ctx, queue, leaseFor, limit)
	return capRows(rows, d.max), err
}

// Both claim paths are capped: a job reaches the rescuer carrying the
// same ceiling it would have carried into the worker loop.
func (d *cappedDriver) FetchExpired(ctx context.Context, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	rows, err := d.Driver.FetchExpired(ctx, leaseFor, limit)
	return capRows(rows, d.max), err
}

func capRows(rows []*driver.JobRow, max int) []*driver.JobRow {
	for _, row := range rows {
		row.MaxAttempts = max
	}
	return rows
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
	return startLoopWith(t, drv, mem, workers, nil)
}

// startLoopWith starts the loop after letting the caller adjust the
// config, for tests that need a particular retry policy.
func startLoopWith(t *testing.T, drv driver.Driver, mem *memdriver.Driver, workers *Workers, tune func(*Config)) *loopHarness {
	t.Helper()
	logs := &syncWriter{}
	cfg := Config{
		Workers:      workers,
		Logger:       newTestLogger(logs),
		PollInterval: 3 * time.Millisecond,
	}
	if tune != nil {
		tune(&cfg)
	}
	c := newClient(drv, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	h := &loopHarness{mem: mem, client: c, logs: logs, cancel: cancel, done: make(chan error, 1)}
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Start no longer blocks for the pool's lifetime, so the harness
	// mirrors the shutdown onto done: a test cancels, then watches the
	// shutdown finish, exactly as it did when Start returned at the end.
	go func() { <-ctx.Done(); h.done <- c.Stop(context.WithoutCancel(ctx)) }()
	return h
}

// stop cancels the pool and asserts the shutdown completes cleanly.
func (h *loopHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("shutdown returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete after cancellation")
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

// ctxDriver refuses writes on a cancelled context, the way a real
// database driver does. memdriver ignores context entirely, so a test
// that used it directly could not tell a finalization deliberately
// protected from cancellation from one that merely never noticed.
type ctxDriver struct{ *memdriver.Driver }

func (d *ctxDriver) MarkCompleted(ctx context.Context, lease driver.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.Driver.MarkCompleted(ctx, lease)
}

func (d *ctxDriver) MarkRetryable(ctx context.Context, lease driver.Lease, retryAt time.Time, errDetail []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.Driver.MarkRetryable(ctx, lease, retryAt, errDetail)
}

// A cancelled job context is what shutdown escalation looks like from
// inside a worker. The handler must see it — otherwise cancellation is
// decorative — and the outcome must still be recorded, because a job
// left running after a clean shutdown is a job nobody can touch until
// its lease lapses.
func TestJobOutcomeIsRecordedEvenWhenItsContextWasCancelled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		handler   error
		wantState string
	}{
		{name: "a job that succeeds is still completed", handler: nil, wantState: "completed"},
		{name: "a job that fails is still scheduled to retry", handler: errors.New("send failed"), wantState: "retryable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mem := memdriver.New()
			ws := NewWorkers()
			sawCancellation := make(chan bool, 1)
			Register(ws, &funcWorker{fn: func(ctx context.Context, _ *Job[greetArgs]) error {
				sawCancellation <- ctx.Err() != nil
				return tt.handler
			}})
			c := newClient(&ctxDriver{mem}, Config{Workers: ws, Logger: newTestLogger(&syncWriter{})})

			row, err := c.Insert(context.Background(), greetArgs{Name: "ada"})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			claimed, err := mem.FetchAvailable(context.Background(), defaultQueue, time.Minute, 1)
			if err != nil {
				t.Fatalf("FetchAvailable: %v", err)
			}

			jobCtx, cancel := context.WithCancel(context.Background())
			cancel()
			c.runJob(jobCtx, claimed[0])

			if !<-sawCancellation {
				t.Error("handler ran on an uncancelled context, so shutdown could never reach a running job")
			}
			stored, ok := mem.Row(row.ID)
			if !ok {
				t.Fatalf("job %d not found", row.ID)
			}
			if stored.State != tt.wantState {
				t.Errorf("state = %q, want %q", stored.State, tt.wantState)
			}
		})
	}
}

// A handler may legitimately run for far longer than a lease — that is
// what the heartbeat is for. The deadline bounding the write that
// records its outcome must therefore start at the write, not at the
// claim, or every long job would finish successfully and then fail to
// say so, and sit running until the rescuer collected it.
func TestALongRunningJobCanStillRecordItsOutcome(t *testing.T) {
	t.Parallel()

	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}})
	// A lease far shorter than the job it covers.
	c := newClient(&ctxDriver{mem}, Config{
		Workers:       ws,
		Logger:        newTestLogger(&syncWriter{}),
		LeaseDuration: 10 * time.Millisecond,
	})

	row, err := c.Insert(context.Background(), greetArgs{Name: "slow"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	claimed, err := mem.FetchAvailable(context.Background(), defaultQueue, time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}

	c.runJob(context.Background(), claimed[0])

	stored, ok := mem.Row(row.ID)
	if !ok {
		t.Fatalf("job %d not found", row.ID)
	}
	if stored.State != "completed" {
		t.Errorf("state = %q, want completed — a job outliving its lease could not record its result", stored.State)
	}
}

// leaseLostDriver refuses every completion the way the database does
// once the job has moved on to an attempt this caller no longer holds.
type leaseLostDriver struct{ *memdriver.Driver }

func (d *leaseLostDriver) MarkCompleted(context.Context, driver.Lease) error {
	return driver.ErrLeaseLost
}

// Losing the lease is the fence working, not a fault: the job was
// rescued and re-claimed while this attempt was still running, and
// another worker owns the outcome now. Reporting it as a write failure
// would put an ERROR in the log for every rescue — training operators to
// ignore the line that matters when a write genuinely does fail.
func TestALostLeaseIsReportedAsATakeoverNotAFailure(t *testing.T) {
	t.Parallel()

	mem := memdriver.New()
	logs := &syncWriter{}
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	c := newClient(&leaseLostDriver{mem}, Config{Workers: ws, Logger: newTestLogger(logs)})

	if _, err := c.Insert(context.Background(), greetArgs{Name: "ada"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	claimed, err := mem.FetchAvailable(context.Background(), defaultQueue, time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}

	c.runJob(context.Background(), claimed[0])

	out := logs.String()
	if !strings.Contains(out, "taken over by another worker") {
		t.Errorf("log does not report the takeover:\n%s", out)
	}
	if strings.Contains(out, `msg="drover: finalize job"`) {
		t.Errorf("a lost lease was reported as a failed write:\n%s", out)
	}
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

func TestStartRetriesFailedJobInsteadOfKillingIt(t *testing.T) {
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
	waitFor(t, h.rowInState(row.ID, "retryable"), "failed job to be queued for retry")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if stored.FinalizedAt != nil {
		t.Error("FinalizedAt set on a job that is only waiting to retry")
	}
	if stored.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", stored.Attempt)
	}
	// The default policy waits attempt⁴ seconds ±10%, so the first retry
	// lands 0.9–1.1s after the job was disposed of, which is itself a few
	// milliseconds after creation. The lower bound catches a retry that
	// never waits at all; the upper bound catches a curve that is not the
	// documented one. The slack absorbs scheduling delay under -race.
	wait := stored.ScheduledAt.Sub(stored.CreatedAt)
	if wait < 900*time.Millisecond || wait > 1600*time.Millisecond {
		t.Errorf("retry scheduled %v after creation, want roughly one second", wait)
	}

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want exactly one entry {Attempt:1 Error:boom}", recorded)
	}
	logs := h.logs.String()
	for _, want := range []string{
		`msg="drover: job failed, retry scheduled"`, "job_id=" + fmt.Sprint(row.ID),
		"kind=greet", "attempt=1", "duration=", "error=boom", "retry_at=",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("retry log missing %q\nlogs:\n%s", want, logs)
		}
	}
}

func TestStartCompletesJobThatFailsThenSucceeds(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	var attempts atomic.Int32
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		if attempts.Add(1) < 3 {
			return fmt.Errorf("attempt %d failed", attempts.Load())
		}
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to succeed on its third attempt")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if stored.Attempt != 3 {
		t.Errorf("Attempt = %d, want 3", stored.Attempt)
	}
	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 2 {
		t.Fatalf("errors = %+v, want the two failures preserved on the completed job", recorded)
	}
	for i, want := range []int{1, 2} {
		if recorded[i].Attempt != want {
			t.Errorf("errors[%d].Attempt = %d, want %d", i, recorded[i].Attempt, want)
		}
	}
}

func TestStartKillsJobOnlyAfterAttemptsAreExhausted(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	capped := &cappedDriver{Driver: mem, max: 3}
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("always broken")
	}})
	h := startLoopWith(t, capped, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "dead"), "job to die once its attempts ran out")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if stored.Attempt != 3 {
		t.Errorf("Attempt = %d, want the job to have used all 3 attempts", stored.Attempt)
	}
	if stored.FinalizedAt == nil {
		t.Error("FinalizedAt not set on a dead job")
	}
	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 3 {
		t.Errorf("errors = %d entries, want exactly one per attempt (3)", len(recorded))
	}
	if !strings.Contains(h.logs.String(), `msg="drover: job dead"`) {
		t.Errorf("logs missing the death record\nlogs:\n%s", h.logs.String())
	}
}

func TestStartKillsJobWithNonPositiveAttemptCeiling(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	capped := &cappedDriver{Driver: mem, max: 0}
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("boom")
	}})
	h := startLoopWith(t, capped, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "dead"), "job with no attempts left to die immediately")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 {
		t.Errorf("errors = %d entries, want exactly one", len(recorded))
	}
}

func TestStartRetriesPanickedJobAndKeepsRunning(t *testing.T) {
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
	waitFor(t, h.rowInState(bad.ID, "retryable"), "panicked job to be queued for retry")
	waitFor(t, h.rowInState(good.ID, "completed"), "loop to keep running after a panic")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, bad.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "kaboom") {
		t.Fatalf("errors = %+v, want one entry mentioning kaboom", recorded)
	}
	if recorded[0].Trace == "" {
		t.Error("panic Trace not recorded")
	}
}

func TestStartRetriesUnregisteredKind(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	h := startLoop(t, mem, mem, NewWorkers())

	row, err := h.client.Insert(context.Background(), ghostArgs{})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// A binary that does not know a kind may simply be the old one
	// mid-deploy; the job has to outlive the rollout (AD-014).
	waitFor(t, h.rowInState(row.ID, "retryable"), "unregistered-kind job to be queued for retry")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, `no worker registered for kind "ghost"`) {
		t.Errorf("errors = %+v, want unregistered-kind entry", recorded)
	}
}

func TestStartRetriesUndecodableArgs(t *testing.T) {
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
	waitFor(t, h.rowInState(row.ID, "retryable"), "undecodable job to be queued for retry")
	h.stop(t)

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "decode args") {
		t.Errorf("errors = %+v, want decode-failure entry", recorded)
	}
}

func TestStartCancelsJobOnCancelSentinel(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	var calls atomic.Int32
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		calls.Add(1)
		return fmt.Errorf("payload rejected: %w", Cancel(errors.New("bad input")))
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "cancelled"), "job to be cancelled by its handler")
	// An immediate retry policy means a job that was wrongly treated as
	// retryable would be re-run within milliseconds; give it the chance.
	time.Sleep(50 * time.Millisecond)
	h.stop(t)

	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1 — a cancelled job must not retry", got)
	}
	stored, _ := mem.Row(row.ID)
	if stored.State != "cancelled" {
		t.Errorf("State = %q, want cancelled", stored.State)
	}
	if stored.FinalizedAt == nil {
		t.Error("FinalizedAt not set on a cancelled job")
	}
	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 || !strings.Contains(recorded[0].Error, "bad input") {
		t.Errorf("errors = %+v, want exactly one entry carrying the reason", recorded)
	}
}

func TestStartSnoozesJobWithoutSpendingAnAttempt(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return fmt.Errorf("not ready: %w", Snooze(time.Hour))
	}})
	h := startLoop(t, mem, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "scheduled"), "snoozed job to be deferred")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if stored.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0 — a snooze must give back the attempt the claim took",
			stored.Attempt)
	}
	if stored.FinalizedAt != nil {
		t.Error("FinalizedAt set on a job that is only deferred")
	}
	wait := stored.ScheduledAt.Sub(stored.CreatedAt)
	if wait < 59*time.Minute || wait > 61*time.Minute {
		t.Errorf("job deferred by %v, want about an hour", wait)
	}
	if recorded := decodeAttemptErrors(t, mem, row.ID); len(recorded) != 0 {
		t.Errorf("errors = %+v, want none — a snooze is not a failure", recorded)
	}
}

func TestStartSnoozeNeverExhaustsAttempts(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	// A ceiling of one: if a snooze spent an attempt, the second pass
	// would find the job exhausted and kill it.
	capped := &cappedDriver{Driver: mem, max: 1}
	var calls atomic.Int32
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		if calls.Add(1) <= 5 {
			return Snooze(0)
		}
		return nil
	}})
	h := startLoop(t, capped, mem, ws)

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "job to complete after five snoozes")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if stored.Attempt < 0 {
		t.Errorf("Attempt = %d, want never negative", stored.Attempt)
	}
	if stored.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1 — only the successful pass should count", stored.Attempt)
	}
	if recorded := decodeAttemptErrors(t, mem, row.ID); len(recorded) != 0 {
		t.Errorf("errors = %+v, want none", recorded)
	}
}

func TestStartSchedulesRetryFromConfiguredPolicy(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	want := time.Now().Add(72 * time.Hour).Round(0)
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("boom")
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = atTimePolicy{at: want}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "failed job to be queued for retry")
	h.stop(t)

	stored, _ := mem.Row(row.ID)
	if !stored.ScheduledAt.Equal(want) {
		t.Errorf("ScheduledAt = %v, want the configured policy's answer %v", stored.ScheduledAt, want)
	}
}

// recordingPolicy captures the row it was handed, so a test can assert
// what the loop actually passes a user-supplied policy rather than only
// what it does with the answer.
type recordingPolicy struct {
	mu   sync.Mutex
	seen *JobRow
}

func (p *recordingPolicy) NextRetry(_ context.Context, job *JobRow) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	clone := *job
	p.seen = &clone
	return time.Now().Add(time.Hour)
}

func (p *recordingPolicy) row() *JobRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen
}

func TestStartHandsTheFailedJobRowToTheRetryPolicy(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	policy := &recordingPolicy{}
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		return errors.New("boom")
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = policy
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(row.ID, "retryable"), "failed job to be queued for retry")
	h.stop(t)

	// NextRetry's contract says the whole row is in scope so a policy can
	// branch on it. Nothing else in the suite would notice if the loop
	// passed a differently-shaped row.
	seen := policy.row()
	if seen == nil {
		t.Fatal("retry policy was never consulted")
	}
	if seen.ID != row.ID {
		t.Errorf("policy saw ID %d, want %d", seen.ID, row.ID)
	}
	if seen.Kind != "greet" {
		t.Errorf("policy saw Kind %q, want greet", seen.Kind)
	}
	if seen.Queue != "default" {
		t.Errorf("policy saw Queue %q, want default", seen.Queue)
	}
	if seen.MaxAttempts != 25 {
		t.Errorf("policy saw MaxAttempts %d, want 25", seen.MaxAttempts)
	}
	// The attempt being scheduled for, not the one before it — a policy
	// computing a backoff curve reads this directly.
	if seen.Attempt != 1 {
		t.Errorf("policy saw Attempt %d, want 1", seen.Attempt)
	}
}

func TestStartSurvivesFailureToRecordARetry(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	blocked := &blockedRetryDriver{Driver: mem}
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		if job.Args.Name == "doomed" {
			return errors.New("boom")
		}
		return nil
	}})
	h := startLoop(t, blocked, mem, ws)

	doomed, err := h.client.Insert(context.Background(), greetArgs{Name: "doomed"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, func() bool { return blocked.calls.Load() > 0 }, "the retry write to be attempted")

	// The loop must still be serving other work after a write it could
	// not land; the stranded job is the rescuer's problem, not a reason
	// to stop.
	fine, err := h.client.Insert(context.Background(), greetArgs{Name: "fine"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	waitFor(t, h.rowInState(fine.ID, "completed"), "loop to keep working after a failed write")
	h.stop(t)

	stored, _ := mem.Row(doomed.ID)
	if stored.State != "running" {
		t.Errorf("State = %q, want the job left running for the rescuer", stored.State)
	}
	if !strings.Contains(h.logs.String(), `msg="drover: finalize job"`) {
		t.Errorf("logs missing the failed-write record\nlogs:\n%s", h.logs.String())
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
	counting := &countingDriver{Driver: memdriver.New()}
	c := newClient(counting, Config{
		Logger:       slog.New(slog.NewTextHandler(&syncWriter{}, nil)),
		PollInterval: 30 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const window = 150 * time.Millisecond
	time.Sleep(window)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}

	fetches := counting.fetches.Load()
	if fetches < 2 {
		t.Fatalf("loop fetched %d times in %v, want repeated polling", fetches, window)
	}
	// 150ms / 30ms ≈ 5 fetches; a loop that skips the sleep fetches
	// thousands of times. Generous bound to stay timing-tolerant.
	if fetches > 20 {
		t.Fatalf("loop fetched %d times in %v with a 30ms interval — polling without sleeping",
			fetches, window)
	}
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

	logs := h.logs.String()
	if !strings.Contains(logs, `level=ERROR msg="drover: fetch jobs"`) {
		t.Errorf("logs missing ERROR-level fetch-error record\nlogs:\n%s", logs)
	}
}

type countingDriver struct {
	*memdriver.Driver
	fetches atomic.Int64
}

func (d *countingDriver) FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	d.fetches.Add(1)
	return d.Driver.FetchAvailable(ctx, queue, leaseFor, limit)
}

type flakyDriver struct {
	*memdriver.Driver
	failures int32
	calls    atomic.Int32
}

func (d *flakyDriver) FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	if d.calls.Add(1) <= d.failures {
		return nil, errors.New("connection refused")
	}
	return d.Driver.FetchAvailable(ctx, queue, leaseFor, limit)
}

// blockedRetryDriver refuses every attempt to queue a retry, standing in
// for a database that went away between claiming a job and recording its
// failure.
type blockedRetryDriver struct {
	*memdriver.Driver
	calls atomic.Int32
}

func (d *blockedRetryDriver) MarkRetryable(context.Context, driver.Lease, time.Time, []byte) error {
	d.calls.Add(1)
	return errors.New("connection refused")
}
