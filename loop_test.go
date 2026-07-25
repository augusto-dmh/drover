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

// immediatePolicy retries with no wait, so a test can watch a job walk
// its whole attempt ladder without waiting out the real backoff.
type immediatePolicy struct{}

func (immediatePolicy) NextRetry(*JobRow) time.Time { return time.Now() }

// atTimePolicy always answers with the same instant, so a test can prove
// the loop scheduled a retry from the configured policy and not from the
// built-in one. It keeps no call counter, so the loop goroutine can
// consult it without a data race.
type atTimePolicy struct{ at time.Time }

func (p atTimePolicy) NextRetry(*JobRow) time.Time { return p.at }

// cappedDriver hands out claimed rows with a lowered attempt ceiling, so
// exhaustion is reachable in a test without burning 25 attempts.
type cappedDriver struct {
	*memdriver.Driver
	max int
}

func (d *cappedDriver) FetchAvailable(ctx context.Context, queue string, limit int) ([]*driver.JobRow, error) {
	rows, err := d.Driver.FetchAvailable(ctx, queue, limit)
	for _, row := range rows {
		row.MaxAttempts = d.max
	}
	return rows, err
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
		Logger:       slog.New(slog.NewTextHandler(logs, nil)),
		PollInterval: 3 * time.Millisecond,
	}
	if tune != nil {
		tune(&cfg)
	}
	c := newClient(drv, cfg)
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
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	const window = 150 * time.Millisecond
	time.Sleep(window)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancellation")
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

// blockedRetryDriver refuses every attempt to queue a retry, standing in
// for a database that went away between claiming a job and recording its
// failure.
type blockedRetryDriver struct {
	*memdriver.Driver
	calls atomic.Int32
}

func (d *blockedRetryDriver) MarkRetryable(context.Context, int64, time.Time, []byte) error {
	d.calls.Add(1)
	return errors.New("connection refused")
}
