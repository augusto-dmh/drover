//go:build integration

package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/testdb"
)

type e2eArgs struct {
	N int `json:"n"`
}

func (e2eArgs) Kind() string { return "e2e" }

type e2eWorker struct {
	WorkerDefaults[e2eArgs]
	mu   sync.Mutex
	runs map[int64]int
	fail func(args e2eArgs) error
	// enter, when set, is called as the job starts, so a test can wait
	// for work to be genuinely under way rather than sleeping.
	enter func(id int64)
}

func (w *e2eWorker) Work(_ context.Context, job *Job[e2eArgs]) error {
	w.mu.Lock()
	w.runs[job.ID]++
	w.mu.Unlock()
	if w.enter != nil {
		w.enter(job.ID)
	}
	if w.fail != nil {
		return w.fail(job.Args)
	}
	return nil
}

// waitForState blocks until exactly n rows sit in the given state.
func waitForState(t *testing.T, pool *pgxpool.Pool, state string, n int) {
	t.Helper()
	waitFor(t, func() bool { return countInState(t, pool, state) == n },
		fmt.Sprintf("%d job(s) to reach %s", n, state))
}

func newE2EClient(t *testing.T, pool *pgxpool.Pool, worker *e2eWorker) *Client {
	t.Helper()
	return newE2EClientWith(t, pool, worker, nil)
}

func newE2EClientWith(t *testing.T, pool *pgxpool.Pool, worker *e2eWorker, tune func(*Config)) *Client {
	t.Helper()
	ws := NewWorkers()
	Register(ws, worker)
	cfg := Config{Workers: ws, PollInterval: 5 * time.Millisecond}
	if tune != nil {
		tune(&cfg)
	}
	c, err := NewClient(pool, cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// runLoop starts c's pool and returns a function that stops it and
// asserts a clean shutdown.
func runLoop(t *testing.T, ctx context.Context, c *Client) func() {
	t.Helper()
	loopCtx, cancel := context.WithCancel(ctx)
	if err := c.Start(loopCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return func() {
		t.Helper()
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Stop(context.Background()) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Stop returned %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("pool did not stop")
		}
	}
}

func readJob(t *testing.T, pool *pgxpool.Pool, id int64) (state string, attempt int, recorded []driver.AttemptError) {
	t.Helper()
	var rawErrors []byte
	if err := pool.QueryRow(context.Background(),
		`SELECT state, attempt, errors FROM drover_jobs WHERE id = $1`, id).
		Scan(&state, &attempt, &rawErrors); err != nil {
		t.Fatalf("read job %d: %v", id, err)
	}
	if err := json.Unmarshal(rawErrors, &recorded); err != nil {
		t.Fatalf("decode errors for job %d: %v", id, err)
	}
	return state, attempt, recorded
}

func countInState(t *testing.T, pool *pgxpool.Pool, state string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM drover_jobs WHERE state = $1`, state).Scan(&n); err != nil {
		t.Fatalf("count %s jobs: %v", state, err)
	}
	return n
}

func TestEndToEndConcurrentLoopsExecuteEachJobExactlyOnce(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{runs: make(map[int64]int)}
	first := newE2EClient(t, pool, worker)
	second := newE2EClient(t, pool, worker)

	const jobs = 50
	for i := range jobs {
		if _, err := first.Insert(ctx, e2eArgs{N: i}, nil); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	stopFirst := runLoop(t, ctx, first)
	stopSecond := runLoop(t, ctx, second)

	waitFor(t, func() bool { return countInState(t, pool, "completed") == jobs },
		fmt.Sprintf("all %d jobs to complete", jobs))
	stopFirst()
	stopSecond()

	worker.mu.Lock()
	defer worker.mu.Unlock()
	if len(worker.runs) != jobs {
		t.Fatalf("executed %d distinct jobs, want %d", len(worker.runs), jobs)
	}
	for id, n := range worker.runs {
		if n != 1 {
			t.Fatalf("job %d executed %d times, want exactly once", id, n)
		}
	}
}

func TestEndToEndFailedJobIsQueuedForRetryInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{
		runs: make(map[int64]int),
		fail: func(e2eArgs) error { return errors.New("smtp unreachable") },
	}
	c := newE2EClient(t, pool, worker)

	row, err := c.Insert(ctx, e2eArgs{N: 1}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	stop := runLoop(t, ctx, c)
	// An unreachable mail server is the archetypal transient failure: the
	// job waits out its backoff rather than dying on the first attempt.
	waitFor(t, func() bool { return countInState(t, pool, "retryable") == 1 }, "job to be queued for retry")
	stop()

	var state string
	var finalizedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT state, finalized_at FROM drover_jobs WHERE id = $1`, row.ID).
		Scan(&state, &finalizedAt); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "retryable" {
		t.Errorf("state = %q, want retryable", state)
	}
	if finalizedAt != nil {
		t.Errorf("finalized_at = %v, want unset on a job awaiting retry", finalizedAt)
	}

	var rawErrors []byte
	if err := pool.QueryRow(ctx,
		`SELECT errors FROM drover_jobs WHERE id = $1`, row.ID).Scan(&rawErrors); err != nil {
		t.Fatalf("read errors: %v", err)
	}
	var recorded []driver.AttemptError
	if err := json.Unmarshal(rawErrors, &recorded); err != nil {
		t.Fatalf("decode errors: %v", err)
	}
	if len(recorded) != 1 || recorded[0].Error != "smtp unreachable" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want one entry {Attempt:1 Error:smtp unreachable}", recorded)
	}
}

func TestEndToEndTransientFailuresEventuallySucceedInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	var attempts atomic.Int32
	worker := &e2eWorker{
		runs: make(map[int64]int),
		fail: func(e2eArgs) error {
			if attempts.Add(1) < 3 {
				return fmt.Errorf("smtp unreachable (attempt %d)", attempts.Load())
			}
			return nil
		},
	}
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	row, err := c.Insert(ctx, e2eArgs{N: 1}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	stop := runLoop(t, ctx, c)
	waitFor(t, func() bool { return countInState(t, pool, "completed") == 1 },
		"job to succeed on a later attempt")
	stop()

	state, attempt, recorded := readJob(t, pool, row.ID)
	if state != "completed" {
		t.Errorf("state = %q, want completed", state)
	}
	if attempt != 3 {
		t.Errorf("attempt = %d, want 3", attempt)
	}
	// The failures stay on the row after success: the history of a job is
	// not erased by it eventually working.
	if len(recorded) != 2 {
		t.Fatalf("errors = %+v, want the two earlier failures preserved", recorded)
	}
}

func TestEndToEndJobDiesOnlyAfterExhaustingAttemptsInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{
		runs: make(map[int64]int),
		fail: func(e2eArgs) error { return errors.New("permanently broken") },
	}
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	row, err := c.Insert(ctx, e2eArgs{N: 1}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE drover_jobs SET max_attempts = 3 WHERE id = $1`, row.ID); err != nil {
		t.Fatalf("lower max_attempts: %v", err)
	}

	stop := runLoop(t, ctx, c)
	waitFor(t, func() bool { return countInState(t, pool, "dead") == 1 }, "job to exhaust its attempts")
	stop()

	state, attempt, recorded := readJob(t, pool, row.ID)
	if state != "dead" {
		t.Errorf("state = %q, want dead", state)
	}
	if attempt != 3 {
		t.Errorf("attempt = %d, want all 3 attempts spent", attempt)
	}
	if len(recorded) != 3 {
		t.Errorf("errors = %d entries, want one per attempt (3)", len(recorded))
	}

	worker.mu.Lock()
	defer worker.mu.Unlock()
	if got := worker.runs[row.ID]; got != 3 {
		t.Errorf("handler ran %d times, want 3", got)
	}
}

func TestEndToEndAbandonedJobIsRescuedAndRerunInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{runs: make(map[int64]int)}
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
		cfg.LeaseDuration = time.Second
		cfg.RescueInterval = 50 * time.Millisecond
	})

	row, err := c.Insert(ctx, e2eArgs{N: 1}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Exactly what a worker killed by SIGKILL leaves behind: claimed, an
	// attempt spent, and a lease nobody is renewing any more.
	if _, err := pool.Exec(ctx,
		`UPDATE drover_jobs
		    SET state = 'running', attempt = 1, leased_until = now() - interval '1 minute'
		  WHERE id = $1`, row.ID); err != nil {
		t.Fatalf("simulate crashed worker: %v", err)
	}

	stop := runLoop(t, ctx, c)
	waitFor(t, func() bool { return countInState(t, pool, "completed") == 1 },
		"abandoned job to be rescued and re-run")
	stop()

	state, attempt, _ := readJob(t, pool, row.ID)
	if state != "completed" {
		t.Errorf("state = %q, want completed", state)
	}
	// One attempt was spent by the worker that died and one by the run
	// that finished it — the rescue itself spends none.
	if attempt != 2 {
		t.Errorf("attempt = %d, want 2", attempt)
	}

	worker.mu.Lock()
	defer worker.mu.Unlock()
	if got := worker.runs[row.ID]; got != 1 {
		t.Errorf("handler ran %d times after the rescue, want 1", got)
	}
}

func TestEndToEndLongRunningJobIsNeverRescuedInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	release := make(chan struct{})
	var runs atomic.Int32
	started := make(chan struct{})
	worker := &e2eWorker{
		runs: make(map[int64]int),
		fail: func(e2eArgs) error {
			if runs.Add(1) == 1 {
				close(started)
			}
			<-release
			return nil
		},
	}
	// The job runs for several lease durations with the sweeper active;
	// only the heartbeat stops it being handed to a second worker.
	// Margins wide enough that a stalled round trip is not mistaken for a
	// rescue: over a real container under -race, a 100ms hiccup is
	// unremarkable, and a tighter budget would fail by reporting a
	// production bug that did not happen.
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.LeaseDuration = 2 * time.Second
		cfg.HeartbeatInterval = 200 * time.Millisecond
		cfg.RescueInterval = 100 * time.Millisecond
	})

	row, err := c.Insert(ctx, e2eArgs{N: 1}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	stop := runLoop(t, ctx, c)
	<-started
	// Several lease durations of renewals, all of which must land.
	time.Sleep(5 * time.Second)

	if state, _, _ := readJob(t, pool, row.ID); state != "running" {
		t.Errorf("state = %q after more than two lease durations, want running", state)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1 — the job was rescued while still running", got)
	}

	close(release)
	waitFor(t, func() bool { return countInState(t, pool, "completed") == 1 }, "long job to complete")
	stop()

	state, attempt, recorded := readJob(t, pool, row.ID)
	if state != "completed" {
		t.Errorf("state = %q, want completed", state)
	}
	if attempt != 1 {
		t.Errorf("attempt = %d, want 1 — a renewed lease means no second claim", attempt)
	}
	if len(recorded) != 0 {
		t.Errorf("errors = %+v, want none on a job that was never abandoned", recorded)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times in total, want 1", got)
	}
}

func TestEndToEndHandlerSentinelsInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{
		runs: make(map[int64]int),
		fail: func(args e2eArgs) error {
			if args.N == 0 {
				return fmt.Errorf("unusable payload: %w", Cancel(errors.New("missing recipient")))
			}
			return fmt.Errorf("not ready: %w", Snooze(time.Hour))
		},
	}
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
	})

	cancelled, err := c.Insert(ctx, e2eArgs{N: 0}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	snoozed, err := c.Insert(ctx, e2eArgs{N: 1}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	stop := runLoop(t, ctx, c)
	waitFor(t, func() bool {
		return countInState(t, pool, "cancelled") == 1 && countInState(t, pool, "scheduled") == 1
	}, "both sentinels to be honored")
	stop()

	state, _, recorded := readJob(t, pool, cancelled.ID)
	if state != "cancelled" {
		t.Errorf("cancelled job state = %q, want cancelled", state)
	}
	if len(recorded) != 1 {
		t.Errorf("cancelled job errors = %+v, want exactly one entry with the reason", recorded)
	}

	state, attempt, recorded := readJob(t, pool, snoozed.ID)
	if state != "scheduled" {
		t.Errorf("snoozed job state = %q, want scheduled", state)
	}
	// The claim spent an attempt and the snooze gave it back.
	if attempt != 0 {
		t.Errorf("snoozed job attempt = %d, want 0", attempt)
	}
	if len(recorded) != 0 {
		t.Errorf("snoozed job errors = %+v, want none — a snooze is not a failure", recorded)
	}
}

// A pool claims through the same SELECT ... FOR UPDATE SKIP LOCKED path
// a single worker used, so the exactly-once property has to survive
// several of this process's own workers racing each other for rows —
// not just several processes.
func TestEndToEndPoolExecutesEachJobExactlyOnce(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	worker := &e2eWorker{runs: make(map[int64]int)}
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.Concurrency = 8
	})

	const queued = 60
	ids := make([]int64, 0, queued)
	for i := 0; i < queued; i++ {
		row, err := c.Insert(ctx, e2eArgs{N: i}, nil)
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids = append(ids, row.ID)
	}

	stop := runLoop(t, ctx, c)
	waitForState(t, pool, "completed", queued)
	stop()

	worker.mu.Lock()
	defer worker.mu.Unlock()
	for _, id := range ids {
		if worker.runs[id] != 1 {
			t.Errorf("job %d ran %d times, want exactly 1", id, worker.runs[id])
		}
	}
}

// A clean shutdown must leave nothing behind: no row still claimed, and
// therefore nothing waiting out a lease before another worker can touch
// it.
func TestEndToEndCleanShutdownLeavesNothingRunning(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	worker := &e2eWorker{runs: make(map[int64]int)}
	c := newE2EClientWith(t, pool, worker, func(cfg *Config) {
		cfg.Concurrency = 4
	})

	const queued = 20
	for i := 0; i < queued; i++ {
		if _, err := c.Insert(ctx, e2eArgs{N: i}, nil); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	stop := runLoop(t, ctx, c)
	waitForState(t, pool, "completed", queued)
	stop()

	if running := countInState(t, pool, "running"); running != 0 {
		t.Errorf("running rows after a clean shutdown = %d, want 0", running)
	}
}

// The shutdown promise that matters under a deploy: work this process
// could not finish is claimable by another process straight away,
// instead of sitting claimed until its lease lapses.
func TestEndToEndUnfinishedJobsAreClaimableByAnotherClient(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	entered := make(chan int64, 2)
	release := make(chan struct{})
	stubborn := &e2eWorker{
		runs:  make(map[int64]int),
		enter: func(id int64) { entered <- id },
		fail: func(e2eArgs) error {
			// Ignores cancellation entirely — the case a bounded shutdown
			// exists for.
			<-release
			return nil
		},
	}

	first := newE2EClientWith(t, pool, stubborn, func(cfg *Config) {
		cfg.Concurrency = 2
		// Long enough that lease expiry cannot be what rescues these jobs:
		// if they become claimable, the shutdown requeue is why.
		cfg.LeaseDuration = 10 * time.Minute
		cfg.RescueInterval = 10 * time.Minute
	})

	const queued = 2
	ids := make([]int64, 0, queued)
	for i := 0; i < queued; i++ {
		row, err := first.Insert(ctx, e2eArgs{N: i}, nil)
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids = append(ids, row.ID)
	}

	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	if err := first.Start(loopCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := 0; i < queued; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("pool did not pick up the jobs")
		}
	}

	budget, cancelBudget := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelBudget()
	if err := first.Stop(budget); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}

	for _, id := range ids {
		state, attempt, _ := readJob(t, pool, id)
		if state != "retryable" {
			t.Errorf("job %d state = %q after an incomplete shutdown, want retryable", id, state)
		}
		if attempt != 1 {
			t.Errorf("job %d attempt = %d, want it left at 1 by the requeue", id, attempt)
		}
	}

	// A second client, standing in for the replacement process a deploy
	// brings up, finishes the work without waiting out any lease.
	second := newE2EClientWith(t, pool, &e2eWorker{runs: make(map[int64]int)}, func(cfg *Config) {
		cfg.Concurrency = 2
	})
	stopSecond := runLoop(t, ctx, second)
	waitForState(t, pool, "completed", queued)
	stopSecond()

	close(release)
}
