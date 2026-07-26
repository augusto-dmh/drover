package drover

import (
	"context"
	"errors"
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

// newPoolClient builds a fast-polling client over drv with the given
// pool size, discarding its logs.
func newPoolClient(drv driver.Driver, ws *Workers, concurrency int, tune func(*Config)) *Client {
	cfg := Config{
		Workers:      ws,
		Logger:       slog.New(slog.NewTextHandler(&syncWriter{}, nil)),
		PollInterval: 3 * time.Millisecond,
		Concurrency:  concurrency,
	}
	if tune != nil {
		tune(&cfg)
	}
	return newClient(drv, cfg)
}

// insertN enqueues n jobs and returns their ids.
func insertN(t *testing.T, c *Client, n int) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		row, err := c.Insert(context.Background(), greetArgs{Name: "ada"})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids = append(ids, row.ID)
	}
	return ids
}

func countMemRows(t *testing.T, mem *memdriver.Driver, ids []int64, state string) int {
	t.Helper()
	n := 0
	for _, id := range ids {
		row, ok := mem.Row(id)
		if ok && row.State == state {
			n++
		}
	}
	return n
}

// blockingWorker enters, announces itself, and waits to be released.
func blockingWorker(entered chan<- int64, release <-chan struct{}) *funcWorker {
	return &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		entered <- job.ID
		<-release
		return nil
	}}
}

// TestPoolRunsJobsConcurrently is the cycle's headline claim. With a
// serial executor only one handler could ever be inside its body at
// once, so the test cannot pass without a real pool.
func TestPoolRunsJobsConcurrently(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const concurrency = 4
	mem := memdriver.New()
	entered := make(chan int64, concurrency)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, blockingWorker(entered, release))

	c := newPoolClient(mem, ws, concurrency, nil)
	insertN(t, c, concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := 0; i < concurrency; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d handlers were running at once", i, concurrency)
		}
	}

	close(release)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
}

// A claimed row that no worker is running is invisible work: it is
// running and leased in the database with nothing executing it. The pool
// must never create one, which means never claiming more than it can
// immediately start (AD-022).
func TestPoolClaimsNoMoreJobsThanItCanRun(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const concurrency = 3
	const queued = 8
	mem := memdriver.New()
	entered := make(chan int64, queued)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, blockingWorker(entered, release))

	c := newPoolClient(mem, ws, concurrency, nil)
	ids := insertN(t, c, queued)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := 0; i < concurrency; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("pool did not fill its workers")
		}
	}

	// Every worker is now blocked. Give the fetch loop ample opportunity
	// to over-claim, then prove it did not.
	time.Sleep(50 * time.Millisecond)
	if running := countMemRows(t, mem, ids, "running"); running != concurrency {
		t.Errorf("running rows = %d, want %d — the pool claimed jobs it had no worker for", running, concurrency)
	}

	close(release)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
}

// Concurrency must not buy throughput with duplicates.
func TestPoolRunsEachJobExactlyOnce(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const queued = 24
	mem := memdriver.New()
	var mu sync.Mutex
	runs := make(map[int64]int)
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		mu.Lock()
		runs[job.ID]++
		mu.Unlock()
		return nil
	}})

	c := newPoolClient(mem, ws, 6, nil)
	ids := insertN(t, c, queued)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, func() bool {
		return countMemRows(t, mem, ids, "completed") == queued
	}, "every job to complete")
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if runs[id] != 1 {
			t.Errorf("job %d ran %d times, want exactly 1", id, runs[id])
		}
	}
}

// One bad job must not take the pool with it: neither a panicking
// handler nor one that never returns may stop the other workers.
func TestOneMisbehavingJobDoesNotStallThePool(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	release := make(chan struct{})
	completed := make(chan int64, 8)
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		switch job.Args.Name {
		case "panic":
			panic("handler exploded")
		case "block":
			<-release
			return nil
		default:
			completed <- job.ID
			return nil
		}
	}})

	c := newPoolClient(mem, ws, 3, nil)
	for _, name := range []string{"panic", "block"} {
		if _, err := c.Insert(context.Background(), greetArgs{Name: name}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	healthy := make([]int64, 0, 4)
	for i := 0; i < 4; i++ {
		row, err := c.Insert(context.Background(), greetArgs{Name: "ada"})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		healthy = append(healthy, row.ID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := 0; i < len(healthy); i++ {
		select {
		case <-completed:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d healthy jobs ran alongside a panic and a stuck handler", i, len(healthy))
		}
	}

	close(release)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
}

func TestStartRefusesToRunTwice(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	c := newPoolClient(memdriver.New(), NewWorkers(), 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("second Start = %v, want ErrAlreadyStarted", err)
	}

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
}

func TestStopWithoutStartIsReportedNotIgnored(t *testing.T) {
	t.Parallel()

	c := newPoolClient(memdriver.New(), NewWorkers(), 2, nil)
	if err := c.Stop(context.Background()); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Stop before Start = %v, want ErrNotStarted", err)
	}
}

func TestStopIsSafeToCallTwice(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	c := newPoolClient(memdriver.New(), NewWorkers(), 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	first := c.Stop(context.Background())
	if first != nil {
		t.Fatalf("first Stop = %v, want nil", first)
	}

	done := make(chan error, 1)
	go func() { done <- c.Stop(context.Background()) }()
	select {
	case second := <-done:
		if !errors.Is(second, first) {
			t.Errorf("second Stop = %v, want the first call's verdict %v", second, first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second Stop blocked; it should report the finished shutdown immediately")
	}
}

// Shutdown must stop taking on work before it starts waiting for work to
// finish, or it would be racing itself.
func TestStopClaimsNothingFurther(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	counting := &countingDriver{Driver: memdriver.New()}
	c := newPoolClient(counting, NewWorkers(), 2, func(cfg *Config) {
		cfg.PollInterval = 5 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, func() bool { return counting.fetches.Load() > 0 }, "the pool to start claiming")

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
	settled := counting.fetches.Load()

	time.Sleep(50 * time.Millisecond)
	if after := counting.fetches.Load(); after != settled {
		t.Errorf("fetches went from %d to %d after Stop returned — the pool is still claiming", settled, after)
	}
}

// Cancelling the context Start was given has to complete the shutdown by
// itself. A caller who wires the pool to a cancellable context and never
// calls Stop is the common case, and for them cancellation is the whole
// lifecycle: if it only stopped the fetch loop, the workers and the
// heartbeat would outlive it.
func TestCancellingStartsContextShutsThePoolDownOnItsOwn(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	ran := make(chan int64, 1)
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		ran <- job.ID
		return nil
	}})

	c := newPoolClient(mem, ws, 3, nil)
	ids := insertN(t, c, 1)

	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("job never ran")
	}
	waitFor(t, func() bool { return countMemRows(t, mem, ids, "completed") == 1 }, "the job to be finalized")

	// Nothing calls Stop from here on.
	cancel()
	select {
	case <-c.runner.done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the context passed to Start did not shut the pool down")
	}
}

// expiredCountDriver counts sweeps for abandoned jobs.
type expiredCountDriver struct {
	*memdriver.Driver
	sweeps atomic.Int64
}

func (d *expiredCountDriver) FetchExpired(ctx context.Context, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	d.sweeps.Add(1)
	return d.Driver.FetchExpired(ctx, leaseFor, limit)
}

// The rescuer claims too — FetchExpired re-claims abandoned rows — so a
// shutdown that stopped only the fetch loop would still be taking on
// work while it drained.
func TestStopStopsTheRescuerClaimingToo(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	sweeping := &expiredCountDriver{Driver: memdriver.New()}
	c := newPoolClient(sweeping, NewWorkers(), 2, func(cfg *Config) {
		cfg.LeaseDuration = 30 * time.Millisecond
		cfg.HeartbeatInterval = 10 * time.Millisecond
		cfg.RescueInterval = 5 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, func() bool { return sweeping.sweeps.Load() > 0 }, "the rescuer to start sweeping")

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
	settled := sweeping.sweeps.Load()

	time.Sleep(50 * time.Millisecond)
	if after := sweeping.sweeps.Load(); after != settled {
		t.Errorf("rescue sweeps went from %d to %d after Stop returned — the rescuer is still claiming", settled, after)
	}
}

// stubbornWorker ignores cancellation entirely — the case a bounded
// shutdown exists for.
func stubbornWorker(entered chan<- int64, release <-chan struct{}) *funcWorker {
	return &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		entered <- job.ID
		<-release
		return nil
	}}
}

// waitForInflightToClear lets the abandoned handlers finish so goleak
// sees a settled process. Stop deliberately does not wait for them.
func waitForInflightToClear(t *testing.T, c *Client) {
	t.Helper()
	waitFor(t, func() bool { return len(c.inflight.snapshot()) == 0 }, "abandoned handlers to finish")
}

// A shutdown that runs out of time must say so and must not strand the
// work: the jobs it could not finish go back to the queue, where another
// worker can pick them up immediately instead of waiting out a lease.
func TestStopReportsAndRequeuesTheJobsItCouldNotFinish(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const concurrency = 2
	mem := memdriver.New()
	entered := make(chan int64, concurrency)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, stubbornWorker(entered, release))

	c := newPoolClient(mem, ws, concurrency, nil)
	ids := insertN(t, c, concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := 0; i < concurrency; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("pool did not fill its workers")
		}
	}

	budget, cancelBudget := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBudget()

	start := time.Now()
	err := c.Stop(budget)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}
	if !strings.Contains(err.Error(), "2 job(s)") {
		t.Errorf("Stop error %q does not name the 2 unfinished jobs", err)
	}
	if elapsed > time.Second {
		t.Errorf("Stop took %v, well past its 50ms budget — a handler that ignores cancellation must not hold shutdown open", elapsed)
	}

	// Returned to the queue, not left running: another worker can take
	// them now rather than after the lease lapses.
	for _, id := range ids {
		row, ok := mem.Row(id)
		if !ok {
			t.Fatalf("job %d not found", id)
		}
		if row.State != "retryable" {
			t.Errorf("job %d state = %q, want retryable", id, row.State)
		}
		if !row.ScheduledAt.After(time.Now()) {
			continue // claimable now, which is what we want
		}
		t.Errorf("job %d is scheduled at %v, in the future — an interrupted job should be claimable immediately", id, row.ScheduledAt)
	}

	close(release)
	waitForInflightToClear(t, c)
}

// The requeue must not give the attempt back. attempt is the fence, so
// decrementing it would let the handler this shutdown abandoned present
// the number the next claim hands out — and its stale write would be
// accepted over the new holder's.
func TestShutdownRequeueDoesNotGiveBackTheAttempt(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	entered := make(chan int64, 1)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, stubbornWorker(entered, release))

	c := newPoolClient(mem, ws, 1, nil)
	ids := insertN(t, c, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	claimed, ok := mem.Row(ids[0])
	if !ok {
		t.Fatalf("job %d not found", ids[0])
	}
	if claimed.Attempt != 1 {
		t.Fatalf("attempt = %d after the first claim, want 1", claimed.Attempt)
	}

	budget, cancelBudget := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBudget()
	if err := c.Stop(budget); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}

	requeued, _ := mem.Row(ids[0])
	if requeued.Attempt != claimed.Attempt {
		t.Errorf("attempt = %d after the requeue, want it unchanged at %d — giving the attempt back breaks the fence",
			requeued.Attempt, claimed.Attempt)
	}

	// The reason is on the record, so an operator can see why it ran twice.
	recorded := decodeAttemptErrors(t, mem, ids[0])
	if len(recorded) != 1 {
		t.Fatalf("recorded %d attempt errors, want 1", len(recorded))
	}
	if !strings.Contains(recorded[0].Error, "shut down") {
		t.Errorf("recorded reason = %q, want it to name the shutdown", recorded[0].Error)
	}

	close(release)
	waitForInflightToClear(t, c)
}

// Escalation has to actually reach the handlers. A shutdown that only
// waited and then gave up would leave a cooperative handler — one
// watching its context, which is the behaviour drover asks for — blocked
// on a cancellation that never came.
func TestExhaustingTheBudgetCancelsTheRunningHandlers(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	entered := make(chan int64, 1)
	cancelled := make(chan struct{})
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(ctx context.Context, job *Job[greetArgs]) error {
		entered <- job.ID
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}})

	c := newPoolClient(mem, ws, 1, nil)
	insertN(t, c, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	budget, cancelBudget := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBudget()
	if err := c.Stop(budget); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown never cancelled the running handler's context")
	}
	waitForInflightToClear(t, c)
}

// orderedRequeueDriver reports whether the handlers had already been
// cancelled by the time their jobs were returned to the queue. It waits
// rather than sampling, so the answer does not depend on how quickly a
// woken handler is scheduled.
type orderedRequeueDriver struct {
	*memdriver.Driver
	cancelled   <-chan struct{}
	cancelFirst atomic.Bool
	seen        atomic.Bool
}

func (d *orderedRequeueDriver) MarkRetryable(ctx context.Context, lease driver.Lease, retryAt time.Time, errDetail []byte) error {
	if !d.seen.Swap(true) {
		select {
		case <-d.cancelled:
			d.cancelFirst.Store(true)
		case <-time.After(500 * time.Millisecond):
		}
	}
	return d.Driver.MarkRetryable(ctx, lease, retryAt, errDetail)
}

// ADR-0003 fixes the order: cancel the per-job contexts, then requeue.
// Doing it the other way round returns a job to the queue while the
// handler that owns it is still running uncancelled — widening the
// window in which two workers are acting on the same job.
func TestEscalationCancelsHandlersBeforeReturningTheirJobs(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	entered := make(chan int64, 1)
	cancelled := make(chan struct{})
	ordered := &orderedRequeueDriver{Driver: mem, cancelled: cancelled}

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(ctx context.Context, job *Job[greetArgs]) error {
		entered <- job.ID
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}})

	c := newPoolClient(ordered, ws, 1, nil)
	insertN(t, c, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	budget, cancelBudget := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBudget()
	if err := c.Stop(budget); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}

	if !ordered.cancelFirst.Load() {
		t.Error("a job was returned to the queue before its handler was cancelled")
	}
	waitForInflightToClear(t, c)
}

// overshootDriver hands back more rows than were asked for, which is how
// a test reproduces the fetch loop holding claimed jobs it has not
// managed to give to a worker.
type overshootDriver struct {
	*memdriver.Driver
	extra int
}

func (d *overshootDriver) FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	return d.Driver.FetchAvailable(ctx, queue, leaseFor, limit+d.extra)
}

// A job claimed but never started is the worst thing to strand: it is
// running and leased with nothing executing it, and nobody can touch it
// until the lease lapses. The pool never keeps one — a driver that hands
// back more than was asked for gets the surplus straight back.
func TestSurplusClaimedJobsAreReturnedImmediately(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	overshoot := &overshootDriver{Driver: mem, extra: 2}
	entered := make(chan int64, 1)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, stubbornWorker(entered, release))

	c := newPoolClient(overshoot, ws, 1, nil)
	ids := insertN(t, c, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var started int64
	select {
	case started = <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no job started")
	}

	// The driver claimed all three; only one has a worker. Give the pool
	// every chance to sit on the other two, then prove it did not.
	time.Sleep(50 * time.Millisecond)
	if running := countMemRows(t, mem, ids, "running"); running != 1 {
		t.Errorf("running rows = %d, want 1 — the pool is holding jobs no worker can run", running)
	}
	for _, id := range ids {
		if id == started {
			continue
		}
		row, _ := mem.Row(id)
		if row.State != "retryable" {
			t.Errorf("job %d was claimed with no worker for it and is %q, want retryable so another worker can take it", id, row.State)
		}
	}

	budget, cancelBudget := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBudget()
	if err := c.Stop(budget); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}

	close(release)
	waitForInflightToClear(t, c)
}

// A caller whose shutdown budget is already spent still deserves the
// ordered shutdown: returning early would leave every in-flight row
// running with nobody coming back for it.
func TestStopWithAnExpiredBudgetStillRequeues(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	entered := make(chan int64, 1)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, stubbornWorker(entered, release))

	c := newPoolClient(mem, ws, 1, nil)
	ids := insertN(t, c, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()

	if err := c.Stop(spent); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}
	row, _ := mem.Row(ids[0])
	if row.State != "retryable" {
		t.Errorf("job state = %q after a Stop with no budget left, want retryable", row.State)
	}

	close(release)
	waitForInflightToClear(t, c)
}

// leaseCountDriver records the largest batch of leases the heartbeat
// ever renewed at once.
type leaseCountDriver struct {
	*memdriver.Driver
	mu       sync.Mutex
	maxBatch int
}

func (d *leaseCountDriver) ExtendLeases(ctx context.Context, leases []driver.Lease, leaseFor time.Duration) error {
	d.mu.Lock()
	if len(leases) > d.maxBatch {
		d.maxBatch = len(leases)
	}
	d.mu.Unlock()
	return d.Driver.ExtendLeases(ctx, leases, leaseFor)
}

func (d *leaseCountDriver) largestBatch() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.maxBatch
}

// The heartbeat has to renew every worker's lease, not just the most
// recent claim: a job whose lease lapses while it runs is handed to a
// second worker, which turns the safety net into a duplicator.
func TestHeartbeatRenewsEveryWorkersLease(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const concurrency = 4
	mem := memdriver.New()
	counting := &leaseCountDriver{Driver: mem}
	entered := make(chan int64, concurrency)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, blockingWorker(entered, release))

	c := newPoolClient(counting, ws, concurrency, func(cfg *Config) {
		cfg.LeaseDuration = 60 * time.Millisecond
		cfg.HeartbeatInterval = 10 * time.Millisecond
	})
	insertN(t, c, concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := 0; i < concurrency; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("pool did not fill its workers")
		}
	}

	waitFor(t, func() bool { return counting.largestBatch() == concurrency },
		"the heartbeat to renew every worker's lease in one call")

	close(release)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
}
