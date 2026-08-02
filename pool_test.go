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
// It never consults its context, which is deliberate: it stands in both
// for a job that is simply slow and for one that ignores cancellation,
// the case a bounded shutdown exists for.
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

// The documented lifecycle runs once. Only the already-running half was
// pinned; nilling c.runner in Stop would quietly make the client
// restartable — reusing an inflightSet the design never reasoned about —
// with the whole suite still green.
func TestStartRefusesToRunAfterStop(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	c := newPoolClient(memdriver.New(), NewWorkers(), 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}

	if err := c.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("Start after Stop = %v, want ErrAlreadyStarted — a client's lifecycle runs once", err)
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
	Register(ws, blockingWorker(entered, release))

	logs := &syncWriter{}
	c := newPoolClient(mem, ws, concurrency, func(cfg *Config) { cfg.Logger = newTestLogger(logs) })
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

	// The abandoned handlers have now finished and tried to record
	// outcomes for jobs this shutdown already handed back. That is the
	// designed consequence of the escalation, not a write failure, and it
	// must not be logged as one — an error per job on every overrunning
	// deploy is how the log stops being read.
	if out := logs.String(); strings.Contains(out, `msg="drover: finalize job"`) {
		t.Errorf("an escalated shutdown reported its own handed-back jobs as failed writes:\n%s", out)
	}
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
	Register(ws, blockingWorker(entered, release))

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

// The fetch loop can be holding rows it claimed but had not yet handed
// to a worker when shutdown began. Nothing is executing those, so they
// are the worst kind of row to strand: running and leased with no worker
// behind them and nobody able to touch them until the lease lapses.
//
// Reaching that state through the running pool takes a race the slot
// accounting exists to make vanishingly rare, so the hand-back is
// exercised directly — the behaviour is what the spec requires, whether
// or not the path is easy to provoke from outside.
func TestClaimsNeverHandedToAWorkerGoBackToTheQueue(t *testing.T) {
	t.Parallel()

	mem := memdriver.New()
	c := newPoolClient(mem, NewWorkers(), 2, nil)
	ids := insertN(t, c, 2)

	claimed, err := mem.FetchAvailable(context.Background(), defaultQueue, time.Minute, 2)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d rows, want 2", len(claimed))
	}

	r := newRunner(context.Background(), c)
	defer r.cancelJobs()
	defer r.cancelBackground()

	// Stand in for the fetch loop: it holds one slot per row it claimed,
	// and each claim is tracked so the heartbeat covers it.
	for _, row := range claimed {
		<-r.slots
		c.inflight.add(driver.Lease{ID: row.ID, Attempt: row.Attempt})
	}

	r.abandon(claimed)

	for _, id := range ids {
		row, ok := mem.Row(id)
		if !ok {
			t.Fatalf("job %d not found", id)
		}
		if row.State != "retryable" {
			t.Errorf("job %d state = %q, want retryable — a claimed job nothing ran was left stranded", id, row.State)
		}
		if row.Attempt != 1 {
			t.Errorf("job %d attempt = %d, want it left at 1 by the hand-back", id, row.Attempt)
		}
	}
	if held := len(c.inflight.snapshot()); held != 0 {
		t.Errorf("in-flight set still tracks %d lease(s) after they were handed back", held)
	}
	if free := len(r.slots); free != 2 {
		t.Errorf("free slots = %d, want 2 — the abandoned rows' capacity was not returned", free)
	}
}

// The previous test proves what abandon does; this one proves the fetch
// loop actually reaches for it. Running the real loop with no workers at
// all is what makes that deterministic: nothing ever receives from the
// hand-off channel, so the loop is parked mid-hand-off holding claimed
// rows — exactly the state shutdown has to unwind — with no race to lose.
func TestTheFetchLoopHandsBackRowsItNeverDispatched(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	c := newPoolClient(mem, NewWorkers(), 2, nil)
	ids := insertN(t, c, 2)

	r := newRunner(context.Background(), c)
	defer r.cancelJobs()
	defer r.cancelBackground()

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		r.fetch()
	}()

	waitFor(t, func() bool { return countMemRows(t, mem, ids, "running") > 0 },
		"the fetch loop to claim a row it cannot hand over")

	close(r.stopFetch)
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("the fetch loop did not return once shutdown began")
	}

	for _, id := range ids {
		row, ok := mem.Row(id)
		if !ok {
			t.Fatalf("job %d not found", id)
		}
		if row.State == "running" {
			t.Errorf("job %d left running — the fetch loop kept a row it never dispatched, so nothing can touch it until the lease lapses", id)
		}
	}
	if held := len(c.inflight.snapshot()); held != 0 {
		t.Errorf("in-flight set still tracks %d lease(s) the fetch loop gave up on", held)
	}
}

// Edge case: a pool of one behaves like the single worker that came
// before it — never more than one job in flight.
func TestAPoolOfOneRunsOneJobAtATime(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	mem := memdriver.New()
	entered := make(chan int64, 4)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, blockingWorker(entered, release))

	c := newPoolClient(mem, ws, 1, nil)
	ids := insertN(t, c, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no job started")
	}

	time.Sleep(50 * time.Millisecond)
	if running := countMemRows(t, mem, ids, "running"); running != 1 {
		t.Errorf("running rows = %d, want 1 — a pool of one claimed more than it can run", running)
	}
	if extra := len(entered); extra != 0 {
		t.Errorf("%d further handlers were entered, want 0 — a pool of one ran jobs concurrently", extra)
	}

	close(release)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
}

// raceLostDriver refuses a hand-back the way the database does when the
// job's own worker got there first.
type raceLostDriver struct {
	*memdriver.Driver
	err error
}

func (d *raceLostDriver) MarkRetryable(context.Context, driver.Lease, time.Time, []byte) error {
	return d.err
}

// Losing the hand-back race is the ordinary case on a shutdown — the
// handler finished while the shutdown was reaching for its row — so it
// must not be logged as a failed write. Inverting that classification
// puts an error in the log on a large share of clean deploys, and
// nothing but a log assertion can tell the two apart.
func TestAHandBackThatLostItsRaceIsNotReportedAsAFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "the job's own worker finished first", err: driver.ErrInvalidTransition},
		{name: "another holder took the job", err: driver.ErrLeaseLost},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mem := memdriver.New()
			logs := &syncWriter{}
			c := newClient(&raceLostDriver{Driver: mem, err: tt.err},
				Config{Workers: NewWorkers(), Logger: newTestLogger(logs), Concurrency: 2})
			insertN(t, c, 1)

			claimed, err := mem.FetchAvailable(context.Background(), defaultQueue, time.Minute, 1)
			if err != nil {
				t.Fatalf("FetchAvailable: %v", err)
			}

			r := newRunner(context.Background(), c)
			defer r.cancelJobs()
			defer r.cancelBackground()
			<-r.slots
			c.inflight.add(driver.Lease{ID: claimed[0].ID, Attempt: claimed[0].Attempt})

			r.abandon(claimed)

			if out := logs.String(); strings.Contains(out, `msg="drover: return job to the queue"`) {
				t.Errorf("a hand-back that lost its race was reported as a failed write:\n%s", out)
			}
		})
	}
}

// failFirstRetryableDriver refuses the first requeue and accepts the
// rest, so a test can prove one row that will not go back does not take
// the others with it.
type failFirstRetryableDriver struct {
	*memdriver.Driver
	refused atomic.Bool
}

func (d *failFirstRetryableDriver) MarkRetryable(ctx context.Context, lease driver.Lease, retryAt time.Time, errDetail []byte) error {
	if !d.refused.Swap(true) {
		return errors.New("connection reset")
	}
	return d.Driver.MarkRetryable(ctx, lease, retryAt, errDetail)
}

// A shutdown is best effort across every job it is handing back. One row
// that will not go back is logged and left for the rescuer; it must not
// abandon the rows behind it in the queue.
func TestOneFailedHandBackDoesNotStopTheRest(t *testing.T) {
	t.Parallel()

	mem := memdriver.New()
	refusing := &failFirstRetryableDriver{Driver: mem}
	c := newPoolClient(refusing, NewWorkers(), 3, nil)
	ids := insertN(t, c, 3)

	claimed, err := mem.FetchAvailable(context.Background(), defaultQueue, time.Minute, 3)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}

	r := newRunner(context.Background(), c)
	defer r.cancelJobs()
	defer r.cancelBackground()
	for _, row := range claimed {
		<-r.slots
		c.inflight.add(driver.Lease{ID: row.ID, Attempt: row.Attempt})
	}

	r.abandon(claimed)

	// The first was refused and stays running for the rescuer. Every one
	// after it must still have gone back.
	stranded, returned := 0, 0
	for _, id := range ids {
		row, _ := mem.Row(id)
		switch row.State {
		case "running":
			stranded++
		case "retryable":
			returned++
		default:
			t.Errorf("job %d state = %q, want running or retryable", id, row.State)
		}
	}
	if stranded != 1 {
		t.Errorf("%d jobs left running, want exactly the 1 the driver refused", stranded)
	}
	if returned != 2 {
		t.Errorf("%d jobs went back to the queue, want 2 — a refused hand-back stopped the ones behind it", returned)
	}
}

// An idle pool spends most of its life asleep between polls. Shutdown
// has to interrupt that sleep rather than wait it out, or every restart
// costs a poll interval — paid on each deploy, on every worker.
func TestStopDoesNotWaitOutThePollInterval(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	counting := &countingDriver{Driver: memdriver.New()}
	c := newPoolClient(counting, NewWorkers(), 2, func(cfg *Config) {
		cfg.PollInterval = 3 * time.Second
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Gated on the observable rather than the clock: the claim is that
	// Stop interrupts a wait already in progress, so the fetch loop has
	// to have reached that wait. A fixed sleep would let a slow runner
	// measure a loop that never got there, passing without exercising
	// anything.
	waitFor(t, func() bool { return counting.fetches.Load() > 0 }, "the pool to reach its idle wait")

	start := time.Now()
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Stop took %v against a 3s poll interval — shutdown waited out an idle tick", elapsed)
	}
}

// The spec's ordering is that claiming ceases before shutdown begins
// waiting, not merely by the time it returns. Sampling twice while a
// blocked handler holds the drain open is what tells those apart.
func TestClaimingStopsBeforeTheDrainBeginsWaiting(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	counting := &countingDriver{Driver: memdriver.New()}
	entered := make(chan int64, 1)
	release := make(chan struct{})
	ws := NewWorkers()
	Register(ws, blockingWorker(entered, release))

	c := newPoolClient(counting, ws, 2, func(cfg *Config) {
		cfg.PollInterval = time.Millisecond
	})
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

	budget, cancelBudget := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancelBudget()
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop(budget) }()

	// Both samples are taken while the drain is still waiting on the
	// blocked handler. At a 1ms poll interval a fetch loop still claiming
	// would add hundreds of calls between them.
	time.Sleep(150 * time.Millisecond)
	duringDrain := counting.fetches.Load()
	time.Sleep(250 * time.Millisecond)
	if later := counting.fetches.Load(); later != duringDrain {
		t.Errorf("fetches went from %d to %d while the drain was waiting — claiming did not stop before shutdown began waiting",
			duringDrain, later)
	}

	if err := <-stopped; !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("Stop = %v, want an error wrapping ErrDrainIncomplete", err)
	}
	close(release)
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
	Register(ws, blockingWorker(entered, release))

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
	Register(ws, blockingWorker(entered, release))

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

// A budget that expires at the very moment the last worker finishes is
// still a clean shutdown: everything recorded its outcome. Reporting it
// as a failure would have a caller that exits non-zero on error failing
// deploys in which nothing went wrong, decided by which of two ready
// channels the runtime happened to pick.
func TestEscalationReportsNothingWhenNothingWasStranded(t *testing.T) {
	t.Parallel()

	c := newPoolClient(memdriver.New(), NewWorkers(), 2, nil)
	r := newRunner(context.Background(), c)
	defer r.cancelJobs()
	defer r.cancelBackground()

	if err := r.escalate(); err != nil {
		t.Errorf("escalate with an empty in-flight set = %v, want nil", err)
	}
}

// Start publishes the runner and launches it; a Stop arriving between
// those two steps must not be able to drain a pool that has no
// goroutines in it and call that a clean shutdown. Driven concurrently
// so the race detector and the WaitGroup's own misuse check both get a
// chance at any window between them.
func TestConcurrentStartAndStopDoNotCorruptTheLifecycle(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	for i := 0; i < 50; i++ {
		c := newPoolClient(memdriver.New(), NewWorkers(), 4, nil)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := c.Start(ctx); err != nil {
				t.Errorf("Start: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			// Either it lands before Start published the runner and reports
			// so, or it drains a pool that is genuinely running. Both are
			// fine; a nil verdict for a pool that never launched is not.
			if err := c.Stop(context.Background()); err != nil && !errors.Is(err, ErrNotStarted) {
				t.Errorf("Stop: %v", err)
			}
		}()
		wg.Wait()

		// Whatever the interleaving, the client must end up stopped rather
		// than half-built: a further Stop must not block or panic.
		if err := c.Stop(context.Background()); err != nil && !errors.Is(err, ErrNotStarted) {
			t.Errorf("final Stop: %v", err)
		}
		cancel()
	}
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
