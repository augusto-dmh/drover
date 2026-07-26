package drover

import (
	"context"
	"errors"
	"log/slog"
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

func countInState(t *testing.T, mem *memdriver.Driver, ids []int64, state string) int {
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
	if running := countInState(t, mem, ids, "running"); running != concurrency {
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
		return countInState(t, mem, ids, "completed") == queued
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
		if second != first {
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
	waitFor(t, func() bool { return countInState(t, mem, ids, "completed") == 1 }, "the job to be finalized")

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
