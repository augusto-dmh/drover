package drover

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

// abandon inserts a job and claims it with a lease that has already
// expired, which is what a worker killed mid-job leaves behind.
func abandon(t *testing.T, mem *memdriver.Driver, kind string) *driver.JobRow {
	t.Helper()
	ctx := context.Background()
	row, err := mem.Insert(ctx, driver.InsertParams{Kind: kind, Queue: "default", Args: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// A negative lease is born already expired — exactly the row a worker
	// killed mid-job leaves behind.
	claimed, err := mem.FetchAvailable(ctx, "default", -time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(claimed))
	}
	return row
}

// rescueClient builds a client wired to drv for direct sweep calls, with
// no loop running.
func rescueClient(drv driver.Driver, tune func(*Config)) *Client {
	cfg := Config{
		Logger:        discardLogger(),
		LeaseDuration: time.Minute,
		RetryPolicy:   immediatePolicy{},
	}
	if tune != nil {
		tune(&cfg)
	}
	return newClient(drv, cfg)
}

func TestRescueReturnsAbandonedJobToTheQueue(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	row := abandon(t, mem, "greet")
	c := rescueClient(mem, nil)

	rescued, err := c.rescueOnce(context.Background())
	if err != nil {
		t.Fatalf("rescueOnce: %v", err)
	}
	if rescued != 1 {
		t.Fatalf("rescued %d jobs, want 1", rescued)
	}

	stored, _ := mem.Row(row.ID)
	if stored.State != "retryable" {
		t.Errorf("State = %q, want retryable", stored.State)
	}
	if stored.FinalizedAt != nil {
		t.Error("FinalizedAt set on a rescued job that still has attempts left")
	}
	// The claim that stranded the row already spent an attempt; charging
	// it again would halve the ceiling of every crashed job (AD-012).
	if stored.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1 — rescue must not spend another attempt", stored.Attempt)
	}

	recorded := decodeAttemptErrors(t, mem, row.ID)
	if len(recorded) != 1 {
		t.Fatalf("errors = %+v, want exactly one entry", recorded)
	}
	if !strings.Contains(recorded[0].Error, "lease expired") {
		t.Errorf("errors[0].Error = %q, want it to name the expired lease", recorded[0].Error)
	}
}

func TestRescueSchedulesFromTheConfiguredRetryPolicy(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	row := abandon(t, mem, "greet")
	// A distinctive answer, deliberately not "now": the other rescue tests
	// use an immediate policy whose answer is indistinguishable from a
	// rescue that skipped the policy altogether and made the job claimable
	// straight away. That regression would put a job whose worker keeps
	// dying into a fetch-speed spin with no backoff at all.
	want := time.Now().Add(72 * time.Hour).Round(0)
	c := rescueClient(mem, func(cfg *Config) {
		cfg.RetryPolicy = atTimePolicy{at: want}
	})

	if _, err := c.rescueOnce(context.Background()); err != nil {
		t.Fatalf("rescueOnce: %v", err)
	}

	stored, _ := mem.Row(row.ID)
	if stored.State != "retryable" {
		t.Fatalf("State = %q, want retryable", stored.State)
	}
	if !stored.ScheduledAt.Equal(want) {
		t.Errorf("ScheduledAt = %v, want the retry policy's answer %v — a rescued job "+
			"must wait out a backoff, not become claimable immediately",
			stored.ScheduledAt, want)
	}
}

func TestStartSupervisesTheRescueSweep(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	// Parked as a crashed worker would leave it, *before* the loop runs:
	// nothing but a supervised sweep can move it on. Without this, every
	// unit-level rescue test drives the sweep directly, and the only test
	// that starts a real loop asserts a job is *not* rescued — which
	// passes trivially if Start never launches the rescuer at all.
	row := abandon(t, mem, "greet")

	var runs atomic.Int32
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		runs.Add(1)
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.RetryPolicy = immediatePolicy{}
		cfg.LeaseDuration = time.Minute
		cfg.RescueInterval = 10 * time.Millisecond
	})

	waitFor(t, h.rowInState(row.ID, "completed"), "abandoned job to be rescued and re-run by the loop")
	h.stop(t)

	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
	stored, _ := mem.Row(row.ID)
	// One attempt spent by the worker that died, one by the run that
	// finished it — the rescue itself spends none.
	if stored.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", stored.Attempt)
	}
}

func TestRescueKillsAbandonedJobAtItsAttemptCeiling(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	// Claimed once, so attempt is 1; a ceiling of 1 means this job has
	// nothing left to spend.
	row := abandon(t, mem, "greet")
	c := rescueClient(&cappedDriver{Driver: mem, max: 1}, nil)

	if _, err := c.rescueOnce(context.Background()); err != nil {
		t.Fatalf("rescueOnce: %v", err)
	}

	stored, _ := mem.Row(row.ID)
	if stored.State != "dead" {
		t.Errorf("State = %q, want dead", stored.State)
	}
	if stored.FinalizedAt == nil {
		t.Error("FinalizedAt not set on a dead job")
	}
	if recorded := decodeAttemptErrors(t, mem, row.ID); len(recorded) != 1 {
		t.Errorf("errors = %+v, want exactly one entry", recorded)
	}
}

func TestRescueLeavesAJobWhoseLeaseIsStillGoodAlone(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	ctx := context.Background()
	row, err := mem.Insert(ctx, driver.InsertParams{Kind: "greet", Queue: "default", Args: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, err := mem.FetchAvailable(ctx, "default", time.Hour, 1); err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	c := rescueClient(mem, nil)

	rescued, err := c.rescueOnce(ctx)
	if err != nil {
		t.Fatalf("rescueOnce: %v", err)
	}
	if rescued != 0 {
		t.Errorf("rescued %d jobs, want 0 — a live lease is not an abandoned job", rescued)
	}

	stored, _ := mem.Row(row.ID)
	if stored.State != "running" {
		t.Errorf("State = %q, want running", stored.State)
	}
	if recorded := decodeAttemptErrors(t, mem, row.ID); len(recorded) != 0 {
		t.Errorf("errors = %+v, want none", recorded)
	}
}

func TestRescueWritesNothingWhenNothingIsAbandoned(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	counter := &expiryCounter{Driver: mem}
	c := rescueClient(counter, nil)

	rescued, err := c.rescueOnce(context.Background())
	if err != nil {
		t.Fatalf("rescueOnce: %v", err)
	}
	if rescued != 0 {
		t.Errorf("rescued %d jobs, want 0", rescued)
	}
	if got := counter.marks.Load(); got != 0 {
		t.Errorf("performed %d state changes on an empty sweep, want 0", got)
	}
}

func TestRescueDispositionsEachJobExactlyOnceUnderConcurrency(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	const jobs = 25
	ids := make([]int64, 0, jobs)
	for range jobs {
		ids = append(ids, abandon(t, mem, "greet").ID)
	}

	// Two clients sweeping at once stands in for two worker processes
	// both noticing the same dead node.
	a, b := rescueClient(mem, nil), rescueClient(mem, nil)
	var wg sync.WaitGroup
	for _, c := range []*Client{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.rescueOnce(context.Background()); err != nil {
				t.Errorf("rescueOnce: %v", err)
			}
		}()
	}
	wg.Wait()

	for _, id := range ids {
		stored, _ := mem.Row(id)
		if stored.State != "retryable" {
			t.Errorf("job %d is %q, want retryable", id, stored.State)
		}
		if stored.Attempt != 1 {
			t.Errorf("job %d has attempt %d, want 1", id, stored.Attempt)
		}
		// Two sweeps both dispositioning the same job would show up as a
		// second recorded failure.
		if recorded := decodeAttemptErrors(t, mem, id); len(recorded) != 1 {
			t.Errorf("job %d recorded %d errors, want exactly 1", id, len(recorded))
		}
	}
}

func TestRescueDrainsABacklogLargerThanOneBatch(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	total := rescueBatch + 7
	for range total {
		abandon(t, mem, "greet")
	}
	c := rescueClient(mem, nil)

	rescued, err := c.rescueOnce(context.Background())
	if err != nil {
		t.Fatalf("rescueOnce: %v", err)
	}
	if rescued != total {
		t.Errorf("rescued %d jobs in one sweep, want all %d — a backlog must not "+
			"drain one batch per tick", rescued, total)
	}
}

func TestRescueLoopKeepsSweepingAfterAFailedSweep(t *testing.T) {
	logs := &syncWriter{}
	synctest.Test(t, func(t *testing.T) {
		broken := &brokenExpiryDriver{Driver: memdriver.New()}
		c := newClient(broken, Config{
			Logger:         newTestLogger(logs),
			LeaseDuration:  30 * time.Second,
			RescueInterval: 10 * time.Second,
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.rescueLoop(ctx)
		}()

		time.Sleep(35 * time.Second)
		synctest.Wait()
		cancel()
		<-done

		if got := broken.calls.Load(); got != 3 {
			t.Fatalf("swept %d times across 35s at a 10s interval, want exactly 3 — "+
				"a failed sweep must not stop the loop", got)
		}
	})
	if !strings.Contains(logs.String(), `msg="drover: rescue expired jobs"`) {
		t.Errorf("logs missing the failed-sweep record\nlogs:\n%s", logs.String())
	}
}

func TestStartDoesNotRescueAJobThatIsStillRunning(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	var runs atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{})
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		if runs.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	}})
	// The job outlives its lease several times over while the sweeper is
	// actively looking for abandoned work. Only the heartbeat stands
	// between it and being handed to a second worker.
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.LeaseDuration = 60 * time.Millisecond
		cfg.HeartbeatInterval = 15 * time.Millisecond
		cfg.RescueInterval = 20 * time.Millisecond
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "slow"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	<-started
	time.Sleep(300 * time.Millisecond)

	stored, _ := mem.Row(row.ID)
	if stored.State != "running" {
		t.Errorf("State = %q after 5 lease durations, want running — the heartbeat "+
			"should have kept the rescuer off it", stored.State)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1 — the job was rescued while still running", got)
	}

	close(release)
	waitFor(t, h.rowInState(row.ID, "completed"), "long job to complete")
	h.stop(t)

	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times in total, want 1", got)
	}
	if recorded := decodeAttemptErrors(t, mem, row.ID); len(recorded) != 0 {
		t.Errorf("errors = %+v, want none on a job that was never abandoned", recorded)
	}
}

// expiryCounter counts state changes so a sweep can be shown to write
// nothing when it finds nothing.
type expiryCounter struct {
	*memdriver.Driver
	marks atomic.Int32
}

func (d *expiryCounter) MarkRetryable(ctx context.Context, lease driver.Lease, retryAt time.Time, errDetail []byte) error {
	d.marks.Add(1)
	return d.Driver.MarkRetryable(ctx, lease, retryAt, errDetail)
}

func (d *expiryCounter) MarkDead(ctx context.Context, lease driver.Lease, errDetail []byte) error {
	d.marks.Add(1)
	return d.Driver.MarkDead(ctx, lease, errDetail)
}

// brokenExpiryDriver stands in for a database that is unreachable when
// the sweep runs.
type brokenExpiryDriver struct {
	*memdriver.Driver
	calls atomic.Int32
}

func (d *brokenExpiryDriver) FetchExpired(context.Context, time.Duration, int) ([]*driver.JobRow, error) {
	d.calls.Add(1)
	return nil, errors.New("connection refused")
}
