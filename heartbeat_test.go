package drover

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

// leaseRecorder records every lease extension so a test can assert both
// when renewals happened and what they asked for.
type leaseRecorder struct {
	*memdriver.Driver
	mu    sync.Mutex
	calls []leaseCall
	fail  error
}

type leaseCall struct {
	at       time.Time
	leases   []driver.Lease
	leaseFor time.Duration
}

func (d *leaseRecorder) ExtendLeases(ctx context.Context, leases []driver.Lease, leaseFor time.Duration) error {
	d.mu.Lock()
	d.calls = append(d.calls, leaseCall{at: time.Now(), leases: slices.Clone(leases), leaseFor: leaseFor})
	failure := d.fail
	d.mu.Unlock()
	if failure != nil {
		return failure
	}
	return d.Driver.ExtendLeases(ctx, leases, leaseFor)
}

func (d *leaseRecorder) snapshot() []leaseCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

// runHeartbeat starts c's heartbeat and returns a function that stops it
// and waits for the goroutine to exit.
func runHeartbeat(c *Client) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.heartbeat(stop)
	}()
	return func() {
		close(stop)
		<-done
	}
}

func TestHeartbeatExtendsLeaseOnEveryInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &leaseRecorder{Driver: memdriver.New()}
		c := newClient(rec, Config{
			Logger:            discardLogger(),
			LeaseDuration:     30 * time.Second,
			HeartbeatInterval: 10 * time.Second,
		})
		c.inflight.add(driver.Lease{ID: 7, Attempt: 1})

		start := time.Now()
		stop := runHeartbeat(c)
		time.Sleep(35 * time.Second)
		synctest.Wait()
		stop()

		calls := rec.snapshot()
		// Exactly three: a fake clock makes the cadence exact, so this
		// pins both that renewals happen and that they are not happening
		// in a tight loop.
		if len(calls) != 3 {
			t.Fatalf("extended %d times across 35s at a 10s interval, want exactly 3", len(calls))
		}
		for i, call := range calls {
			wantAt := start.Add(time.Duration(i+1) * 10 * time.Second)
			if !call.at.Equal(wantAt) {
				t.Errorf("extension %d happened at %v, want %v", i, call.at, wantAt)
			}
			// Every renewal buys a full lease, measured by the database
			// from the moment it lands.
			if call.leaseFor != 30*time.Second {
				t.Errorf("extension %d renewed for %v, want 30s", i, call.leaseFor)
			}
			if !slices.Equal(call.leases, []driver.Lease{{ID: 7, Attempt: 1}}) {
				t.Errorf("extension %d covered %v, want the held lease on job 7", i, call.leases)
			}
		}
	})
}

func TestHeartbeatStopsExtendingOnceJobFinalizes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &leaseRecorder{Driver: memdriver.New()}
		c := newClient(rec, Config{
			Logger:            discardLogger(),
			LeaseDuration:     30 * time.Second,
			HeartbeatInterval: 10 * time.Second,
		})
		c.inflight.add(driver.Lease{ID: 7, Attempt: 1})

		stop := runHeartbeat(c)
		time.Sleep(25 * time.Second)
		synctest.Wait()
		c.inflight.remove(driver.Lease{ID: 7, Attempt: 1})
		time.Sleep(40 * time.Second)
		synctest.Wait()
		stop()

		// Two renewals in the first 25s and none afterwards: a heartbeat
		// that kept extending a finished job would show more.
		if got := len(rec.snapshot()); got != 2 {
			t.Fatalf("extended %d times, want 2 — renewal must stop when the job does", got)
		}
	})
}

func TestHeartbeatWritesNothingWhenIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &leaseRecorder{Driver: memdriver.New()}
		c := newClient(rec, Config{
			Logger:            discardLogger(),
			LeaseDuration:     30 * time.Second,
			HeartbeatInterval: 10 * time.Second,
		})

		stop := runHeartbeat(c)
		time.Sleep(55 * time.Second)
		synctest.Wait()
		stop()

		if got := len(rec.snapshot()); got != 0 {
			t.Errorf("extended %d times with nothing running, want 0", got)
		}
	})
}

func TestHeartbeatKeepsBeatingAfterAFailedExtension(t *testing.T) {
	logs := &syncWriter{}
	synctest.Test(t, func(t *testing.T) {
		rec := &leaseRecorder{Driver: memdriver.New(), fail: errors.New("connection refused")}
		c := newClient(rec, Config{
			Logger:            newTestLogger(logs),
			LeaseDuration:     30 * time.Second,
			HeartbeatInterval: 10 * time.Second,
		})
		c.inflight.add(driver.Lease{ID: 7, Attempt: 1})

		stop := runHeartbeat(c)
		time.Sleep(35 * time.Second)
		synctest.Wait()
		stop()

		// A failing renewal is a reason to log and try again next tick,
		// never a reason to give up on a job that is still running.
		if got := len(rec.snapshot()); got != 3 {
			t.Fatalf("extended %d times, want 3 — failures must not stop the heartbeat", got)
		}
	})
	if !strings.Contains(logs.String(), `msg="drover: extend job leases"`) {
		t.Errorf("logs missing the failed-extension record\nlogs:\n%s", logs.String())
	}
}

func TestExtendLeasesDoesNotResurrectAFinalizedJob(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	ctx := context.Background()

	row, err := mem.Insert(ctx, driver.InsertParams{Kind: "greet", Queue: "default", Args: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, err := mem.FetchAvailable(ctx, "default", time.Minute, 1); err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	lease := driver.Lease{ID: row.ID, Attempt: 1}
	if err := mem.MarkCompleted(ctx, lease); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	// The heartbeat races finalization routinely: a beat can be in
	// flight when the job ends. Landing it must not revive the row.
	if err := mem.ExtendLeases(ctx, []driver.Lease{lease}, time.Hour); err != nil {
		t.Fatalf("ExtendLeases on a finalized job returned %v, want nil", err)
	}

	stored, _ := mem.Row(row.ID)
	if stored.State != "completed" {
		t.Errorf("State = %q, want completed", stored.State)
	}
	if stored.LeasedUntil != nil {
		t.Errorf("LeasedUntil = %v, want nil on a finalized job", stored.LeasedUntil)
	}
}

func TestStartKeepsLeaseAliveWhileJobRuns(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	release := make(chan struct{})
	started := make(chan struct{})
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		close(started)
		<-release
		return nil
	}})
	// A job that runs for many lease durations: without renewal its
	// lease would be long expired by the time it finishes.
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.LeaseDuration = 60 * time.Millisecond
		cfg.HeartbeatInterval = 15 * time.Millisecond
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "slow"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	<-started

	claimed, _ := mem.Row(row.ID)
	firstLease := *claimed.LeasedUntil

	waitFor(t, func() bool {
		current, ok := mem.Row(row.ID)
		return ok && current.LeasedUntil != nil && current.LeasedUntil.After(firstLease)
	}, "the lease to be renewed while the job runs")

	// Run well past the original lease, then confirm the lease is still
	// ahead of the clock rather than lapsed.
	time.Sleep(120 * time.Millisecond)
	current, _ := mem.Row(row.ID)
	if current.LeasedUntil == nil || current.LeasedUntil.Before(time.Now()) {
		t.Errorf("LeasedUntil = %v at %v — the lease lapsed while the job was still running",
			current.LeasedUntil, time.Now())
	}

	close(release)
	waitFor(t, h.rowInState(row.ID, "completed"), "long job to complete")
	h.stop(t)
}

func TestHeartbeatOutlivesCancellationUntilTheDrainFinishes(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	release := make(chan struct{})
	started := make(chan struct{})
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		close(started)
		<-release
		return nil
	}})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.LeaseDuration = 60 * time.Millisecond
		cfg.HeartbeatInterval = 15 * time.Millisecond
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "slow"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	<-started

	// Shutting down does not kill the in-flight job — the loop drains it.
	// A heartbeat that stopped here would let the draining job's lease
	// lapse, and every clean shutdown would hand a duplicate to another
	// worker: the most routine event in the system's life (AD-018).
	h.cancel()
	atCancel, _ := mem.Row(row.ID)
	leaseAtCancel := *atCancel.LeasedUntil

	waitFor(t, func() bool {
		current, ok := mem.Row(row.ID)
		return ok && current.LeasedUntil != nil && current.LeasedUntil.After(leaseAtCancel)
	}, "the lease to keep being renewed after cancellation")

	// Well past the original lease, still draining, still leased.
	time.Sleep(100 * time.Millisecond)
	draining, _ := mem.Row(row.ID)
	if draining.State != "running" {
		t.Fatalf("State = %q mid-drain, want running", draining.State)
	}
	if draining.LeasedUntil == nil || draining.LeasedUntil.Before(time.Now()) {
		t.Errorf("LeasedUntil = %v at %v — the lease lapsed while the job was draining",
			draining.LeasedUntil, time.Now())
	}

	close(release)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after the drained job finished")
	}
	waitFor(t, h.rowInState(row.ID, "completed"), "drained job to be finalized")
}

func TestInflightSetTracksJobsConcurrently(t *testing.T) {
	t.Parallel()
	set := newInflightSet()
	if got := set.snapshot(); got != nil {
		t.Errorf("snapshot of an empty set = %v, want nil", got)
	}

	var wg sync.WaitGroup
	for id := int64(1); id <= 50; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			set.add(driver.Lease{ID: id, Attempt: 1})
			if id%2 == 0 {
				set.remove(driver.Lease{ID: id, Attempt: 1})
			}
		}()
	}
	wg.Wait()

	got := set.snapshot()
	if len(got) != 25 {
		t.Fatalf("snapshot has %d leases, want the 25 still running", len(got))
	}
	if !slices.IsSortedFunc(got, func(a, b driver.Lease) int { return int(a.ID - b.ID) }) {
		t.Errorf("snapshot = %v, want ascending id order", got)
	}
	for _, held := range got {
		if held.ID%2 == 0 {
			t.Errorf("snapshot contains %d, which was removed", held.ID)
		}
	}
}
