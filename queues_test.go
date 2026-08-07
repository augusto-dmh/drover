package drover

import (
	"context"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

func TestCheckedQueuesDefaultsToOneQueue(t *testing.T) {
	t.Parallel()

	for _, in := range []map[string]int{nil, {}} {
		got := checkedQueues(in, slog.Default())
		want := []weightedQueue{{name: "default", weight: 1}}
		if !slices.Equal(got, want) {
			t.Errorf("checkedQueues(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestCheckedQueuesSortsByNameAndCorrectsWeights(t *testing.T) {
	t.Parallel()

	logs := &syncWriter{}
	got := checkedQueues(map[string]int{
		"low": 0, "high": 9, "bulk": -3, "mid": 2,
	}, newTestLogger(logs))

	want := []weightedQueue{
		{name: "bulk", weight: 1},
		{name: "high", weight: 9},
		{name: "low", weight: 1},
		{name: "mid", weight: 2},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("checkedQueues = %v, want %v", got, want)
	}

	out := logs.String()
	for _, name := range []string{"low", "bulk"} {
		if !strings.Contains(out, "queue="+name) {
			t.Errorf("no warning naming the corrected queue %q\nlogs:\n%s", name, out)
		}
	}
	if strings.Contains(out, "queue=high") || strings.Contains(out, "queue=mid") {
		t.Errorf("a valid weight was reported as corrected:\n%s", out)
	}
}

// A weight large enough to overflow the running total would reach
// rand.Int64N as a non-positive bound and panic on the fetch goroutine,
// which has nothing above it to recover — so an absurd but legal config
// would terminate the process on its first round. The ceiling makes that
// unreachable rather than merely commented about.
func TestCheckedQueuesClampsAnEnormousWeight(t *testing.T) {
	t.Parallel()

	logs := &syncWriter{}
	got := checkedQueues(map[string]int{
		"huge":   math.MaxInt,
		"bigger": math.MaxInt - 1,
		"normal": 3,
	}, newTestLogger(logs))

	for _, q := range got {
		if q.weight > maxQueueWeight {
			t.Errorf("queue %q kept weight %d, want it clamped to %d", q.name, q.weight, maxQueueWeight)
		}
	}
	if !strings.Contains(logs.String(), "queue=huge") {
		t.Errorf("no warning naming the clamped queue\nlogs:\n%s", logs.String())
	}

	// The clamped set must still order without overflowing or panicking.
	var dst []string
	var scratch []weightedQueue
	for i := 0; i < 200; i++ {
		dst, scratch = weightedOrder(got, dst, scratch)
		if len(dst) != len(got) {
			t.Fatalf("ordering %v dropped a queue", dst)
		}
	}
}

func TestEmptyQueueNamePanics(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("an empty queue name did not panic")
		}
	}()
	checkedQueues(map[string]int{"": 1}, slog.Default())
}

// Every configured queue must appear in every ordering. This is the
// starvation property itself: weighting decides how often a queue is
// tried first, never whether it is tried at all.
func TestWeightedOrderIsAlwaysAFullPermutation(t *testing.T) {
	t.Parallel()

	qs := []weightedQueue{
		{name: "bulk", weight: 1},
		{name: "high", weight: 1000},
		{name: "mid", weight: 50},
	}
	want := []string{"bulk", "high", "mid"}

	var dst []string
	var scratch []weightedQueue
	for i := 0; i < 500; i++ {
		dst, scratch = weightedOrder(qs, dst, scratch)
		got := slices.Clone(dst)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("ordering %v is not a permutation of %v", dst, want)
		}
	}
}

func TestWeightedOrderWithOneQueueYieldsThatQueue(t *testing.T) {
	t.Parallel()

	got, _ := weightedOrder([]weightedQueue{{name: "only", weight: 3}}, nil, nil)
	if !slices.Equal(got, []string{"only"}) {
		t.Errorf("weightedOrder = %v, want [only]", got)
	}
}

func TestWeightedOrderWithNoQueuesYieldsNothing(t *testing.T) {
	t.Parallel()

	if got, _ := weightedOrder(nil, nil, nil); len(got) != 0 {
		t.Errorf("weightedOrder(nil) = %v, want empty", got)
	}
}

// The distribution is the whole point of weighting, and it can only be
// asserted over many samples because the draw is deliberately not
// injectable (AD-017).
func TestWeightedOrderPicksFirstInProportionToWeight(t *testing.T) {
	t.Parallel()

	qs := []weightedQueue{
		{name: "high", weight: 7},
		{name: "low", weight: 1},
		{name: "mid", weight: 2},
	}
	const rounds = 60000
	total := 10.0

	first := map[string]int{}
	var dst []string
	var scratch []weightedQueue
	for i := 0; i < rounds; i++ {
		dst, scratch = weightedOrder(qs, dst, scratch)
		first[dst[0]]++
	}

	for _, q := range qs {
		want := float64(q.weight) / total
		got := float64(first[q.name]) / rounds
		if math.Abs(got-want) > 0.02 {
			t.Errorf("queue %q was first %.3f of the time, want %.3f (±0.02)", q.name, got, want)
		}
	}
}

// A large weight must not overflow the running total or push the other
// queues out of the ordering.
func TestWeightedOrderSurvivesAVeryLargeWeight(t *testing.T) {
	t.Parallel()

	qs := []weightedQueue{
		{name: "huge", weight: math.MaxInt32},
		{name: "tiny", weight: 1},
	}

	// Whatever the weighting, "tiny" is still reached in every round —
	// which is what the fetch loop relies on.
	var dst []string
	var scratch []weightedQueue
	for i := 0; i < 2000; i++ {
		dst, scratch = weightedOrder(qs, dst, scratch)
		if len(dst) != 2 {
			t.Fatalf("ordering %v dropped a queue", dst)
		}
		if !slices.Contains(dst, "tiny") {
			t.Fatalf("the low-weight queue vanished from the ordering %v", dst)
		}
	}
}

// queueTrackingDriver records which queues were asked for work, so a test can
// assert what one fetch round actually did.
type queueTrackingDriver struct {
	*memdriver.Driver

	mu     sync.Mutex
	asked  []string
	failOn string
}

func (d *queueTrackingDriver) FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	d.mu.Lock()
	d.asked = append(d.asked, queue)
	fail := d.failOn == queue
	d.mu.Unlock()
	if fail {
		return nil, context.DeadlineExceeded
	}
	return d.Driver.FetchAvailable(ctx, queue, leaseFor, limit)
}

func (d *queueTrackingDriver) queriesFor(queue string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, q := range d.asked {
		if q == queue {
			n++
		}
	}
	return n
}

func TestPoolWorksEveryConfiguredQueue(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Queues = map[string]int{"high": 9, "low": 1}
	})

	var ids []int64
	for _, q := range []string{"high", "low"} {
		row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"}, &InsertOpts{Queue: q})
		if err != nil {
			t.Fatalf("Insert into %s: %v", q, err)
		}
		ids = append(ids, row.ID)
	}

	for _, id := range ids {
		waitFor(t, h.rowInState(id, "completed"), "a job in every configured queue to run")
	}
	h.stop(t)

	// slog quotes the value because it contains '='.
	if logs := h.logs.String(); !strings.Contains(logs, `queues="high=9,low=1"`) {
		t.Errorf("start record does not name the queues and weights\nlogs:\n%s", logs)
	}
}

// A saturated high-weight queue must not stop a low-weight one draining:
// the low queue is reached within every round, not merely eventually.
func TestNoWeightingStarvesAQueue(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Queues = map[string]int{"high": 1000, "low": 1}
		cfg.Concurrency = 2
	})

	for i := 0; i < 40; i++ {
		if _, err := h.client.Insert(context.Background(), greetArgs{Name: "bulk"},
			&InsertOpts{Queue: "high"}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	starved, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"},
		&InsertOpts{Queue: "low"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	waitFor(t, h.rowInState(starved.ID, "completed"),
		"the low-weight queue to be served alongside a saturated high-weight one")
	h.stop(t)
}

// An empty queue sampled first must cost one query, not a whole poll
// interval — otherwise weighting would turn into latency for whichever
// queue actually has work.
func TestAnEmptyQueueDoesNotCostAPollInterval(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	drv := &queueTrackingDriver{Driver: mem}

	done := make(chan struct{})
	var once sync.Once
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		once.Do(func() { close(done) })
		return nil
	}})

	// Enqueued before the pool starts, so the very first fetch round is
	// the one under test. Inserting afterwards would let that round come
	// up empty for an ordinary reason and then sleep out the poll
	// interval, which would fail the test without saying anything about
	// queue selection.
	if _, err := mem.Insert(context.Background(), driver.InsertParams{
		Kind: "greet", Queue: "work", Args: []byte(`{"name":"ada"}`),
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	h := startLoopWith(t, drv, mem, ws, func(cfg *Config) {
		// Every queue but one is permanently empty, so most orderings put
		// an empty queue first.
		cfg.Queues = map[string]int{"a": 1, "b": 1, "c": 1, "work": 1}
		// Far longer than the test will wait: if the round gave up after
		// its first empty queue, the job could only run after this elapsed.
		cfg.PollInterval = 30 * time.Second
	})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("job did not run within one round — the fetch round stopped at an empty queue")
	}
	h.stop(t)

	// Deterministic regardless of which ordering came up: one job cannot
	// fill the pool's capacity, so a round that walks its whole ordering
	// asks every queue exactly because none of them filled it.
	for _, q := range []string{"a", "b", "c", "work"} {
		if drv.queriesFor(q) == 0 {
			t.Errorf("queue %q was never queried — the round did not walk its full ordering", q)
		}
	}
}

// One configured queue must issue exactly one query per round: the
// default configuration should be no chattier than before queues existed.
func TestOneQueueIssuesOneFetchPerRound(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	drv := &queueTrackingDriver{Driver: mem}
	h := startLoopWith(t, drv, mem, NewWorkers(), func(cfg *Config) {
		cfg.PollInterval = 5 * time.Millisecond
	})

	time.Sleep(60 * time.Millisecond)
	h.stop(t)

	drv.mu.Lock()
	asked := slices.Clone(drv.asked)
	drv.mu.Unlock()

	if len(asked) == 0 {
		t.Fatal("the fetch loop never queried")
	}
	for _, q := range asked {
		if q != "default" {
			t.Fatalf("queried queue %q, want only default", q)
		}
	}
	// An upper bound as well as a lower one. Without it a loop issuing
	// ten queries per round, or one that dropped its poll-interval sleep
	// and spun, passes unchanged. A 5ms interval over ~60ms should yield
	// on the order of a dozen queries; a spinning loop yields thousands,
	// so the bound is loose enough not to be timing-sensitive and tight
	// enough to catch either fault.
	if len(asked) > 40 {
		t.Errorf("fetch loop issued %d queries in ~60ms at a 5ms interval — it is not sleeping between rounds", len(asked))
	}
}

// The surplus clamp is bounded by what is left of the round, not by the
// round's original capacity. Only a multi-queue round can tell those
// apart: with one queue the remaining capacity is always the full
// capacity, so the existing single-queue coverage would stay green while
// a multi-queue round handed the pool more rows than it has workers.
func TestSurplusIsClampedToWhatIsLeftOfTheRound(t *testing.T) {
	t.Parallel()

	const capacity = 4
	for i := 0; i < 200; i++ {
		mem := memdriver.New()
		seed := func(queue string, n int) {
			for j := 0; j < n; j++ {
				if _, err := mem.Insert(context.Background(), driver.InsertParams{
					Kind: "greet", Queue: queue, Args: []byte(`{"name":"ada"}`),
				}); err != nil {
					t.Fatalf("seed job: %v", err)
				}
			}
		}
		seed("thin", 1)
		seed("fat", 10)

		// Overshoots every limit it is given, so the clamp runs on
		// whichever queue is visited second.
		drv := &overshootDriver{Driver: mem, extra: 3}
		c := newClient(drv, Config{
			Logger: newTestLogger(&syncWriter{}),
			Queues: map[string]int{"thin": 1, "fat": 1},
		})
		r := newRunner(context.Background(), c, nil)

		got := r.claimRound(capacity)
		if len(got) > capacity {
			t.Fatalf("claimRound(%d) kept %d rows after ordering %v", capacity, len(got), r.order)
		}

		// Everything beyond the capacity must have gone back, not been
		// left running with nothing to execute it.
		running := 0
		for id := int64(1); id <= 11; id++ {
			if row, ok := mem.Row(id); ok && row.State == "running" {
				running++
			}
		}
		if running > capacity {
			t.Fatalf("%d rows left running after a round of capacity %d (ordering %v)", running, capacity, r.order)
		}
	}
}

// Rows claimed from an earlier queue are already running and leased. A
// later queue's failure must not throw them away, or each would sit
// stranded until its lease lapsed.
func TestAFetchErrorDoesNotStrandRowsAlreadyClaimed(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	drv := &queueTrackingDriver{Driver: mem, failOn: "broken"}

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoopWith(t, drv, mem, ws, func(cfg *Config) {
		cfg.Queues = map[string]int{"broken": 1, "healthy": 1}
	})

	row, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"},
		&InsertOpts{Queue: "healthy"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	waitFor(t, h.rowInState(row.ID, "completed"),
		"a job claimed from a healthy queue to run despite another queue failing")
	h.stop(t)

	if drv.queriesFor("broken") == 0 {
		t.Error("the failing queue was never queried, so the test proved nothing")
	}
}

// AD-022 across queues: a fetch round may claim only as many jobs as the
// pool has idle workers, so a claimed row always has a worker about to
// run it. Spreading the work over several queues must not let a round
// take capacity-worth of jobs from each of them.
//
// The round is driven directly because the interesting case depends on
// which ordering comes up: it only appears when an early queue partly
// fills the capacity and a later one could overrun what is left. Driving
// it here covers every ordering instead of waiting for one.
func TestAClaimRoundNeverExceedsTheCapacityItWasGiven(t *testing.T) {
	t.Parallel()

	const capacity = 3
	for i := 0; i < 200; i++ {
		mem := memdriver.New()
		// "thin" cannot fill the capacity on its own and "fat" can more
		// than fill what would be left, so an ordering that starts with
		// "thin" overruns unless the remaining capacity bounds the
		// second claim.
		seed := func(queue string, n int) {
			for j := 0; j < n; j++ {
				if _, err := mem.Insert(context.Background(), driver.InsertParams{
					Kind: "greet", Queue: queue, Args: []byte(`{"name":"ada"}`),
				}); err != nil {
					t.Fatalf("seed job: %v", err)
				}
			}
		}
		seed("thin", 1)
		seed("fat", 10)

		c := newClient(mem, Config{
			Logger: newTestLogger(&syncWriter{}),
			Queues: map[string]int{"thin": 1, "fat": 1},
		})
		r := newRunner(context.Background(), c, nil)

		got := r.claimRound(capacity)
		if len(got) > capacity {
			t.Fatalf("claimRound(%d) claimed %d jobs after ordering %v", capacity, len(got), r.order)
		}

		running := 0
		for id := int64(1); id <= 11; id++ {
			if row, ok := mem.Row(id); ok && row.State == "running" {
				running++
			}
		}
		if running > capacity {
			t.Fatalf("%d rows left running after a round of capacity %d", running, capacity)
		}
	}
}

// The same invariant end to end, so the wiring between the fetch loop and
// the pool is covered and not only the round in isolation.
func TestARoundNeverClaimsMoreThanTheIdleWorkerCount(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()

	const concurrency = 3
	release := make(chan struct{})
	var running atomic.Int32
	var peak atomic.Int32

	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error {
		n := running.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil
	}})

	queues := map[string]int{"a": 1, "b": 1, "c": 1, "d": 1}
	for q := range queues {
		for i := 0; i < 10; i++ {
			if _, err := mem.Insert(context.Background(), driver.InsertParams{
				Kind: "greet", Queue: q, Args: []byte(`{"name":"ada"}`),
			}); err != nil {
				t.Fatalf("seed job: %v", err)
			}
		}
	}

	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Queues = queues
		cfg.Concurrency = concurrency
	})

	// Let the fetch loop take as much as it is willing to.
	time.Sleep(150 * time.Millisecond)

	// Every claimed row is running in the store, whether or not a worker
	// has picked it up yet — which is exactly what the invariant bounds.
	claimed := 0
	for id := int64(1); id <= int64(10*len(queues)); id++ {
		if row, ok := mem.Row(id); ok && row.State == "running" {
			claimed++
		}
	}
	if claimed > concurrency {
		t.Errorf("%d jobs claimed at once, want at most %d", claimed, concurrency)
	}
	if got := peak.Load(); got > concurrency {
		t.Errorf("%d handlers ran at once, want at most %d", got, concurrency)
	}

	close(release)
	h.stop(t)
}

// limitRecordingDriver remembers the limit each queue was asked for, so a
// test can assert what bounded the claim rather than only what came back.
type limitRecordingDriver struct {
	*memdriver.Driver

	mu     sync.Mutex
	limits []int
}

func (d *limitRecordingDriver) FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error) {
	d.mu.Lock()
	d.limits = append(d.limits, limit)
	d.mu.Unlock()
	return d.Driver.FetchAvailable(ctx, queue, leaseFor, limit)
}

// Each queue in a round must be asked only for the capacity still
// unspoken-for. Asking every queue for the full capacity happens to be
// corrected further downstream, but only by claiming rows and handing
// them straight back — which blames the driver in a warning, records an
// attempt error saying a shutdown interrupted the job when none did, and
// spends an attempt that is never given back. The limit is where this
// has to be right.
func TestEachQueueIsAskedOnlyForTheRemainingCapacity(t *testing.T) {
	t.Parallel()

	const capacity = 4
	for i := 0; i < 200; i++ {
		mem := memdriver.New()
		// One job in "thin" cannot fill the round, so whatever is asked of
		// the queue visited after it must be strictly less than capacity.
		if _, err := mem.Insert(context.Background(), driver.InsertParams{
			Kind: "greet", Queue: "thin", Args: []byte(`{"name":"ada"}`),
		}); err != nil {
			t.Fatalf("seed job: %v", err)
		}
		for j := 0; j < 10; j++ {
			if _, err := mem.Insert(context.Background(), driver.InsertParams{
				Kind: "greet", Queue: "fat", Args: []byte(`{"name":"ada"}`),
			}); err != nil {
				t.Fatalf("seed job: %v", err)
			}
		}

		drv := &limitRecordingDriver{Driver: mem}
		c := newClient(drv, Config{
			Logger: newTestLogger(&syncWriter{}),
			Queues: map[string]int{"thin": 1, "fat": 1},
		})
		r := newRunner(context.Background(), c, nil)
		r.claimRound(capacity)

		drv.mu.Lock()
		limits := slices.Clone(drv.limits)
		drv.mu.Unlock()

		if len(limits) == 0 {
			t.Fatal("no queue was queried")
		}
		if limits[0] != capacity {
			t.Fatalf("first queue asked for %d, want the full capacity %d", limits[0], capacity)
		}
		// Only the ordering that visits "thin" first exercises the
		// interesting case; the other one fills the round immediately.
		if len(limits) > 1 && r.order[0] == "thin" {
			if limits[1] != capacity-1 {
				t.Fatalf("after claiming 1 job, the next queue was asked for %d, want %d (ordering %v)",
					limits[1], capacity-1, r.order)
			}
		}
	}
}

// A job enqueued to a queue no running client works must simply wait,
// not be rejected, lost, or picked up by a client working other queues.
func TestAJobInAnUnworkedQueueStaysClaimable(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mem := memdriver.New()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(context.Context, *Job[greetArgs]) error { return nil }})
	h := startLoopWith(t, mem, mem, ws, func(cfg *Config) {
		cfg.Queues = map[string]int{"worked": 1}
	})

	stranded, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"},
		&InsertOpts{Queue: "nobody-works-this"})
	if err != nil {
		t.Fatalf("Insert into an unworked queue returned %v, want nil", err)
	}
	marker, err := h.client.Insert(context.Background(), greetArgs{Name: "ada"},
		&InsertOpts{Queue: "worked"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// The marker running proves the pool made a full pass, so the other
	// job was not merely still in flight.
	waitFor(t, h.rowInState(marker.ID, "completed"), "the worked queue to drain")
	h.stop(t)

	row, ok := mem.Row(stranded.ID)
	if !ok {
		t.Fatal("the job in the unworked queue disappeared")
	}
	if row.State != "available" {
		t.Errorf("state = %q, want it still available for whoever works that queue", row.State)
	}
	if row.Attempt != 0 {
		t.Errorf("attempt = %d, want 0 — nothing should have claimed it", row.Attempt)
	}
}
