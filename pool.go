package drover

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// runner is one queue's running pool: a fetch loop, a fixed set of
// worker goroutines, and the lifecycle state that starts and stops them
// as a unit.
//
// It is a value created by Start and finished by Stop rather than a set
// of fields on Client, because all of it is per-run state. A client that
// has never started holds none of it, so there is no half-initialized
// pool to reason about — and the queue is a field rather than an
// assumption, so a second queue is a second runner rather than a rewrite
// of this one.
type runner struct {
	client      *Client
	queue       string
	concurrency int

	// jobs is deliberately unbuffered: a completed send means a worker
	// has taken the row, so no claimed job is ever parked in a buffer
	// holding a live lease with nothing executing it.
	jobs chan *driver.JobRow

	// slots holds one token per idle worker. It is what stops the fetch
	// loop claiming work the pool cannot start (AD-022).
	slots chan struct{}

	// stopFetch is closed to stop claiming. It is separate from any
	// context because it must also interrupt a poll-interval wait and a
	// blocked hand-off, neither of which is a database call.
	stopFetch chan struct{}

	// fetchCtx covers the database calls that claim work — the fetch
	// loop's and the rescuer's. Cancelling it is how shutdown stops
	// taking on anything new.
	fetchCtx         context.Context
	cancelBackground context.CancelFunc

	// jobCtx is what handlers run on. Cancelling it is the escalation a
	// shutdown reaches for once its budget is spent, and it is the only
	// thing in here a handler can observe.
	jobCtx     context.Context
	cancelJobs context.CancelFunc

	stopHeartbeat chan struct{}

	goroutines sync.WaitGroup // fetch loop and workers
	background sync.WaitGroup // heartbeat and rescuer

	shutdown sync.Once
	done     chan struct{} // closed once the shutdown has finished
	stopErr  error         // written before done is closed
}

func newRunner(ctx context.Context, c *Client) *runner {
	// Both are detached from the caller's context. Cancelling that
	// context must begin an orderly shutdown, not kill work outright: the
	// watcher goroutine turns it into a full Stop, and the ordering there
	// is what keeps a draining job's lease alive.
	fetchCtx, cancelBackground := context.WithCancel(context.WithoutCancel(ctx))
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))

	r := &runner{
		client:           c,
		queue:            defaultQueue,
		concurrency:      c.concurrency,
		jobs:             make(chan *driver.JobRow),
		slots:            make(chan struct{}, c.concurrency),
		stopFetch:        make(chan struct{}),
		fetchCtx:         fetchCtx,
		cancelBackground: cancelBackground,
		jobCtx:           jobCtx,
		cancelJobs:       cancelJobs,
		stopHeartbeat:    make(chan struct{}),
		done:             make(chan struct{}),
	}
	for i := 0; i < r.concurrency; i++ {
		r.slots <- struct{}{}
	}
	return r
}

// start launches the pool: the heartbeat and rescuer that support it,
// the fetch loop that feeds it, and one goroutine per worker.
func (r *runner) start(ctx context.Context) {
	r.background.Add(2)
	go func() {
		defer r.background.Done()
		r.client.heartbeat(r.stopHeartbeat)
	}()
	go func() {
		defer r.background.Done()
		r.client.rescueLoop(r.fetchCtx)
	}()

	r.goroutines.Add(1)
	go func() {
		defer r.goroutines.Done()
		r.fetch()
	}()
	for i := 0; i < r.concurrency; i++ {
		r.goroutines.Add(1)
		go func() {
			defer r.goroutines.Done()
			r.work()
		}()
	}

	go r.watch(ctx)
}

// watch turns cancellation of the context Start was given into a full
// shutdown, rather than merely the beginning of one.
//
// The budget is unbounded because the caller expressed no deadline: they
// said stop, not stop by when. A caller who wants a bound calls Stop
// with one.
func (r *runner) watch(ctx context.Context) {
	select {
	case <-ctx.Done():
		r.stop(context.WithoutCancel(ctx))
	case <-r.done:
	}
}

// stop runs the shutdown once and reports the same verdict to everyone
// who asks. Whoever arrives first does the work; anyone else — an
// explicit Stop racing the watcher, or a second Stop — waits for it and
// reads the result.
func (r *runner) stop(ctx context.Context) error {
	r.shutdown.Do(func() {
		r.stopErr = r.drain(ctx)
		close(r.done)
	})
	<-r.done
	return r.stopErr
}

// drain performs the ordered shutdown: stop claiming, wait for what is
// already running, then release the machinery that supported it.
func (r *runner) drain(ctx context.Context) error {
	c := r.client
	c.logger.Info("drover: worker pool stopping", "queue", r.queue)

	// Released once nothing is left to run under it.
	defer r.cancelJobs()

	// Claiming stops before anything waits. The rescuer is included
	// because FetchExpired is a claim like any other: a shutdown that
	// left it running would keep taking on work while it drained.
	r.cancelBackground()
	close(r.stopFetch)

	finished := make(chan struct{})
	go func() {
		r.goroutines.Wait()
		close(finished)
	}()

	var err error
	select {
	case <-finished:
	case <-ctx.Done():
		err = r.escalate()
	}

	// Only now. Every lease held through the wait above had to keep being
	// renewed: stopping the heartbeat any earlier would strand each
	// draining worker with a lapsing lease, and the rescuer — this
	// process's or another's — would hand out a duplicate on every clean
	// shutdown, which is the most routine event in the system's life
	// (AD-018, widened here from the last job to the last worker).
	close(r.stopHeartbeat)
	r.background.Wait()

	c.logger.Info("drover: worker pool stopped", "queue", r.queue)
	return err
}

// escalate reports a drain that ran out of time, naming how many jobs
// were still running when it did. Those jobs keep running — Go cannot
// stop a goroutine — so the honest thing to return is a count, not a
// promise.
func (r *runner) escalate() error {
	stranded := len(r.client.inflight.snapshot())
	r.client.logger.Warn("drover: shutdown deadline reached with jobs still running",
		"queue", r.queue, "jobs", stranded)
	return fmt.Errorf("%w: %d job(s) still running", ErrDrainIncomplete, stranded)
}

// fetch claims due jobs and hands them to workers until shutdown.
func (r *runner) fetch() {
	// Closing this is what retires the workers: they range over it, so a
	// closed and drained channel ends each of them.
	defer close(r.jobs)

	c := r.client
	for {
		n := r.acquireSlots()
		if n == 0 {
			return
		}

		rows, err := c.drv.FetchAvailable(r.fetchCtx, r.queue, c.leaseDuration, n)
		if err != nil {
			r.releaseSlots(n)
			if r.stopping() {
				return
			}
			c.logger.Error("drover: fetch jobs", "error", err)
			if !r.sleep() {
				return
			}
			continue
		}

		// The driver may hand back fewer rows than there was room for.
		r.releaseSlots(n - len(rows))
		if len(rows) == 0 {
			if !r.sleep() {
				return
			}
			continue
		}

		for _, row := range rows {
			// Tracked from the claim, not from the start of execution: a
			// row waiting to be handed to a worker is already running and
			// already leased, and the heartbeat has to cover that window
			// or the rescuer will take the job away from a pool that is
			// about to run it.
			c.inflight.add(driver.Lease{ID: row.ID, Attempt: row.Attempt})
			select {
			case r.jobs <- row:
			case <-r.stopFetch:
				return
			}
		}
	}
}

// work runs jobs off the hand-off channel until it closes, returning the
// worker's slot each time so the fetch loop knows it is free again.
func (r *runner) work() {
	for row := range r.jobs {
		r.client.runJob(r.jobCtx, row)
		r.slots <- struct{}{}
	}
}

// acquireSlots waits for one idle worker, then takes every other idle
// worker it can without waiting. What it returns is how many jobs the
// next claim may take: never more than the pool can begin running
// immediately, so a claimed row always has a worker waiting for it
// (AD-022). It returns zero when shutdown has begun.
func (r *runner) acquireSlots() int {
	select {
	case <-r.slots:
	case <-r.stopFetch:
		return 0
	}

	n := 1
	for n < r.concurrency {
		select {
		case <-r.slots:
			n++
		default:
			return n
		}
	}
	return n
}

func (r *runner) releaseSlots(n int) {
	for i := 0; i < n; i++ {
		r.slots <- struct{}{}
	}
}

func (r *runner) stopping() bool {
	select {
	case <-r.stopFetch:
		return true
	default:
		return false
	}
}

// sleep waits one poll interval, reporting false when shutdown began
// first so a stopping pool never sits out an idle tick.
func (r *runner) sleep() bool {
	timer := time.NewTimer(r.client.pollInterval)
	defer timer.Stop()
	select {
	case <-r.stopFetch:
		return false
	case <-timer.C:
		return true
	}
}
