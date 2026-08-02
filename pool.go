package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

// runner is one running pool: a fetch loop, a fixed set of worker
// goroutines, and the lifecycle state that starts and stops them as a
// unit.
//
// It is a value created by Start and finished by Stop rather than a set
// of fields on Client, because all of it is per-run state. A client that
// has never started holds none of it, so there is no half-initialized
// pool to reason about.
//
// One runner serves every configured queue. A second queue is another
// entry in the weighted set the fetch loop samples, not another pool:
// splitting the pool per queue would pin workers to a queue that may be
// idle while another backs up.
type runner struct {
	client      *Client
	queues      []weightedQueue
	concurrency int

	// order is the scratch buffer weightedOrder fills each round. It
	// belongs to the fetch loop goroutine alone.
	order []string

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
		queues:           c.queues,
		concurrency:      c.concurrency,
		order:            make([]string, 0, len(c.queues)),
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
		// The verdict is discarded because there is nobody to hand it to:
		// this caller cancelled rather than calling Stop, so no return
		// value carries it. Nothing is lost — the budget here is
		// unbounded, so the one error Stop can report cannot arise, and
		// the shutdown logs what it did either way.
		_ = r.stop(context.WithoutCancel(ctx))
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
	c.logger.Info("drover: worker pool stopping", "queues", queueNames(r.queues))

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

	c.logger.Info("drover: worker pool stopped", "queues", queueNames(r.queues))
	return err
}

// errShutdownRequeued is recorded against an attempt a shutdown cut
// short. It is not a failure of the job: the attempt simply did not get
// to finish, and the record exists so an operator reading the job's
// history can see why it ran more than once.
var errShutdownRequeued = errors.New("drover: worker shut down before this attempt finished")

// escalate spends what is left of the shutdown: it cancels the contexts
// the running handlers were given, returns their jobs to the queue, and
// reports how many there were.
//
// The count is the honest answer. Go cannot stop a goroutine, so a
// handler that ignores its cancelled context is still running when this
// returns — which is exactly why the jobs go back to the queue. Another
// worker can make progress on them immediately, and if the abandoned
// handler later tries to record an outcome, the fence refuses it
// (AD-019).
func (r *runner) escalate() error {
	// Checked before anything else. The budget can expire at the very
	// moment the last worker finishes, and when both channels are ready
	// the runtime picks between them at random — so arriving here is not
	// by itself evidence that anything was left running. A shutdown in
	// which every job recorded its outcome is a clean one however the
	// race fell, and reporting it as a failure would have callers who
	// exit non-zero on error failing perfectly good deploys.
	stranded := r.client.inflight.snapshot()
	if len(stranded) == 0 {
		return nil
	}

	// The one place a handler's context is cancelled. Everything else
	// about shutdown is cooperative; this is not.
	r.cancelJobs()

	r.client.logger.Warn("drover: shutdown deadline reached with jobs still running",
		"queues", queueNames(r.queues), "jobs", len(stranded))
	r.requeueAll(stranded)

	return fmt.Errorf("%w: %d job(s) still running, returned to the queue",
		ErrDrainIncomplete, len(stranded))
}

// abandon returns rows the fetch loop claimed but never handed to a
// worker. Nothing is executing them, so leaving them behind would keep
// them running and leased until the lease lapsed — a delay a clean
// shutdown has no reason to impose on work nobody ever started.
func (r *runner) abandon(rows []*driver.JobRow) {
	if len(rows) == 0 {
		return
	}
	leases := leasesOf(rows)
	r.requeueAll(leases)
	for _, lease := range leases {
		r.client.inflight.remove(lease)
	}
	// One token per row: each of these rows was dispatched a slot that no
	// worker will now return.
	r.releaseSlots(len(rows))
}

func leasesOf(rows []*driver.JobRow) []driver.Lease {
	leases := make([]driver.Lease, 0, len(rows))
	for _, row := range rows {
		leases = append(leases, driver.Lease{ID: row.ID, Attempt: row.Attempt})
	}
	return leases
}

// requeueAll returns every named lease to the queue, best effort: one
// that will not go back is reported and the rest still do.
func (r *runner) requeueAll(leases []driver.Lease) {
	if len(leases) == 0 {
		return
	}

	for _, lease := range leases {
		// A bound per hand-back rather than one for the batch. This runs
		// after the caller's budget is already spent, so a single shared
		// allowance would let one wedged write consume what the rest of
		// the slice needed and leave the tail unattempted — and the
		// slower the database, the likelier both that the drain overran
		// and that the tail is the part that matters.
		//
		// Deliberately not derived from the job context, which escalate
		// has just cancelled: these writes are how the shutdown keeps its
		// promise, and they have to outlive the cancellation that
		// prompted them.
		if err := r.requeueOne(lease); err != nil {
			r.requeueFailed(lease, err)
			continue
		}
		r.client.logger.Warn("drover: returned an unfinished job to the queue",
			"job_id", lease.ID, "attempt", lease.Attempt)
	}
}

func (r *runner) requeueOne(lease driver.Lease) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.jobCtx), r.client.heartbeatInterval)
	defer cancel()
	return r.client.requeue(ctx, lease)
}

// requeueFailed reports a job that would not go back.
//
// Losing the race with the job's own worker is the ordinary case, not a
// fault: the handler finished while the shutdown was reaching for the
// row, recorded its outcome, and the row is accounted for. A genuine
// failure leaves the row running with a lease that will lapse, and the
// rescuer collects it — the same backstop that covers a process dying
// outright.
func (r *runner) requeueFailed(lease driver.Lease, err error) {
	if errors.Is(err, driver.ErrLeaseLost) || errors.Is(err, driver.ErrInvalidTransition) {
		r.client.logger.Debug("drover: job finished before it could be returned to the queue",
			"job_id", lease.ID, "attempt", lease.Attempt, "error", err)
		return
	}
	r.client.logger.Error("drover: return job to the queue",
		"job_id", lease.ID, "attempt", lease.Attempt, "error", err)
}

// requeue makes a claimed job claimable again, immediately.
//
// It does not give back the attempt the claim consumed, and that is the
// point. attempt is the fence every state change is checked against, so
// handing it back would let a handler this shutdown abandoned — one that
// ignored its cancelled context and is still running — present the same
// number the next claim hands out, and its stale write would be accepted
// over the new holder's. The attempt was spent; a shutdown cutting it
// short does not unspend it (AD-026, on the same reasoning as AD-012).
func (c *Client) requeue(ctx context.Context, lease driver.Lease) error {
	detail, err := json.Marshal(driver.AttemptError{
		Attempt: lease.Attempt,
		At:      time.Now().UTC(),
		Error:   errShutdownRequeued.Error(),
	})
	if err != nil {
		return fmt.Errorf("drover: encode requeue reason for job %d: %w", lease.ID, err)
	}
	// At now, not after a backoff: the job did not fail, it was
	// interrupted, and there is nothing to wait out.
	return c.drv.MarkRetryable(ctx, lease, time.Now(), detail)
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

		rows := r.claimRound(n)
		if rows == nil && r.stopping() {
			r.releaseSlots(n)
			return
		}

		// The round may claim fewer jobs than there was room for.
		r.releaseSlots(n - len(rows))
		if len(rows) == 0 {
			if !r.sleep() {
				return
			}
			continue
		}

		for i, row := range rows {
			// Tracked from the claim, not from the start of execution: a
			// row waiting to be handed to a worker is already running and
			// already leased, and the heartbeat has to cover that window
			// or the rescuer will take the job away from a pool that is
			// about to run it.
			c.inflight.add(driver.Lease{ID: row.ID, Attempt: row.Attempt})
			select {
			case r.jobs <- row:
			case <-r.stopFetch:
				r.abandon(rows[i:])
				return
			}
		}
	}
}

// claimRound claims up to capacity jobs for one pass of the fetch loop,
// visiting the queues in a freshly weighted order.
//
// The remaining capacity is what bounds each individual claim, so the
// round as a whole never takes more jobs than the pool has idle workers
// — AD-022's invariant, held across queues rather than within one.
//
// Every configured queue is visited before the round gives up, so an
// empty queue that happened to be sampled first costs one query rather
// than a whole poll interval of latency for the queues behind it. That
// is also what keeps weighting from starving anything: a low weight
// makes a queue likely to be tried last, never skipped.
//
// A claim that fails ends the round rather than failing it. Rows already
// claimed from earlier queues are running and leased, so abandoning them
// here would strand each one until its lease lapsed; they are returned
// for dispatch and the error is reported. A round that claimed nothing
// returns nil, which the caller distinguishes from an empty result only
// to notice shutdown.
func (r *runner) claimRound(capacity int) []*driver.JobRow {
	c := r.client
	var claimed []*driver.JobRow
	remaining := capacity

	r.order = weightedOrder(r.queues, r.order)
	for _, queue := range r.order {
		rows, err := c.drv.FetchAvailable(r.fetchCtx, queue, c.leaseDuration, remaining)
		if err != nil {
			if r.stopping() {
				return claimed
			}
			c.logger.Error("drover: fetch jobs", "queue", queue, "error", err)
			return claimed
		}

		// A driver that hands back more rows than the limit allowed has
		// claimed work this pool has no worker for. Holding it would
		// recreate exactly the state the slot accounting exists to
		// prevent — rows running and leased with nothing executing them —
		// so the surplus goes straight back (AD-022).
		if len(rows) > remaining {
			surplus := rows[remaining:]
			c.logger.Warn("drover: driver returned more jobs than requested; returning the surplus",
				"queue", queue, "requested", remaining, "returned", len(rows))
			r.requeueAll(leasesOf(surplus))
			rows = rows[:remaining]
		}

		claimed = append(claimed, rows...)
		remaining -= len(rows)
		if remaining == 0 || r.stopping() {
			break
		}
	}
	return claimed
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
