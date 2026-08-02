// Command email is a small, runnable drover pipeline: it enqueues a
// batch of "welcome email" jobs plus one delayed "digest" job on a
// second, lower-priority queue, works them concurrently on a pool of
// workers wrapped in a small middleware chain, and retries the
// deliveries that the flaky stub in delivery.go fails on their first
// attempt. Send SIGINT (Ctrl-C) to see the graceful shutdown drain
// whatever is still in flight.
//
// It needs a reachable PostgreSQL database; see README.md for how to
// run it.
package main

import (
	"context"
	"fmt"
	"log"
	"maps"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover"
)

// recipientCount is how many welcome emails this run enqueues, addressed
// user1@example.com through userN@example.com. Because isFlaky in
// delivery.go is a pure function of the address, a reader can run it
// over that same range before ever starting the program and know
// exactly which of these jobs will need a retry.
const recipientCount = 20

// workerConcurrency is the pool size. It is deliberately more than one
// so the run visibly overlaps deliveries rather than working the queue
// one job at a time.
const workerConcurrency = 4

// digestQueue is a second, lower-priority queue: it shares the same
// worker pool as "default" rather than getting workers of its own, and
// is only claimed about a fifth as often — see the Queues weight below.
const digestQueue = "digests"

// digestDelay is how long after startup the one digest job becomes
// claimable. It is short enough that a reader watching the log does not
// have to wait long to see a scheduled job come due.
const digestDelay = 5 * time.Second

// handlerTimeout bounds how long any one delivery may run, enforced by
// the built-in Timeout middleware. The flaky stub in delivery.go
// returns immediately either way, so this timeout never actually fires
// here — it is wired up to show the configuration, not to be exercised.
const handlerTimeout = 10 * time.Second

// SendWelcomeEmail is the job payload: who to email and which template
// to render.
type SendWelcomeEmail struct {
	To       string `json:"to"`
	Template string `json:"template"`
}

// Kind implements drover.JobArgs.
func (SendWelcomeEmail) Kind() string { return "send_welcome_email" }

// EmailWorker delivers SendWelcomeEmail jobs through the flaky stub in
// delivery.go. Returning its error is what drives drover's ordinary
// retry path — nothing about that path is example-specific.
type EmailWorker struct {
	drover.WorkerDefaults[SendWelcomeEmail]
}

// Work implements drover.Worker[SendWelcomeEmail]. It handles both the
// immediate "welcome" jobs and the delayed "digest" job the same way:
// the worker does not need to know which queue a job arrived on.
func (EmailWorker) Work(_ context.Context, job *drover.Job[SendWelcomeEmail]) error {
	if err := deliver(job.Args.To, job.Attempt); err != nil {
		return fmt.Errorf("send %s to %s (attempt %d): %w", job.Args.Template, job.Args.To, job.Attempt, err)
	}
	log.Printf("delivered %-24s attempt %d", job.Args.To, job.Attempt)
	return nil
}

// perQueueCounts is a custom middleware — the smallest useful one this
// example can show, wrapping the handler drover already built for
// logging and the timeout below to add one more concern of its own. It
// counts each successful delivery by the queue it ran on, so the totals
// printed at shutdown show the two queues configured in run() being
// worked from the one shared pool.
func perQueueCounts(counts *queueCounts) drover.Middleware {
	return func(next drover.Handler) drover.Handler {
		return func(ctx context.Context, job *drover.JobRow) error {
			err := next(ctx, job)
			if err == nil {
				counts.record(job.Queue)
			}
			return err
		}
	}
}

// queueCounts tallies completed jobs per queue. Every pool worker calls
// record concurrently, so the map needs guarding — a plain mutex rather
// than sync.Map, whose documented cases are append-mostly caches and
// disjoint per-goroutine keys, neither of which this is. The key set is
// the two queues configured in run().
type queueCounts struct {
	mu sync.Mutex
	n  map[string]int
}

func newQueueCounts() *queueCounts {
	return &queueCounts{n: make(map[string]int)}
}

func (c *queueCounts) record(queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[queue]++
}

// total returns a copy, so the caller reads a stable snapshot rather
// than the live map.
func (c *queueCounts) total() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.n)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL must be set, e.g. postgres://localhost:5432/drover_example")
	}

	// A context of its own for setup and for the running pool. SIGINT is
	// handled separately below with its own context: Start's own context
	// cancelling would begin an unbounded shutdown on its own (documented
	// on Client.Start), and this program wants to demonstrate the bounded
	// Stop path instead, so the two are kept apart on purpose.
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	if err := drover.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	workers := drover.NewWorkers()
	drover.Register(workers, EmailWorker{})

	counts := newQueueCounts()
	client, err := drover.NewClient(pool, drover.Config{
		Workers:     workers,
		Concurrency: workerConcurrency,
		// "default" (unset in InsertOpts) is claimed about four times as
		// often as "digests" — never exclusively, so digests still get
		// worked promptly even while welcome emails are flowing.
		Queues: map[string]int{"default": 4, digestQueue: 1},
		// Timeout is one of the two built-in middleware; perQueueCounts
		// is a custom one. The client always installs Logging outermost
		// of both, so per-job logging is unaffected by either.
		Middleware: []drover.Middleware{drover.Timeout(handlerTimeout), perQueueCounts(counts)},
	})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	if err := enqueueBatch(ctx, client); err != nil {
		return fmt.Errorf("enqueue batch: %w", err)
	}
	if err := enqueueDigest(ctx, client); err != nil {
		return fmt.Errorf("enqueue digest: %w", err)
	}

	if err := client.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	log.Printf("working the queue with %d workers; press ctrl-c to stop", workerConcurrency)

	sigCtx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	<-sigCtx.Done()
	stopSignals() // a second ctrl-c falls through to the default handler

	log.Print("received interrupt, draining in-flight jobs (up to 30s)")
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Stop(shutdown); err != nil {
		// Reported through the exit code, not just the log. This is the
		// one shutdown an operator most needs to notice, and a process
		// that exits 0 here tells its orchestrator everything went fine.
		return fmt.Errorf("shutdown incomplete: %w", err)
	}
	log.Print("shutdown complete: every in-flight job finished and recorded its outcome")

	total := counts.total()
	for _, queue := range slices.Sorted(maps.Keys(total)) {
		log.Printf("completed %d job(s) on queue %q", total[queue], queue)
	}
	return nil
}

// enqueueBatch inserts recipientCount welcome emails and reports up
// front how many of them the flaky stub will fail on their first
// attempt, so the retries that follow are not a surprise.
func enqueueBatch(ctx context.Context, client *drover.Client) error {
	flaky := 0
	for i := 1; i <= recipientCount; i++ {
		to := fmt.Sprintf("user%d@example.com", i)
		if isFlaky(to) {
			flaky++
		}
		if _, err := client.Insert(ctx, SendWelcomeEmail{To: to, Template: "welcome"}, nil); err != nil {
			return err
		}
	}
	log.Printf("enqueued %d emails; %d of them will fail their first delivery attempt and retry",
		recipientCount, flaky)
	return nil
}

// enqueueDigest inserts one digest email on digestQueue, held back by
// digestDelay: InsertOpts is what lets a caller pick a job's queue and
// earliest run time, instead of every job running immediately on
// "default".
func enqueueDigest(ctx context.Context, client *drover.Client) error {
	digest := SendWelcomeEmail{To: "digest-subscribers@example.com", Template: "digest"}
	_, err := client.Insert(ctx, digest, &drover.InsertOpts{
		Queue:       digestQueue,
		ScheduledAt: time.Now().Add(digestDelay),
	})
	if err != nil {
		return err
	}
	log.Printf("scheduled a digest email on queue %q, claimable in %s", digestQueue, digestDelay)
	return nil
}
