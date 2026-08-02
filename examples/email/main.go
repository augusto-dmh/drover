// Command email is a small, runnable drover pipeline: it enqueues a
// batch of "welcome email" jobs, works them concurrently on a pool of
// workers, and retries the deliveries that the flaky stub in delivery.go
// fails on their first attempt. Send SIGINT (Ctrl-C) to see the
// graceful shutdown drain whatever is still in flight.
//
// It needs a reachable PostgreSQL database; see README.md for how to
// run it.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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

// Work implements drover.Worker[SendWelcomeEmail].
func (EmailWorker) Work(_ context.Context, job *drover.Job[SendWelcomeEmail]) error {
	if err := deliver(job.Args.To, job.Attempt); err != nil {
		return fmt.Errorf("send %s to %s (attempt %d): %w", job.Args.Template, job.Args.To, job.Attempt, err)
	}
	log.Printf("delivered %-24s attempt %d", job.Args.To, job.Attempt)
	return nil
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

	client, err := drover.NewClient(pool, drover.Config{
		Workers:     workers,
		Concurrency: workerConcurrency,
	})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	if err := enqueueBatch(ctx, client); err != nil {
		return fmt.Errorf("enqueue batch: %w", err)
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
