package drover_test

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover"
)

type SendEmail struct {
	To       string `json:"to"`
	Template string `json:"template"`
}

func (SendEmail) Kind() string { return "send_email" }

type SendEmailWorker struct {
	drover.WorkerDefaults[SendEmail]
}

func (SendEmailWorker) Work(_ context.Context, job *drover.Job[SendEmail]) error {
	log.Printf("sending %s to %s", job.Args.Template, job.Args.To)
	return nil
}

// Example wires the full path: migrate, register a typed worker,
// enqueue, work the queue on a pool, and shut down cleanly.
func Example() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://localhost:5432/app")
	if err != nil {
		log.Fatal(err)
	}
	if err := drover.Migrate(ctx, pool); err != nil {
		log.Fatal(err)
	}

	workers := drover.NewWorkers()
	drover.Register(workers, SendEmailWorker{})

	client, err := drover.NewClient(pool, drover.Config{
		Workers:     workers,
		Concurrency: 8,
	})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Insert(ctx, SendEmail{To: "ada@example.com", Template: "welcome"}); err != nil {
		log.Fatal(err)
	}

	// Start returns as soon as the pool is running; the process stays
	// alive doing whatever else it does.
	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// On the way out, give the in-flight jobs a bounded chance to finish.
	// Whatever does not finish in time is returned to the queue, and Stop
	// says how much that was.
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Stop(shutdown); err != nil {
		log.Printf("drover: shutdown incomplete: %v", err)
	}
}
