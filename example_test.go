package drover_test

import (
	"context"
	"log"

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
// enqueue, and run the worker loop until the context ends.
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

	client, err := drover.NewClient(pool, drover.Config{Workers: workers})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Insert(ctx, SendEmail{To: "ada@example.com", Template: "welcome"}); err != nil {
		log.Fatal(err)
	}

	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
