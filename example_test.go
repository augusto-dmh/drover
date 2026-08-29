package drover_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

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
		// Two named queues sharing the one pool above, "default" claimed
		// roughly four times as often as "bulk" — never exclusively, so
		// "bulk" is slower but not starved.
		Queues: map[string]int{"default": 4, "bulk": 1},
		// Timeout is one of the two built-in middleware; the client
		// always installs Logging outermost of whatever is configured
		// here, so job logging survives regardless.
		Middleware: []drover.Middleware{drover.Timeout(30 * time.Second)},
	})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Insert(ctx, SendEmail{To: "ada@example.com", Template: "welcome"}, nil); err != nil {
		log.Fatal(err)
	}

	// A nil *InsertOpts means the "default" queue, runnable now; here a
	// reminder is routed to "bulk" and held back for a day.
	reminder := SendEmail{To: "ada@example.com", Template: "reminder"}
	if _, err := client.Insert(ctx, reminder, &drover.InsertOpts{
		Queue:       "bulk",
		ScheduledAt: time.Now().Add(24 * time.Hour),
	}); err != nil {
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

// ExampleConfig_observability mirrors the README observability snippet
// so a renamed or removed Config field fails `go test` instead of
// shipping a non-compiling example again.
func ExampleConfig_observability() {
	workers := drover.NewWorkers()
	_ = drover.Config{
		Workers:         workers,
		Concurrency:     8,
		Queues:          map[string]int{"default": 1, "bulk": 9},
		OpsAddr:         "127.0.0.1:9090", // /metrics, /healthz, /readyz
		StatsInterval:   15 * time.Second, // how often depth/age gauges refresh
		MetricsRegistry: prometheus.NewRegistry(),
	}
}

// ExampleConfig_notifyWakeup mirrors the README Planned API Config snippet.
func ExampleConfig_notifyWakeup() {
	workers := drover.NewWorkers()
	_ = drover.Config{
		Workers:      workers,
		Concurrency:  8,
		Queues:       map[string]int{"default": 1, "bulk": 9},
		Middleware:   []drover.Middleware{drover.Timeout(30 * time.Second)},
		NotifyWakeup: false, // opt-in LISTEN/NOTIFY; needs session pooling
	}
}

// ExampleClient_InsertMany mirrors the README Planned API batch-enqueue snippet.
func ExampleClient_InsertMany() {
	ctx := context.Background()
	var client *drover.Client
	var user struct{ Email string }
	user.Email = "ada@example.com"
	_, _ = client.InsertMany(ctx, []drover.InsertItem{
		{Args: SendEmail{To: user.Email, Template: "welcome"}},
		{Args: SendEmail{To: user.Email, Template: "digest"}, Opts: &drover.InsertOpts{Queue: "bulk"}},
	})
}

// ExampleMiddleware builds a chain by hand, the same way a Client
// builds Config.Middleware around its dispatch: the first middleware
// applied is outermost, so it sees the job first and its result last.
func ExampleMiddleware() {
	var trace []string
	record := func(name string) drover.Middleware {
		return func(next drover.Handler) drover.Handler {
			return func(ctx context.Context, job *drover.JobRow) error {
				trace = append(trace, name+":start")
				err := next(ctx, job)
				trace = append(trace, name+":end")
				return err
			}
		}
	}

	base := func(ctx context.Context, job *drover.JobRow) error {
		trace = append(trace, "handler")
		return nil
	}

	// outer wraps inner wraps Timeout wraps base — outer runs first and
	// last, exactly as Config.Middleware's index 0 would.
	chain := record("outer")(drover.Timeout(time.Second)(record("inner")(base)))

	if err := chain(context.Background(), &drover.JobRow{ID: 1, Kind: "demo"}); err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(strings.Join(trace, " "))
	// Output:
	// outer:start inner:start handler inner:end outer:end
}
