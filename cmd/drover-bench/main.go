// Command drover-bench measures enqueue throughput and drain latency.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

const (
	modeEnqueue = "enqueue"
	modeDrain   = "drain"
)

type benchConfig struct {
	dsn         string
	mode        string
	jobs        int
	batch       int
	concurrency int
	queue       string
	notify      bool
}

type benchOutcome struct {
	postgresVersion string
	elapsed         time.Duration
	latencies       []time.Duration
}

type executeFn func(ctx context.Context, cfg benchConfig) (benchOutcome, error)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv, nil))
}

func run(args []string, stdout, stderr io.Writer, getenv func(string) string, exec executeFn) int {
	if exec == nil {
		exec = defaultExecute
	}

	fs := flag.NewFlagSet("drover-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		cliPrintf(stderr, `Usage: drover-bench [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	database := fs.String("database", "", "Postgres URL (default: $DATABASE_URL)")
	mode := fs.String("mode", modeDrain, "enqueue or drain")
	jobs := fs.Int("jobs", 10000, "number of no-op jobs")
	batch := fs.Int("batch", 256, "InsertMany chunk size")
	concurrency := fs.Int("concurrency", 10, "worker pool size for drain")
	queue := fs.String("queue", "default", "queue name")
	notify := fs.Bool("notify", false, "set Config.NotifyWakeup")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		return usageErrorf(fs, stderr, "unexpected arguments %v", fs.Args())
	}
	if *jobs < 1 {
		return usageErrorf(fs, stderr, "--jobs must be at least 1")
	}
	if *batch < 1 {
		return usageErrorf(fs, stderr, "--batch must be at least 1")
	}
	if *mode != modeEnqueue && *mode != modeDrain {
		return usageErrorf(fs, stderr, "--mode must be enqueue or drain")
	}

	dsn, err := resolveDSN(*database, getenv)
	if err != nil {
		return usageErrorf(fs, stderr, "%s", err.Error())
	}

	cfg := benchConfig{
		dsn:         dsn,
		mode:        *mode,
		jobs:        *jobs,
		batch:       *batch,
		concurrency: *concurrency,
		queue:       *queue,
		notify:      *notify,
	}
	outcome, err := exec(context.Background(), cfg)
	if err != nil {
		cliPrintf(stderr, "drover-bench: %v\n", err)
		return 1
	}
	printReport(stdout, cfg, outcome)
	return 0
}

func resolveDSN(flagValue string, getenv func(string) string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if getenv == nil {
		return "", fmt.Errorf("database URL required: pass --database or set DATABASE_URL")
	}
	if v := getenv("DATABASE_URL"); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("database URL required: pass --database or set DATABASE_URL")
}

func usageErrorf(fs *flag.FlagSet, stderr io.Writer, format string, args ...any) int {
	cliPrintf(stderr, "drover-bench: "+format+"\n", args...)
	fs.Usage()
	return 2
}

func printReport(stdout io.Writer, cfg benchConfig, outcome benchOutcome) {
	elapsed := outcome.elapsed
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	cliPrintf(stdout, "GOOS=%s\n", runtime.GOOS)
	cliPrintf(stdout, "GOARCH=%s\n", runtime.GOARCH)
	cliPrintf(stdout, "NumCPU=%d\n", runtime.NumCPU())
	cliPrintf(stdout, "postgres_version=%s\n", outcome.postgresVersion)
	cliPrintf(stdout, "jobs=%d\n", cfg.jobs)
	cliPrintf(stdout, "batch=%d\n", cfg.batch)
	cliPrintf(stdout, "concurrency=%d\n", cfg.concurrency)
	cliPrintf(stdout, "queue=%s\n", cfg.queue)
	cliPrintf(stdout, "notify=%t\n", cfg.notify)
	cliPrintf(stdout, "handlers=no-op\n")
	cliPrintf(stdout, "jobs/sec=%.2f\n", float64(cfg.jobs)/elapsed.Seconds())
	if cfg.mode == modeDrain {
		p50, p95, p99 := reportPercentiles(outcome.latencies)
		cliPrintf(stdout, "p50=%s\n", p50)
		cliPrintf(stdout, "p95=%s\n", p95)
		cliPrintf(stdout, "p99=%s\n", p99)
	}
}

func cliPrintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}
