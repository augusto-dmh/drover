package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover"
)

const noopKind = "noop"

type inserter interface {
	InsertMany(ctx context.Context, items []drover.InsertItem) ([]*drover.JobRow, error)
}

type noopArgs struct{}

func (noopArgs) Kind() string { return noopKind }

type noopWorker struct {
	drover.WorkerDefaults[noopArgs]
	insertedAt map[int64]time.Time
	latencies  []time.Duration
	latN       atomic.Int64
	done       atomic.Int64
	target     int64
	allDone    chan struct{}
}

func newNoopWorker(jobs int, insertedAt map[int64]time.Time) *noopWorker {
	return &noopWorker{
		insertedAt: insertedAt,
		latencies:  make([]time.Duration, jobs),
		target:     int64(jobs),
		allDone:    make(chan struct{}),
	}
}

func (w *noopWorker) Work(_ context.Context, job *drover.Job[noopArgs]) error {
	start, ok := w.insertedAt[job.ID]
	if !ok {
		return nil
	}
	i := int(w.latN.Add(1) - 1)
	if i >= 0 && i < len(w.latencies) {
		w.latencies[i] = time.Since(start)
	}
	if w.done.Add(1) == w.target {
		close(w.allDone)
	}
	return nil
}

func waitDrain(ctx context.Context, allDone <-chan struct{}) error {
	select {
	case <-allDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func defaultExecute(ctx context.Context, cfg benchConfig) (benchOutcome, error) {
	pool, err := pgxpool.New(ctx, cfg.dsn)
	if err != nil {
		return benchOutcome{}, fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	if err := drover.Migrate(ctx, pool); err != nil {
		return benchOutcome{}, fmt.Errorf("migrate: %w", err)
	}

	var pgVersion string
	if err := pool.QueryRow(ctx, "SELECT version()").Scan(&pgVersion); err != nil {
		return benchOutcome{}, fmt.Errorf("postgres version: %w", err)
	}

	insertedAt := make(map[int64]time.Time, cfg.jobs)
	workers := drover.NewWorkers()
	var worker *noopWorker
	if cfg.mode == modeDrain {
		worker = newNoopWorker(cfg.jobs, insertedAt)
		drover.Register(workers, worker)
	}

	clientCfg := drover.Config{
		Workers:      workers,
		Logger:       slog.New(slog.DiscardHandler),
		Concurrency:  cfg.concurrency,
		NotifyWakeup: cfg.notify,
	}
	if cfg.queue != "" && cfg.queue != "default" {
		clientCfg.Queues = map[string]int{cfg.queue: 1}
	}
	client, err := drover.NewClient(pool, clientCfg)
	if err != nil {
		return benchOutcome{}, fmt.Errorf("build client: %w", err)
	}

	insertStart := time.Now()
	if _, err := insertNoopJobs(ctx, client, cfg.jobs, cfg.batch, cfg.queue, insertedAt); err != nil {
		return benchOutcome{}, fmt.Errorf("insert jobs: %w", err)
	}
	out := benchOutcome{
		postgresVersion: pgVersion,
		elapsed:         time.Since(insertStart),
	}
	if cfg.mode != modeDrain {
		return out, nil
	}

	if err := client.Start(ctx); err != nil {
		return benchOutcome{}, fmt.Errorf("start: %w", err)
	}
	drainStart := time.Now()
	if err := waitDrain(ctx, worker.allDone); err != nil {
		if stopErr := client.Stop(context.Background()); stopErr != nil {
			return benchOutcome{}, fmt.Errorf("drain: %w (stop: %w)", err, stopErr)
		}
		return benchOutcome{}, fmt.Errorf("drain: %w", err)
	}
	out.elapsed = time.Since(drainStart)
	out.latencies = append([]time.Duration(nil), worker.latencies...)
	if err := client.Stop(ctx); err != nil {
		return benchOutcome{}, fmt.Errorf("stop: %w", err)
	}
	return out, nil
}

func insertNoopJobs(ctx context.Context, ins inserter, jobs, batch int, queue string, insertedAt map[int64]time.Time) ([]*drover.JobRow, error) {
	var opts *drover.InsertOpts
	if queue != "" && queue != "default" {
		opts = &drover.InsertOpts{Queue: queue}
	}
	out := make([]*drover.JobRow, 0, jobs)
	for start := 0; start < jobs; start += batch {
		n := batch
		if remain := jobs - start; remain < n {
			n = remain
		}
		items := make([]drover.InsertItem, n)
		for i := range items {
			items[i] = drover.InsertItem{Args: noopArgs{}, Opts: opts}
		}
		rows, err := ins.InsertMany(ctx, items)
		if err != nil {
			return out, err
		}
		now := time.Now()
		for _, row := range rows {
			if insertedAt != nil && row != nil {
				insertedAt[row.ID] = now
			}
			out = append(out, row)
		}
	}
	return out, nil
}

func reportPercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	slices.Sort(sorted)
	return percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
