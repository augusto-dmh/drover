//go:build integration

package drover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/testdb"
)

type e2eArgs struct {
	N int `json:"n"`
}

func (e2eArgs) Kind() string { return "e2e" }

type e2eWorker struct {
	WorkerDefaults[e2eArgs]
	mu   sync.Mutex
	runs map[int64]int
	fail func(args e2eArgs) error
}

func (w *e2eWorker) Work(_ context.Context, job *Job[e2eArgs]) error {
	w.mu.Lock()
	w.runs[job.ID]++
	w.mu.Unlock()
	if w.fail != nil {
		return w.fail(job.Args)
	}
	return nil
}

func newE2EClient(t *testing.T, pool *pgxpool.Pool, worker *e2eWorker) *Client {
	t.Helper()
	ws := NewWorkers()
	Register(ws, worker)
	c, err := NewClient(pool, Config{Workers: ws, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func countInState(t *testing.T, pool *pgxpool.Pool, state string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM drover_jobs WHERE state = $1`, state).Scan(&n); err != nil {
		t.Fatalf("count %s jobs: %v", state, err)
	}
	return n
}

func TestEndToEndConcurrentLoopsExecuteEachJobExactlyOnce(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{runs: make(map[int64]int)}
	first := newE2EClient(t, pool, worker)
	second := newE2EClient(t, pool, worker)

	const jobs = 50
	for i := range jobs {
		if _, err := first.Insert(ctx, e2eArgs{N: i}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() { done <- first.Start(loopCtx) }()
	go func() { done <- second.Start(loopCtx) }()

	waitFor(t, func() bool { return countInState(t, pool, "completed") == jobs },
		fmt.Sprintf("all %d jobs to complete", jobs))
	cancel()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Start returned %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a loop did not stop after cancellation")
		}
	}

	worker.mu.Lock()
	defer worker.mu.Unlock()
	if len(worker.runs) != jobs {
		t.Fatalf("executed %d distinct jobs, want %d", len(worker.runs), jobs)
	}
	for id, n := range worker.runs {
		if n != 1 {
			t.Fatalf("job %d executed %d times, want exactly once", id, n)
		}
	}
}

func TestEndToEndFailedJobIsDeadWithRecordedErrorInPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	worker := &e2eWorker{
		runs: make(map[int64]int),
		fail: func(e2eArgs) error { return errors.New("smtp unreachable") },
	}
	c := newE2EClient(t, pool, worker)

	row, err := c.Insert(ctx, e2eArgs{N: 1})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Start(loopCtx) }()
	waitFor(t, func() bool { return countInState(t, pool, "dead") == 1 }, "job to be marked dead")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}

	var rawErrors []byte
	if err := pool.QueryRow(ctx,
		`SELECT errors FROM drover_jobs WHERE id = $1`, row.ID).Scan(&rawErrors); err != nil {
		t.Fatalf("read errors: %v", err)
	}
	var recorded []driver.AttemptError
	if err := json.Unmarshal(rawErrors, &recorded); err != nil {
		t.Fatalf("decode errors: %v", err)
	}
	if len(recorded) != 1 || recorded[0].Error != "smtp unreachable" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want one entry {Attempt:1 Error:smtp unreachable}", recorded)
	}
}
