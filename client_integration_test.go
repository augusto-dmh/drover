//go:build integration

package drover

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.RunMain(m)) }

func TestInsertTxVisibilityFollowsCallerTransaction(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	c, err := NewClient(pool, Config{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	countJobs := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
			t.Fatalf("count jobs: %v", err)
		}
		return n
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.InsertTx(ctx, tx, greetArgs{Name: "ada"}, nil); err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := countJobs(); n != 0 {
		t.Fatalf("after rollback: %d jobs, want 0", n)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	row, err := c.InsertTx(ctx, tx, greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := countJobs(); n != 1 {
		t.Fatalf("after commit: %d jobs, want 1", n)
	}
	if row.State != StateAvailable {
		t.Errorf("State = %q, want %q", row.State, StateAvailable)
	}
}

func TestInsertManyTxVisibilityFollowsCallerTransaction(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	c, err := NewClient(pool, Config{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	countJobs := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
			t.Fatalf("count jobs: %v", err)
		}
		return n
	}

	items := []InsertItem{
		{Args: greetArgs{Name: "ada"}},
		{Args: greetArgs{Name: "grace"}},
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.InsertManyTx(ctx, tx, items); err != nil {
		t.Fatalf("InsertManyTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := countJobs(); n != 0 {
		t.Fatalf("after rollback: %d jobs, want 0", n)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	rows, err := c.InsertManyTx(ctx, tx, items)
	if err != nil {
		t.Fatalf("InsertManyTx: %v", err)
	}
	if len(rows) != len(items) {
		t.Fatalf("InsertManyTx returned %d rows, want %d", len(rows), len(items))
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := countJobs(); n != len(items) {
		t.Fatalf("after commit: %d jobs, want %d", n, len(items))
	}
	for _, row := range rows {
		if row.State != StateAvailable {
			t.Errorf("State = %q, want %q", row.State, StateAvailable)
		}
	}
}

// Options must reach storage through the transactional path too, and the
// job must still exist only if the caller's transaction commits.
func TestInsertTxHonoursInsertOpts(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	c, err := NewClient(pool, Config{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	due := time.Now().Add(time.Hour)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	row, err := c.InsertTx(ctx, tx, greetArgs{Name: "ada"},
		&InsertOpts{Queue: "digest", ScheduledAt: due})
	if err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if row.Queue != "digest" {
		t.Errorf("Queue = %q, want digest", row.Queue)
	}
	if row.State != StateScheduled {
		t.Errorf("State = %q, want %q", row.State, StateScheduled)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs WHERE id = $1`, row.ID).Scan(&n); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if n != 0 {
		t.Errorf("job survived a rolled-back transaction")
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	committed, err := c.InsertTx(ctx, tx, greetArgs{Name: "ada"},
		&InsertOpts{Queue: "digest", ScheduledAt: due})
	if err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var queue, state string
	var scheduledAt time.Time
	err = pool.QueryRow(ctx,
		`SELECT queue, state::text, scheduled_at FROM drover_jobs WHERE id = $1`, committed.ID,
	).Scan(&queue, &state, &scheduledAt)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}
	if queue != "digest" || state != "scheduled" {
		t.Errorf("stored queue/state = %q/%q, want digest/scheduled", queue, state)
	}
	if want := due.Truncate(time.Microsecond); !scheduledAt.Equal(want) {
		t.Errorf("stored scheduled_at = %v, want %v", scheduledAt.UTC(), want.UTC())
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startNotifyWorker(t *testing.T, pool *pgxpool.Pool, entered chan int64) *Client {
	t.Helper()
	ws := NewWorkers()
	Register(ws, &funcWorker{fn: func(_ context.Context, job *Job[greetArgs]) error {
		entered <- job.ID
		return nil
	}})
	c, err := NewClient(pool, Config{
		Workers:      ws,
		Logger:       quietLogger(),
		PollInterval: notifyWakePollInterval,
		NotifyWakeup: true,
		Concurrency:  1,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Fetch idles on the first empty round; LISTEN must be registered
	// before a NOTIFY can land — Postgres does not queue it.
	waitUntilListening(t, pool)
	return c
}

func waitUntilListening(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	waitFor(t, func() bool {
		var n int
		err := pool.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND query ILIKE 'LISTEN drover%'
		`).Scan(&n)
		return err == nil && n > 0
	}, "the worker to LISTEN on drover")
}

func TestNotifyWakeupCrossClientInsertRunsBeforePollInterval(t *testing.T) {
	pool := testdb.NewDB(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	entered := make(chan int64, 1)
	worker := startNotifyWorker(t, pool, entered)
	defer func() {
		if err := worker.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	producer, err := NewClient(pool, Config{Logger: quietLogger(), NotifyWakeup: true})
	if err != nil {
		t.Fatalf("producer NewClient: %v", err)
	}

	start := time.Now()
	row, err := producer.Insert(ctx, greetArgs{Name: "ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	select {
	case id := <-entered:
		if id != row.ID {
			t.Errorf("ran job %d, want %d", id, row.ID)
		}
	case <-time.After(notifyWakeUpperBound):
		t.Fatalf("job was not claimed within %v against a %v poll interval", notifyWakeUpperBound, notifyWakePollInterval)
	}
	if elapsed := time.Since(start); elapsed > notifyWakeUpperBound {
		t.Errorf("claim took %v against a %v poll interval — LISTEN did not wake idle fetch", elapsed, notifyWakePollInterval)
	}
}

func TestInsertTxDoesNotWakeUntilCommit(t *testing.T) {
	pool := testdb.NewDB(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	entered := make(chan int64, 1)
	worker := startNotifyWorker(t, pool, entered)
	defer func() {
		if err := worker.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	producer, err := NewClient(pool, Config{Logger: quietLogger(), NotifyWakeup: true})
	if err != nil {
		t.Fatalf("producer NewClient: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	row, err := producer.InsertTx(ctx, tx, greetArgs{Name: "ada"}, nil)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("InsertTx: %v", err)
	}

	select {
	case id := <-entered:
		t.Fatalf("job %d ran before the producer committed", id)
	case <-time.After(notifyWakeQuietWindow):
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	start := time.Now()
	select {
	case id := <-entered:
		if id != row.ID {
			t.Errorf("ran job %d, want %d", id, row.ID)
		}
	case <-time.After(notifyWakeUpperBound):
		t.Fatalf("job was not claimed within %v after commit against a %v poll interval", notifyWakeUpperBound, notifyWakePollInterval)
	}
	if elapsed := time.Since(start); elapsed > notifyWakeUpperBound {
		t.Errorf("claim took %v after commit against a %v poll interval — listeners must wake at commit", elapsed, notifyWakePollInterval)
	}
}

func TestInsertDoesNotNotifyWhenWakeupDisabled(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	c, err := NewClient(pool, Config{Logger: quietLogger()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN drover"); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	if _, err := c.Insert(ctx, greetArgs{Name: "ada"}, nil); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, notifyWakeQuietWindow)
	defer cancel()
	n, err := conn.Conn().WaitForNotification(waitCtx)
	if err == nil {
		t.Fatalf("received NOTIFY on %q with NotifyWakeup unset", n.Channel)
	}
}

func TestEmptyInsertManyDoesNotNotifyWhenWakeupEnabled(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	c, err := NewClient(pool, Config{Logger: quietLogger(), NotifyWakeup: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN drover"); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	rows, err := c.InsertMany(ctx, nil)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("InsertMany returned %d rows, want 0", len(rows))
	}

	waitCtx, cancel := context.WithTimeout(ctx, notifyWakeQuietWindow)
	defer cancel()
	n, err := conn.Conn().WaitForNotification(waitCtx)
	if err == nil {
		t.Fatalf("received NOTIFY on %q after an empty InsertMany", n.Channel)
	}
}

func TestInsertConcurrentUniqueKeyCreatesAtMostOneRow(t *testing.T) {
	t.Parallel()
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	c, err := NewClient(pool, Config{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	const writers = 2
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Insert(ctx, greetArgs{Name: "ada"}, &InsertOpts{UniqueKey: "u"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	var ok, dup int
	for err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrDuplicateJob):
			dup++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if ok != 1 || dup != 1 {
		t.Errorf("success=%d duplicate=%d, want 1 and 1", ok, dup)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if n != 1 {
		t.Errorf("store has %d rows, want 1", n)
	}
}
