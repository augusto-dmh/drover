//go:build integration

package drover

import (
	"context"
	"os"
	"testing"
	"time"

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
