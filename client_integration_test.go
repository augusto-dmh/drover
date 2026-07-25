//go:build integration

package drover

import (
	"context"
	"os"
	"testing"

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
	if _, err := c.InsertTx(ctx, tx, greetArgs{Name: "ada"}); err != nil {
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
	row, err := c.InsertTx(ctx, tx, greetArgs{Name: "ada"})
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
