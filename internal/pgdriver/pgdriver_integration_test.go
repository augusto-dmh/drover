//go:build integration

package pgdriver_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/pgdriver"
	"github.com/augusto-dmh/drover/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.RunMain(m)) }

func newDriver(t *testing.T) (*pgdriver.Driver, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.NewDB(t)
	d := pgdriver.New(pool)
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return d, pool
}

func mustInsert(t *testing.T, d *pgdriver.Driver, kind, queue string) *driver.JobRow {
	t.Helper()
	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind:  kind,
		Queue: queue,
		Args:  []byte(`{"n":1}`),
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return row
}

func TestInsertPersistsAvailableJob(t *testing.T) {
	d, _ := newDriver(t)

	row := mustInsert(t, d, "send_email", "default")

	if row.State != "available" {
		t.Errorf("State = %q, want available", row.State)
	}
	if row.Attempt != 0 || row.MaxAttempts != 25 {
		t.Errorf("Attempt/MaxAttempts = %d/%d, want 0/25", row.Attempt, row.MaxAttempts)
	}
	if string(row.Errors) != "[]" {
		t.Errorf("Errors = %s, want []", row.Errors)
	}
	if string(row.Args) != `{"n": 1}` && string(row.Args) != `{"n":1}` {
		t.Errorf("Args = %s, want {\"n\":1}", row.Args)
	}
	if row.LeasedUntil != nil || row.FinalizedAt != nil {
		t.Error("LeasedUntil/FinalizedAt should be unset on insert")
	}
}

func TestInsertTxVisibilityFollowsTransaction(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()

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
	if _, err := d.InsertTx(ctx, tx, driver.InsertParams{Kind: "k", Queue: "default", Args: []byte(`{}`)}); err != nil {
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
	if _, err := d.InsertTx(ctx, tx, driver.InsertParams{Kind: "k", Queue: "default", Args: []byte(`{}`)}); err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := countJobs(); n != 1 {
		t.Fatalf("after commit: %d jobs, want 1", n)
	}
}

func TestInsertTxRejectsNonPgxTransaction(t *testing.T) {
	d, _ := newDriver(t)

	_, err := d.InsertTx(context.Background(), "not a tx", driver.InsertParams{Kind: "k", Queue: "default"})
	if !errors.Is(err, driver.ErrTxUnsupported) {
		t.Fatalf("error = %v, want ErrTxUnsupported", err)
	}
}

func TestFetchAvailableClaimSemantics(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	first := mustInsert(t, d, "a", "default")
	second := mustInsert(t, d, "b", "default")
	mustInsert(t, d, "c", "other")
	future := mustInsert(t, d, "d", "default")
	if _, err := pool.Exec(ctx,
		`UPDATE drover_jobs SET scheduled_at = now() + interval '1 hour' WHERE id = $1`,
		future.ID); err != nil {
		t.Fatalf("push job to the future: %v", err)
	}

	claimed, err := d.FetchAvailable(ctx, "default", 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != first.ID {
		t.Fatalf("first claim = %+v, want job %d", claimed, first.ID)
	}
	if claimed[0].State != "running" || claimed[0].Attempt != 1 {
		t.Errorf("claimed State/Attempt = %s/%d, want running/1", claimed[0].State, claimed[0].Attempt)
	}
	if claimed[0].LeasedUntil == nil || !claimed[0].LeasedUntil.After(time.Now()) {
		t.Errorf("LeasedUntil = %v, want a future lease", claimed[0].LeasedUntil)
	}

	claimed, err = d.FetchAvailable(ctx, "default", 10)
	if err != nil {
		t.Fatalf("second FetchAvailable: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != second.ID {
		t.Fatalf("second claim = %d jobs, want only job %d (other queue and future job excluded)",
			len(claimed), second.ID)
	}

	claimed, err = d.FetchAvailable(ctx, "default", 1)
	if err != nil {
		t.Fatalf("third FetchAvailable: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("third claim = %d jobs, want 0", len(claimed))
	}
}

func TestFetchAvailableReturnsBatchInIDOrder(t *testing.T) {
	d, _ := newDriver(t)
	var want []int64
	for range 3 {
		want = append(want, mustInsert(t, d, "k", "default").ID)
	}
	mustInsert(t, d, "k", "default")

	claimed, err := d.FetchAvailable(context.Background(), "default", 3)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d jobs, want 3", len(claimed))
	}
	for i, row := range claimed {
		if row.ID != want[i] {
			t.Fatalf("claimed[%d].ID = %d, want %d (FIFO)", i, row.ID, want[i])
		}
	}
}

func TestConcurrentClaimersNeverDoubleClaim(t *testing.T) {
	d, _ := newDriver(t)
	const jobs = 25
	for range jobs {
		mustInsert(t, d, "k", "default")
	}

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rows, err := d.FetchAvailable(context.Background(), "default", 1)
				if err != nil {
					t.Errorf("FetchAvailable: %v", err)
					return
				}
				if len(rows) == 0 {
					return
				}
				mu.Lock()
				seen[rows[0].ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d claimed %d times, want exactly once", id, n)
		}
	}
}

func TestMarkCompletedFinalizesRunningJob(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")
	claimed, err := d.FetchAvailable(ctx, "default", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v (%d rows)", err, len(claimed))
	}

	if err := d.MarkCompleted(ctx, claimed[0].ID); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	var state string
	var finalized bool
	if err := pool.QueryRow(ctx,
		`SELECT state, finalized_at IS NOT NULL FROM drover_jobs WHERE id = $1`,
		claimed[0].ID).Scan(&state, &finalized); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "completed" || !finalized {
		t.Errorf("state/finalized = %s/%t, want completed/true", state, finalized)
	}
}

func TestMarkDeadAppendsErrorDetail(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")
	claimed, err := d.FetchAvailable(ctx, "default", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v (%d rows)", err, len(claimed))
	}

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now().UTC(), Error: "boom"})
	if err := d.MarkDead(ctx, claimed[0].ID, detail); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	var state string
	var rawErrors []byte
	var finalized bool
	if err := pool.QueryRow(ctx,
		`SELECT state, errors, finalized_at IS NOT NULL FROM drover_jobs WHERE id = $1`,
		claimed[0].ID).Scan(&state, &rawErrors, &finalized); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "dead" || !finalized {
		t.Errorf("state/finalized = %s/%t, want dead/true", state, finalized)
	}
	var recorded []driver.AttemptError
	if err := json.Unmarshal(rawErrors, &recorded); err != nil {
		t.Fatalf("decode errors: %v", err)
	}
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want one entry {Attempt:1 Error:boom}", recorded)
	}
}

func TestFinalizeTransitionGuards(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	available := mustInsert(t, d, "k", "default")

	if err := d.MarkCompleted(ctx, available.ID); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkCompleted on available job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkDead(ctx, available.ID, []byte(`{}`)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkDead on available job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkCompleted(ctx, 99999); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkCompleted on unknown id: %v, want ErrNotFound", err)
	}
	if err := d.MarkDead(ctx, 99999, []byte(`{}`)); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkDead on unknown id: %v, want ErrNotFound", err)
	}
}
